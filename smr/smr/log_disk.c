/*
 * Copyright 2015 Naver Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "crc16.h"
#include "dlist.h"
#include "gpbuf.h"
#include "smr.h"
#include "log_internal.h"

#define ERRNO_FILE_ID DISK_FILE_ID

/* e.g.) 9223372036854775807.log */
#define MAX_DISK_FILE_NAME 23

/* 9223372036854775807.log.tombstone */
#define MAX_TOMBSTONE_FILE_NAME 33

typedef struct diskLogDev_ diskLogDev;
struct diskLogDev_
{
  logDev dev;
  char *base_dir;
};
#define init_disk_log_dev(d)  do {   \
  init_log_dev(&(d)->dev);           \
  (d)->base_dir = NULL;              \
} while (0)

/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */
static int check_log_or_tombstone_name (diskLogDev * disk, char *name,
					long long *seq);
static int check_log_name (diskLogDev * disk, char *name, long long *seq);
static int get_seqs (diskLogDev * disk, long long **seqs, int *seqs_size,
		     int (*cmpr) (diskLogDev *, char *, long long *));
static int get_path (long long base_seq, char *path, int path_sz,
		     char *base_dir);
static int get_path2 (diskLogDev * disk, long long seq, char *log, int logsz,
		      char *tomb, int tombsz);

/* log device implementation */
static int disk_get_seqs (logDev * dev, long long **seqs, int *seqs_size);
static int disk_decache (logDev * dev, long long seq);
static int disk_synced (logDev * dev, long long seq);
static int disk_purge (logDev * dev, long long seq);
static int disk_purge_after (logDev * dev, long long seq);
static smrLogAddr *disk_get_mmap (logDev * dev, long long seq, int for_read,
				  int create);
static int disk_munmap (logDev * dev, smrLogAddr * addr);
int disk_remove_one (logDev * dev, long long upper, int gap_in_sec,
		     long long retain_lb, int *removed,
		     long long *removed_seq, int *has_more);
static void disk_close (logDev * dev);

/* --------------- */
/* LOCAL FUNCTIONS */
/* --------------- */
static int
check_log_or_tombstone_name (diskLogDev * disk, char *name, long long *seq)
{
  char *ep;
  long long seq_;

  if (name == NULL || seq == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  if ((strlen (name) != MAX_DISK_FILE_NAME
       || strcmp (name + MAX_DISK_FILE_NAME - 4, ".log") != 0)
      && (strlen (name) != MAX_TOMBSTONE_FILE_NAME
	  || strcmp (name + MAX_TOMBSTONE_FILE_NAME - 14, ".log.tombstone")))
    {
      ERRNO_POINT ();
      return -1;
    }

  ep = NULL;
  seq_ = strtoll (name, &ep, 10);
  if (seq_ < 0LL || *ep != '.' || seq_round_down (seq_) != seq_)
    {
      ERRNO_POINT ();
      return -1;
    }

  *seq = seq_;
  return 0;
}

static int
check_log_name (diskLogDev * disk, char *name, long long *seq)
{
  char *ep;
  long long seq_;

  if (name == NULL || seq == NULL)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return -1;
    }

  if (strlen (name) != MAX_DISK_FILE_NAME
      || strcmp (name + MAX_DISK_FILE_NAME - 4, ".log") != 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  ep = NULL;
  seq_ = strtoll (name, &ep, 10);
  if (seq_ < 0LL || *ep != '.' || seq_round_down (seq_) != seq_)
    {
      ERRNO_POINT ();
      return -1;
    }

  *seq = seq_;
  return 0;
}

static int
get_seqs (diskLogDev * disk, long long **seqs, int *seqs_size,
	  int (*cmpr) (diskLogDev *, char *, long long *))
{
  DIR *dir = NULL;
  struct dirent *ent;
  long long seq;
  long long *fseqs = NULL;
  int fseqs_size = 0;
  int num_files = 0;

  dir = opendir (disk->base_dir);
  if (dir == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }

  errno = 0;
  while ((ent = readdir (dir)) != NULL)
    {
      if (cmpr (disk, ent->d_name, &seq) != -1)
	{
	  if (num_files >= fseqs_size)
	    {
	      long long *tmp;
	      int new_size = num_files > 0 ? num_files * 2 : 256;

	      tmp = realloc (fseqs, sizeof (long long) * new_size);
	      if (tmp == NULL)
		{
		  ERRNO_POINT ();
		  goto error;
		}
	      fseqs = tmp;
	      fseqs_size = new_size;
	    }
	  fseqs[num_files++] = seq;
	}
      errno = 0;
    }
  closedir (dir);
  dir = NULL;
  if (errno != 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  if (num_files > 0)
    {
      long long *distinct;
      long long prev = -1LL;
      int i, j;

      // sort long in sequence order
      qsort (fseqs, num_files, sizeof (long long), ll_cmpr);

      // elimitate duplicated sequences
      distinct = malloc (num_files * (sizeof (long long)));
      if (distinct == NULL)
	{
	  goto error;
	}
      for (i = 0, j = 0; i < num_files; i++)
	{
	  if (prev == fseqs[i])
	    {
	      continue;
	    }
	  distinct[j++] = fseqs[i];
	  prev = fseqs[i];
	}

      free (fseqs);
      *seqs = distinct;
      *seqs_size = j;
    }
  else
    {
      *seqs = NULL;
      *seqs_size = 0;
    }
  return 0;

error:
  if (dir != NULL)
    {
      closedir (dir);
    }
  if (fseqs != NULL)
    {
      free (fseqs);
    }
  return -1;
}

static int
get_path (long long base_seq, char *buf, int buf_sz, char *base_dir)
{
  int ret;

  /* zero front filling, 19 width, lld */
  ret = snprintf (buf, buf_sz, "%s/%019lld.log", base_dir, base_seq);
  if (ret < 0 || ret >= buf_sz)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static int
get_path2 (diskLogDev * disk, long long seq, char *log, int logsz, char *tomb,
	   int tombsz)
{
  int ret;
  assert (log != NULL && logsz > 0);
  assert (tomb != NULL && tombsz > 0);

  log[0] = '\0';
  tomb[0] = '\0';
  ret = get_path (seq, log, logsz, disk->base_dir);
  if (ret < 0 || ret >= logsz)
    {
      ERRNO_POINT ();
      return -1;
    }

  ret = snprintf (tomb, tombsz, "%s.tombstone", log);
  if (ret < 0 || ret >= tombsz)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static int
disk_get_seqs (logDev * dev, long long **seqs, int *seqs_size)
{
  diskLogDev *disk = (diskLogDev *) dev;

  if (disk == NULL || seqs == NULL || seqs_size == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  return get_seqs (disk, seqs, seqs_size, check_log_name);
}

static int
disk_decache (logDev * dev, long long seq)
{
  diskLogDev *disk = (diskLogDev *) dev;
  char path[PATH_MAX];
  int fd;
  int ret;

  if (dev == NULL || seq < 0)
    {
      errno = EINVAL;
      return -1;
    }
  seq = seq_round_down (seq);

  ret = get_path (seq, path, sizeof (path), disk->base_dir);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  fd = open (path, O_RDWR);
  if (fd == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  (void) fdatasync (fd);
  ret = posix_fadvise (fd, 0, 0, POSIX_FADV_DONTNEED);
  close (fd);
  if (ret != 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  return 0;
}

static int
disk_synced (logDev * dev, long long seq)
{
  return 0;
}

static int
disk_purge (logDev * dev, long long seq)
{
  diskLogDev *disk = (diskLogDev *) dev;
  char org[PATH_MAX];
  char tomb[PATH_MAX];
  int ret;

  if (disk == NULL || seq < 0LL)
    {
      errno = EINVAL;
      return -1;
    }
  seq = seq_round_down (seq);

  ret = get_path2 (disk, seq, org, PATH_MAX, tomb, PATH_MAX);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  (void) rename (org, tomb);
  return 0;
}

static int
disk_purge_after (logDev * dev, long long seq_inc)
{
  diskLogDev *disk = (diskLogDev *) dev;
  int ret;
  long long seq;
  long long *seqs = NULL;
  int seqs_size = 0;
  int i;

  if (disk == NULL || seq_inc < 0LL)
    {
      errno = EINVAL;
      return -1;
    }
  seq = seq_round_down (seq_inc);

  ret = get_seqs (disk, &seqs, &seqs_size, check_log_or_tombstone_name);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  for (i = seqs_size - 1; i >= 0; i++)
    {
      char org_file[PATH_MAX];
      char tomb_file[PATH_MAX];

      if (seqs[i] < seq)
	{
	  break;
	}
      ret =
	get_path2 (disk, seqs[i], org_file, PATH_MAX, tomb_file, PATH_MAX);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      (void) rename (org_file, tomb_file);
    }

  if (seqs != NULL)
    {
      free (seqs);
    }
  return 0;

error:
  if (seqs != NULL)
    {
      free (seqs);
    }
  return -1;
}

static smrLogAddr *
disk_get_mmap (logDev * dev, long long seq, int for_read, int create)
{
  diskLogDev *disk = (diskLogDev *) dev;
  int fd;
  char path[PATH_MAX];
  smrLogAddr *addr;
  int o_flag;
  int prot;
  long long begin_seq;

  if (dev == NULL || seq < 0)
    {
      errno = EINVAL;
      return NULL;
    }

  begin_seq = seq_round_down (seq);
  if (get_path (begin_seq, path, PATH_MAX, disk->base_dir) == -1)
    {
      return NULL;
    }

  if (for_read)
    {
      o_flag = O_RDONLY;
      prot = PROT_READ;
    }
  else
    {
      o_flag = O_RDWR;
      prot = PROT_READ | PROT_WRITE;
    }

  fd = open (path, o_flag);
  if (fd == -1)
    {
      if (create && errno == ENOENT)
	{
	  o_flag = o_flag | O_CREAT;
	  fd = open (path, o_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
	  if (init_log_file (fd) == -1)
	    {
	      ERRNO_POINT ();
	      close (fd);
	      (void) unlink (path);
	      return NULL;
	    }
	}
      else if (errno == ENOENT)
	{
	  errno = ERRNO_NO_ENTRY;
	  return NULL;
	}
      else
	{
	  ERRNO_POINT ();
	  return NULL;
	}
    }

  addr = malloc (sizeof (smrLogAddr));
  if (addr == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  init_smr_log_addr (addr);
  addr->loc = IN_DISK;
  addr->seq = seq;
  addr->addr = mmap (NULL, SMR_LOG_FILE_ACTUAL_SIZE, prot, MAP_SHARED, fd, 0);
  if (addr->addr == MAP_FAILED)
    {
      ERRNO_POINT ();
      free (addr);
      close (fd);
      return NULL;
    }
  close (fd);

  return addr;
}

static int
disk_munmap (logDev * dev, smrLogAddr * addr)
{
  if (dev == NULL || addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  munmap (addr->addr, SMR_LOG_FILE_ACTUAL_SIZE);
  free (addr);
  return 0;
}

int
disk_remove_one (logDev * dev, long long upper, int gap_in_sec,
		 long long retain_lb, int *removed, long long *removed_seq,
		 int *has_more)
{
  diskLogDev *disk = (diskLogDev *) dev;
  int ret;
  long long *seqs = NULL;
  int seqs_size = 0;
  long long upper_seq_inc;
  int i;
  char path[PATH_MAX];
  char tomb[PATH_MAX];
  struct stat st;
  time_t upper_time;

  if (disk == NULL || removed == NULL || removed_seq == NULL
      || has_more == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  *removed = 0;
  *has_more = 0;

  if (upper < 0)
    {
      return 0;
    }
  upper_seq_inc = seq_round_down (upper);

  ret = get_seqs (disk, &seqs, &seqs_size, check_log_or_tombstone_name);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  upper_time = time (NULL) - gap_in_sec;

  for (i = 0; i < seqs_size; i++)
    {
      if (seqs[i] <= upper_seq_inc)
	{
	  ret = get_path2 (disk, seqs[i], path, PATH_MAX, tomb, PATH_MAX);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      goto error;
	    }

	  ret = stat (path, &st);
	  if (ret == 0)
	    {
	      if (seqs[i] < retain_lb || st.st_mtime < upper_time)
		{
		  (void) unlink (path);
		  *removed = 1;
		  *removed_seq = seqs[i];
		}
	    }
	  ret = stat (tomb, &st);
	  if (ret == 0)
	    {
	      unlink (tomb);
	      *removed = 1;
	      *removed_seq = seqs[i];
	    }
	  if (*removed)
	    {
	      break;
	    }
	}
    }

  /* check has_more */
  if (i + 1 < seqs_size && seqs[i + 1] <= upper_seq_inc)
    {
      ret = get_path2 (disk, seqs[i + 1], path, PATH_MAX, tomb, PATH_MAX);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      ret = stat (path, &st);
      if (ret == 0)
	{
	  if (seqs[i + 1] < retain_lb || st.st_mtime < upper_time)
	    {
	      *has_more = 1;
	    }
	}
      ret = stat (tomb, &st);
      if (ret == 0)
	{
	  *has_more = 1;
	}
    }

  if (seqs != NULL)
    {
      free (seqs);
    }
  return 0;

error:
  if (seqs != NULL)
    {
      free (seqs);
    }
  return -1;
}

static void
disk_close (logDev * dev)
{
  diskLogDev *disk = (diskLogDev *) dev;

  if (dev == NULL)
    {
      return;
    }

  if (disk->base_dir != NULL)
    {
      free (disk->base_dir);
    }

  free (disk);
}

/* ----------------- */
/* Exported Function */
/* ----------------- */
logDev *
open_disk_dev (char *basedir)
{
  diskLogDev *disk;
  char base[PATH_MAX];
  char *base_dir;
  struct stat st;
  int ret;

  if (basedir == NULL)
    {
      errno = EINVAL;
      return NULL;
    }

  base_dir = realpath (basedir, base);
  if (base_dir == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }

  ret = stat (base_dir, &st);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return NULL;
    }

  if (!S_ISDIR (st.st_mode))
    {
      ERRNO_POINT ();
      return NULL;
    }

  disk = malloc (sizeof (diskLogDev));
  if (disk == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  init_disk_log_dev (disk);

  disk->base_dir = strdup (base_dir);
  if (!disk->base_dir)
    {
      ERRNO_POINT ();
      free (disk);
      return NULL;
    }

  disk->dev.get_seqs = disk_get_seqs;
  disk->dev.decache = disk_decache;
  disk->dev.synced = disk_synced;
  disk->dev.purge = disk_purge;
  disk->dev.purge_after = disk_purge_after;
  disk->dev.get_mmap = disk_get_mmap;
  disk->dev.munmap = disk_munmap;
  disk->dev.remove_one = disk_remove_one;
  disk->dev.close = disk_close;
  return (logDev *) disk;
}


int
create_disk_dev (char *basedir)
{
  return 0;
}

int
unlink_disk_dev (char *basedir)
{
  return 0;
}

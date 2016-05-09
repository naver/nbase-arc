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
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "gpbuf.h"
#include "log_internal.h"

#define ERRNO_FILE_ID MEM_FILE_ID

/* shared memory log buffer related */
#define MAX_MEMLOG      3
#define SHM_NAME_SIZE 256	//NAME_MAX + 1
#define MASTER_LOG_NAME "master-log"
#define MASTER_SHM_NAME "master-shm"

typedef struct masterLog_ masterLog;
typedef struct masterWal_ masterWal;
typedef struct logSeq_ logSeq;
typedef struct bufLog_ bufLog;
typedef struct masterShm_ masterShm;
typedef struct memLogDev_ memLogDev;

/* master log is located in log base directory to identify master shared memory file */
struct masterLog_
{
  char prefix[SHM_NAME_SIZE];
};

typedef enum
{
  OP_NONE = 0,
  OP_OPEN,
  OP_CREATE,
  OP_SYNC,
  OP_UNLINK,
  OP_SET_INITIALIZED,
} shmOp;

struct masterWal_
{
  pthread_mutex_t mutex;
  int need_completion;
  shmOp op;
  long long seq;
  int seq_idx;
  int buflog_idx;
  int flag;
};
#define init_master_wal(w) do {            \
  pthread_mutex_init(&(w)->mutex, NULL);   \
  (w)->need_completion = 0;                \
  (w)->op = OP_NONE;                       \
  (w)->seq = -1LL;                         \
  (w)->seq_idx = -1;                       \
  (w)->buflog_idx = -1;                    \
  (w)->flag = 0;                           \
} while(0)

struct logSeq_
{
  long long seq;
  int synced;
  int buflog_idx;
};
#define init_log_seq(s) do {    \
  (s)->seq = -1LL;              \
  (s)->synced = 0;              \
  (s)->buflog_idx = -1;         \
} while(0)

struct bufLog_
{
  int offset;			// offset from masterShm (immutable)
  unsigned short sem_num;	// semaphore number (immutable)
  long long seq;		// log sequence
  int deleted;			// deleted flag
};
#define init_buf_log(b,_off,_sn) do {     \
  (b)->offset = _off;                     \
  (b)->sem_num = _sn;                     \
  (b)->seq = -1LL;                        \
  (b)->deleted = 0;                       \
} while(0)

/* master shared memory file contents */
#define master_page_mask (~(4096-1))
#define master_buflog_off ((sizeof(masterShm) + ~master_page_mask) & master_page_mask)
#define master_shm_size (master_buflog_off + SMR_LOG_FILE_ACTUAL_SIZE * MAX_MEMLOG)
#define MSHM_SEM_PROJ_ID 1	// proj_id for ftok(3)

struct masterShm_
{
  int size;			// size of master shared memory (immutable)
  int semid;			// semaphore set id (immutable)
  int initialized;		// initialized (when 0, do machine restart recovery)
  masterWal wal;		// write ahead log about shared memory modification used for recovery
  logSeq seqs[MAX_MEMLOG];
  bufLog logs[MAX_MEMLOG];
  char prefix[SHM_NAME_SIZE];
  // extensions
  int isLocked;
};
#define init_mastershm(s) do {                                    \
  int i_;                                                         \
  (s)->size = master_shm_size;                                    \
  (s)->semid = -1;                                                \
  (s)->initialized = 0;                                           \
  for(i_=0;i_<MAX_MEMLOG;i_++) {                                  \
    init_log_seq(&(s)->seqs[i_]);                                 \
  }                                                               \
  for(i_=0;i_<MAX_MEMLOG;i_++) {                                  \
    int _o =  master_buflog_off + i_ * SMR_LOG_FILE_ACTUAL_SIZE;  \
    init_buf_log(&(s)->logs[i_], _o, i_+1);                       \
  }                                                               \
  (s)->prefix[0] = '\0';                                          \
  (s)->isLocked = 0;                                              \
} while(0)

struct memLogDev_
{
  logDev dev;
  masterLog *mlog;
  masterShm *mshm;
};
#define init_mem_log_dev(d) do {           \
  init_log_dev(&(d)->dev);                 \
  (d)->mlog = NULL;                        \
  (d)->mshm = NULL;                        \
} while(0)

#define MS_ENTER(s,r) do {                 \
  if (mastershm_enter(s) < 0) {            \
      ERRNO_POINT();                       \
      return r;                            \
  }                                        \
} while(0)

#define MS_LEAVE(s,r) do {                 \
  if (mastershm_leave(s) < 0) {            \
      ERRNO_POINT();                       \
      return r;                            \
  }                                        \
} while(0)

#ifdef SFI_ENABLED
#define SFI_PROBE() sfi_mshmcs_probe(__FILE__,__LINE__)
#else
#define SFI_PROBE()
#endif


/* --------------------------- */
/* Local Function Declarations */
/* --------------------------- */

/* shared memory log specific */
static int shmlog_get_name (char *prefix, long long seq, char *path,
			    int path_size);
static void *shmlog_open (char *prefix, long long seq,
			  int for_read, int create, int excl);
static int shmlog_unlink (char *prefix, long long seq);

/* buf log reference */
static int semref_inc (masterShm * shm, int sem_num);
static int semref_dec (masterShm * shm, int sem_num);

/* log interface (in buffer log or shared memory log) */
static int log_get_seqs_buf (masterShm * shm, long long *seqs,
			     int *seqs_size);
static int get_free_buflog_idx (masterShm * shm, int *idx);
static int wal_prepare_for_open (masterShm * shm, long long seq,
				 int for_read, int create,
				 masterWal * wal, int *retry);
static void wal_prepare_for_sync (masterShm * shm, long long seq,
				  masterWal * arg, int *done);
static void wal_prepare_for_unlink (masterShm * shm, long long seq,
				    int if_synced, masterWal * arg,
				    int *done);
static void wal_prepare_for_set_initialized (masterShm * shm,
					     masterWal * arg, int *done);
static int wal_open (masterShm * shm, int is_recovery,
		     smrLogAddr ** ret_addr);
static int wal_create (masterShm * shm, int is_recovery,
		       smrLogAddr ** ret_addr);
static int wal_sync (masterShm * shm, int is_recovery);
static int wal_unlink (masterShm * shm, int is_recovery);
static int wal_set_initialized (masterShm * shm, int is_recovery);
static int wal_process (masterShm * shm, int is_recovery,
			smrLogAddr ** ret_addr);
static smrLogAddr *log_open (char *prefix, masterShm * shm, long long seq,
			     int for_read, int create);
static int log_on_sync (masterShm * shm, long long seq);
static int log_unlink (char *prefix, masterShm * shm, long long seq,
		       int if_synced);

/* master shamred memory */
static int make_mastershm_prefix (char *masterlog_abs, char *prefix,
				  int size);
static masterShm *open_mastershm (char *prefix, char *name, char *path,
				  int create);
static void close_mastershm (masterShm * mshm);
static int mastershm_initialize (masterShm * shm, char *prefix, char *path);
static void mastershm_finalize (masterShm * shm);
static int mastershm_enter (masterShm * shm);
static int mastershm_leave (masterShm * shm);
static int mastershm_recover (masterShm * shm);
static void mastershm_wal (masterShm * shm, shmOp op, long long seq,
			   int seq_idx, int buflog_idx);
static void mastershm_complete (masterShm * shm);

/* master log */
static int get_masterlog_abs (char *basedir, char *path, int size);
static masterLog *open_masterlog (char *path, int create_excl);
static void close_masterlog (masterLog * mlog);

/* logDev implementation */
static int shmlog_get_master_name (char *prefix, char *path, int path_size);
static memLogDev *open_mem_dev_internal (char *basedir, int create_excl);
static int mem_get_seqs (logDev * dev, long long **seqs, int *seqs_size);
static int mem_decache (logDev * dev, long long seq);
static int mem_synced (logDev * dev, long long seq);
static int mem_purge (logDev * dev, long long seq);
static int mem_purge_after (logDev * dev, long long seq);
static smrLogAddr *mem_get_mmap (logDev * dev, long long seq, int for_read,
				 int create);
static void mem_munmap_internal (masterShm * shm, smrLogAddr * addr);
static int mem_munmap (logDev * dev, smrLogAddr * addr);
int mem_remove_one (logDev * dev, long long upper, int gap_in_sec,
		    int *removed, long long *removed_seq, int *has_more);
static void mem_close (logDev * dev);
static void mem_info_cs (masterShm * shm, gpbuf_t * gp);

/* other */
static void mem_info (memLogDev * dev, char **buf);
static int handle_machine_restart (masterShm * shm, logDev * disk);
static int mem_get_max_seq (logDev * mem, long long *max_seq);
static int sync_mem_dev_internal (smrLog * handle);


/* -------------------------- */
/* Local Function Definitions */
/* -------------------------- */
static int
shmlog_get_name (char *prefix, long long seq, char *path, int path_size)
{
  int ret;

  ret = snprintf (path, path_size, "%s-log-%019lld", prefix, seq);
  if (ret < 0 || ret >= path_size)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static void *
shmlog_open (char *prefix, long long seq, int for_read, int create, int excl)
{
  char name[SHM_NAME_SIZE];
  int ret;
  int do_init = 0;
  int oflag;
  int prot;
  int fd = -1;
  void *addr = NULL;

  ret = shmlog_get_name (prefix, seq, name, sizeof (name));
  if (ret < 0)
    {
      ERRNO_POINT ();
      return NULL;
    }

  if (for_read)
    {
      oflag = O_RDONLY;
      prot = PROT_READ;
    }
  else
    {
      if (create)
	{
	  oflag = O_RDWR | O_CREAT;
	  if (excl)
	    {
	      oflag = oflag | O_EXCL;
	    }
	  prot = PROT_READ | PROT_WRITE;
	  do_init = 1;
	}
      else
	{
	  oflag = O_RDWR;
	  prot = PROT_READ | PROT_WRITE;
	}
    }

  fd = shm_open (name, oflag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (fd < 0)
    {
      ERRNO_POINT ();
      return NULL;
    }

  if (do_init)
    {
      ret = init_log_file (fd);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }

  addr = mmap (NULL, SMR_LOG_FILE_ACTUAL_SIZE, prot, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED)
    {
      ERRNO_POINT ();
      goto error;
    }
  close (fd);
  return addr;

error:
  if (fd >= 0)
    {
      close (fd);
      if (do_init)
	{
	  (void) shm_unlink (name);
	}
    }
  if (addr != NULL)
    {
      munmap (addr, SMR_LOG_FILE_ACTUAL_SIZE);
    }

  return NULL;
}

static int
shmlog_unlink (char *prefix, long long seq)
{
  char name[SHM_NAME_SIZE];
  int ret;

  ret = shmlog_get_name (prefix, seq, name, sizeof (name));
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  (void) shm_unlink (name);
  return 0;
}

static int
semref_inc (masterShm * shm, int sem_num)
{
  struct sembuf sop;
  int ret;

  sop.sem_num = sem_num;
  sop.sem_op = 1;
  sop.sem_flg = SEM_UNDO;

  ret = semop (shm->semid, &sop, 1);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static int
semref_dec (masterShm * shm, int sem_num)
{
  struct sembuf sop;
  int ret;

  sop.sem_num = sem_num;
  sop.sem_op = -1;
  sop.sem_flg = SEM_UNDO;

  ret = semop (shm->semid, &sop, 1);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static int
log_get_seqs_buf (masterShm * shm, long long *seqs, int *seqs_size)
{
  int size;
  int i;

  MS_ENTER (shm, -1);
  for (i = 0, size = 0; i < MAX_MEMLOG; i++)
    {
      if (shm->seqs[i].seq != -1)
	{
	  seqs[size++] = shm->seqs[i].seq;
	}
    }
  MS_LEAVE (shm, -1);

  if (size > 0)
    {
      qsort (seqs, size, sizeof (long long), ll_cmpr);
    }
  *seqs_size = size;
  return 0;
}

static int
get_free_buflog_idx (masterShm * shm, int *idx)
{
  int i;

  for (i = 0; i < MAX_MEMLOG; i++)
    {
      int refcount;
      int j;

      bufLog *b = &shm->logs[i];
      if (b->seq == -1)
	{
	  *idx = i;
	  return 0;
	}

      // check reference count
      refcount = semctl (shm->semid, b->sem_num, GETVAL);
      if (refcount < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}

      if (refcount == 0)
	{
	  int hasref = 0;
	  if (b->deleted)
	    {
	      *idx = i;
	      return 0;
	    }
	  // no reference from seqs array means free buflog
	  for (j = 0; j < MAX_MEMLOG; j++)
	    {
	      if (shm->seqs[j].buflog_idx == i)
		{
		  hasref = 1;
		  break;
		}
	    }
	  if (!hasref)
	    {
	      *idx = i;
	      return 0;
	    }
	}
    }

  *idx = -1;
  return 0;
}

static int
wal_prepare_for_open (masterShm * shm, long long seq, int for_read,
		      int create, masterWal * arg, int *retry)
{
  int i, ret;
  int found_seq_idx = -1;
  int found_buflog_idx = -1;
  long long victim = -1LL;
  int victim_seq_idx = -1;
  int victim_buflog_idx = -1;
  int room_seq_idx = -1;

  SFI_PROBE ();
  for (i = 0; i < MAX_MEMLOG; i++)
    {
      logSeq *sq = &shm->seqs[i];

      if (sq->seq == seq)
	{
	  found_seq_idx = i;
	  found_buflog_idx = sq->buflog_idx;
	}
      else if (sq->seq == -1LL)
	{
	  room_seq_idx = i;
	}
      else if (sq->synced == 1)
	{
	  if ((victim == -1LL || victim > sq->seq))
	    {
	      victim = sq->seq;
	      victim_seq_idx = i;
	      victim_buflog_idx = sq->buflog_idx;
	    }
	}
    }

  if (for_read)
    {
      if (found_seq_idx == -1)
	{
	  errno = ENOENT;
	  ERRNO_POINT ();
	  return -1;
	}
      arg->op = OP_OPEN;
      arg->seq = seq;
      arg->seq_idx = found_seq_idx;
      arg->buflog_idx = found_buflog_idx;
      arg->flag = for_read;
    }
  else
    {
      if (found_seq_idx != -1)
	{
	  arg->op = OP_OPEN;
	  arg->seq = seq;
	  arg->seq_idx = found_seq_idx;
	  arg->buflog_idx = found_buflog_idx;
	  arg->flag = for_read;
	}
      else if (create && room_seq_idx != -1)
	{
	  arg->op = OP_CREATE;
	  arg->seq = seq;
	  arg->seq_idx = room_seq_idx;
	  ret = get_free_buflog_idx (shm, &arg->buflog_idx);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      return -1;
	    }
	  arg->flag = for_read;
	}
      else if (create && victim != -1LL)
	{
	  arg->op = OP_UNLINK;
	  arg->seq = victim;
	  arg->seq_idx = victim_seq_idx;
	  arg->buflog_idx = victim_buflog_idx;
	  arg->flag = 0;
	  *retry = 1;
	}
      else
	{
	  if (create)
	    {
	      errno = ERRNO_NO_SPACE;
	    }
	  else
	    {
	      errno = ERRNO_NO_ENTRY;
	    }
	  ERRNO_POINT ();
	  return -1;
	}
    }

  assert (arg->op != OP_NONE);
  return 0;
}

static void
wal_prepare_for_sync (masterShm * shm, long long seq,
		      masterWal * arg, int *done)
{
  int i;

  // no such seq means already_done
  *done = 1;

  SFI_PROBE ();
  for (i = 0; i < MAX_MEMLOG; i++)
    {
      logSeq *sq = &shm->seqs[i];

      if (sq->seq == seq)
	{
	  if (sq->synced == 0)
	    {
	      arg->op = OP_SYNC;
	      arg->seq = seq;
	      arg->seq_idx = i;
	      arg->buflog_idx = sq->buflog_idx;
	      arg->flag = 0;
	      *done = 0;
	    }
	  return;
	}
    }
  return;
}

static void
wal_prepare_for_unlink (masterShm * shm, long long seq, int if_synced,
			masterWal * arg, int *done)
{
  int i;

  // no such seq means already_done
  *done = 1;

  SFI_PROBE ();
  for (i = 0; i < MAX_MEMLOG; i++)
    {
      logSeq *sq = &shm->seqs[i];

      if (sq->seq == seq)
	{
	  if (if_synced && !sq->synced)
	    {
	      return;
	    }
	  else
	    {
	      arg->op = OP_UNLINK;
	      arg->seq = seq;
	      arg->seq_idx = i;
	      arg->buflog_idx = sq->buflog_idx;
	      arg->flag = 0;
	      *done = 0;
	    }
	  return;
	}
    }
  return;
}

static void
wal_prepare_for_set_initialized (masterShm * shm, masterWal * arg, int *done)
{
  SFI_PROBE ();
  if (shm->initialized)
    {
      *done = 1;
    }
  else
    {
      arg->op = OP_SET_INITIALIZED;
      arg->seq = -1LL;
      arg->seq_idx = -1;
      arg->buflog_idx = -1;
      arg->flag = 0;
    }
  return;
}

static int
wal_open (masterShm * shm, int is_recovery, smrLogAddr ** ret_addr)
{
  masterWal *wal = &shm->wal;
  smrLogAddr *addr = NULL;

  if (!is_recovery)
    {
      assert (ret_addr != NULL);
      addr = malloc (sizeof (smrLogAddr));
      if (addr == NULL)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      init_smr_log_addr (addr);
    }

  if (wal->buflog_idx >= 0)
    {
      int idx = wal->buflog_idx;

      shm->logs[idx].seq = wal->seq;
      shm->logs[idx].deleted = 0;

      if (!is_recovery)
	{
	  addr->loc = IN_BUF;
	  addr->seq = wal->seq;
	  addr->addr = (char *) shm + shm->logs[idx].offset;
	  addr->ref = (void *) (long) shm->logs[idx].sem_num;
	  semref_inc (shm, shm->logs[idx].sem_num);
	  SFI_PROBE ();
	  *ret_addr = addr;
	}
      return 0;
    }
  else
    {
      if (is_recovery)
	{
	  return 0;
	}

      addr->loc = IN_SHM;
      addr->seq = wal->seq;
      addr->addr = shmlog_open (shm->prefix, wal->seq, wal->flag, 0, 0);
      if (addr->addr == NULL)
	{
	  free (addr);
	  ERRNO_POINT ();
	  return -1;
	}
      SFI_PROBE ();
      *ret_addr = addr;
      return 0;
    }
}

static int
wal_create (masterShm * shm, int is_recovery, smrLogAddr ** ret_addr)
{
  masterWal *wal = &shm->wal;
  logSeq *sq;
  smrLogAddr *addr = NULL;

  sq = &shm->seqs[wal->seq_idx];
  sq->seq = wal->seq;
  sq->synced = 0;
  sq->buflog_idx = wal->buflog_idx;

  if (!is_recovery)
    {
      assert (ret_addr != NULL);
      addr = malloc (sizeof (smrLogAddr));
      if (addr == NULL)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      init_smr_log_addr (addr);
    }

  if (wal->buflog_idx >= 0)
    {
      int idx = wal->buflog_idx;
      char *cp;


      shm->logs[idx].seq = wal->seq;
      shm->logs[idx].deleted = 0;
      // reset checksum only
      cp = (char *) shm + shm->logs[idx].offset + SMR_LOG_FILE_DATA_SIZE;
      memset (cp, 0, SMR_LOG_FILE_CHECKSUM_SIZE);

      if (!is_recovery)
	{
	  addr->loc = IN_BUF;
	  addr->seq = wal->seq;
	  addr->addr = (char *) shm + shm->logs[idx].offset;
	  addr->ref = (void *) (long) shm->logs[idx].sem_num;
	  semref_inc (shm, shm->logs[idx].sem_num);
	  SFI_PROBE ();
	  *ret_addr = addr;
	}
      return 0;
    }
  else
    {
      void *addr_addr = NULL;

      addr_addr =
	shmlog_open (shm->prefix, wal->seq, wal->flag, 1, is_recovery == 0);

      if (!is_recovery)
	{
	  SFI_PROBE ();
	  addr->loc = IN_SHM;
	  addr->seq = wal->seq;
	  addr->addr = addr_addr;
	  if (addr->addr == NULL)
	    {
	      free (addr);
	      ERRNO_POINT ();
	      return -1;
	    }
	  *ret_addr = addr;
	}
      else
	{
	  SFI_PROBE ();
	  munmap (addr_addr, SMR_LOG_FILE_ACTUAL_SIZE);
	}
      return 0;
    }
}

static int
wal_sync (masterShm * shm, int is_recovery)
{
  masterWal *wal = &shm->wal;
  logSeq *sq;

  sq = &shm->seqs[wal->seq_idx];
  sq->synced = 1;
  return 0;
}

static int
wal_unlink (masterShm * shm, int is_recovery)
{
  masterWal *wal = &shm->wal;
  logSeq *sq;

  // redo
  sq = &shm->seqs[wal->seq_idx];
  init_log_seq (sq);

  if (wal->buflog_idx >= 0)
    {
      SFI_PROBE ();
      shm->logs[wal->buflog_idx].deleted = 1;
      SFI_PROBE ();
    }
  else
    {
      SFI_PROBE ();
      (void) shmlog_unlink (shm->prefix, wal->seq);
      SFI_PROBE ();
    }
  return 0;
}

static int
wal_set_initialized (masterShm * shm, int is_recovery)
{
  if (is_recovery)
    {
      shm->initialized = 0;
    }
  else
    {
      shm->initialized = 1;
    }
  return 0;
}

static int
wal_process (masterShm * shm, int is_recovery, smrLogAddr ** ret_addr)
{
  masterWal *wal = &shm->wal;
  int ret;

#if 0
  do
    {
      gpbuf_t gp;
      char tmp[4096];

      gpbuf_init (&gp, tmp, sizeof (tmp));
      gpbuf_printf (&gp,
		    "===================================================\n");
      mem_info_cs (shm, &gp);
      gpbuf_printf (&gp,
		    "===================================================\n");
      printf ("%s", gp.bp);
      gpbuf_cleanup (&gp);
    }
  while (0);
#endif

  switch (wal->op)
    {
    case OP_OPEN:
      ret = wal_open (shm, is_recovery, ret_addr);
      break;
    case OP_CREATE:
      ret = wal_create (shm, is_recovery, ret_addr);
      break;
    case OP_SYNC:
      ret = wal_sync (shm, is_recovery);
      break;
    case OP_UNLINK:
      ret = wal_unlink (shm, is_recovery);
      break;
    case OP_SET_INITIALIZED:
      ret = wal_set_initialized (shm, is_recovery);
      break;
    default:
      ERRNO_POINT ();
      ret = -1;
      break;
    }
  return ret;
}

static smrLogAddr *
log_open (char *prefix, masterShm * shm, long long seq, int for_read,
	  int create)
{
  int ret = 0;
  masterWal arg;
  smrLogAddr *addr = NULL;
  int retry;

retry_open:
  retry = 0;
  init_master_wal (&arg);

  MS_ENTER (shm, NULL);
  ret = wal_prepare_for_open (shm, seq, for_read, create, &arg, &retry);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto unlock_return;
    }

  mastershm_wal (shm, arg.op, arg.seq, arg.seq_idx, arg.buflog_idx);
  ret = wal_process (shm, 0, &addr);
  mastershm_complete (shm);

unlock_return:
  MS_LEAVE (shm, NULL);

  if (ret == 0 && retry)
    {
      goto retry_open;
    }

  return addr;
}

static int
log_on_sync (masterShm * shm, long long seq)
{
  int ret = 0;
  masterWal arg;
  int done = 0;

  init_master_wal (&arg);

  MS_ENTER (shm, -1);
  wal_prepare_for_sync (shm, seq, &arg, &done);
  if (done)
    {
      goto unlock_return;
    }

  mastershm_wal (shm, arg.op, arg.seq, arg.seq_idx, arg.buflog_idx);
  ret = wal_process (shm, 0, NULL);
  mastershm_complete (shm);

unlock_return:
  MS_LEAVE (shm, -1);

  return ret;
}

static int
log_unlink (char *prefix, masterShm * shm, long long seq, int if_synced)
{
  int ret = 0;
  masterWal arg;
  int done = 0;

  init_master_wal (&arg);

  MS_ENTER (shm, -1);
  wal_prepare_for_unlink (shm, seq, if_synced, &arg, &done);
  if (done)
    {
      goto unlock_return;
    }

  mastershm_wal (shm, arg.op, arg.seq, arg.seq_idx, arg.buflog_idx);
  ret = wal_process (shm, 0, NULL);
  mastershm_complete (shm);

unlock_return:
  MS_LEAVE (shm, -1);

  return ret;
}

static int
make_mastershm_prefix (char *masterlog_abs, char *prefix, int size)
{
  long long r;
  long long usec;
  char path_hint[16];
  int i;
  int ret;
  char *bp, *cp;

  srandom (getpid ());
  r = random ();
  usec = currtime_usec ();

  for (i = 0; i < sizeof (path_hint) - 1; i++)
    {
      path_hint[i] = '_';
    }
  path_hint[i] = '\0';

  bp = masterlog_abs;
  cp = strrchr (masterlog_abs, '/');
  if (cp != NULL)
    {
      cp--;
    }

  for (i = sizeof (path_hint) - 2; i >= 0 && cp >= bp; i--, cp--)
    {
      if (*cp == '/')
	{
	  continue;
	}
      path_hint[i] = *cp;
    }

  ret = snprintf (prefix, size, "/%s-%lld-%lld", path_hint, usec, r);
  if (ret < 0 || ret >= size)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static masterShm *
open_mastershm (char *prefix, char *name, char *path, int create)
{
  int ret;
  int fd;
  masterShm *ms = NULL;
  int oflag;
  int do_init = 0;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;

  oflag = O_RDWR;
  fd = shm_open (name, oflag, mode);
  if (fd < 0)
    {
      if (create)
	{
	  oflag = O_RDWR | O_CREAT | O_EXCL;
	  do_init = 1;
	  fd = shm_open (name, oflag, mode);
	}
      if (fd < 0)
	{
	  ERRNO_POINT ();
	  return NULL;
	}
    }

  if (do_init)
    {
      ret = ftruncate (fd, master_shm_size);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  close (fd);
	  unlink (name);
	  return NULL;
	}
    }

  ms =
    mmap (NULL, master_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close (fd);
  fd = 0;
  if (ms == MAP_FAILED)
    {
      ERRNO_POINT ();
      goto error;
    }

  /* try mlock master shm */
  ret = mlock (ms, master_shm_size);
  if (ret == 0)
    {
      ms->isLocked = 1;
    }
  else
    {
      ms->isLocked = 0;
    }

  if (do_init)
    {
      init_mastershm (ms);

      ret = mastershm_initialize (ms, prefix, path);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }
  return ms;

error:
  if (ms != NULL && ms != MAP_FAILED)
    {
      if (do_init && ms->semid >= 0)
	{
	  mastershm_finalize (ms);
	}
      munmap (ms, master_shm_size);
    }
  if (fd > 0)
    {
      close (fd);
      unlink (name);
    }
  return NULL;
}

static void
close_mastershm (masterShm * mshm)
{
  if (mshm != NULL && mshm != MAP_FAILED)
    {
      munmap (mshm, mshm->size);
    }
}

static int
mastershm_initialize (masterShm * shm, char *prefix, char *path)
{
  int ret;
  key_t sem_key;
  int semid;
  int i;
  union senum
  {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
  } arg;

  sem_key = ftok (path, MSHM_SEM_PROJ_ID);
  if (sem_key < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  semid = semget (sem_key, 1 + MAX_MEMLOG, IPC_CREAT | IPC_EXCL | 0666);
  if (semid < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  // initialize the semaphores
  for (i = 0; i < 1 + MAX_MEMLOG; i++)
    {
      if (i == 0)
	{
	  arg.val = 1;
	}
      else
	{
	  arg.val = 0;
	}

      ret = semctl (semid, i, SETVAL, arg);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }

  shm->semid = semid;
  strncpy (shm->prefix, prefix, SHM_NAME_SIZE);
  return 0;

error:
  if (semid >= 0)
    {
      semctl (semid, 0, IPC_RMID);
    }
  return -1;
}

static void
mastershm_finalize (masterShm * shm)
{
  if (shm->semid >= 0)
    {
      semctl (shm->semid, 0, IPC_RMID);
      shm->semid = -1;
    }
  return;
}


static int
mastershm_enter (masterShm * shm)
{
  struct sembuf sop;
  int ret;

  sop.sem_num = 0;
  sop.sem_op = -1;
  sop.sem_flg = SEM_UNDO;

  ret = semop (shm->semid, &sop, 1);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  SFI_PROBE ();
  return mastershm_recover (shm);
}


static int
mastershm_leave (masterShm * shm)
{
  struct sembuf sop;
  int ret;

  sop.sem_num = 0;
  sop.sem_op = 1;
  sop.sem_flg = SEM_UNDO;

  ret = semop (shm->semid, &sop, 1);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  SFI_PROBE ();
  return 0;
}

/*
  case 1: locked (crash within lock)
  case 1-1) wal      -> need nothing (crash before do anything)
  case 1-2) complete -> need nothing (crash after do everything)

  case 2: unlocked 
  case 2-1) need completion == 0 (normal case)
  case 2-2) need completion == 1 (crash during processing: recovery needed
*/
static int
mastershm_recover (masterShm * shm)
{
  int ret;
  masterWal *wal = &shm->wal;

  errno = ret = pthread_mutex_trylock (&wal->mutex);
  if (ret == 0)
    {
      // unlocked
      pthread_mutex_unlock (&wal->mutex);
      SFI_PROBE ();
      if (wal->need_completion)
	{
	  // crash during processing: recovery needed
	  ret = wal_process (shm, 1, NULL);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      return -1;
	    }
	  SFI_PROBE ();
	  mastershm_complete (shm);
	  return 0;
	}
      else
	{
	  //normal case
	  return 0;
	}
    }
  else if (errno == EBUSY)
    {
      // locked
      init_master_wal (wal);
      SFI_PROBE ();
      pthread_mutex_unlock (&wal->mutex);
      return 0;
    }
  else
    {
      ERRNO_POINT ();
      return -1;
    }
}

static void
mastershm_wal (masterShm * shm, shmOp op, long long seq, int seq_idx,
	       int buflog_idx)
{
  masterWal *wal = &shm->wal;

  pthread_mutex_lock (&wal->mutex);
  SFI_PROBE ();
  wal->op = op;
  wal->seq = seq;
  wal->seq_idx = seq_idx;
  wal->buflog_idx = buflog_idx;
  wal->need_completion = 1;
  SFI_PROBE ();
  pthread_mutex_unlock (&wal->mutex);
}

static void
mastershm_complete (masterShm * shm)
{
  masterWal *wal = &shm->wal;

  pthread_mutex_lock (&wal->mutex);
  SFI_PROBE ();
  wal->need_completion = 0;
  SFI_PROBE ();
  pthread_mutex_unlock (&wal->mutex);
}

static int
get_masterlog_abs (char *basedir, char *path, int size)
{
  int ret;
  char buf[PATH_MAX];
  char *base_dir;

  base_dir = realpath (basedir, buf);
  if (base_dir == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }

  ret = snprintf (path, size, "%s/%s", base_dir, MASTER_LOG_NAME);
  if (ret < 0 || ret >= size)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static masterLog *
open_masterlog (char *path, int create_excl)
{
  int ret;
  int fd = 0;
  masterLog *master;
  int oflag;
  int do_init = 0;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;

  if (path == NULL)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return NULL;
    }

  master = malloc (sizeof (masterLog));
  if (master == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }

  if (create_excl)
    {
      oflag = O_RDWR | O_CREAT | O_EXCL;
      do_init = 1;
    }
  else
    {
      oflag = O_RDONLY;
    }

  fd = open (path, oflag, mode);
  if (fd < 0)
    {
      if (!create_excl && errno == ENOENT)
	{
	  errno = ERRNO_NO_MASTER_FILE;
	}
      ERRNO_POINT ();
      goto error;
    }

  if (do_init)
    {
      memset (master, 0, sizeof (masterLog));
      ret =
	make_mastershm_prefix (path, master->prefix, sizeof (master->prefix));
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}

      ret = write (fd, master, sizeof (masterLog));
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}

      ret = fsync (fd);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }
  else
    {
      ret = read (fd, master, sizeof (masterLog));
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }

  close (fd);
  fd = 0;

  return master;

error:
  if (master != NULL)
    {
      free (master);
    }

  if (fd > 0)
    {
      close (fd);
      unlink (path);
    }
  return NULL;
}

static void
close_masterlog (masterLog * mlog)
{
  if (mlog != NULL)
    {
      free (mlog);
    }
}

static int
shmlog_get_master_name (char *prefix, char *path, int path_size)
{
  int ret;

  ret = snprintf (path, path_size, "%s-%s", prefix, MASTER_SHM_NAME);
  if (ret < 0 || ret >= path_size)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static memLogDev *
open_mem_dev_internal (char *basedir, int create_excl)
{
  int ret;
  char path[PATH_MAX];
  memLogDev *mem = NULL;
  masterLog *mlog = NULL;
  masterShm *mshm = NULL;
  char name[SHM_NAME_SIZE];

  mem = malloc (sizeof (memLogDev));
  if (mem == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }

  ret = get_masterlog_abs (basedir, path, sizeof (path));
  if (ret < 0)
    {
      ERRNO_POINT ();
      return NULL;
    }

  mlog = open_masterlog (path, create_excl);
  if (mlog == NULL)
    {
      ERRNO_POINT ();
      goto error;
    }

  ret = shmlog_get_master_name (mlog->prefix, name, sizeof (name));
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  // when OS restarts there can be no shared memory files 
  mshm = open_mastershm (mlog->prefix, name, path, 1);
  if (mshm == NULL)
    {
      ERRNO_POINT ();
      goto error;
    }

  mem->dev.get_seqs = mem_get_seqs;
  mem->dev.decache = mem_decache;
  mem->dev.synced = mem_synced;
  mem->dev.purge = mem_purge;
  mem->dev.purge_after = mem_purge_after;
  mem->dev.get_mmap = mem_get_mmap;
  mem->dev.munmap = mem_munmap;
  mem->dev.remove_one = mem_remove_one;
  mem->dev.close = mem_close;
  mem->mlog = mlog;
  mem->mshm = mshm;

  return mem;
error:
  if (mem != NULL)
    {
      free (mem);
    }
  if (mlog != NULL)
    {
      close_masterlog (mlog);
    }
  if (mshm != NULL)
    {
      close_mastershm (mshm);
    }
  return NULL;
}

static int
mem_get_seqs (logDev * dev, long long **seqs, int *seqs_size)
{
  memLogDev *mem = (memLogDev *) dev;
  long long *fseqs;
  int fseqs_size;
  int ret;

  if (dev == NULL || seqs == NULL || seqs_size == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  fseqs = malloc (MAX_MEMLOG * sizeof (long long));
  if (fseqs == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }

  fseqs_size = 0;
  ret = log_get_seqs_buf (mem->mshm, fseqs, &fseqs_size);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  *seqs = fseqs;
  *seqs_size = fseqs_size;
  return 0;

error:
  if (fseqs != NULL)
    {
      free (fseqs);
    }
  return -1;
}

static int
mem_decache (logDev * dev, long long seq)
{
  return 0;
}

static int
mem_synced (logDev * dev, long long seq)
{
  memLogDev *mem = (memLogDev *) dev;
  masterShm *shm;

  if (dev == NULL || seq < 0)
    {
      errno = EINVAL;
      return -1;
    }
  seq = seq_round_down (seq);
  shm = mem->mshm;

  return log_on_sync (shm, seq);
}

static int
mem_purge (logDev * dev, long long seq)
{
  memLogDev *mem = (memLogDev *) dev;

  if (dev == NULL || seq < 0)
    {
      errno = EINVAL;
      return -1;
    }
  seq = seq_round_down (seq);

  return log_unlink (mem->mlog->prefix, mem->mshm, seq, 0);
}

static int
mem_purge_after (logDev * dev, long long seq)
{
  memLogDev *mem = (memLogDev *) dev;
  int ret;
  long long seqs[MAX_MEMLOG];
  int seqs_size = 0;
  int i;

  if (dev == NULL || seq < 0)
    {
      errno = EINVAL;
      return -1;
    }

  ret = log_get_seqs_buf (mem->mshm, seqs, &seqs_size);
  if (ret < 0)
    {
      return -1;
    }

  for (i = 0; i < seqs_size; i++)
    {
      if (seqs[i] >= seq)
	{
	  ret = log_unlink (mem->mlog->prefix, mem->mshm, seqs[i], 0);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      return -1;
	    }
	}
    }
  return 0;
}

static smrLogAddr *
mem_get_mmap (logDev * dev, long long seq, int for_read, int create)
{
  memLogDev *mem = (memLogDev *) dev;
  long long begin_seq;

  if (dev == NULL || seq < 0)
    {
      errno = EINVAL;
      return NULL;
    }
  begin_seq = seq_round_down (seq);

  return log_open (mem->mlog->prefix, mem->mshm, begin_seq, for_read, create);
}

static void
mem_munmap_internal (masterShm * shm, smrLogAddr * addr)
{
  int sem_num = (int) (long) addr->ref;

  if (sem_num > 0)
    {
      assert (addr->loc == IN_BUF);
      (void) semref_dec (shm, sem_num);
    }
  else
    {
      assert (addr->loc == IN_SHM);
      munmap (addr->addr, SMR_LOG_FILE_ACTUAL_SIZE);
    }
  free (addr);
}

static int
mem_munmap (logDev * dev, smrLogAddr * addr)
{
  memLogDev *mem = (memLogDev *) dev;

  if (dev == NULL || addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  mem_munmap_internal (mem->mshm, addr);
  return 0;
}

int
mem_remove_one (logDev * dev, long long upper, int gap_in_sec,
		int *removed, long long *removed_seq, int *has_more)
{
  // notihg to implement
  return 0;
}

static void
mem_close (logDev * dev)
{
  memLogDev *mem = (memLogDev *) dev;

  if (mem == NULL)
    {
      return;
    }
  if (mem->mlog)
    {
      close_masterlog (mem->mlog);
    }
  if (mem->mshm)
    {
      close_mastershm (mem->mshm);
    }
  free (mem);
}

static void
mem_info_cs (masterShm * shm, gpbuf_t * gp)
{
  int i;
  masterWal *wal;

  gpbuf_printf (gp, "size:%d\n", shm->size);
  gpbuf_printf (gp, "mlocked:%d\n", shm->isLocked);
  gpbuf_printf (gp, "semid:%d\n", shm->semid);
  gpbuf_printf (gp, "initialized:%d\n", shm->initialized);
  wal = &shm->wal;
  gpbuf_printf (gp,
		"wal: need_completion:%d op:%d seq:%lld seq_idx:%d buflog_idx:%d flag:%d\n",
		wal->need_completion, wal->op, wal->seq, wal->seq_idx,
		wal->buflog_idx, wal->flag);

  for (i = 0; i < MAX_MEMLOG; i++)
    {
      logSeq *seq = &shm->seqs[i];
      gpbuf_printf (gp, "seqs[%d]: seq:%lld synced:%d buflog_idx:%d\n", i,
		    seq->seq, seq->synced, seq->buflog_idx);
    }

  for (i = 0; i < MAX_MEMLOG; i++)
    {
      bufLog *b = &shm->logs[i];
      gpbuf_printf (gp,
		    "logs[%d]: sem_num:%d sem_val:%d seq:%lld deleted:%d\n",
		    i, b->sem_num, semctl (shm->semid, b->sem_num,
					   GETVAL), b->seq, b->deleted);
    }
}

static void
mem_info (memLogDev * mem, char **buf)
{
  int ret;
  gpbuf_t gp;
  char tmp[4096];
  masterShm *shm;

  if (mem == NULL || buf == NULL)
    {
      errno = EINVAL;
      return;
    }
  gpbuf_init (&gp, tmp, sizeof (tmp));
  shm = mem->mshm;

  ret = mastershm_enter (shm);
  if (ret < 0)
    {
      gpbuf_printf (&gp, "Failed to enter master shared memory errno:%d\n");
      goto done;
    }
  mem_info_cs (shm, &gp);

  (void) mastershm_leave (shm);

done:
  gpbuf_gut (&gp, buf);
  gpbuf_cleanup (&gp);
  return;
}

static int
handle_machine_restart (masterShm * shm, logDev * disk)
{
  int i, idx;
  int ret;
  long long *seqs = NULL;
  int seqs_size = 0;

  // partially reintialize logSeq and bufLog
  for (i = 0; i < MAX_MEMLOG; i++)
    {
      shm->seqs[i].seq = -1LL;
      shm->seqs[i].synced = 0;
      shm->seqs[i].buflog_idx = -1;

      shm->logs[i].seq = -1LL;
      shm->logs[i].deleted = 0;
    }

  ret = disk->get_seqs (disk, &seqs, &seqs_size);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto ret_return;
    }
  else if (seqs == NULL || seqs_size == 0)
    {
      ret = 0;
      goto ret_return;
    }

  idx = 0;
  for (i = seqs_size - 3; i < seqs_size; i++)
    {
      smrLogAddr *addr = NULL;

      if (i < 0)
	{
	  continue;
	}

      addr = disk->get_mmap (disk, seqs[i], 1, 0);
      if (addr == NULL)
	{
	  ret = -1;
	  ERRNO_POINT ();
	  goto ret_return;
	}

      // second condition: no disk log is copyed to mem and last disk log is finalized also.
      if (addr_is_finalized (addr) == 0 || (idx == 0 && i == seqs_size - 1))
	{
	  logSeq *seq = &shm->seqs[idx];
	  bufLog *buf = &shm->logs[idx];
	  char *bp;

	  seq->seq = seqs[i];
	  seq->buflog_idx = idx;
	  buf->seq = seqs[i];
	  //do copy
	  bp = (char *) shm + buf->offset;
	  memcpy (bp, addr->addr, SMR_LOG_FILE_ACTUAL_SIZE);
	  idx++;
	}

      disk->munmap (disk, addr);
      addr = NULL;
    }
  ret = 0;

ret_return:
  if (seqs != NULL)
    {
      free (seqs);
    }

  return ret;
}

static int
mem_get_max_seq (logDev * mem, long long *max_seq)
{
  long long *seqs;
  int seqs_size;
  int ret;

retry:
  seqs = NULL;
  seqs_size = 0;
  ret = mem->get_seqs (mem, &seqs, &seqs_size);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (seqs_size > 0)
    {
      long long base_seq;
      int offset;
      smrLogAddr *addr;

      base_seq = seqs[seqs_size - 1];
      addr = mem->get_mmap (mem, base_seq, 1, 0);
      if (addr == NULL)
	{
	  if (errno == ERRNO_NO_ENTRY)
	    {
	      free (seqs);
	      goto retry;
	    }
	  ERRNO_POINT ();
	  goto error;
	}
      offset = addr_offset (addr);
      if (offset < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      mem->munmap (mem, addr);
      *max_seq = base_seq + offset;
    }
  else
    {
      *max_seq = 0LL;
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

static int
sync_mem_dev_internal (smrLog * handle)
{
  int ret;
  long long max_seq = 0LL;

  ret = mem_get_max_seq (handle->mem, &max_seq);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (max_seq > 0LL)
    {
      ret = smrlog_sync_upto (handle, max_seq);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

  return 0;
}

/* ----------------- */
/* Exported Function */
/* ----------------- */
logDev *
open_mem_dev (char *basedir)
{
  if (basedir == NULL)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return NULL;
    }
  return (logDev *) open_mem_dev_internal (basedir, 0);
}

int
create_mem_dev (char *basedir)
{
  memLogDev *mem;

  if (basedir == NULL)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return -1;
    }

  mem = open_mem_dev_internal (basedir, 1);
  if (mem == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  mem->dev.close ((logDev *) mem);
  return 0;
}

int
info_mem_dev (char *basedir, char **buf)
{
  memLogDev *mem;

  if (basedir == NULL || buf == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  mem = open_mem_dev_internal (basedir, 0);
  if (mem == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }

  mem_info (mem, buf);
  mem->dev.close ((logDev *) mem);
  return 0;
}

int
unlink_mem_dev (char *basedir)
{
  smrLog *handle;
  memLogDev *mem;
  int ret;
  char path[PATH_MAX];

  handle = smrlog_init (basedir);
  if (handle == NULL || handle->mem == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  mem = (memLogDev *) handle->mem;

  //flush mem to disk
  ret = sync_mem_dev_internal (handle);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  //delete contents
  ret = mem->dev.purge_after ((logDev *) mem, 0LL);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  //delete master shm
  mastershm_finalize (mem->mshm);

  ret = shmlog_get_master_name (mem->mlog->prefix, path, sizeof (path));
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }
  (void) shm_unlink (path);

  //delete master log
  ret = get_masterlog_abs (basedir, path, sizeof (path));
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }
  (void) unlink (path);

  mem->dev.close ((logDev *) mem);
  return 0;

error:
  if (mem != NULL)
    {
      mem->dev.close ((logDev *) mem);
    }
  return -1;
}

int
sync_mem_dev (char *basedir)
{
  smrLog *handle;
  memLogDev *mem;
  int ret;

  handle = smrlog_init (basedir);
  if (handle == NULL || handle->mem == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  mem = (memLogDev *) handle->mem;

  ret = sync_mem_dev_internal (handle);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  mem->dev.close ((logDev *) mem);
  return 0;

error:
  if (mem != NULL)
    {
      mem->dev.close ((logDev *) mem);
    }
  return -1;
}

int
mem_handle_machine_restart (logDev * mem, logDev * disk)
{
  int ret = 0;
  masterWal arg;
  int done = 0;
  masterShm *shm;

  assert (mem != NULL);
  init_master_wal (&arg);
  shm = ((memLogDev *) mem)->mshm;

  MS_ENTER (shm, -1);
  wal_prepare_for_set_initialized (shm, &arg, &done);
  if (done)
    {
      goto unlock_return;
    }

  mastershm_wal (shm, arg.op, arg.seq, arg.seq_idx, arg.buflog_idx);
  // no wal_process (disk is from external source)
  ret = handle_machine_restart (shm, disk);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto unlock_return;
    }
  wal_set_initialized (shm, 0);
  mastershm_complete (shm);

unlock_return:
  MS_LEAVE (shm, -1);

  return ret;
}

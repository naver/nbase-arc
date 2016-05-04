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

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>

#include "smr.h"
#include "smr_log.h"
#include "crc16.h"


/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */
/* common util */
static void print_usage (void);
static int parse_ll (char *tok, long long *seq);
static void *get_addr (char *file);
static void release_addr (void *addr);
static void verify_checksum (int argc, char *argv[]);
static int data_scanner (void *arg, long long seq, long long timestamp,
			 int hash, unsigned char *buf, int size);
static void data_dump (int argc, char *argv[]);
static void createlog (int argc, char *argv[]);
static void deletelog (int argc, char *argv[]);
static void synclog (int argc, char *argv[]);
static void infomem (int argc, char *argv[]);

/* --------------- */
/* LOCAL VARIABLES */
/* --------------- */
static const char *_usage =
  "usage: smr-logutil <subcommand>                                            \n"
  "                                                                           \n"
  "Available subcommands:                                                     \n"
  "  vc <file>                                                                \n"
  "    - verify checksum portion of the file                                  \n"
  "  datadump <log_dir> <begin_seq> [<end_seq>]                               \n"
  "    - dump all session data portion begin from <begin_seq> to the stdout.  \n"
  "      if <end_seq> is specified, it quits emitting after <end_seq> has met \n"
  "  createlog <log_dir>                                                      \n"
  "      create master log in the <log_dir> for share memory logging          \n"
  "  deletelog <log_dir>                                                      \n"
  "      delete master log in the <log_dir> and all shared memory logs        \n"
  "  synclog <log_dir>                                                        \n"
  "      sync log to disk                                                     \n"
  "  infomem <log_dir>                                                        \n"
  "      prints memory information                                            \n";

/* ----------------------------- */
/* LOCAL FUNCTION IMPLEMENTATION */
/* ----------------------------- */
static void
print_usage (void)
{
  printf ("%s", _usage);
}

static int
parse_ll (char *tok, long long *seq)
{
  assert (tok != NULL);
  assert (seq != NULL);

  *seq = strtoll (tok, NULL, 10);
  if (*seq < 0 || errno == ERANGE)
    {
      return -1;
    }
  return 0;
}

static void *
get_addr (char *file)
{
  int fd;
  void *addr = NULL;

  fd = open (file, O_RDONLY);
  if (fd == -1)
    {
      printf ("-ERR failed to open:%s\n", file);
      return NULL;
    }

  addr = mmap (NULL, SMR_LOG_FILE_ACTUAL_SIZE, PROT_READ, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED)
    {
      printf ("-ERR failed to mmap:%d\n", errno);
      close (fd);
      return NULL;
    }

  return addr;
}

static void
release_addr (void *addr)
{
  assert (addr != NULL);
  munmap (addr, SMR_LOG_FILE_ACTUAL_SIZE);
}

#define is_finalized(m) (m->off == SMR_LOG_NUM_CHECKSUM)
static void
verify_checksum (int argc, char *argv[])
{
  void *addr = NULL;
  char *bp, *ep;
  logChecksum *master, *checksums;
  unsigned short mcsum;
  int num_pages;


  if (argc != 1)
    {
      printf ("-ERR usage: vc <file>\n");
      return;
    }
  addr = get_addr (argv[0]);
  if (addr == NULL)
    {
      return;
    }

  /* set up pointers */
  bp = (char *) addr;
  ep = bp + SMR_LOG_FILE_DATA_SIZE;
  master = (logChecksum *) ep;
  checksums = master + 1;

  if (is_finalized (master))
    {
      num_pages = SMR_LOG_NUM_CHECKSUM - 1;
    }
  else
    {
      num_pages = master->off;
    }
  printf ("num_pages:%d\n", num_pages);

  mcsum = crc16 ((char *) checksums, sizeof (logChecksum) * num_pages, 0);
  if (mcsum != master->checksum)
    {
      printf ("master checksum error. expected:0x%4x master:0x%4x\n", mcsum,
	      master->checksum);
    }
  else
    {
      printf ("+OK checksum verified. 0x%4x\n", mcsum);
    }

  if (addr)
    {
      release_addr (addr);
    }
}

/* -------- */
/* DATADUMP */
/* -------- */
static int
data_scanner (void *arg, long long seq, long long timestamp, int hash,
	      unsigned char *buf, int size)
{
  printf ("%lld timestamp=%lld hash=%d size=%d\n", seq, timestamp, hash,
	  size);
  return 1;
}

static void
data_dump (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  char *log_dir;
  long long begin_seq;
  long long end_seq = -1;
  smrLog *smrlog;
  int ret;

  if (argc < 2 || argc > 3)
    {
      printf ("datadump <log_dir> <begin_seq> [<end_seq>]\n");
      exit (1);
    }

  /* check parameter */
  log_dir = realpath (argv[0], log_dir_buf);

  if (parse_ll (argv[1], &begin_seq) < 0)
    {
      printf ("bad <begin_seq>\n");
      exit (1);
    }

  if (argc == 3 && parse_ll (argv[2], &end_seq) < 0)
    {
      printf ("bad <end_seq>\n");
      exit (1);
    }

  smrlog = smrlog_init (log_dir);
  if (smrlog == NULL)
    {
      printf ("Failed to initialize from log_dir:%s\n", log_dir);
      exit (1);
    }

  ret = smrlog_scan (smrlog, begin_seq, end_seq, data_scanner, NULL);
  if (ret < 0)
    {
      printf ("smrlog_scan failed: %d\n", ret);
      goto fini;
    }

fini:
  if (smrlog != NULL)
    {
      smrlog_destroy (smrlog);
    }
}

static void
createlog (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  char *log_dir;
  int ret;

  if (argc != 1)
    {
      printf ("createlog <log_dir>\n");
      exit (1);
    }

  log_dir = realpath (argv[0], log_dir_buf);
  ret = smrlog_open_master (log_dir);
  if (ret < 0)
    {
      printf ("Failed to createlog errno:%d\n", errno);
      exit (1);
    }
  printf ("OK createlog succeeded log dir:%s\n", log_dir);
}

static void
deletelog (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  char *log_dir;
  int ret;

  if (argc != 1)
    {
      printf ("deletelog <log_dir>\n");
      exit (1);
    }

  log_dir = realpath (argv[0], log_dir_buf);
  ret = smrlog_unlink_master (log_dir);
  if (ret < 0)
    {
      printf ("Failed to deletelog errno:%d\n", errno);
      exit (1);
    }
  printf ("OK deletelog succeeded log dir:%s\n", log_dir);
}

static void
synclog (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  char *log_dir;
  int ret;

  if (argc != 1)
    {
      printf ("sync <log_dir>\n");
      exit (1);
    }

  log_dir = realpath (argv[0], log_dir_buf);
  ret = smrlog_sync_master (log_dir);
  if (ret < 0)
    {
      printf ("Failed to sync errno:%d\n", errno);
      exit (1);
    }
  printf ("OK synclog succeeded log dir:%s\n", log_dir);
}

static void
infomem (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  char *log_dir;
  int ret;
  char *buf = NULL;

  if (argc != 1)
    {
      printf ("infomem <log_dir>\n");
      exit (1);
    }
  log_dir = realpath (argv[0], log_dir_buf);
  ret = smrlog_info_master (log_dir, &buf);
  if (ret < 0)
    {
      printf ("Failed to createlog errno:%d\n", errno);
      exit (1);
    }
  if (buf != NULL)
    {
      printf ("%s\n", buf);
      free (buf);
    }
  printf ("OK infomem succeeded log dir:%s\n", log_dir);
}


/* ---- */
/* MAIN */
/* ---- */
int
main (int argc, char *argv[])
{
  if (argc < 2)
    {
      print_usage ();
      exit (1);
    }

  errno = 0;
  if (strcmp (argv[1], "vc") == 0)
    {
      verify_checksum (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "datadump") == 0)
    {
      data_dump (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "createlog") == 0)
    {
      createlog (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "deletelog") == 0)
    {
      deletelog (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "synclog") == 0)
    {
      synclog (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "infomem") == 0)
    {
      infomem (argc - 2, argv + 2);
    }
  else
    {
      print_usage ();
      exit (1);
    }
}

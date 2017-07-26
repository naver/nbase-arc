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
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>

#include "log_internal.h"
#include "smr.h"
#include "smr_log.h"
#include "crc16.h"

/* --------------------- */
/* LOCAL TYPE DEFINITION */
/* --------------------- */
typedef enum
{
  REL_NONE = 0,
  REL_UNDEF = 1,
  REL_LEFT_TO_LOG = 2,
  REL_IN_LOG = 4,
  REL_RIGHT_TO_LOG = 8
} logRel;

/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */
/* common util */
static void print_usage (void);
static int parse_ll (char *tok, long long *seq);
static int parse_opt_o (char *opt);
static long long usertime2ts (long long usertime);
static long long ts2usertime (long long ts);
static long long get_startmsgseq (smrLog * log, long long seq);
static int get_nearestseq_and_rel (smrLog * log, long long ts, long long *seq,
				   logRel * rel);
static int get_beginseq_bisect (smrLog * log, long long ts, logRel * rels,
				long long *seqs, int left, int right);
static long long get_beginseq_bytime (smrLog * log, long long ts);
static void *get_addr (char *file);
static void release_addr (void *addr);
static void verify_checksum (int argc, char *argv[]);
static void mincorelog (int argc, char *argv[]);
static void decachelog (int argc, char *argv[]);
static void print_raw (unsigned char *buf, int len);
static void print_logdata (long long seq, long long timestamp,
			   int hash, unsigned char *buf, int size);
static int data_scanner (void *arg, long long seq, long long timestamp,
			 int hash, unsigned char *buf, int size);
static void data_dump (int argc, char *argv[]);
static int tr_scanner (void *arg, long long seq, long long timestamp,
		       int hash, unsigned char *buf, int size);
static void dumpbytime (int argc, char *argv[]);
static void createlog (int argc, char *argv[]);
static void deletelog (int argc, char *argv[]);
static void synclog (int argc, char *argv[]);
static void infomem (int argc, char *argv[]);

/* --------------- */
/* LOCAL VARIABLES */
/* --------------- */
static const char *_usage =
  "usage: smr-logutil <option> <subcommand>                                   \n"
  "                                                                           \n"
  "Available options:                                                         \n"
  "  -o <data output format specifier string> (default: Tshld)                \n"
  "    - t(timestamp) T(human timestamp) s(sequence number) h(hash)           \n"
  "      l(data length) d[<len>] (data [at most len])                         \n"
  "    - no duplicated format specifier is allowed                            \n"
  "                                                                           \n"
  "Available subcommands:                                                     \n"
  "  vc <file>                                                                \n"
  "    - verify checksum portion of the file                                  \n"
  "  datadump <log_dir> <begin_seq> [<end_seq>]                               \n"
  "    - dump all session data portion begin from <begin_seq> to the stdout.  \n"
  "      if <end_seq> is specified, it quits emitting after <end_seq> has met \n"
  "  dumpbytime <log_dir> <begin_time> [<end_time>]                           \n"
  "    - dump all session data portion from <begin_time> to <end_time>        \n"
  "    - input human time format is YYYYMMDDHHMMSSsss                         \n"
  "      e.g.) 20160801143130000                                              \n"
  "    - output data format is escaped redis string                           \n"
  "  mincorelog <log_dir> [<seq>]                                             \n"
  "    - print number of memory resident pages for each log                   \n"
  "    - when <seq> is given, print all page info of the file containing <seq>\n"
  "  decachelog <log_dir> <seq> [force]                                       \n"
  "    - decache log file pages upto <seq> from os                            \n"
  "    - last two log files are not decached unless 'force' option is set     \n"
  "  = MEMORY LOG RELATED =                                                   \n"
  "  createlog <log_dir>                                                      \n"
  "      create master log in the <log_dir> for share memory logging          \n"
  "  deletelog <log_dir>                                                      \n"
  "      delete master log in the <log_dir> and all shared memory logs        \n"
  "  synclog <log_dir>                                                        \n"
  "      sync log to disk                                                     \n"
  "  infomem <log_dir>                                                        \n"
  "      prints memory information                                            \n";


// -o option
#define MAX_FIELDS 5
static char opt_o_format[MAX_FIELDS + 1] = { 'T', 's', 'h', 'l', 'd', '\0' };

static int opt_o_len = -1;

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

static int
parse_opt_o (char *opt)
{
  int i;
  int idx = 0, len = -1;
  char c, *p = opt;

  for (i = 0; i < MAX_FIELDS; i++)
    {
      opt_o_format[i] = '\0';
    }

  while ((c = *p++) != 0 && idx < MAX_FIELDS)
    {
      // check duplicated format specifier
      if (strchr (opt_o_format, c) != NULL)
	{
	  printf ("Duplicated format specifier:%c\n", c);
	  return -1;
	}

      switch (c)
	{
	case 'T':
	case 't':
	case 's':
	case 'h':
	case 'l':
	case 'd':
	  opt_o_format[idx++] = c;
	  if (c == 'd' && isdigit (*p) && *p != '0')
	    {
	      len = 0;
	      while (isdigit (*p))
		{
		  len = len * 10 + (*p - '0');
		  p++;
		}
	    }
	  break;
	default:
	  printf ("Invalid format specifier:%c\n", c);
	  return -1;
	}
    }

  if (idx < 1 || idx > MAX_FIELDS)
    {
      printf ("Too many format specifier\n");
      return -1;
    }

  if (len != -1 && len < 0)
    {
      printf ("Invalid data length:%d\n", len);
      return -1;
    }
  opt_o_len = len;
  return 0;
}

#define _func_check(c) if(!(c)) return -1LL
// Argument example:
// 20160801111111000 (2016-08-01 11:11:11.000)
static long long
usertime2ts (long long usertime)
{
  long long msec;
  struct tm tm;
  long long ts;

  if (usertime < 0)
    {
      return -1LL;
    }

  msec = usertime % 1000;
  usertime /= 1000;

  tm.tm_sec = usertime % 100;
  _func_check (tm.tm_sec < 60);
  usertime /= 100;

  tm.tm_min = usertime % 100;
  _func_check (tm.tm_min < 60);
  usertime /= 100;

  tm.tm_hour = usertime % 100;
  _func_check (tm.tm_hour < 24);
  usertime /= 100;

  tm.tm_mday = usertime % 100;
  _func_check (tm.tm_mday > 0 && tm.tm_mday < 32);
  usertime /= 100;

  tm.tm_mon = usertime % 100 - 1;
  _func_check (tm.tm_mon >= 0 && tm.tm_mon < 12);
  usertime /= 100;

  tm.tm_year = usertime - 1900;
  tm.tm_isdst = 0;		// no daylight saving option

  ts = mktime (&tm);
  if (ts == -1)
    {
      return -1LL;
    }

  ts = ts * 1000LL + msec;
  return ts;
}

#undef _func_check

static long long
ts2usertime (long long msec)
{
  struct tm tm;
  time_t t;
  long long ut;

  t = (time_t) (msec / 1000);
  localtime_r (&t, &tm);

  ut = tm.tm_year + 1900;
  ut *= 100;
  ut += tm.tm_mon + 1;
  ut *= 100;
  ut += tm.tm_mday;
  ut *= 100;
  ut += tm.tm_hour;
  ut *= 100;
  ut += tm.tm_min;
  ut *= 100;
  ut += tm.tm_sec;
  ut *= 1000;
  ut += msec % 1000;
  return ut;
}

static long long
get_startmsgseq (smrLog * log, long long seq)
{
  smrLogAddr *addr;
  long long begin_seq;
  int decache = 0;
  int ret, found = 0;
  long long nearest = -1LL;
  int start_off = 0;

  begin_seq = seq_round_down (seq);
  if (begin_seq < 0)
    {
      return -1LL;
    }

  addr = smrlog_read_mmap (log, begin_seq);
  if (addr == NULL)
    {
      return -1LL;
    }

  if (addr->loc == IN_DISK)
    {
      decache =
	(addr_npage_in_memory (addr, log->page_size, NULL, NULL) == 0);
    }

  ret = smrlog_get_first_msg_offset (log, addr, &found, &start_off);
  if (ret < 0 || found == 0)
    {
      goto done;
    }
  nearest = begin_seq + start_off;

done:
  if (addr != NULL)
    {
      smrlog_munmap (log, addr);
    }
  if (decache)
    {
      (void) smrlog_os_decache (log, begin_seq);
    }
  return nearest;
}



// 'seq' is in/out parameter
static int
get_nearestseq_and_rel (smrLog * log, long long ts, long long *seq,
			logRel * r)
{
  long long begin_seq = *seq;
  smrLogAddr *addr;
  int ret;
  int decache = 0;
  int found = 0;
  long long ts_min, ts_max, nearest_seq;

  addr = smrlog_read_mmap (log, begin_seq);
  if (addr == NULL)
    {
      return -1;
    }

  if (addr->loc == IN_DISK)
    {
      decache =
	(addr_npage_in_memory (addr, log->page_size, NULL, NULL) == 0);
    }

  ret =
    smrlog_get_tsrange_and_nearestseq (log, addr, ts, &found, &ts_min,
				       &ts_max, &nearest_seq);
  if (ret < 0)
    {
      goto done;
    }

  if (found == 0)
    {
      *r = REL_UNDEF;
      goto done;
    }

  *seq = nearest_seq;
  if (ts < ts_min)
    {
      *r = REL_LEFT_TO_LOG;
    }
  else if (ts <= ts_max)
    {
      *r = REL_IN_LOG;
    }
  else
    {
      *r = REL_RIGHT_TO_LOG;
    }

done:
  if (addr != NULL)
    {
      smrlog_munmap (log, addr);
    }
  if (decache)
    {
      (void) smrlog_os_decache (log, begin_seq);
    }
  return ret;
}

//
// returns the index of the seqs to start with
// seqs are modified by the get_nearestseq_and_rel
//
static int
get_beginseq_bisect (smrLog * log, long long ts, logRel * rels,
		     long long *seqs, int left, int right)
{
  int ret;
  int middle;
  logRel ts_rel;

  if (left > right)
    {
      return -2;		// special end marker
    }
  middle = (left + right) / 2;

  ret = get_nearestseq_and_rel (log, ts, &seqs[middle], &ts_rel);
  if (ret < 0)
    {
      return -1;
    }
  rels[middle] = ts_rel;

  if (ts_rel == REL_IN_LOG)
    {
      return middle;
    }
  else if (ts_rel == REL_LEFT_TO_LOG)
    {
      return get_beginseq_bisect (log, ts, rels, seqs, left, middle - 1);
    }
  else if (ts_rel == REL_RIGHT_TO_LOG)
    {
      return get_beginseq_bisect (log, ts, rels, seqs, middle + 1, right);
    }
  else
    {
      int idx;

      assert (ts_rel == REL_UNDEF);
      idx = get_beginseq_bisect (log, ts, rels, seqs, middle + 1, right);
      if (idx >= 0)
	{
	  return idx;
	}
      else if (idx == -1)
	{
	  return -1;
	}
      assert (idx == -2);
      return get_beginseq_bisect (log, ts, rels, seqs, left, middle - 1);
    }
}

static long long
get_beginseq_bytime (smrLog * log, long long ts)
{
  long long ret_seq = -1LL;
  int ret;
  int i;
  long long *fseqs = NULL;
  int fseqs_size;
  logRel rel_buf[4096], *rels = NULL;

  // get file sequences from the disk
  ret = log->disk->get_seqs (log->disk, &fseqs, &fseqs_size);
  if (ret < 0)
    {
      return -1LL;
    }

  // setup rels
  if (fseqs_size > sizeof (rel_buf))
    {
      rels = malloc (fseqs_size);
      if (rels == NULL)
	{
	  goto done;
	}
    }
  else
    {
      rels = &rel_buf[0];
    }
  for (i = 0; i < fseqs_size; i++)
    {
      rels[i] = REL_NONE;
    }

  ret = get_beginseq_bisect (log, ts, rels, fseqs, 0, fseqs_size - 1);
  if (ret == -1)
    {
      goto done;
    }

  do
    {
      long long right_ub = -1LL;
      long long left_lb = -1LL;

      for (i = 0; i < fseqs_size; i++)
	{
	  if (rels[i] == REL_IN_LOG)
	    {
	      ret_seq = fseqs[i];
	      break;
	    }
	  else if (rels[i] == REL_LEFT_TO_LOG && left_lb == -1LL)
	    {
	      left_lb = fseqs[i];
	    }
	  else if (rels[i] == REL_RIGHT_TO_LOG)
	    {
	      right_ub = fseqs[i];
	    }
	}

      if (ret_seq == -1LL && left_lb != -1LL)
	{
	  if (right_ub != -1LL)
	    {
	      ret_seq = right_ub;
	    }
	  else
	    {
	      ret_seq = left_lb;
	    }
	}
    }
  while (0);

done:
  if (fseqs != NULL)
    {
      free (fseqs);
    }
  if (rels != NULL && rels != &rel_buf[0])
    {
      free (rels);
    }
  return ret_seq;
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

static void
print_pages (int page, void *arg)
{
  int *count = (int *) arg;
  fprintf (stdout, "%d ", page);
  if (*count % 16 == 15)
    {
      fprintf (stdout, "\n");
    }
  *count = *count + 1;
}

static void
mincorelog (int argc, char *argv[])
{
  char *log_dir, log_dir_buf[PATH_MAX];
  smrLog *smrlog;
  long long *fseqs = NULL;
  int fseqs_size = 0;
  int ret, i;

  if (argc != 1 && argc != 2)
    {
      printf ("dumpbytime <log_dir> [<seq>]\n");
      exit (1);
    }

  log_dir = realpath (argv[0], log_dir_buf);
  smrlog = smrlog_init (log_dir);
  if (smrlog == NULL)
    {
      printf ("Failed to initialize from log_dir:%s\n", log_dir);
      exit (1);
    }

  if (argc == 2)
    {
      long long file_seq = 0LL;
      smrLogAddr *addr;
      int nr, count = 0;

      if (parse_ll (argv[1], &file_seq) < 0)
	{
	  printf ("bad <seq>\n");
	  exit (1);
	}
      file_seq = seq_round_down (file_seq);
      addr = smrlog_read_mmap (smrlog, file_seq);
      if (addr == NULL)
	{
	  printf ("Failed to mmap seq:%lld\n", file_seq);
	  goto done;
	}
      nr =
	addr_npage_in_memory (addr, smrlog->page_size, print_pages, &count);
      fprintf (stdout, "%sTotal %d pages are in memory\n",
	       (nr > 0 && nr % 16 != 0) ? "\n" : "", nr);
      smrlog_munmap (smrlog, addr);
      goto done;
    }

  // get memory residence of the
  ret = smrlog->disk->get_seqs (smrlog->disk, &fseqs, &fseqs_size);
  if (ret < 0)
    {
      printf ("Internal error (disk->get_seqs)\n");
      goto done;
    }

  for (i = 0; i < fseqs_size; i++)
    {
      smrLogAddr *addr = smrlog_read_mmap (smrlog, fseqs[i]);
      int nres;

      if (addr == NULL)
	{
	  printf ("Failed to mmap seq:%lld\n", fseqs[i]);
	  goto done;
	}
      nres = addr_npage_in_memory (addr, smrlog->page_size, NULL, NULL);
      fprintf (stdout, "%019lld %d pages are in memory\n", fseqs[i], nres);
      smrlog_munmap (smrlog, addr);
    }

done:
  if (smrlog != NULL)
    {
      smrlog_destroy (smrlog);
    }

  if (fseqs != NULL)
    {
      free (fseqs);
    }
}

static void
decachelog (int argc, char *argv[])
{
  char *log_dir, log_dir_buf[PATH_MAX];
  smrLog *smrlog;
  long long upto_seq = 0;
  long long *fseqs = NULL;
  int fseqs_size = 0;
  int ret, i;
  int force = 0;

  if (argc != 2 && argc != 3)
    {
      printf ("decachelog <log_dir> <seq> [force]\n");
      exit (1);
    }

  log_dir = realpath (argv[0], log_dir_buf);
  smrlog = smrlog_init (log_dir);
  if (smrlog == NULL)
    {
      printf ("Failed to initialize from log_dir:%s\n", log_dir);
      exit (1);
    }

  if (parse_ll (argv[1], &upto_seq) < 0 || upto_seq < 0LL)
    {
      printf ("bad <seq>\n");
      goto done;
    }
  upto_seq = seq_round_down (upto_seq);

  if (argc == 3)
    {
      force = (strcmp (argv[2], "force") == 0);
    }

  // get memory residence of the
  ret = smrlog->disk->get_seqs (smrlog->disk, &fseqs, &fseqs_size);
  if (ret < 0)
    {
      printf ("Internal error (disk->get_seqs)\n");
      goto done;
    }

  if (!force)
    {
      fseqs_size = fseqs_size - 2;
    }

  for (i = 0; i < fseqs_size; i++)
    {
      long long seq = fseqs[i];

      if (seq > upto_seq)
	{
	  break;
	}

      (void) smrlog_os_decache (smrlog, seq);
    }

done:
  if (smrlog != NULL)
    {
      smrlog_destroy (smrlog);
    }

  if (fseqs != NULL)
    {
      free (fseqs);
    }
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
static void
print_raw (unsigned char *buf, int len)
{
  unsigned char *dp;
  char out_buf[8010];
  char *op, *start;
  const static char hexa[] =
    { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
    'e', 'f'
  };

  dp = buf;
  op = start = &out_buf[0];
  while (len--)
    {
      if (op - start > 8000)
	{
	  fwrite (start, 1, op - start, stdout);
	  op = start;
	}
      switch (*dp)
	{
	case '\\':
	case '"':
	  *op++ = '\\';
	  *op++ = *dp;
	  break;
	case '\n':
	  *op++ = '\\';
	  *op++ = 'n';
	  break;
	case '\r':
	  *op++ = '\\';
	  *op++ = 'r';
	  break;
	case '\t':
	  *op++ = '\\';
	  *op++ = 't';
	  break;
	case '\a':
	  *op++ = '\\';
	  *op++ = 'a';
	  break;
	case '\b':
	  *op++ = '\\';
	  *op++ = 'a';
	  break;
	default:
	  if (isprint (*dp))
	    {
	      *op++ = *dp;
	    }
	  else
	    {
	      *op++ = '\\';
	      *op++ = 'x';
	      *op++ = hexa[*dp >> 4];
	      *op++ = hexa[*dp & 15];
	    }
	  break;
	}
      dp++;
    }

  if (op - start > 0)
    {
      fwrite (start, 1, op - start, stdout);
    }
}


static void
print_logdata (long long seq, long long timestamp,
	       int hash, unsigned char *buf, int size)
{
  char *fmt = &opt_o_format[0];

  while (*fmt)
    {
      char *sep = (*(fmt + 1) == 0 ? "" : " ");
      int len;

      switch (*fmt)
	{
	case 'T':
	  fprintf (stdout, "%lld%s", ts2usertime (timestamp), sep);
	  break;
	case 't':
	  fprintf (stdout, "%lld%s", timestamp, sep);
	  break;
	case 's':
	  fprintf (stdout, "%lld%s", seq, sep);
	  break;
	case 'h':
	  fprintf (stdout, "%d%s", hash, sep);
	  break;
	case 'l':
	  fprintf (stdout, "%d%s", size, sep);
	  break;
	case 'd':
	  if (opt_o_len != -1 && opt_o_len < size)
	    {
	      len = opt_o_len;
	    }
	  else
	    {
	      len = size;
	    }
	  print_raw (buf, len);
	  break;
	default:
	  assert (0);
	  break;
	}
      fmt++;
    }
  fprintf (stdout, "\n");
}

static int
data_scanner (void *arg, long long seq, long long timestamp, int hash,
	      unsigned char *buf, int size)
{
  long long begin_seq = *(long long *) arg;

  if (begin_seq > seq)
    {
      return 1;
    }

  print_logdata (seq, timestamp, hash, buf, size);
  return 1;
}

static void
data_dump (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  char *log_dir;
  long long begin_seq, start_seq;
  long long end_seq = -1;
  smrLog *smrlog;
  int ret;
  int exit_code = 1;

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

  start_seq = get_startmsgseq (smrlog, begin_seq);
  if (start_seq < 0)
    {
      printf ("Faild to find msg from file for %lld\n", begin_seq);
      goto fini;
    }

  ret = smrlog_scan (smrlog, start_seq, end_seq, data_scanner, &begin_seq);
  if (ret < 0)
    {
      if (!(end_seq == -1LL && errno == ERRNO_NO_ENTRY))
	{
	  printf ("smrlog_scan failed: %d (errno=%d)\n", ret, errno);
	  goto fini;
	}
    }

  exit_code = 0;
fini:
  if (smrlog != NULL)
    {
      smrlog_destroy (smrlog);
    }
  exit (exit_code);
}

static int
tr_scanner (void *arg, long long seq, long long timestamp,
	    int hash, unsigned char *buf, int size)
{
  long long end_ts = *(long long *) arg;

  if (end_ts != -1LL && end_ts < timestamp)
    {
      return 0;
    }
  print_logdata (seq, timestamp, hash, buf, size);
  return 1;

}

static void
dumpbytime (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  char *log_dir;
  long long begin_ts, begin_seq;
  long long end_ts = -1LL;
  smrLog *smrlog;
  int ret;

  if (argc < 2 || argc > 3)
    {
      printf ("dumpbytime <log_dir> <begin_time> [<end_time>]\n");
      exit (1);
    }

  if (parse_ll (argv[1], &begin_ts) < 0
      || (begin_ts = usertime2ts (begin_ts)) < 0)
    {
      printf ("bad <begin_time>\n");
      exit (1);
    }

  if (argc == 3
      && (parse_ll (argv[2], &end_ts) < 0
	  || (end_ts = usertime2ts (end_ts)) < 0))
    {
      printf ("bad <end_ts>\n");
      exit (1);
    }

  if (end_ts != -1LL && (begin_ts > end_ts))
    {
      printf ("bad time range\n");
      exit (1);
    }

  log_dir = realpath (argv[0], log_dir_buf);
  smrlog = smrlog_init (log_dir);
  if (smrlog == NULL)
    {
      printf ("Failed to initialize from log_dir:%s\n", log_dir);
      exit (1);
    }

  begin_seq = get_beginseq_bytime (smrlog, begin_ts);
  if (begin_seq < 0)
    {
      printf ("Failed to get sequence for time:%s\n", argv[1]);
      goto done;
    }

  ret = smrlog_scan (smrlog, begin_seq, -1LL, tr_scanner, &end_ts);
  if (ret < 0)
    {
      printf ("smrlog_scan failed: %d\n", ret);
      goto done;
    }

done:
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
      goto error;
    }

  if (strcmp (argv[1], "-o") == 0)
    {
      int ret;
      if (argc < 3)
	{
	  printf ("-o option needs value\n");
	  goto error;
	}

      ret = parse_opt_o (argv[2]);
      if (ret < 0)
	{
	  printf ("Failed to parse -o option:%s\n", argv[2]);
	  goto error;
	}
      argc = argc - 2;
      argv = argv + 2;
    }
  // argv[0] may not be program name by options

  errno = 0;
  if (strcmp (argv[1], "vc") == 0)
    {
      verify_checksum (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "datadump") == 0)
    {
      data_dump (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "dumpbytime") == 0)
    {
      dumpbytime (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "mincorelog") == 0)
    {
      mincorelog (argc - 2, argv + 2);
    }
  else if (strcmp (argv[1], "decachelog") == 0)
    {
      decachelog (argc - 2, argv + 2);
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
      printf ("Unsupported subcommand:%s\n", argv[1]);
      goto error;
    }
  return 0;

error:
  print_usage ();
  exit (1);
}

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
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "smr.h"
#include "dlist.h"
#include "log_internal.h"
#include "crc16.h"

#define ERRNO_FILE_ID SCAN_FILE_ID

typedef struct logMmap_ logMmap;
typedef struct logScan_ logScan;
typedef struct scanArg_ scanArg;

struct logMmap_
{
  dlisth head;
  long long seq;		//start address
  smrLogAddr *addr;		// mempory mapped address
  int offset;			// available offset
};
#define init_log_mmap(s) do {   \
  dlisth_init(&(s)->head);      \
  (s)->seq = 0LL;               \
  (s)->addr = NULL;             \
  (s)->offset = 0LL;            \
} while (0)

struct logScan_
{
  smrLog *smrlog;		// smrLog
  dlisth mmaps;			// list of logMmap
  long long seq;		// current sequence number
  long long seq_end;		// sequence end
  long long end_limit;		// -1 means no limit
};
#define init_log_scan(s, smrlog_) do { \
  (s)->smrlog = smrlog_;               \
  dlisth_init(&(s)->mmaps);            \
  (s)->seq = 0LL;                      \
  (s)->seq_end = 0LL;                  \
  (s)->end_limit = -1LL;               \
} while (0)

struct scanArg_
{
  smrlog_scanner scanner;
  void *arg;
  int hash;
  long long timestamp;
  int length;
  int cont;
};
#define init_scan_arg(s) do {  \
  (s)->scanner = NULL;         \
  (s)->arg = NULL;             \
  (s)->hash = 0;               \
  (s)->timestamp = 0LL;        \
  (s)->length = 0;             \
  (s)->cont = 0;               \
} while (0)

static long long min_ll (long long a, long long b);
static int scan_consec_size (logScan * scan, long long seq);
static char *scan_seq_to_addr (logScan * scan, long long seq);
static int scan_map (logScan * scan, long long seq, smrLogAddr ** addr);
static void scan_unmap (logScan * scan, long long seq, smrLogAddr * addr);
static int scan_open (logScan * scan, long long seq);
static int scan_next (logScan * scan);
static int scan_close (logScan * scan);
static int scan_release_unused_maps (logScan * scan);
static int scan_read (logScan * scan, void *buf_, int size, long long seq);
static int scan_read_cb (logScan * scan, scanArg * arg, int size,
			 long long seq);
static int scan_process (logScan * scan, smrlog_scanner scanner, void *arg);

/* --------------- */
/* LOCAL FUNCTIONS */
/* --------------- */
static long long
min_ll (long long a, long long b)
{
  return a < b ? a : b;
}

static int
scan_consec_size (logScan * scan, long long seq)
{
  long long end_pos;

  end_pos = (scan->end_limit == -1LL) ? scan->seq_end : scan->end_limit;
  return min_ll (end_pos, seq_round_up (seq)) - seq;
}

static char *
scan_seq_to_addr (logScan * scan, long long seq)
{
  dlisth *h;

  for (h = scan->mmaps.next; h != &scan->mmaps; h = h->next)
    {
      logMmap *lm = (logMmap *) h;

      if (seq >= lm->seq && seq < lm->seq + SMR_LOG_FILE_DATA_SIZE)
	{
	  return (char *) lm->addr->addr + (seq - seq_round_down (seq));
	}
    }
  return NULL;
}

static int
scan_map (logScan * scan, long long seq, smrLogAddr ** addr)
{
  assert (seq == seq_round_down (seq));
  assert (addr != NULL);

  *addr = smrlog_read_mmap (scan->smrlog, seq);
  if (*addr == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static void
scan_unmap (logScan * scan, long long seq, smrLogAddr * addr)
{
  smrlog_munmap (scan->smrlog, addr);
  (void) smrlog_os_decache (scan->smrlog, seq);
}

static int
scan_open (logScan * scan, long long seq)
{
  assert (scan != NULL);
  assert (seq >= 0);

  scan->seq = seq;
  return 0;
}

static int
scan_next (logScan * scan)
{
  long long target_seq = -1LL;
  logMmap *lm = NULL;
  smrLogAddr *addr = NULL;
  int ret;

  assert (scan != NULL);

  if (dlisth_is_empty (&scan->mmaps))
    {
      target_seq = seq_round_down (scan->seq);
    }
  else
    {
      logMmap *endlm;

      endlm = (logMmap *) scan->mmaps.prev;
      if (endlm->offset < SMR_LOG_FILE_DATA_SIZE)
	{
	  //no more
	  return 0;
	}
      target_seq = endlm->seq + SMR_LOG_FILE_DATA_SIZE;
    }

  /* check end poistion */
  if (scan->end_limit != -1LL && target_seq > scan->end_limit)
    {
      //no more 
      return 0;
    }

  lm = malloc (sizeof (logMmap));
  if (lm == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  init_log_mmap (lm);

  ret = scan_map (scan, target_seq, &addr);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }
  assert (addr != NULL);

  lm->seq = target_seq;
  lm->addr = addr;
  lm->offset = smrlog_get_offset (scan->smrlog, addr);
  if (lm->offset < 0)
    {
      goto error;
    }
  dlisth_insert_before (&lm->head, &scan->mmaps);
  scan->seq_end = lm->seq + lm->offset;
  return 1;			//has more

error:
  if (lm != NULL)
    {
      if (lm->addr != NULL)
	{
	  scan_unmap (scan, lm->seq, lm->addr);
	}
      free (lm);
    }
  return ret;
}

static int
scan_close (logScan * scan)
{
  assert (scan != NULL);

  while (!dlisth_is_empty (&scan->mmaps))
    {
      logMmap *lm;

      lm = (logMmap *) scan->mmaps.next;
      dlisth_delete (&lm->head);
      scan_unmap (scan, lm->seq, lm->addr);
      free (lm);
    }

  init_log_scan (scan, NULL);
  return 0;
}

static int
scan_release_unused_maps (logScan * scan)
{
  while (!dlisth_is_empty (&scan->mmaps))
    {
      logMmap *lm;

      lm = (logMmap *) scan->mmaps.next;
      if (lm->seq + SMR_LOG_FILE_DATA_SIZE <= scan->seq)
	{
	  dlisth_delete (&lm->head);
	  scan_unmap (scan, lm->seq, lm->addr);
	  free (lm);
	}
      else
	{
	  break;
	}
    }
  return 0;
}

static int
scan_read (logScan * scan, void *buf_, int size, long long seq)
{
  int once;
  char *buf = buf_;

  assert (scan != NULL);
  assert (buf_ != NULL);
  assert (size > 0);

  while (size > 0 && size >= (once = scan_consec_size (scan, seq)))
    {
      if (buf != NULL)
	{
	  char *bp = scan_seq_to_addr (scan, seq);

	  memcpy (buf, bp, once);
	  buf += once;
	}
      size -= once;
      seq += once;
    }

  if (size > 0)
    {
      if (buf != NULL)
	{
	  char *bp = scan_seq_to_addr (scan, seq);

	  memcpy (buf, bp, size);
	}
      seq = seq + size;
    }
  return 0;
}

static int
scan_read_cb (logScan * scan, scanArg * arg, int size, long long seq)
{
  int ret;
  smrlog_scanner cb;
  int need_merge;
  int org_size = size;
  char *smr_data = NULL;
  char *sdp = NULL;
  int once;

  assert (scan != NULL);
  assert (arg != NULL);
  cb = arg->scanner;
  assert (cb != NULL);
  assert (size > 0);

  need_merge = (scan_consec_size (scan, seq) < size);
  if (need_merge)
    {
      smr_data = malloc (size);
      if (smr_data == NULL)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      sdp = smr_data;
    }

  while (size > 0)
    {
      char *bp;

      once = scan_consec_size (scan, seq);
      if (once > size)
	{
	  once = size;
	}

      bp = scan_seq_to_addr (scan, seq);
      if (!need_merge)
	{
	  smr_data = bp;
	  break;
	}
      else
	{
	  memcpy (sdp, bp, once);
	  sdp += once;
	}
      size -= once;
      seq += once;
    }

  ret =
    cb (arg->arg, seq, arg->timestamp, arg->hash,
	(unsigned char *) smr_data, org_size);
  if (ret == -1)
    {
      ERRNO_POINT ();
    }
  else
    {
      arg->cont = ret;
    }

  if (need_merge && smr_data != NULL)
    {
      free (smr_data);
    }
  return ret;
}

#define mm_read(buf,_sz) do {           \
  int ret;                              \
  if(tsz < _sz) { goto done; }          \
  ret = scan_read(scan,buf,_sz,seq);    \
  if(ret == -1) {                       \
    ERRNO_POINT();                      \
    return -1;                          \
  }                                     \
  tsz = tsz - _sz;                      \
  seq = seq + _sz;                      \
} while(0)

#define mm_read_cb(_sz, arg) do {       \
  int ret;                              \
  if(tsz < _sz) { goto done; }          \
  ret = scan_read_cb(scan,arg,_sz,seq); \
  if(ret == -1) {                       \
    ERRNO_POINT();                      \
    return -1;                          \
  } else if (ret == 0) {                \
    goto done;                          \
  }                                     \
  tsz = tsz - _sz;                      \
  seq = seq + _sz;                      \
} while(0)

#define mm_commit() do {                \
  saved_tsz = tsz;                      \
  saved_seq = seq;                      \
} while(0)

#define mm_rollback() do {              \
  tsz = saved_tsz;                      \
  seq = saved_seq;                      \
} while(0)

static int
scan_process (logScan * scan, smrlog_scanner scanner, void *arg)
{
  long long seq, saved_seq;	// mm_ macro variable
  long long tsz, saved_tsz;	// mm_ macro variable
  char cmd;
  scanArg scan_arg;

  assert (scan != NULL);
  assert (scanner != NULL);

  init_scan_arg (&scan_arg);
  scan_arg.cont = 1;

  saved_tsz = tsz =
    ((scan->end_limit == -1LL) ? scan->seq_end : scan->end_limit) - scan->seq;
  saved_seq = seq = scan->seq;

  /* set invariants */
  scan_arg.scanner = scanner;
  scan_arg.arg = arg;

  while (tsz > 0)
    {
      short nid;
      int sid, length, hash;
      int seq_magic;
      unsigned short seq_crc16, net_crc16;
      long long net_seq;
      long long timestamp;

      mm_commit ();
      mm_read (&cmd, 1);

      switch (cmd)
	{
	  /* -------------------- */
	case SMR_OP_EXT:
	case SMR_OP_SESSION_CLOSE:
	  /* -------------------- */
	  mm_read (&sid, sizeof (int));
	  sid = ntohl (sid);
	  mm_commit ();
	  break;

	  /* ------------------- */
	case SMR_OP_SESSION_DATA:	//sid, hash, timestamp, length, data
	  /* ------------------- */
	  mm_read (&sid, sizeof (int));
	  sid = ntohl (sid);
	  mm_read (&hash, sizeof (int));
	  hash = ntohl (hash);
	  mm_read (&timestamp, sizeof (long long));
	  timestamp = ntohll (timestamp);
	  mm_read (&length, sizeof (int));
	  length = ntohl (length);
	  scan_arg.hash = hash;
	  scan_arg.timestamp = timestamp;
	  scan_arg.length = length;
	  mm_read_cb (length, &scan_arg);
	  mm_commit ();
	  break;
	  /* ------------------ */
	case SMR_OP_NODE_CHANGE:
	  /* ------------------ */
	  mm_read (&nid, sizeof (short));
	  mm_commit ();
	  break;
	  /* -------------------- */
	case SMR_OP_SEQ_COMMITTED:
	  /* -------------------- */
	  mm_read (&seq_magic, sizeof (int));
	  seq_magic = ntohl (seq_magic);
	  if (seq_magic != SEQ_COMMIT_MAGIC)
	    {
	      mm_rollback ();
	      ERRNO_POINT ();
	      return -1;
	    }
	  mm_read (&net_seq, sizeof (long long));
	  net_crc16 = crc16 ((char *) &net_seq, sizeof (long long), 0);
	  mm_read (&seq_crc16, sizeof (unsigned short));
	  seq_crc16 = ntohs (seq_crc16);
	  if (net_crc16 != seq_crc16)
	    {
	      mm_rollback ();
	      ERRNO_POINT ();
	      return -1;
	    }
	  mm_commit ();
	  break;
	default:
	  mm_rollback ();
	  ERRNO_POINT ();
	  return -1;
	}
    }

done:
  mm_rollback ();
  scan->seq = seq;
  scan_release_unused_maps (scan);
  return scan_arg.cont;
}

int
smrlog_scan (smrLog * smrlog, long long start, long long end,
	     smrlog_scanner scanner, void *arg)
{
  logScan scan;
  int ret;

  errno = 0;
  if (smrlog == NULL || start < 0 || (end != -1LL && (start >= end))
      || scanner == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  init_log_scan (&scan, smrlog);
  scan.end_limit = end;

  ret = scan_open (&scan, start);
  if (ret < 0)
    {
      return ret;
    }

  while ((ret = scan_next (&scan)) > 0)
    {
      ret = scan_process (&scan, scanner, arg);
      if (ret <= 0)
	{
	  break;
	}
    }

  scan_close (&scan);
  return ret;
}

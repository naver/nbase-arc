#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#include "crc16.h"
#include "dlist.h"
#include "smr.h"
#include "log_internal.h"

#define ERRNO_FILE_ID RECOVER_FILE_ID

/* log constraints */
#define CONSTRAINT_CONSEC 0x1
#define CONSTRAINT_NOGAP  0x2
#define CONSTRAINT_MEMNEW 0x4
#define CONSTRAINT_DISKOLD 0x8

typedef struct logSeqs_ logSeqs;
typedef struct recoverState_ recoverState;
typedef struct msgSeek_ msgSeek;


struct logSeqs_
{
  int seqs_size;
  long long *seqs;
  long long min_seq;
  long long max_seq;
};
#define init_log_seqs(s) do {  \
  (s)->seqs_size = 0;          \
  (s)->seqs = NULL;            \
  (s)->min_seq = -1LL;         \
  (s)->max_seq = -1LL;         \
} while(0)

struct recoverState_
{
  logSeqs disk_ls;
  logSeqs mem_ls;
  /* physical recovery results */
  long long min_seq;
  long long max_seq;
  /* logical recovery results */
  long long msg_min_seq;
  long long msg_max_seq;
  long long commit_max_seq;
};
#define init_recover_state(s) do {   \
  init_log_seqs(&(s)->disk_ls);      \
  init_log_seqs(&(s)->mem_ls);       \
  (s)->min_seq = 0LL;                \
  (s)->max_seq = 0LL;                \
  (s)->msg_min_seq = 0LL;            \
  (s)->msg_max_seq = 0LL;            \
  (s)->commit_max_seq = 0LL;         \
} while(0)

#define MAX_MSG_SEEK_ADDRS 4
struct msgSeek_
{
  smrLogAddr *addrs[MAX_MSG_SEEK_ADDRS];
  int idx;
  long long begin;
  long long limit;
};
#define init_msg_seek(s) do {              \
  int _i;                                  \
  for(_i=0;_i<MAX_MSG_SEEK_ADDRS;_i++) {   \
      (s)->addrs[_i] = NULL;               \
  }                                        \
  (s)->idx = MAX_MSG_SEEK_ADDRS - 1;       \
  (s)->begin = 0LL;                        \
  (s)->limit = 0LL;                        \
} while(0)


/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */
static void clear_msg_seek (smrLog * handle, msgSeek * seek);
static int msg_seek_read (msgSeek * seek, long long seq, int size, char *buf);
static int msg_seek_find_msg_end (msgSeek * seek, long long begin,
				  long long *last);
static void clear_recover_state (recoverState * rs);
static int check_commit_msg_body (char *addr, long long *commit_seq);
static int find_commit_seq_msg (void *addr, int from_off, int to_off, int fwd,
				int *found, int *roff, long long *commit_seq);
static int logical_recover (smrLog * handle, recoverState * rs);
static int recover_one_file (logDev * dev, long long fseq, long long *seq,
			     int *partial);
static int log_recover (logDev * dev, logSeqs * ls);
static int is_consec (logSeqs * ls);
static int check_and_get_minmax (logSeqs * m, logSeqs * d, int *need_patch,
				 long long *min_seq, long long *max_seq);
static int log_patch (long long seq, logDev * dest, logDev * src);
static int physical_recover_synthesize (smrLog * handle, recoverState * rs);
static int physical_recover (smrLog * handle, recoverState * rs);

/* --------------- */
/* LOCAL FUNCTIONS */
/* --------------- */

static void
clear_msg_seek (smrLog * handle, msgSeek * seek)
{
  int i;

  for (i = 0; i < MAX_MSG_SEEK_ADDRS; i++)
    {
      if (seek->addrs[i] != NULL)
	{
	  smrlog_munmap (handle, seek->addrs[i]);
	}
    }
  init_msg_seek (seek);
}

/* 
 * Returns number of bytes read.
 */
static int
msg_seek_read (msgSeek * seek, long long seq, int size, char *buf)
{
  int remain;
  long long curr_seq;
  char *bp;

  if (seq < seek->begin || seq + size > seek->limit)
    {
      return 0;
    }

  remain = size;
  curr_seq = seq;
  bp = buf;
  while (remain > 0)
    {
      int idx;
      smrLogAddr *addr;
      long long limit;
      int begin;
      int end;
      int nr;
      char *sp;

      idx = seek->idx + 1 + (curr_seq - seek->begin) / SMR_LOG_FILE_DATA_SIZE;
      assert (idx >= 0 && idx < MAX_MSG_SEEK_ADDRS);
      addr = seek->addrs[idx];

      limit = addr->seq + SMR_LOG_FILE_DATA_SIZE;
      if (limit > seek->limit)
	{
	  limit = seek->limit;
	}
      begin = curr_seq - addr->seq;
      end = limit - addr->seq;

      nr = end - begin > remain ? remain : end - begin;
      sp = (char *) addr->addr + begin;
      memcpy (bp, sp, nr);

      remain -= nr;
      curr_seq += nr;
      bp += nr;
    }

  return size;
}

#define CHECK_NOMORE() if (ret==0) break
static int
msg_seek_find_msg_end (msgSeek * seek, long long begin, long long *last)
{
  long long curr = begin;
  long long msg_end = begin;

  while (curr < seek->limit)
    {
      char cmd;
      int ret;
      int length, skip;

      ret = msg_seek_read (seek, curr, 1, &cmd);
      CHECK_NOMORE ();

      switch (cmd)
	{
	case SMR_OP_EXT:
	case SMR_OP_SESSION_CLOSE:
	  // command, sid
	  skip = 1 + sizeof (int);
	  msg_end = curr += skip;
	  break;
	case SMR_OP_SESSION_DATA:
	  // command, sid, hash, timestamp 
	  skip = 1 + sizeof (int) + sizeof (int) + sizeof (long long);
	  // read length
	  ret =
	    msg_seek_read (seek, curr + skip, sizeof (int), (char *) &length);
	  CHECK_NOMORE ();
	  length = ntohl (length);
	  // skip length and data
	  skip += sizeof (int) + length;
	  msg_end = curr += skip;
	  break;
	case SMR_OP_NODE_CHANGE:
	  // 
	  skip = 1 + sizeof (short);
	  msg_end = curr += skip;
	  break;
	case SMR_OP_SEQ_COMMITTED:
	  skip = SMR_OP_SEQ_COMMITTED_SZ;
	  msg_end = curr += skip;
	  break;
	default:
	  ERRNO_POINT ();
	  return -1;
	}
    }
  *last = msg_end;
  return 0;
}

static void
clear_recover_state (recoverState * rs)
{
  if (rs == NULL)
    {
      return;
    }

  if (rs->disk_ls.seqs != NULL)
    {
      free (rs->disk_ls.seqs);
    }
  if (rs->mem_ls.seqs != NULL)
    {
      free (rs->mem_ls.seqs);
    }
}

static int
check_commit_msg_body (char *addr, long long *commit_seq)
{
  char *tp = addr;
  int magic;
  long long cseq;
  unsigned short csum1, csum2;

  memcpy (&magic, tp, sizeof (int));
  magic = ntohl (magic);
  if (magic != SEQ_COMMIT_MAGIC)
    {
      return 0;
    }
  tp += sizeof (int);

  /* checksum over network byte order */
  csum1 = crc16 (tp, sizeof (long long), 0);
  tp += sizeof (long long);

  memcpy (&csum2, tp, sizeof (unsigned short));
  csum2 = ntohs (csum2);

  if (csum1 != csum2)
    {
      return 0;
    }

  /* okay */
  memcpy (&cseq, (char *) addr + sizeof (int), sizeof (long long));
  cseq = ntohll (cseq);
  *commit_seq = cseq;
  return 1;
}

static int
find_commit_seq_msg (void *addr, int from_off, int to_off, int fwd,
		     int *found, int *roff, long long *commit_seq)
{
  char *bp;
  char *ep;
  long long cseq = 0LL;

  assert (addr != NULL);
  assert (from_off <= to_off);
  assert (to_off <= SMR_LOG_FILE_DATA_SIZE);
  assert (roff != NULL);

  if (fwd)
    {
      bp = (char *) addr + from_off;
      ep = (char *) addr + to_off;

      while (bp < ep - SMR_OP_SEQ_COMMITTED_SZ)
	{
	  if (*bp == SMR_OP_SEQ_COMMITTED
	      && check_commit_msg_body (bp + 1, &cseq))
	    {
	      *found = 1;
	      *commit_seq = cseq;
	      *roff = (bp - (char *) addr);
	      return 0;
	    }
	  bp++;
	}
    }
  else
    {
      bp = (char *) addr + from_off;
      ep = (char *) addr + to_off - SMR_OP_SEQ_COMMITTED_SZ;

      while (ep >= bp)
	{
	  if (*ep == SMR_OP_SEQ_COMMITTED
	      && check_commit_msg_body (ep + 1, &cseq))
	    {
	      *found = 1;
	      *commit_seq = cseq;
	      *roff = (ep - (char *) addr);
	      return 0;
	    }
	  ep--;
	}
    }

  *found = 0;
  return 0;
}

static int
logical_recover (smrLog * handle, recoverState * rs)
{
  long long seq;
  msgSeek seek;
  long long min_seq = rs->min_seq;
  long long max_seq = rs->max_seq;
  long long msg_min_seq = 0LL;
  long long msg_max_seq = 0LL;
  long long commit_max_seq = 0LL;
  smrLogAddr *addr = NULL;
  int from, to;
  int found = 0, roff;
  int ret;
  long long cseq;

  // check trivial case
  if (min_seq == max_seq)
    {
      assert (min_seq == 0LL);
      return 0;
    }


  /* 
   * - find msg_min_seq, msg_max_seq 
   * - find commit_max_seq
   */
  for (seq = seq_round_down (min_seq);
       seq <= seq_round_down (max_seq); seq += SMR_LOG_FILE_DATA_SIZE)
    {
      from = 0;
      to = SMR_LOG_FILE_DATA_SIZE;
      addr = smrlog_read_mmap (handle, seq);
      if (addr == NULL)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      if (seq <= min_seq)
	{
	  from = min_seq - seq;
	}
      if (find_commit_seq_msg (addr->addr, from, to, 1, &found, &roff, &cseq)
	  == -1)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      smrlog_munmap (handle, addr);
      addr = NULL;
      if (found)
	{
	  msg_min_seq = seq + roff;
	  break;
	}
    }

  init_msg_seek (&seek);
  seek.limit = max_seq;
  /* -1 for the case where max_seq is end of the log file */
  for (seq = seq_round_down (max_seq - 1);
       seq >= seq_round_down (min_seq); seq -= SMR_LOG_FILE_DATA_SIZE)
    {
      if (seek.idx < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}

      addr = smrlog_read_mmap (handle, seq);
      if (addr == NULL)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      seek.addrs[seek.idx--] = addr;
      seek.begin = addr->seq;

      from = 0;
      if (seq + to > max_seq)
	{
	  to = max_seq - seq;
	}
      else
	{
	  to = SMR_LOG_FILE_DATA_SIZE;
	}

      ret =
	find_commit_seq_msg (addr->addr, from, to, 0, &found, &roff, &cseq);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}

      if (found)
	{
	  long long begin;

	  commit_max_seq = cseq;

	  begin = seq + roff + SMR_OP_SEQ_COMMITTED_SZ;
	  ret = msg_seek_find_msg_end (&seek, begin, &msg_max_seq);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      goto error;
	    }
	  break;
	}
    }
  clear_msg_seek (handle, &seek);

  /* 
   * check and adjust sequence numbers 
   * 0 <= min_seq <= msg_min_seq <= commit_max_seq <= msg_max_seq <= max_seq
   */
  if (min_seq > msg_min_seq || msg_min_seq > commit_max_seq
      || commit_max_seq > msg_max_seq || msg_max_seq > max_seq)
    {
      ERRNO_POINT ();
      goto error;
    }


  /* truncate to msg_max_seq */
  ret = smrlog_purge_after (handle, msg_max_seq);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  rs->msg_min_seq = msg_min_seq;
  rs->msg_max_seq = msg_max_seq;
  rs->commit_max_seq = commit_max_seq;
  return 0;

error:
  if (addr != NULL)
    {
      smrlog_munmap (handle, addr);
    }
  clear_msg_seek (handle, &seek);
  return -1;
}

static int
recover_one_file (logDev * dev, long long fseq, long long *seq, int *partial)
{
  smrLogAddr *addr;
  char *bp;
  char *ep;
  logChecksum *master;
  logChecksum *checksums;
  unsigned short csum;
  int idx;
  int modified;
  int finalized;
  int total_offset;

  assert (fseq >= 0);
  assert (seq_round_down (fseq) == fseq);
  assert (seq != NULL);
  assert (partial != NULL);

  addr = dev->get_mmap (dev, fseq, 0, 0);
  if (addr == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }

  /* adjust pointrs */
  bp = addr->addr;
  ep = bp + SMR_LOG_FILE_DATA_SIZE;
  master = (logChecksum *) ep;
  checksums = master + 1;

  /* skip if finalized */
  finalized = (master->off == SMR_LOG_NUM_CHECKSUM);
  if (finalized)
    {
      *partial = 0;
      *seq = fseq + SMR_LOG_FILE_DATA_SIZE;
      dev->munmap (dev, addr);
      return 0;
    }

  /* recover the log file from the start */
  idx = 0;
  modified = 0;
  total_offset = 0;

  while (idx < SMR_LOG_NUM_CHECKSUM - 1)
    {
      char *pagep = bp + idx * SMR_LOG_PAGE_SIZE;
      int offset = checksums[idx].off;

      csum = (offset == 0) ? 0 : crc16 (pagep, offset, 0);
      if (csum != checksums[idx].checksum)
	{
	  modified++;
	  checksums[idx].off = 0;
	  checksums[idx].checksum = 0;
	  break;
	}

      total_offset += offset;
      if (offset < SMR_LOG_PAGE_SIZE)
	{
	  break;
	}
      idx++;
    }

  /* set return value */
  *seq = fseq + total_offset;
  *partial = (total_offset != SMR_LOG_FILE_DATA_SIZE);

  /* update master record */
  if (master->off != idx)
    {
      modified++;
      master->off = idx;
    }

  csum = crc16 ((char *) checksums, sizeof (logChecksum) * idx, 0);
  if (csum != master->checksum)
    {
      modified++;
      master->checksum = csum;
    }

  /* reset remaining checksum fields */
  idx++;
  while (idx < SMR_LOG_NUM_CHECKSUM - 1)
    {
      if (checksums[idx].off != 0)
	{
	  modified++;
	  checksums[idx].off = 0;
	}
      if (checksums[idx].checksum != 0)
	{
	  modified++;
	  checksums[idx].checksum = 0;
	}
      idx++;
    }

  if (modified && addr->loc == IN_DISK)
    {
      msync (addr->addr, SMR_LOG_FILE_ACTUAL_SIZE, MS_ASYNC);
    }
  dev->munmap (dev, addr);
  return 0;
}

static int
log_recover (logDev * dev, logSeqs * ls)
{
  int ret;
  long long *fseqs = NULL;
  int fseqs_size = 0;
  int i;
  int has_hole = 0;
  int partial = 0;
  long long min_seq = 0LL;
  long long max_seq = 0LL;

  if (dev == NULL || ls == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  ret = dev->get_seqs (dev, &fseqs, &fseqs_size);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  if (fseqs_size == 0)
    {
      goto done;
    }
  /*
   * checksum based physical log file recovery. find min_seq, max_seq
   * Note: only the last log need to be recovered.
   */
  if (recover_one_file (dev, fseqs[fseqs_size - 1], &max_seq, &partial) < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  min_seq = fseqs[fseqs_size - 1];
  for (i = fseqs_size - 2; i >= 0; i--)
    {
      has_hole = (fseqs[i] + SMR_LOG_FILE_DATA_SIZE != fseqs[i + 1]);
      if (has_hole)
	{
	  break;
	}
      min_seq = fseqs[i];
    }

  if (has_hole)
    {
      int j;
      int remain;
      for (j = 0; j <= i; j++)
	{
	  // purge. it will be deleted at the right time (by replicator)
	  ret = dev->purge (dev, fseqs[j]);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      goto error;
	    }
	}
      remain = fseqs_size - (i + 1);
      memmove (fseqs, &fseqs[i + 1], sizeof (long long) * remain);
      fseqs_size = remain;
    }

done:
  assert (min_seq <= max_seq);
  ls->seqs_size = fseqs_size;
  ls->seqs = fseqs;
  ls->min_seq = min_seq;
  ls->max_seq = max_seq;
  return 0;

error:
  if (fseqs != NULL)
    {
      free (fseqs);
    }
  return -1;
}

static int
is_consec (logSeqs * ls)
{
  int i;
  long long prev;
  for (i = 0; i < ls->seqs_size; i++)
    {
      if (i == 0)
	{
	  prev = ls->seqs[i];
	  continue;
	}
      if (prev + SMR_LOG_FILE_DATA_SIZE != ls->seqs[i])
	{
	  return 0;
	}
      prev = ls->seqs[i];
    }
  return 1;
}


static int
check_and_get_minmax (logSeqs * m, logSeqs * d, int *need_patch,
		      long long *min_seq, long long *max_seq)
{
  int v = 0;

  /* handle special cases */
  if (m == NULL)
    {
      if (!is_consec (d))
	{
	  v |= CONSTRAINT_CONSEC;
	  return v;
	}
      else
	{
	  *min_seq = d->min_seq;
	  *max_seq = d->max_seq;
	  return 0;
	}
    }
  else if (m->seqs_size == 0 && is_consec (d) && d->seqs_size > 0)
    {
      // Shared memory is empty. OS restarts or mem devices is newly installed.
      *need_patch = 1;
      *min_seq = d->min_seq;
      *max_seq = d->max_seq;
      return 0;
    }

  // Note: empty shared memory and empty disk does not violates the log constraints.
  /* Consec */
  if (!is_consec (m) || !is_consec (d))
    {
      ERRNO_POINT ();
      v |= CONSTRAINT_CONSEC;
    }
  /* NoGap */
  if (d->max_seq < m->min_seq)
    {
      ERRNO_POINT ();
      v |= CONSTRAINT_NOGAP;
    }
  /* MemNew */
  if (d->max_seq > m->max_seq)
    {
      ERRNO_POINT ();
      v |= CONSTRAINT_MEMNEW;
    }
  /* DiskOld */
  if (d->min_seq > m->min_seq)
    {
      ERRNO_POINT ();
      v |= CONSTRAINT_DISKOLD;
    }
  if (!v)
    {
      *min_seq = d->min_seq;
      *max_seq = m->max_seq;
    }
  return v;
}

static int
log_patch (long long seq, logDev * dest, logDev * src)
{
  smrLogAddr *src_addr = NULL;
  smrLogAddr *dest_addr = NULL;

  src_addr = src->get_mmap (src, seq, 1, 0);
  if (src_addr == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }

  dest_addr = dest->get_mmap (dest, seq, 0, 1);
  if (dest_addr == NULL)
    {
      ERRNO_POINT ();
      goto error;
    }

  memcpy (dest_addr->addr, src_addr->addr, SMR_LOG_FILE_ACTUAL_SIZE);
  src->munmap (src, src_addr);
  dest->munmap (dest, dest_addr);
  return 0;

error:
  if (src_addr != NULL)
    {
      src->munmap (src, src_addr);
    }
  if (dest_addr != NULL)
    {
      dest->munmap (dest, dest_addr);
    }
  return -1;
}

static int
physical_recover_synthesize (smrLog * handle, recoverState * rs)
{
  int ret;
  logSeqs *disk_ls = NULL;
  logSeqs *mem_ls = NULL;
  int v;
  int need_patch = 0;

  disk_ls = &rs->disk_ls;
  if (handle->mem != NULL)
    {
      mem_ls = &rs->mem_ls;
    }

  v =
    check_and_get_minmax (mem_ls, disk_ls, &need_patch, &rs->min_seq,
			  &rs->max_seq);
  if (v != 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (need_patch)
    {
      // Note: patch does not change the min/max range
      ret =
	log_patch (disk_ls->seqs[disk_ls->seqs_size - 1], handle->mem,
		   handle->disk);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

  return 0;
}

static int
physical_recover (smrLog * handle, recoverState * rs)
{
  int ret;
  logSeqs *disk_ls = NULL;
  logSeqs *mem_ls = NULL;

  disk_ls = &rs->disk_ls;
  ret = log_recover (handle->disk, disk_ls);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (handle->mem != NULL)
    {
      mem_ls = &rs->mem_ls;
      ret = log_recover (handle->mem, mem_ls);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

  ret = physical_recover_synthesize (handle, rs);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  return 0;
}

int
smrlog_recover (smrLog * handle, long long *minseq, long long *maxseq,
		long long *msgmin, long long *msgmax, long long *maxcseq)
{
  recoverState rs;
  int ret;

  errno = 0;
  if (handle == NULL || minseq == NULL || maxseq == NULL || msgmin == NULL
      || msgmax == NULL || maxcseq == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  init_recover_state (&rs);

  // machine restart recovery
  if (handle->mem != NULL)
    {
      ret = mem_handle_machine_restart (handle->mem, handle->disk);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

  // physical recovery
  ret = physical_recover (handle, &rs);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  // logical recovery 
  ret = logical_recover (handle, &rs);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  *minseq = rs.min_seq;
  *maxseq = rs.max_seq;
  *msgmin = rs.msg_min_seq;
  *msgmax = rs.msg_max_seq;
  *maxcseq = rs.commit_max_seq;

  clear_recover_state (&rs);
  return 0;
}

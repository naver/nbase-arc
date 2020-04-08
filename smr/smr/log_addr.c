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
#include <sys/mman.h>

#include "crc16.h"
#include "smr.h"
#include "log_internal.h"

#define ERRNO_FILE_ID ADDR_FILE_ID

/////////////////////////////////
// Local Function Declarations //
/////////////////////////////////
static int sync_data (void *addr);
static int sync_checksum (void *addr);
static int sync_master_checksum (void *addr);
static int advise_willlog (void *addr, int page_size, int off, int size);

/////////////////////
// Local Functions //
/////////////////////
static int
sync_data (void *addr)
{
  return msync (addr, SMR_LOG_FILE_DATA_SIZE, MS_SYNC);
}

static int
sync_checksum (void *addr)
{
  return msync ((char *) addr + SMR_LOG_FILE_DATA_SIZE,
		SMR_LOG_FILE_CHECKSUM_SIZE, MS_SYNC);
}

static int
sync_master_checksum (void *addr)
{
  return msync ((char *) addr + SMR_LOG_FILE_DATA_SIZE, sizeof (logChecksum),
		MS_SYNC);
}

static int
advise_willlog (void *addr, int page_size, int off, int size)
{
  int ret;
  int start, end;

  assert (off >= 0 && size >= 0);
  assert (off + size <= SMR_LOG_FILE_DATA_SIZE);

  /* data */
  start = syspg_off (off);
  end = off + size;
  ret = madvise ((char *) addr + start, end - start, MADV_WILLNEED);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  /* master checksum (skip) */
  /* range checksum */
  start = syspg_off (cksum_off (off));
  end = cksum_off (off) + sizeof (logChecksum);
  ret = madvise ((char *) addr + start, end - start, MADV_WILLNEED);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

////////////////////////
// Exported Functions //
////////////////////////
void
smrlog_munmap (smrLog * handle, smrLogAddr * addr)
{
  if (addr != NULL)
    {
      if (addr->loc == IN_DISK)
	{
	  handle->disk->munmap (handle->disk, addr);
	}
      else
	{
	  assert (handle->mem != NULL);
	  handle->mem->munmap (handle->mem, addr);
	}
    }
}

/*
 * returns size written to this mapping
 *         -1 if error
 */
int
smrlog_append (smrLog * handle, smrLogAddr * addr, char *buf, int sz)
{
  char *bp, *cp, *ep;
  logChecksum *master;
  logChecksum *checksums, *cksum;
  int avail, remain, nwritten;

  errno = 0;
  if (handle == NULL || addr == NULL || buf == NULL || sz < 0)
    {
      errno = EINVAL;
      return -1;
    }

  /* setup pointers */
  bp = (char *) addr->addr;
  ep = bp + SMR_LOG_FILE_DATA_SIZE;
  master = (logChecksum *) ep;
  checksums = master + 1;

  /* check finalized */
  if (master->off == SMR_LOG_NUM_CHECKSUM)
    {
      return 0;
    }

  /* get current pointer */
  cp = bp + master->off * SMR_LOG_PAGE_SIZE + checksums[master->off].off;

  avail = ep - cp;
  if (avail == 0)
    {
      return 0;
    }

  nwritten = avail > sz ? sz : avail;
  if (cp == buf && nwritten <= avail)
    {
      ;				// zero copy can be occurred 
    }
  else
    {
      /* need to check memory overlaps? */
      memcpy (cp, buf, nwritten);
    }

  /* update checksums and master checksum */
  remain = nwritten;
  cksum = &checksums[master->off];
  while (remain > 0)
    {
      int av = SMR_LOG_PAGE_SIZE - cksum->off;
      int nw = 0;

      if (av > 0)
	{
	  nw = av > remain ? remain : av;
	  cksum->off += nw;
	  cksum->checksum = crc16 (cp, nw, cksum->checksum);
	  cp = cp + nw;
	  remain -= nw;
	}

      if (nw == av)
	{
	  master->off++;
	  master->checksum =
	    crc16 ((char *) cksum, sizeof (logChecksum), master->checksum);
	  cksum = cksum + 1;
	}
    }

  return nwritten;
}

int
addr_truncate (smrLogAddr * addr, int offset)
{
  char *bp;
  char *ep;
  logChecksum *master;
  logChecksum *checksums;
  int idx;
  int ret;
  int curr_offset;

  errno = 0;
  if (addr == NULL || offset < 0 || offset > SMR_LOG_FILE_DATA_SIZE)
    {
      errno = EINVAL;
      return -1;
    }

  curr_offset = addr_offset (addr);
  if (curr_offset < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  if (offset > curr_offset)
    {
      errno = ERRNO_OFFSET_EXCEED;
      return -1;
    }

  /* adjust pointrs (master and checksums) */
  bp = addr->addr;
  ep = bp + SMR_LOG_FILE_DATA_SIZE;
  master = (logChecksum *) ep;
  checksums = master + 1;

  /* update checksum field where offset resides  */
  idx = offset / SMR_LOG_PAGE_SIZE;
  if (checksums[idx].off != offset - idx * SMR_LOG_PAGE_SIZE)
    {
      int off = offset - idx * SMR_LOG_PAGE_SIZE;
      checksums[idx].checksum = crc16 (bp + idx * SMR_LOG_PAGE_SIZE, off, 0);
      checksums[idx].off = off;
    }

  /* update master checksum (not includes idx itself) */
  master->off = idx;
  if (idx > 0)
    {
      master->checksum =
	crc16 ((char *) checksums, (idx - 1) * sizeof (logChecksum), 0);
    }
  else
    {
      master->checksum = 0;
    }

  /* reset remaining checksum fields */
  idx++;
  while (idx < SMR_LOG_NUM_CHECKSUM - 1)
    {
      memset (&checksums[idx], 0, sizeof (logChecksum));
      idx++;
    }

  if (addr->loc == IN_DISK)
    {
      ret = sync_checksum (addr->addr);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

  return 0;
}

int
addr_is_finalized (smrLogAddr * addr)
{
  logChecksum *master;

  master = (logChecksum *) ((char *) addr->addr + SMR_LOG_FILE_DATA_SIZE);
  return master->off == SMR_LOG_NUM_CHECKSUM;
}

int
addr_finalize (smrLogAddr * addr)
{
  logChecksum *master;
  logChecksum *checksums;

  master = (logChecksum *) ((char *) addr->addr + SMR_LOG_FILE_DATA_SIZE);
  checksums = master + 1;

  if (master->off == SMR_LOG_NUM_CHECKSUM)
    {
      return 0;
    }
  if (master->off != (SMR_LOG_NUM_CHECKSUM - 1)
      || checksums[master->off - 1].off != SMR_LOG_PAGE_SIZE)
    {
      // not yet full
      ERRNO_POINT ();
      return -1;
    }

  if (addr->loc == IN_DISK)
    {
      if (sync_data (addr->addr) == -1 || sync_checksum (addr->addr) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      master->off++;
      return sync_master_checksum (addr->addr);
    }
  else
    {
      master->off++;
      return 0;
    }
}

int
addr_offset (smrLogAddr * log_addr)
{
  char *bp, *cp, *ep;
  logChecksum *master, *checksums;
  void *addr = log_addr->addr;

  /* setup pointers */
  bp = (char *) addr;
  ep = bp + SMR_LOG_FILE_DATA_SIZE;
  master = (logChecksum *) ep;
  checksums = master + 1;

  if (master->off == SMR_LOG_NUM_CHECKSUM)
    {
      return SMR_LOG_FILE_DATA_SIZE;
    }
  else if (master->off == SMR_LOG_NUM_CHECKSUM - 1)
    {
      if (checksums[master->off - 1].off != SMR_LOG_PAGE_SIZE)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      return SMR_LOG_FILE_DATA_SIZE;
    }
  else
    {
      int offset;

      cp = bp + master->off * SMR_LOG_PAGE_SIZE + checksums[master->off].off;
      offset = cp - bp;
      if (offset > SMR_LOG_FILE_DATA_SIZE)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      return offset;
    }
}

int
addr_npage_in_memory (smrLogAddr * addr, int page_size,
		      void (*reside_callback) (int, void *), void *arg)
{
  int ret;
  unsigned char buf[16640];	// 65M/4096 bytes
  unsigned char *vec = NULL;
  int need_bytes;
  int i, nreside = 0;

  if (addr == NULL)
    {
      return 0;
    }

  /* check data portion only (TODO) */
  need_bytes = (SMR_LOG_FILE_DATA_SIZE + page_size - 1) / page_size;
  if (need_bytes > sizeof (buf))
    {
      vec = malloc (need_bytes);
      if (vec == NULL)
	{
	  goto done;
	}
    }
  else
    {
      vec = &buf[0];
    }

  memset (vec, 0, need_bytes);	// make valgrind happy
  ret = mincore (addr->addr, SMR_LOG_FILE_DATA_SIZE, vec);
  if (ret < 0)
    {
      goto done;
    }

  for (i = 0; i < need_bytes; i++)
    {
      int r = vec[i] & 1;
      if (r && reside_callback != NULL)
	{
	  reside_callback (i, arg);
	}
      nreside += r;
    }

done:
  if (vec != NULL && vec != &buf[0])
    {
      free (vec);
    }
  return nreside;
}

int
dev_truncate (logDev * dev, long long base_seq, long long seq)
{
  smrLogAddr *addr = NULL;
  int offset;
  int ret;

  offset = seq - base_seq;

  if (dev == NULL || base_seq < 0 || seq < base_seq
      || offset >= SMR_LOG_FILE_DATA_SIZE)
    {
      errno = EINVAL;
      return -1;
    }

  addr = dev->get_mmap (dev, base_seq, 0, 0);
  if (addr == NULL)
    {
      errno = ENOENT;
      ERRNO_POINT ();
      return -1;
    }

  ret = addr_truncate (addr, offset);
  dev->munmap (dev, addr);

  if (ret < 0)
    {
      return -1;
    }
  return 0;
}

int
smrlog_sync (smrLog * handle, smrLogAddr * addr)
{
  errno = 0;
  if (handle == NULL || addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  if (addr->loc != IN_DISK)
    {
      return 0;
    }

  if (sync_data (addr->addr) == -1 || sync_checksum (addr->addr) == -1)
    {
      return -1;
    }
  return 0;
}

//
// Sync src_addr data to dest_addr.  Note that this function assumes
// (1) non-concurrent access to dest_addr
// (2) data upto src + off is valid
//
int
smrlog_sync_maps (smrLog * handle, smrLogAddr * src_addr, int src_off,
		  smrLogAddr * dest_addr)
{
  int dest_off;
  char *bp;
  int size;
  int nw;

  errno = 0;
  if (handle == NULL || src_addr == NULL || dest_addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  if (src_addr == dest_addr)
    {
      return 0;
    }

  dest_off = addr_offset (dest_addr);
  if (dest_off < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (src_off < dest_off)
    {
      ERRNO_POINT ();
      return -1;
    }
  else if (src_off == dest_off)
    {
      return 0;
    }

  bp = (char *) src_addr->addr + dest_off;
  size = src_off - dest_off;
  nw = smrlog_append (handle, dest_addr, bp, size);
  if (nw != size)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

int
smrlog_sync_partial (smrLog * handle, smrLogAddr * addr, int from, int to,
		     int sync)
{
  int start;
  int end;
  int ret;
  int mode;
  int page_size;

  errno = 0;
  if (handle == NULL || addr == NULL || from < 0 || to < 0 || from > to
      || to > SMR_LOG_FILE_DATA_SIZE)
    {
      errno = EINVAL;
      return -1;
    }

  if (addr->loc != IN_DISK)
    {
      return 0;
    }

  page_size = handle->page_size;
  if (sync)
    {
      mode = MS_SYNC;
    }
  else
    {
      mode = MS_ASYNC;
    }

  /* partial data sync */
  start = syspg_off (from);
  //end = syspg_off (to);
  end = to;
  ret = msync ((char *) addr->addr + start, end - start, mode);
  if (ret < 0)
    {
      return -1;
    }

  /* checksum sync (master) */
  start = syspg_off (SMR_LOG_FILE_DATA_SIZE);
  end = SMR_LOG_FILE_DATA_SIZE + sizeof (logChecksum);
  ret = msync ((char *) addr->addr + start, end - start, mode);
  if (ret < 0)
    {
      return -1;
    }

  /* checksum sync (range) */
  start = syspg_off (cksum_off (from));
  end = cksum_off (to) + sizeof (logChecksum);
  ret = msync ((char *) addr->addr + start, end - start, mode);
  if (ret < 0)
    {
      return -1;
    }

  return 0;
}

int
smrlog_finalize (smrLog * handle, smrLogAddr * addr)
{
  int ret;

  errno = 0;
  if (handle == NULL || addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  ret = addr_finalize (addr);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (addr->loc == IN_DISK)
    {
      ret = handle->disk->synced (handle->disk, addr->seq);
    }
  else
    {
      ret = handle->mem->synced (handle->mem, addr->seq);
    }
  return ret;
}

int
smrlog_willlog (smrLog * handle, smrLogAddr * addr, int off, int size)
{
  errno = 0;
  if (off < 0 || size < 0 || off + size > SMR_LOG_FILE_DATA_SIZE)
    {
      errno = EINVAL;
      return -1;
    }
  return advise_willlog (addr->addr, handle->page_size, off, size);
}

int
smrlog_get_offset (smrLog * handle, smrLogAddr * addr)
{
  errno = 0;
  if (handle == NULL || addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  return addr_offset (addr);
}

int
smrlog_get_remain (smrLog * handle, smrLogAddr * addr)
{
  int offset;

  errno = 0;
  if (handle == NULL || addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  offset = addr_offset (addr);
  if (offset < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  return SMR_LOG_FILE_DATA_SIZE - offset;
}

int
smrlog_get_buf (smrLog * handle, smrLogAddr * addr, char **buf, int *avail)
{
  int offset;

  errno = 0;
  if (handle == NULL || addr == NULL || buf == NULL || avail == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  offset = addr_offset (addr);
  if (offset < 0)
    {
      ERRNO_POINT ();
      return -1;
    }

  *buf = (char *) addr->addr + offset;
  *avail = SMR_LOG_FILE_DATA_SIZE - offset;
  return 0;
}


int
smrlog_is_finalized (smrLog * handle, smrLogAddr * addr)
{
  errno = 0;
  if (handle == NULL || addr == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  return addr_is_finalized (addr);
}


logChecksum *
smrlog_get_master_checksum (smrLog * handle, smrLogAddr * addr)
{
  errno = 0;
  if (handle == NULL || addr == NULL)
    {
      errno = EINVAL;
      return NULL;
    }

  return (logChecksum *) ((char *) addr->addr + SMR_LOG_FILE_DATA_SIZE);
}

logChecksum *
smrlog_get_checksums (smrLog * handle, smrLogAddr * addr, int *count)
{
  errno = 0;
  if (handle == NULL || addr == NULL || count == NULL)
    {
      errno = EINVAL;
      return NULL;
    }

  *count = 1 << (SMR_LOG_FILE_DATA_BITS - SMR_LOG_PAGE_BITS);
  return (logChecksum *) ((char *) addr->addr + SMR_LOG_FILE_DATA_SIZE) + 1;
}

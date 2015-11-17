#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include "smr.h"
#include "smr_log.h"
#include "log_internal.h"

/*
 * This inteface assumes that log files are fixed size (defiend in smr.h)
 *
 * Note on: log file layout and checksum
 *
 * log file
 * +------------------+
 * |                  | ----+
 * | data             |     |
 * | ....             |     | SMR_LOG_FILE_DATA_SIZE
 * |                  | ----+
 * +------------------+
 * | logCheckSum      | master checksum  --+
 * +------------------+                    | SMR_LOG_FILE_CHECKSUM_SIZE
 * | ....             | data checksum -----+
 * +------------------+
 *
 * master checksum:
 *   - off: offset of data checksum record
 *   - checksum: crc16 checksum of data-checksum records upto 'off' (exclusive)
 *
 * data checksum:
 * represend a page whose size  is 1<<SMR_LOG_PAGE_BITS
 *   - off: offset of within a page (0 start)
 *   - checksum: crc16 checksum of data page upto 'off' (exclusive)
 *
 *
 * [Disk commit protocol]
 * 1) msync on data
 * 2) msync on checksum
 * Keep this order so that valid checksum means valid data.
 * (with high probability)
 *
 * Above protocol is simple but has a fitfall. 
 * 1) OS may sync partial data at arbitary time (without request)
 *    - OS may flush checksum pages only
 * 2) And OS crashed!!
 *
 * So, no-finalized log file is recovered from the start
 *
 * [Finalization]
 * No longer logged files must be finalized.
 * Finalized log is safe.
 *
 * log finalization is as follows
 * 1) msync data
 * 2) msync checksum
 * 2) master->off++
 * 3) msync page that contains the master checksum
 *
 * [Local log recovery]
 *
 * It seeks at most two log files at the end. (see smrlog_recover function)
 * It is the responsibility of the logger (smr case: replicator) to assure
 * the contents of other logs are sync'ed to disk.
 *
 */

/*
 * Note on shared memory log buffering
 *
 * seq         1      2      3      4      5      6
 *      +------+------+------+------+------+------+
 * Mem  |      |      |      |******|****  |      |
 *      +------+------+------+------+------+------+
 * Disk |******|******|******|***   |      |      |
 *      +------+------+------+------+------+------+
 *                      (a)   (b)     (c)
 *
 * [Invariants]
 * Following constraints must be always met after recovery.
 *   Consec  : Mem and Disk log files are consecutive
 *   NoGap   : Max(Disk) >= Min(Mem)
 *   MemNew  : Max(Disk) <= Max(Mem)
 *   DiskOld : Min(Disk) <= Min(Mem)
 *
 */
#define ERRNO_FILE_ID LOG_FILE_ID

/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */

/* --------------- */
/* LOCAL FUNCTIONS */
/* --------------- */

/* ------------------ */
/* Exported Functions */
/* ------------------ */
int
smrlog_open_master (char *basedir)
{
  return create_mem_dev (basedir);
}

int
smrlog_unlink_master (char *basedir)
{
  return unlink_mem_dev (basedir);
}

int
smrlog_sync_master (char *basedir)
{
  return sync_mem_dev (basedir);
}

int
smrlog_info_master (char *basedir, char **buf)
{
  return info_mem_dev (basedir, buf);
}


smrLog *
smrlog_init (char *basedir)
{
  smrLog *smrlog = NULL;

  errno = 0;
  if (basedir == NULL)
    {
      errno = EINVAL;
      return NULL;
    }

  smrlog = malloc (sizeof (smrLog));
  if (smrlog == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  init_smrlog (smrlog);

  smrlog->page_size = sysconf (_SC_PAGESIZE);
  if (smrlog->page_size < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  smrlog->mem = open_mem_dev (basedir);
  if (smrlog->mem == NULL)
    {
      if (errno != ERRNO_NO_MASTER_FILE)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }

  smrlog->disk = open_disk_dev (basedir);
  if (smrlog->disk == NULL)
    {
      ERRNO_POINT ();
      goto error;
    }

  return smrlog;
error:
  if (smrlog != NULL)
    {
      smrlog_destroy (smrlog);
    }
  return NULL;
}

void
smrlog_destroy (smrLog * handle)
{
  if (handle != NULL)
    {
      if (handle->mem)
	{
	  handle->mem->close (handle->mem);
	}

      if (handle->disk)
	{
	  handle->disk->close (handle->disk);
	}

      free (handle);
    }
}

int
smrlog_purge_after (smrLog * handle, long long seq)
{
  int ret;
  long long target_seq;
  long long *dseqs = NULL;
  int dseqs_size = 0;
  long long *mseqs = NULL;
  int mseqs_size = 0;
  logDev *mem;
  logDev *disk;
  int i, j;
  int last_mem_idx, last_disk_idx;

  errno = 0;

  if (handle == NULL || seq < 0)
    {
      errno = EINVAL;
      return -1;
    }

  target_seq = seq_round_up (seq);
  mem = handle->mem;
  disk = handle->disk;

  if (mem != NULL)
    {
      ret = mem->get_seqs (mem, &mseqs, &mseqs_size);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }

  ret = disk->get_seqs (disk, &dseqs, &dseqs_size);
  if (ret < 0)
    {
      ERRNO_POINT ();
      goto error;
    }

  for (i = mseqs_size - 1, j = dseqs_size - 1; i >= 0; i--)
    {
      if (mseqs[i] < target_seq)
	{
	  break;
	}

      /* 
       * zig-zag purging to meet the constraints 
       * we purge log in disk, mem order
       */
      if (j >= 0 && dseqs[j] == mseqs[i])
	{
	  ret = disk->purge (disk, dseqs[j]);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      goto error;
	    }
	  j--;
	}

      ret = mem->purge (mem, mseqs[i]);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }
  last_mem_idx = i;

  for (i = j; i >= 0; i--)
    {
      if (dseqs[i] < target_seq)
	{
	  break;
	}
      ret = disk->purge (disk, dseqs[i]);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }
  last_disk_idx = i;

  // truncate 
  if (seq < target_seq)
    {
      long long base_seq = seq_round_down (seq);

      if (last_disk_idx >= 0 && dseqs[last_disk_idx] == base_seq)
	{
	  ret = dev_truncate (disk, base_seq, seq);
	  if (ret < 0)
	    {
	      if (errno != ERRNO_OFFSET_EXCEED)
		{
		  ERRNO_POINT ();
		  goto error;
		}
	    }
	}
      if (last_mem_idx >= 0 && mseqs[last_mem_idx] == base_seq)
	{
	  assert (mem != NULL);
	  ret = dev_truncate (mem, base_seq, seq);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      goto error;
	    }
	}
    }

  // sync needed if log resides only in disk
  if (last_disk_idx >= 0 && (mem != NULL && last_mem_idx < 0))
    {
      smrLogAddr *src_addr = NULL;
      smrLogAddr *dest_addr = NULL;

      src_addr = disk->get_mmap (disk, dseqs[last_disk_idx], 1, 0);
      if (src_addr == NULL)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      dest_addr = mem->get_mmap (mem, dseqs[last_disk_idx], 0, 1);
      if (dest_addr == NULL)
	{
	  disk->munmap (disk, src_addr);
	  ERRNO_POINT ();
	  goto error;
	}

      ret = smrlog_sync_maps (handle, src_addr, dest_addr);
      disk->munmap (disk, src_addr);
      mem->munmap (mem, dest_addr);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  goto error;
	}
    }

  if (mseqs != NULL)
    {
      free (mseqs);
    }
  if (dseqs != NULL)
    {
      free (dseqs);
    }
  return 0;

error:
  if (mseqs != NULL)
    {
      free (mseqs);
    }
  if (dseqs != NULL)
    {
      free (dseqs);
    }
  return -1;
}

int
smrlog_os_decache (smrLog * handle, long long seq)
{
  logDev *mem;
  logDev *disk;
  int ret;

  errno = 0;
  if (handle == NULL || seq < 0)
    {
      errno = EINVAL;
      return -1;
    }
  mem = handle->mem;
  disk = handle->disk;

  if (mem != NULL)
    {
      ret = mem->decache (mem, seq);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }
  return disk->decache (disk, seq);
}

int
smrlog_remove_one (smrLog * handle, long long upper, int gap_in_sec,
		   int *removed, long long *removed_seq, int *has_more)
{
  logDev *mem;
  logDev *disk;
  int ret;

  errno = 0;
  if (handle == NULL || upper < 0 || gap_in_sec < 0 || removed == NULL
      || removed_seq == NULL || has_more == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  mem = handle->mem;
  disk = handle->disk;

  if (mem != NULL)
    {
      long long *seqs = NULL;
      int seqs_size = 0;

      /* if upper resides in mem, adjust upper bound (includsive) */
      ret = mem->get_seqs (mem, &seqs, &seqs_size);
      if (ret < 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      if (seqs_size > 0)
	{
	  long long mem_min;

	  mem_min = seqs[0];
	  if (upper >= mem_min)
	    {
	      upper = mem_min - SMR_LOG_FILE_DATA_SIZE;
	    }
	}
      if (seqs != NULL)
	{
	  free (seqs);
	}
    }

  if (upper < 0)
    {
      return 0;
    }
  return disk->remove_one (disk, upper, gap_in_sec, removed, removed_seq,
			   has_more);
}

int
smrlog_purge_all (smrLog * handle)
{
  return smrlog_purge_after (handle, 0LL);
}

smrLogAddr *
smrlog_read_mmap (smrLog * handle, long long seq)
{
  logDev *mem = NULL;
  logDev *disk = NULL;
  smrLogAddr *addr;

  errno = 0;
  if (handle == NULL || seq < 0)
    {
      errno = EINVAL;
      return NULL;
    }

  mem = handle->mem;
  disk = handle->disk;

  if (mem != NULL)
    {
      addr = mem->get_mmap (mem, seq, 1, 0);
      if (addr != NULL)
	{
	  return addr;
	}
    }
  addr = disk->get_mmap (disk, seq, 1, 0);
  return addr;
}

smrLogAddr *
smrlog_write_mmap (smrLog * handle, long long seq, int create)
{
  logDev *mem = NULL;
  logDev *disk = NULL;
  smrLogAddr *addr;

  errno = 0;
  if (handle == NULL || seq < 0)
    {
      errno = EINVAL;
      return NULL;
    }

  mem = handle->mem;
  disk = handle->disk;

  if (mem != NULL)
    {
      addr = mem->get_mmap (mem, seq, 0, create);
      if (addr != NULL)
	{
	  return addr;
	}

      if (create || errno != ERRNO_NO_ENTRY)
	{
	  return NULL;
	}
    }
  addr = disk->get_mmap (disk, seq, 0, create);
  return addr;
}

smrLogAddr *
smrlog_get_disk_mmap (smrLog * handle, long long seq)
{
  logDev *disk = NULL;

  errno = 0;
  if (handle == NULL || seq < 0)
    {
      errno = EINVAL;
      return NULL;
    }

  disk = handle->disk;
  return disk->get_mmap (disk, seq, 0, 1);
}


int
smrlog_sync_upto (smrLog * handle, long long upto)
{
  long long from;
  long long upto_base;
  long long seq;
  logDev *mem;
  logDev *disk;

  errno = 0;
  if (handle == NULL || upto < 0LL)
    {
      errno = EINVAL;
      return -1;
    }

  mem = handle->mem;
  disk = handle->disk;

  //
  // possible cases. ( u: upto, f: finalized, -----: log file )
  //
  // AS IS                        TO BE
  // (1)
  //                       u                    u 
  // mem     ----- ----- -----    ----f ----f -----  
  // disk    ---                  ----f ----f --
  // (2)
  //                       u                    u
  // disk    ----- ----- -----    ----f ----f -----
  // (3)
  //                 u                    u       
  // mem     ----- ----- -----    ----f ----- -----  
  // disk    ---                  ----f --
  //
  upto_base = seq_round_down (upto);
  from = upto_base - 2 * SMR_LOG_FILE_DATA_SIZE;
  for (seq = from; seq >= 0 && seq < upto; seq += SMR_LOG_FILE_DATA_SIZE)
    {
      int ret;
      smrLogAddr *src = NULL;
      smrLogAddr *dest = NULL;
      int create_dest = 0;
      int finalize = (seq < upto_base);

      if (mem != NULL)
	{
	  src = mem->get_mmap (mem, seq, 0, 0);
	  if (src == NULL)
	    {
	      if (errno == ERRNO_NO_ENTRY)
		{
		  continue;	// no disk log (by MemNew invariant)
		}
	      else
		{
		  goto err_ret;
		}
	    }
	  create_dest = 1;

	  if (finalize)
	    {
	      ret = smrlog_finalize (handle, src);
	      if (ret < 0)
		{
		  ERRNO_POINT ();
		  goto err_ret;
		}
	    }
	}

      dest = disk->get_mmap (disk, seq, 0, create_dest);
      if (dest == NULL)
	{
	  if (mem == NULL && errno == ERRNO_NO_ENTRY)
	    {
	      continue;		// no disk log
	    }
	  else
	    {
	      ERRNO_POINT ();
	      goto err_ret;
	    }
	}

      if (src != NULL)
	{
	  ret = smrlog_sync_maps (handle, src, dest);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      goto err_ret;
	    }
	  (void) mem->munmap (mem, src);
	  src = NULL;
	}

      if (finalize)
	{
	  ret = smrlog_finalize (handle, dest);
	  if (ret < 0)
	    {
	      ERRNO_POINT ();
	      goto err_ret;
	    }
	}
      (void) disk->munmap (disk, dest);
      dest = NULL;

      continue;
    err_ret:
      if (src != NULL)
	{
	  mem->munmap (mem, src);
	}
      if (dest != NULL)
	{
	  disk->munmap (disk, dest);
	}
      return -1;
    }
  return 0;
}

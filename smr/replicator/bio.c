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

#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>

#include "common.h"
#include "log_mmap.h"
#include "sfi.h"

/* --------------- */
/* Type Definition */
/* --------------- */
#define ERR_LINE (-1 * __LINE__)

#define LOG(l, ...) do {                        \
  if((l) <= s_Server.log_level)                 \
    log_msg(&s_Server, l,__VA_ARGS__);          \
} while (0)

typedef struct bioState_ bioState;
struct bioState_
{
  /* common */
  smrReplicator *rep;
  long long curr_ms;
  /* sync */
  long long sync_ms;
  long long sync_seq;
  logMmapEntry *src;
  logMmapEntry *sync;
  /* decache */
  long long decache_base_seq;
  /* del */
  long long del_ms;
  long long del_interval_ms;
};
#define init_bio_state(b) do {    \
  (b)->rep = NULL;                \
  (b)->curr_ms = 0LL;             \
  (b)->sync_ms = 0LL;             \
  (b)->sync_seq = 0LL;            \
  (b)->sync = NULL;               \
  (b)->src = NULL;                \
  (b)->decache_base_seq = 0LL;    \
  (b)->del_ms = 0LL;              \
  (b)->del_interval_ms = 1000;    \
} while(0)

typedef struct bioCallback_ bioCallback;
struct bioCallback_
{
  char *name;
  char sub1;
  char sub2;
  int (*pred) (bioState * bs);
  int (*proc) (bioState * bs);
  int (*term) (bioState * bs);
  // below fields are used by scheduler
  long long last_call_ms;
};

/* -------------------------- */
/* Local Function Declaration */
/* -------------------------- */
static int flush_mmap (smrReplicator * rep, long long seq,
		       logMmapEntry ** src, logMmapEntry ** sync);

static int sync_pred (bioState * bs);
static int sync_proc (bioState * bs);
static int sync_term (bioState * bs);

static int decache_pred (bioState * bs);
static int decache_proc (bioState * bs);
static int decache_term (bioState * bs);

static int del_pred (bioState * bs);
static int del_proc (bioState * bs);
static int del_term (bioState * bs);

static int unmap_pred (bioState * bs);
static int unmap_proc (bioState * bs);
static int unmap_term (bioState * bs);

static void initialize_bs (bioState * bs, smrReplicator * rep);
static void finalize_bs (bioState * bs);
static bioCallback *bio_schedule (bioState * bs);
static void bio_block_signal (void);
static void bio_idle (bioState * bs);
static void bio_loop (bioState * bs);


/* --------------- */
/* Local Variables */
/* --------------- */
static bioCallback callbackTable[] = {
  {"sync", 'B', 's', sync_pred, sync_proc, sync_term, 0LL},
  {"deca", 'B', 'd', decache_pred, decache_proc, decache_term, 0LL},
  {"del", 'B', 'D', del_pred, del_proc, del_term, 0LL},
  {"umap", 'B', 'u', unmap_pred, unmap_proc, unmap_term, 0LL},
};

/* -------------- */
/* Local Function */
/* -------------- */
static int
flush_mmap (smrReplicator * rep, long long seq, logMmapEntry ** src,
	    logMmapEntry ** sync)
{
  logMmapEntry *s = NULL, *d = NULL;
  smrLogAddr *addr = NULL;

  s = logmmap_entry_get (rep, seq, GET_EXIST, 1);
  if (s == NULL)
    {
      goto error;
    }

  if (s->addr->loc != IN_DISK)
    {
      /* make sync map directly */
      d = malloc (sizeof (logMmapEntry));
      if (d == NULL)
	{
	  goto error;
	}
      addr = smrlog_get_disk_mmap (rep->smrlog, seq);
      if (addr == NULL)
	{
	  free (d);
	  d = NULL;
	  goto error;
	}
      LOG (LG_INFO, "LOG new mmap for sync seq:%lld loc:%d", seq, addr->loc);
      init_log_mmap_entry (d);
      d->ref_count = 1;
      d->start_seq = seq_round_down (seq);
      d->addr = addr;
      addr = NULL;
    }
  else
    {
      d = logmmap_entry_addref (rep, s);
      if (d == NULL)
	{
	  goto error;
	}
    }

  *src = s;
  *sync = d;
  return 0;

error:
  if (s != NULL)
    {
      logmmap_entry_release (rep, s);
    }
  if (d != NULL)
    {
      logmmap_entry_release (rep, d);
    }
  if (addr != NULL)
    {
      smrlog_munmap (rep->smrlog, addr);
    }
  return -1;
}

static int
sync_pred (bioState * bs)
{
  long long log_seq;
  long long log_interval;
  long long time_interval;

  log_seq = aseq_get (&bs->rep->log_seq);
  log_interval = log_seq - bs->sync_seq;
  time_interval = bs->curr_ms - bs->sync_ms;

  return (time_interval > 1 * 1000 || log_interval > 10 * 1024 * 1024);
}

static int
sync_proc (bioState * bs)
{
  smrReplicator *rep = bs->rep;
  long long log_seq;
  long long log_base_seq;
  long long sync_base_seq;
  int from;
  int to;
  int sync = 1;
  int ret;

  log_seq = aseq_get (&bs->rep->log_seq);
  log_base_seq = seq_round_down (log_seq);
  sync_base_seq = seq_round_down (bs->sync_seq);

  while (log_base_seq > sync_base_seq)
    {
      ret = smrlog_sync_maps (rep->smrlog, bs->src->addr, bs->sync->addr);
      if (ret < 0)
	{
	  return ERR_LINE;
	}

      ret = smrlog_finalize (rep->smrlog, bs->src->addr);
      if (ret < 0)
	{
	  return ERR_LINE;
	}
      LOG (LG_INFO, "LOG finalize. seq:%lld loc:%d", bs->src->addr->seq,
	   bs->src->addr->loc);

      if (bs->src->addr != bs->sync->addr)
	{
	  ret = smrlog_finalize (rep->smrlog, bs->sync->addr);
	  if (ret < 0)
	    {
	      return ERR_LINE;
	    }
	  LOG (LG_INFO, "LOG finalize. seq:%lld loc:%d", bs->sync->addr->seq,
	       bs->sync->addr->loc);
	}
      logmmap_entry_release_raw (rep, bs->src);
      bs->src = NULL;
      logmmap_entry_release_raw (rep, bs->sync);
      bs->sync = NULL;

      sync_base_seq = seq_round_down (sync_base_seq + SMR_LOG_FILE_DATA_SIZE);
      ret = flush_mmap (rep, sync_base_seq, &bs->src, &bs->sync);
      if (ret < 0)
	{
	  return ERR_LINE;
	}

      if (sync_base_seq > bs->sync_seq)
	{
	  bs->sync_seq = sync_base_seq;
	}
    }

  ret = smrlog_sync_maps (rep->smrlog, bs->src->addr, bs->sync->addr);
  if (ret < 0)
    {
      return ERR_LINE;
    }
  from = (int) (bs->sync_seq - log_base_seq);
  to = (int) (log_seq - log_base_seq);
  assert (from <= to);
  assert (to <= SMR_LOG_FILE_DATA_SIZE);
  if ((to > from)
      && smrlog_sync_partial (rep->smrlog, bs->sync->addr, from, to,
			      sync) == -1)
    {
      return ERR_LINE;
    }
  bs->sync_ms = bs->curr_ms;
  bs->sync_seq = log_seq;
  aseq_set (&rep->sync_seq, log_seq);
  return 0;
}

static int
sync_term (bioState * bs)
{
  smrReplicator *rep = bs->rep;

  if (bs->sync != NULL)
    {
      logmmap_entry_release (rep, bs->sync);
      bs->sync = NULL;
    }

  if (bs->src != NULL)
    {
      logmmap_entry_release (rep, bs->src);
      bs->src = NULL;
    }
  return 0;
}

static int
decache_pred (bioState * bs)
{
  long long est_min;
  long long cand;

  est_min = aseq_get (&bs->rep->cron_est_min_seq);
  /* TODO justify below under estimation */
  cand = est_min - 2 * SMR_LOG_FILE_DATA_SIZE;
  return cand >= 0 && cand >= bs->decache_base_seq;
}

static int
decache_proc (bioState * bs)
{
  smrReplicator *rep = bs->rep;
  int ret;

  LOG (LG_INFO, "LOG decache. seq:%lld", bs->decache_base_seq);
  ret = smrlog_os_decache (rep->smrlog, bs->decache_base_seq);
  if (ret < 0)
    {
      return ERR_LINE;
    }
  bs->decache_base_seq += SMR_LOG_FILE_DATA_SIZE;
  return 0;
}

static int
decache_term (bioState * bs)
{
  return 0;
}

static int
del_pred (bioState * bs)
{
  /* prefer decache */
  return !decache_pred (bs) &&
    (bs->curr_ms - bs->del_ms >= bs->del_interval_ms);
}

static int
del_proc (bioState * bs)
{
  smrReplicator *rep = bs->rep;
  long long target_seq;
  long long retain_lb;
  int removed = 0;
  int has_more = 0;
  long long removed_seq = 0LL;

  /* 
   * we remove log files (including tombstone file) that meets all of following
   * conditions.
   *
   * - target_seq < rep->be_ckpt_seq
   * - target_seq < decache base seq (fadvised)
   * - log file is older than rep->log_delete_gap (1 day by default)
   *   or (inclusive) file seqence is less than rep->log_delete_seq
   */
  retain_lb = aseq_get (&rep->log_delete_seq);
  bs->del_ms = bs->curr_ms;
  target_seq = seq_round_down (rep->be_ckpt_seq) - SMR_LOG_FILE_DATA_SIZE;

  if (bs->decache_base_seq <= target_seq)
    {
      target_seq = bs->decache_base_seq - SMR_LOG_FILE_DATA_SIZE;
    }

  if (aseq_get (&rep->cron_est_min_seq) <= target_seq)
    {
      target_seq = aseq_get (&rep->cron_est_min_seq) - SMR_LOG_FILE_DATA_SIZE;
    }

  if (target_seq < 0)
    {
      bs->del_interval_ms = 10000;
      return 0;
    }

  if (smrlog_remove_one
      (rep->smrlog, target_seq, rep->log_delete_gap, retain_lb,
       &removed, &removed_seq, &has_more) == -1)
    {
      LOG (LG_ERROR,
	   "Failed smrlog_remove_one: target_seq:%lld, gap:%lld, errno=%d",
	   target_seq, rep->log_delete_gap, errno);
      return ERR_LINE;
    }

  if (removed)
    {
      LOG (LG_INFO, "LOG files for %lld are deleted", removed_seq);
      /*
       * Note:
       * - min_seq is used when a new slave connect to the master
       * - It is possible with low probability
       *   1) new slave connect to the master and get min_seq during handshake
       *   2) master deletes the log and advanced tie min_seq
       *   3) slave requires the deleted log and failed to connect to the master
       *      --> CC will retry
       */
      if (removed_seq + SMR_LOG_FILE_DATA_SIZE > aseq_get (&rep->min_seq))
	{
	  aseq_set (&rep->min_seq, removed_seq + SMR_LOG_FILE_DATA_SIZE);
	}
    }

  if (has_more)
    {
      /* faster the deletion */
      if (bs->del_interval_ms > 1000)
	{
	  bs->del_interval_ms = 1000;
	}
      else
	{
	  bs->del_interval_ms = bs->del_interval_ms - 100;
	  if (bs->del_interval_ms < 100)
	    {
	      /* 100 msec is the lower bound */
	      bs->del_interval_ms = 100;
	    }
	}
    }
  else
    {
      bs->del_interval_ms = 10000;
    }

  return 0;
}

static int
del_term (bioState * bs)
{
  return 0;
}

static int
unmap_pred (bioState * bs)
{
  smrReplicator *rep = bs->rep;
  int pred = 0;

  pthread_mutex_lock (&rep->unmaph_mutex);
  pred = (dlisth_is_empty (&rep->unmaph) == 0);
  pthread_mutex_unlock (&rep->unmaph_mutex);
  return pred;
}


static int
unmap_proc (bioState * bs)
{
  smrReplicator *rep = bs->rep;
  dlisth *h;
  logMmapEntry *entry = NULL;
  int has_more;

again:
  has_more = 0;
  pthread_mutex_lock (&rep->unmaph_mutex);
  if (!dlisth_is_empty (&rep->unmaph))
    {
      h = rep->unmaph.next;
      dlisth_delete (h);
      entry = (logMmapEntry *) h;
    }
  has_more = !dlisth_is_empty (&rep->unmaph);
  pthread_mutex_unlock (&rep->unmaph_mutex);

  if (entry != NULL)
    {
      assert (entry->ref_count == 0);
      LOG (LG_INFO, "LOG unmap. seq:%lld loc:%d", entry->start_seq,
	   entry->addr->loc);
      smrlog_munmap (rep->smrlog, entry->addr);
      free (entry);
    }

  if (has_more)
    {
      goto again;
    }
  return 0;
}

static int
unmap_term (bioState * bs)
{
  smrReplicator *rep = bs->rep;
  dlisth *h;
  logMmapEntry *entry = NULL;

  pthread_mutex_lock (&rep->unmaph_mutex);
  while (!dlisth_is_empty (&rep->unmaph))
    {
      h = rep->unmaph.next;
      dlisth_delete (h);
      entry = (logMmapEntry *) h;
      smrlog_munmap (rep->smrlog, entry->addr);
      free (entry);
    }
  pthread_mutex_unlock (&rep->unmaph_mutex);
  return 0;
}

static void
initialize_bs (bioState * bs, smrReplicator * rep)
{
  long long sync_base_seq;
  int ret;

  init_bio_state (bs);
  bs->rep = rep;

  applog_enter_session (bs->rep, BIO_CONN, 0);
  bs->sync_seq = aseq_get (&rep->sync_seq);
  bs->decache_base_seq = seq_round_down (bs->sync_seq);
  sync_base_seq = seq_round_down (bs->sync_seq);

  // CASE
  // 1. replicator write log spanning log file 1, 2 
  // 2. replicator role changed to LCONN
  // 3. bio stopped before finalizing log 1
  // 4. replicator get the role with sync sequence somewhre in the log file 2
  ret = smrlog_sync_upto (rep->smrlog, sync_base_seq);
  if (ret < 0)
    {
      smr_panic ("log file management error (bio)", __FILE__, __LINE__);
    }

  ret = flush_mmap (rep, sync_base_seq, &bs->src, &bs->sync);
  if (ret < 0)
    {
      smr_panic ("log file management error (bio)", __FILE__, __LINE__);
    }
  bs->del_ms = bs->sync_ms = currtime_ms ();
}

static void
finalize_bs (bioState * bs)
{
  int i;

  for (i = 0; i < sizeof (callbackTable) / sizeof (callbackTable[0]); i++)
    {
      bioCallback *cb = &callbackTable[i];
      cb->term (bs);
    }
}

static bioCallback *
bio_schedule (bioState * bs)
{
  int i;
  long long call_dist = -1;
  bioCallback *callback = NULL;

  for (i = 0; i < sizeof (callbackTable) / sizeof (callbackTable[0]); i++)
    {
      bioCallback *cb = &callbackTable[i];
      if (cb->pred (bs))
	{
	  if (bs->curr_ms - cb->last_call_ms > call_dist)
	    {
	      callback = cb;
	      call_dist = bs->curr_ms - cb->last_call_ms;
	    }
	}
    }
  return callback;
}

static void
bio_block_signal (void)
{
  sigset_t sigset;

  sigemptyset (&sigset);
  sigaddset (&sigset, SIGALRM);
  if (pthread_sigmask (SIG_BLOCK, &sigset, NULL) != 0)
    {
      smr_panic ("pthread_sigmask failed", __FILE__, __LINE__);
    }
}

static void
bio_idle (bioState * bs)
{
  usleep (10000);
}

static void
bio_loop (bioState * bs)
{
  while (!bs->rep->interrupted && !bs->rep->bio_interrupted)
    {
      bioCallback *callback;

      bs->curr_ms = currtime_ms ();
      callback = bio_schedule (bs);
      if (callback == NULL)
	{
	  bio_idle (bs);
	}
      else
	{
	  int ret;
	  SLOWLOG_DECLARE (callback->sub1, callback->sub2);

	  SLOWLOG_ENTER ();
	  ret = callback->proc (bs);
	  SLOWLOG_LEAVE (bs->rep);
	  if (ret < 0)
	    {
	      LOG (LG_ERROR,
		   "Callback returns error %s:%d errno:%d", callback->name,
		   ret, errno);
	      smr_panic ("Callback failed", __FILE__, __LINE__);
	    }
	  callback->last_call_ms = bs->curr_ms;
	}
    }
  return;
}

/* ----------------- */
/* Exported Function */
/* ----------------- */

/*
 * Backgound IO thread
 *   - sync log file in sequence order for local log recovery after crash.
 *   - unmap mapping
 *   - decache no more actively used file mapping
 *   - delete no more used log files
 */
void *
bio_thread (void *arg)
{
  smrReplicator *rep = (smrReplicator *) arg;
  bioState bs;

  assert (rep != NULL);

  initialize_bs (&bs, rep);
  bio_block_signal ();
  bio_loop (&bs);
  finalize_bs (&bs);

  return NULL;
}

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
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>

#include "log_mmap.h"

#define LOG LOG_TEMPLATE
/* --------------------------- */
/* Local Function Declarations */
/* --------------------------- */
static long long currusec (void);
static smrLogAddr *log_create_mapping (smrReplicator * rep, long long seq,
				       entryGetOpt opt, int is_write);
static void logmmap_entry_release_act (smrReplicator * rep,
				       logMmapEntry * entry);

/* ------------------------- */
/* Local Function Definition */
/* ------------------------- */
static long long
currusec (void)
{
  struct timeval tv;
  long long usec;

  gettimeofday (&tv, NULL);
  usec = tv.tv_sec * 1000000;
  usec += tv.tv_usec;
  return usec;
}

static smrLogAddr *
log_create_mapping (smrReplicator * rep, long long seq, entryGetOpt opt,
		    int is_write)
{
  smrLogAddr *addr = NULL;
  long long st, et;

  assert (rep != NULL);
  seq = seq_round_down (seq);

  st = currusec ();
  if (opt == GET_CREATE)
    {
      addr = smrlog_write_mmap (rep->smrlog, seq, 1);
    }
  else
    {
      assert (opt == GET_EXIST);
      if (is_write)
	{
	  addr = smrlog_write_mmap (rep->smrlog, seq, 0);
	}
      else
	{
	  addr = smrlog_read_mmap (rep->smrlog, seq);
	}
    }
  et = currusec ();

  if (addr == NULL)
    {
      return NULL;
    }
  LOG (LG_INFO,
       "LOG new mmap. seq:%lld loc:%d opt:%d msec:%.2f",
       seq, addr->loc, opt, (et - st) / 1000.0);

  return addr;
}

static void
logmmap_entry_release_act (smrReplicator * rep, logMmapEntry * entry)
{
  assert (entry->ref_count == 0);
  if (rep->bio_thr != NULL)
    {
      LOG (LG_INFO, "LOG entry released to bio. seq:%lld loc:%d",
	   entry->start_seq, entry->addr->loc);
      UNMAPH_LOCK (rep);
      dlisth_insert_before (&entry->head, &rep->unmaph);
      UNMAPH_UNLOCK (rep);
    }
  else
    {
      LOG (LG_INFO, "LOG entry released foreground. seq:%lld",
	   entry->start_seq);
      pthread_mutex_destroy (&entry->mutex);
      smrlog_munmap (rep->smrlog, entry->addr);
      free (entry);
    }
}


/* ----------------- */
/* Exported Function */
/* ----------------- */

logMmapEntry *
logmmap_entry_get (smrReplicator * rep, long long seq_, entryGetOpt opt,
		   int is_write)
{
  long long seq;
  logMmapEntry *entry = NULL;
  smrLogAddr *addr;
  assert (rep != NULL);
  seq = seq_round_down (seq_);

  entry = malloc (sizeof (logMmapEntry));
  if (entry == NULL)
    {
      return NULL;
    }
  init_log_mmap_entry (entry);

  /* create new entry */
  addr = log_create_mapping (rep, seq, opt, is_write);
  if (addr == NULL)
    {
      free (entry);
      return NULL;
    }
  entry->ref_count = 1;
  entry->start_seq = seq;
  entry->addr = addr;

  LOG (LG_INFO, "LOG new mmap entry seq:%lld loc:%d", seq, entry->addr->loc);
  return entry;
}

logMmapEntry *
logmmap_entry_addref (smrReplicator * rep, logMmapEntry * entry)
{
  ENTRY_LOCK (entry);
  assert (entry->ref_count >= 0);
  entry->ref_count++;
  ENTRY_UNLOCK (entry);
  return entry;
}

void
logmmap_entry_release (smrReplicator * rep, logMmapEntry * entry)
{
  int do_unmap = 0;

  LOG (LG_DEBUG,
       "entry_release called. seq:%lld addr:%p age:%d", entry->start_seq,
       entry, entry->age);
  ENTRY_LOCK (entry);
  entry->ref_count--;
  if (entry->ref_count <= 0)
    {
      do_unmap = 1;
    }
  ENTRY_UNLOCK (entry);
  if (do_unmap)
    {
      logmmap_entry_release_act (rep, entry);
    }
}

void
logmmap_entry_release_raw (smrReplicator * rep, logMmapEntry * entry)
{
  int do_unmap = 0;

  ENTRY_LOCK (entry);
  entry->ref_count--;
  if (entry->ref_count <= 0)
    {
      do_unmap = 1;
    }
  ENTRY_UNLOCK (entry);
  if (do_unmap)
    {
      pthread_mutex_destroy (&entry->mutex);
      smrlog_munmap (rep->smrlog, entry->addr);
      free (entry);
    }
}

#include <pthread.h>
#include <stdlib.h>

#include "slowlog.h"
#include "dlist.h"

typedef struct logImpl_ logImpl;
struct logImpl_
{
  slowLog log;
  slowLogStat stat;
  pthread_mutex_t mutex;
  long long seq;
  dlisth head;
};
#define init_log_impl(s) do {                  \
  init_slow_log(&(s)->log);                    \
  init_slow_log_stat(&(s)->stat);              \
  pthread_mutex_init(&(s)->mutex, NULL);       \
  (s)->seq = 0LL;                              \
  dlisth_init(&(s)->head);                     \
} while (0)

typedef struct entryImpl_ entryImpl;
struct entryImpl_
{
  dlisth head;
  slowLogEntry entry;
};
#define init_entry_impl(e) do {                \
  dlisth_init(&(e)->head);                     \
  init_slow_log_entry(&(e)->entry);            \
} while (0)


/* ----------------- */
/* Exported Function */
/* ----------------- */
slowLog *
new_slowlog (int cap, int bar, slowLog * esc)
{
  logImpl *impl;

  if (cap < 1 || bar < 0)
    {
      return NULL;
    }

  impl = malloc (sizeof (logImpl));
  if (impl == NULL)
    {
      return NULL;
    }
  init_log_impl (impl);
  impl->log.cap = cap;
  impl->log.bar = bar;
  impl->log.esc = esc;

  return (slowLog *) impl;
}

void
delete_slowlog (slowLog * sl)
{
  logImpl *impl = (logImpl *) sl;
  dlisth *h;

  if (impl == NULL)
    {
      return;
    }
  pthread_mutex_destroy (&impl->mutex);
  while (!dlisth_is_empty (&impl->head))
    {
      h = impl->head.next;
      dlisth_delete (h);
      free (h);
    }
  free (impl);
}

int
slowlog_add (slowLog * sl, long long start_ms, int duration, char sub1,
	     char sub2)
{
  logImpl *impl = (logImpl *) sl;
  entryImpl *e;
  int ret = 0;

  if (impl == NULL || start_ms < 0 || duration < 0)
    {
      return -1;
    }

  if (duration < impl->log.bar)
    {
      return 0;
    }

  e = malloc (sizeof (entryImpl));
  init_entry_impl (e);
  e->entry.start_ms = start_ms;
  e->entry.duration = duration;
  e->entry.sub1 = sub1;
  e->entry.sub2 = sub2;

  pthread_mutex_lock (&impl->mutex);
  e->entry.id = impl->seq++;
  impl->stat.tot_count++;
  impl->stat.sum_duration += duration;
  impl->stat.count++;

  dlisth_insert_before ((dlisth *) e, &impl->head);
  if (impl->stat.count > impl->log.cap)
    {
      dlisth *h = impl->head.next;
      dlisth_delete (h);
      free (h);
      impl->stat.count--;
    }

  if (impl->log.esc != NULL && duration >= impl->log.bar)
    {
      ret = slowlog_add (impl->log.esc, start_ms, duration, sub1, sub2);
    }
  pthread_mutex_unlock (&impl->mutex);

  return ret;
}

int
slowlog_map (slowLog * sl, slowlog_map_func func, void *arg, int asc)
{
  logImpl *impl = (logImpl *) sl;
  dlisth *h;
  int ret = 0;

  if (impl == NULL || func == NULL)
    {
      return -1;
    }

  pthread_mutex_lock (&impl->mutex);
  if (asc)
    {
      for (h = impl->head.next; h != &impl->head; h = h->next)
	{
	  entryImpl *e = (entryImpl *) h;

	  ret = func (&e->entry, arg);
	  if (ret <= 0)
	    {
	      break;
	    }
	}
    }
  else
    {
      for (h = impl->head.prev; h != &impl->head; h = h->prev)
	{
	  entryImpl *e = (entryImpl *) h;

	  ret = func (&e->entry, arg);
	  if (ret <= 0)
	    {
	      break;
	    }
	}
    }
  pthread_mutex_unlock (&impl->mutex);
  return ret;
}

int
slowlog_stat (slowLog * sl, slowLogStat * stat)
{
  logImpl *impl = (logImpl *) sl;

  if (impl == NULL || stat == NULL)
    {
      return -1;
    }

  pthread_mutex_lock (&impl->mutex);
  *stat = impl->stat;
  pthread_mutex_unlock (&impl->mutex);

  return 0;
}

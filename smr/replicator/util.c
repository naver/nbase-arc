#include <assert.h>
#include <stdlib.h>

#include "common.h"

void
smr_panic (char *msg, char *file, int line)
{
  FILE *fp = stdout;

  if (s_Server.app_log_fp != NULL)
    {
      fp = s_Server.app_log_fp;
    }

  /* do not use log_msg */
  fprintf (fp, "Software error\n");
  fprintf (fp, "%s: file:%s line:%d\n", msg, file, line);
  fprintf (fp, "Aborting...\n");
  fflush (fp);
  abort ();
}

void
aseq_init (smrAtomicSeq * aseq)
{
  assert (aseq != NULL);
  if (pthread_spin_init (&aseq->spin, PTHREAD_PROCESS_SHARED) != 0)
    {
      smr_panic ("spinlock invalid", __FILE__, __LINE__);
    }
  aseq->seq = 0LL;
}

long long
aseq_get (smrAtomicSeq * aseq)
{
  long long seq;

  assert (aseq != NULL);
  if (pthread_spin_lock (&aseq->spin) != 0)
    {
      smr_panic ("spin lock protocol violated", __FILE__, __LINE__);
    }
  seq = aseq->seq;
  (void) pthread_spin_unlock (&aseq->spin);
  return seq;
}

long long
aseq_set (smrAtomicSeq * aseq, long long seq)
{
  long long seq_save;

  assert (aseq != NULL);
  if (pthread_spin_lock (&aseq->spin) != 0)
    {
      smr_panic ("spin lock protocol violated", __FILE__, __LINE__);
    }
  seq_save = aseq->seq;
  aseq->seq = seq;
  (void) pthread_spin_unlock (&aseq->spin);
  return seq_save;
}

long long
aseq_add (smrAtomicSeq * aseq, int delta)
{
  long long seq_save;

  assert (aseq != NULL);
  if (pthread_spin_lock (&aseq->spin) != 0)
    {
      smr_panic ("spin lock protocol violated", __FILE__, __LINE__);
    }
  seq_save = aseq->seq;
  aseq->seq = aseq->seq + delta;
  (void) pthread_spin_unlock (&aseq->spin);
  return seq_save + delta;
}

long long
usec_from_tv (struct timeval *tv)
{
  long long usec;

  usec = ((long long) tv->tv_sec) * 1000000;
  usec += tv->tv_usec;
  return usec;
}

long long
msec_from_tv (struct timeval *tv)
{
  return usec_from_tv (tv) / 1000;
}

long long
currtime_ms (void)
{
  struct timeval tv;

  gettimeofday (&tv, NULL);
  return msec_from_tv (&tv);
}

long long
tvdiff_usec (struct timeval *et, struct timeval *st)
{
  long long usec;
  usec =
    ((long long) et->tv_sec) * 1000000LL -
    ((long long) st->tv_sec) * 1000000LL;
  return usec + (et->tv_usec - st->tv_usec);
}

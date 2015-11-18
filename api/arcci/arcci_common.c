#ifdef _WIN32
#pragma warning(push)
#pragma warning(disable : 4244)	// for converting datatype from __int64 to int
#endif
#include "fmacros.h"

#include <assert.h>
#include <string.h>
#ifndef _WIN32
#include <sys/time.h>
#endif
#include <errno.h>

#include "arcci.h"
#include "arcci_common.h"

void 
init_fe(fe_t *c)
{
	c->pipe_fd = INVALID_PIPE;
#ifdef _WIN32
	InitializeCriticalSection(&c->pipe_lock);
#endif
	c->commands = NULL;
	c->be = NULL;
}

static int
string2ll (const char *s, size_t slen, long long *value)
{
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen)
    return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0')
    {
      if (value != NULL)
	*value = 0;
      return 1;
    }

  if (p[0] == '-')
    {
      negative = 1;
      p++;
      plen++;

      /* Abort on only a negative sign. */
      if (plen == slen)
	return 0;
    }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9')
    {
      v = p[0] - '0';
      p++;
      plen++;
    }
  else if (p[0] == '0' && slen == 1)
    {
      *value = 0;
      return 1;
    }
  else
    {
      return 0;
    }

  while (plen < slen && p[0] >= '0' && p[0] <= '9')
    {
      if (v > (ULLONG_MAX / 10))	/* Overflow. */
	return 0;
      v *= 10;

      if (v > (ULLONG_MAX - (p[0] - '0')))	/* Overflow. */
	return 0;
      v += p[0] - '0';

      p++;
      plen++;
    }

  /* Return if not all bytes were used. */
  if (plen < slen)
    return 0;

  if (negative)
    {
      if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1))	/* Overflow. */
	return 0;
      if (value != NULL)
	*value = -v;
    }
  else
    {
      if (v > LLONG_MAX)	/* Overflow. */
	return 0;
      if (value != NULL)
	*value = v;
    }
  return 1;
}


int
parse_redis_response (char *p, int len, int *size, redis_parse_cb_t * cb,
		      void *ctx)
{
  char *newline = NULL, *bp = NULL;
  int ok = 0;
  long long ll = 0;
  int ret = PARSE_CB_OK_CONTINUE;


  if (len == 0)
    {
      return -1;
    }

  /* Input should be null terminiated string */
  assert (*(p + len) == '\0');

  bp = p;
  while (ret == PARSE_CB_OK_CONTINUE && len > 0)
    {
      char *saved_p;
      char *dsp = NULL;		// binary data start point
      char *dep = NULL;		// binary data end point
      int is_null = 0;

      saved_p = p;
      switch (*p)
	{
	case '-':
	case '+':
	case ':':
	  newline = strchr (p, '\n');
	  if (newline == NULL)
	    {
	      goto resp_need_more;
	    }

	  len -= newline + 1 - p;
	  p = newline + 1;
	  /* callback */
	  if (cb != NULL)
	    {
	      dsp = saved_p;
	      dep = p - 2;	// skip \r\n

	      if (*saved_p == '-')
		{
		  ret = cb->on_error (ctx, dsp, dep - dsp);
		}
	      else if (*saved_p == '+')
		{
		  ret = cb->on_status (ctx, dsp, dep - dsp);
		}
	      else if (*saved_p == ':')
		{
		  ok = string2ll (dsp + 1, dep - dsp - 1, &ll);
		  if (!ok)
		    {
		      return -1;
		    }
		  ret = cb->on_integer (ctx, ll);
		}
	    }
	  break;
	case '$':
	  newline = strchr (p, '\r');
	  if (newline == NULL)
	    {
	      goto resp_need_more;
	    }

	  ok = string2ll (p + 1, newline - p - 1, &ll);

	  /* spec limit: 512M */
	  if (!ok || ll < -1 || ll > 512 * 1024 * 1024)
	    {
	      return -1;
	    }

	  if (ll == -1)
	    {
	      /* Null Bulk String */
	      if (newline + 2 - p > len)
		{
		  goto resp_need_more;
		}
	      len -= newline + 2 - p;
	      p = newline + 2;
	      is_null = 1;
	      /* callback */
	      if (cb != NULL)
		{
		  ret = cb->on_string (ctx, NULL, 0, is_null);
		}
	    }
	  else
	    {
	      /* Normal Case */
	      if (newline + 2 - p + ll + 2 > len)
		{
		  goto resp_need_more;
		}
	      len -= newline + 2 - p + ll + 2;
	      p = newline + 2 + ll + 2;
	      /* callback */
	      if (cb != NULL)
		{
		  ret = cb->on_string (ctx, newline + 2, ll, is_null);
		}
	    }
	  break;
	case '*':
	  newline = strchr (p, '\r');
	  if (newline == NULL || newline + 2 - p > len)
	    {
	      goto resp_need_more;
	    }

	  ok = string2ll (p + 1, newline - p - 1, &ll);

	  if (ll == -1)
	    {
	      is_null = 1;
	      ll = 0;		/* Null Array */
	    }

	  len -= newline + 2 - p;
	  p = newline + 2;
	  /* callback */
	  if (cb != NULL)
	    {
	      ret = cb->on_array (ctx, ll, is_null);
	    }
	  break;
	default:
	  return -1;
	}
    }

resp_need_more:
  *size = p - bp;
  return 0;
}

void
get_timespec_from_millis (struct timespec *ts, long long to)
{
  ts->tv_sec = (int) (to / 1000);
  ts->tv_nsec = (int) (to % 1000) * 1000000;
}

long long
millis_from_tv (struct timeval *tv)
{
  long long usec;

  usec = ((long long) tv->tv_sec) * 1000000;
  usec += tv->tv_usec;

  return usec / 1000;
}

long long
currtime_millis (void)
{
  struct timeval tv;

  gettimeofday (&tv, NULL);
  return millis_from_tv (&tv);
}

be_job_t *
create_be_job (be_job_type_t type, void (*free_func) (void *), void *data,
	       int cost)
{
  be_job_t *r;

  r = malloc (sizeof (be_job_t));
  if (r == NULL)
    {
      return NULL;
    }
  init_be_job (r);

  r->ref_count = 1;		// +1 for the caller
  r->type = type;
  r->free_func = free_func;
  r->data = data;
  r->cost = cost;
  return r;
}

void
addref_be_job (be_job_t * job)
{
  int need_mutex;

  if (job == NULL)
    {
      return;
    }

  need_mutex = is_job_need_mutex (job);
  if (need_mutex)
    {
      pthread_mutex_lock (&job->ref_mutex);
    }

  job->ref_count++;

  if (need_mutex)
    {
      pthread_mutex_unlock (&job->ref_mutex);
    }
}

void
decref_be_job (be_job_t * job)
{
  int need_mutex;
  int need_free = 0;

  if (job == NULL)
    {
      return;
    }

  need_mutex = is_job_need_mutex (job);
  if (need_mutex)
    {
      pthread_mutex_lock (&job->ref_mutex);
    }

  job->ref_count--;
  need_free = (job->ref_count <= 0);

  if (need_mutex)
    {
      pthread_mutex_unlock (&job->ref_mutex);
    }

  if (need_free)
    {
      assert (dlisth_is_empty (&job->head));
      assert (dlisth_is_empty (&job->htwhead));
      pthread_mutex_destroy (&job->ref_mutex);
      if (job->free_func)
	{
	  job->free_func (job->data);
	}
      free (job);
    }
}

#ifdef FI_ENABLED

static unsigned int
fi_entry_hash (const void *key)
{
  fi_entry_t *e;
  char buf[1024];

  e = (fi_entry_t *) key;
  sprintf (buf, "%s%d", e->file, e->line);
  return dictGenCaseHashFunction ((unsigned char *) buf, strlen (buf));
}

static int
fi_entry_compare (void *priv_data, const void *key1, const void *key2)
{
  fi_entry_t *e1, *e2;

  e1 = (fi_entry_t *) key1;
  e2 = (fi_entry_t *) key2;

  if (strcmp (e1->file, e2->file) == 0 && e1->line == e2->line)
    {
      return 1;
    }
  else
    {
      return 0;
    }
}

void
fi_entry_destructor (void *privdata, void *val)
{
  fi_entry_t *e = (fi_entry_t *) val;
  free (e);
}

static dictType fiTableDictType = {
  fi_entry_hash,		// hash function
  NULL,				// key dup
  NULL,				// val dup
  fi_entry_compare,		// key compare
  fi_entry_destructor,		// key destructor
  NULL				// val destructor
};

int
arc_fault_inject (fi_t * fi, const char *file, int line, ...)
{
  va_list ap;
  fi_entry_t *e;
  int err;
  int cnt;
  int ret;
  fi_entry_t key;
  dictEntry *de;

  /* find or lazy create */
  key.file = (char *) file;
  key.line = line;
  de = dictFind (fi->tbl, &key);
  if (de == NULL)
    {
      /* check if the fault inject entry is in */
      e = malloc (sizeof (fi_entry_t));
      assert (e != NULL);
      init_fi_entry (e);
      e->file = (char *) file;
      e->line = line;

      cnt = 0;
      assert (file != NULL);
      va_start (ap, line);
      while ((err = va_arg (ap, int)) != 0)
	{
	  e->err_nums[cnt++] = err;
	  if (cnt >= MAX_ERRNO_INJECTION)
	    {
	      assert (0);
	      return 0;
	    }
	}
      va_end (ap);
      if (cnt == 0)
	{
	  /* at least one fault injection is enforced */
	  e->err_nums[0] = 0;
	  e->err_cnt = 1;
	}
      else
	{
	  e->err_cnt = cnt;
	}
      ret = dictAdd (fi->tbl, e, e);
      assert (ret == DICT_OK);
    }
  else
    {
      e = de->v.val;
    }

  if (e->err_idx < e->err_cnt)
    {
      errno = e->err_nums[e->err_idx++];
      return 1;
    }
  else
    {
      return 0;
    }
}

fi_t *
arc_fi_new (void)
{
  fi_t *fi;

  fi = malloc (sizeof (fi_t));
  if (fi == NULL)
    {
      return NULL;
    }
  init_fi (fi);

  fi->tbl = dictCreate (&fiTableDictType, NULL);
  if (fi->tbl == NULL)
    {
      free (fi);
      return NULL;
    }

  return fi;
}

void
arc_fi_destroy (fi_t * fi)
{
  if (fi == NULL)
    {
      return;
    }

  dictRelease (fi->tbl);
  free (fi);
}
#endif

#ifdef _WIN32
#pragma warning(pop)
#endif
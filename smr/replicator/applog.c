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
#include <stdarg.h>
#include <stdlib.h>
#include <sys/time.h>

#include "common.h"
#include "smr.h"

/* -------------- */
/* Local variable */
/* -------------- */
static pthread_mutex_t log_Mutex = PTHREAD_MUTEX_INITIALIZER;

static const char *const s_Log_level_str[] = { "ERR", "WRN", "INF", "DBG" };

static const char *const s_Conn_type_str[] =
  { "MAN", "LOC<", "LOC>", "SLV<", "SLV>", "CLI<", "CLI>", "MAS<",
  "MAS>", "MGM<", "MGM>", "MIG<", "MIG>", "BIO", "MAP"
};


static void
del_log_session (void *arg)
{
  logSession *s = (logSession *) arg;
  if (s)
    {
      free (s);
    }
}

static logSession *
get_log_session (smrReplicator * rep)
{
  logSession *s = pthread_getspecific (rep->log_key);
  if (s == NULL)
    {
      s = malloc (sizeof (logSession));
      if (s == NULL)
	{
	  smr_panic ("Failed to allocate", __FILE__, __LINE__);
	  return NULL;
	}
      init_log_session (s);
      pthread_setspecific (rep->log_key, s);
    }
  return s;
}

/* ------------------ */
/* Exported Functions */
/* ------------------ */
int
applog_init (smrReplicator * rep)
{
  int ret;

  ret = pthread_key_create (&rep->log_key, del_log_session);
  if (ret < 0)
    {
      return -1;
    }
  return 0;
}

void
applog_term (smrReplicator * rep)
{
  (void) pthread_key_delete (rep->log_key);
}

void
applog_enter_session (smrReplicator * rep, connType conn_type, int conn_fd)
{
  logSession *s;
  s = get_log_session (rep);
  s->type = conn_type;
  s->fd = conn_fd;
  s->sid++;
}

void
applog_leave_session (smrReplicator * rep)
{
  logSession *s;
  s = get_log_session (rep);
  s->type = NO_CONN;
  s->fd = 0;
}

void
log_msg (smrReplicator * rep, int level, char *fmt, ...)
{
  //? is reserved
  static const char ids[] = { '!', '@', '#', '$', '%', '^', '*', '-', '+' };
  va_list ap;
  char msg_buf[8196];
  char time_buf[128];
  struct timeval tv;
  int off;
  int wsz;
  logSession *ss;
  char color;


  assert (rep != NULL);
  assert (fmt != NULL);
  if (level > rep->log_level)
    {
      return;
    }
  ss = get_log_session (rep);
  color = ids[ss->sid % sizeof (ids)];

  va_start (ap, fmt);
  vsnprintf (msg_buf, sizeof (msg_buf), fmt, ap);
  va_end (ap);

  gettimeofday (&tv, NULL);
  off =
    strftime (time_buf, sizeof (time_buf), "%d %b %H:%M:%S.",
	      localtime (&tv.tv_sec));
  snprintf (time_buf + off, sizeof (time_buf) - off, "%03d",
	    (int) tv.tv_usec / 1000);


  pthread_mutex_lock (&log_Mutex);
  /* lazy create log file */
  if (rep->app_log_fp == NULL && rep->app_log_prefix_size > 0)
    {
      char *cp = &rep->app_log_buf[0] + rep->app_log_prefix_size;

      strftime (cp, sizeof (rep->app_log_buf) / 2, "-%d-%b-%H-%M-%S",
		localtime (&tv.tv_sec));
      rep->app_log_fp = fopen (rep->app_log_buf, "a");
      if (rep->app_log_fp == NULL)
	{
	  goto done;
	}
    }

  wsz =
    fprintf (rep->app_log_fp, "%s %c[%s:%s(%d:%d)] %s\n", time_buf, color,
	     s_Log_level_str[level], s_Conn_type_str[ss->type], rep->nid,
	     ss->fd, msg_buf);

  if (rep->app_log_prefix_size > 0)
    {
      rep->app_log_size += wsz;
      if (rep->app_log_size > MAX_APP_LOG_SIZE)
	{
	  fclose (rep->app_log_fp);
	  rep->app_log_fp = NULL;
	  rep->app_log_size = 0;
	}
      else
	{
	  fflush (rep->app_log_fp);
	}
    }
done:
  pthread_mutex_unlock (&log_Mutex);
}


char *
get_command_string (char cmd)
{
  switch (cmd)
    {
    case SMR_OP_EXT:
      return "SMR_OP_EXT";
    case SMR_OP_SESSION_CLOSE:
      return "SMR_OP_SESSION_CLOSE";
    case SMR_OP_SESSION_DATA:
      return "SMR_OP_SESSION_DATA";
    case SMR_OP_NODE_CHANGE:
      return "SMR_OP_NODE_CHANGE";
    case SMR_OP_SEQ_COMMITTED:
      return "SMR_OP_SEQ_COMMITTED";
    case SMR_OP_SEQ_RECEIVED:
      return "SMR_OP_SEQ_RECEIVED";
    case SMR_OP_SEQ_CONSUMED:
      return "SMR_OP_SEQ_CONSUMED";
    case SMR_OP_SEQ_CKPTED:
      return "SMR_OP_SEQ_CKPTED";
    case SMR_OP_SEQ_SAFE:
      return "SMR_OP_SEQ_SAFE";
    default:
      return "UNKNOWN";
    }
}

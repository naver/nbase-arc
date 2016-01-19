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

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include "ae.h"
#include "crc16.h"
#include "smrmp.h"

#define MAX_ID               64
#define SINGLE_TOKEN_VAL     10000
#define WORKER_CRON_INTERVAL 2
#define RECONNECT_INTERVAL   1000
#define DEFAULT_SIZE         128
#define DEFAULT_TPS          1000
#define MAX_HOST_LEN         64

typedef struct workerArg_ workerArg;
typedef struct workerCmd_ workerCmd;
typedef struct workerState_ workerState;
typedef struct masterState_ masterState;

struct workerArg_
{
  int id;
  char host[MAX_HOST_LEN];
  int port;
  int size;
  int tps;
  int rfd;
  int wfd;
};
#define init_worker_arg(a) do {  \
  (a)->id = -1;                  \
  (a)->host[0] = 0;              \
  (a)->port = 0;                 \
  (a)->size = DEFAULT_SIZE;      \
  (a)->tps = DEFAULT_TPS;        \
  (a)->rfd = -1;                 \
  (a)->wfd = -1;                 \
} while(0)

typedef enum
{
  CMD_NONE = 0,
  CMD_SIZE,
  CMD_TPS,
  CMD_START,
  CMD_STOP,
  CMD_STAT,
  CMD_QUIT,
} commentType;

#define MAX_HISTO 9
struct workerCmd_
{
  int command;
  int ival;
  long long num_rqst;
  long long num_resp;
  long long num_reconn;
  struct
  {
    long long count;
    long long sum;
  } histo[MAX_HISTO];
  int ret;
  char ebuf[512];
};
#define init_worker_cmd(c) do {         \
  int i;                                \
  (c)->command = CMD_NONE;              \
  (c)->ival = 0;                        \
  (c)->num_rqst = 0LL;                  \
  (c)->num_resp = 0LL;                  \
  (c)->num_reconn = 0LL;                \
  for(i=0;i<MAX_HISTO;i++) {            \
    (c)->histo[i].count = 0LL;          \
    (c)->histo[i].sum = 0LL;            \
  }                                     \
  (c)->ret = 0;                         \
  (c)->ebuf[0] = 0;                     \
} while (0)

typedef struct
{
  int cap;
  char *buf;
  char *cp;
  char *ep;
} simpleBuf;
#define init_simple_buf(s) do { \
  (s)->cap = 0;                 \
  (s)->buf = NULL;              \
  (s)->cp = NULL;               \
  (s)->ep = NULL;               \
} while(0)

struct workerState_
{
  simpleBuf cin;
  aeEventLoop *el;
  long long teid;
  workerArg *arg;
  int size;
  int tps;
  int key;
  int is_sync;
  /* connection state purged at every disconnect */
  int fd;
  long long reconn_after;
  int wprepared;
  int wprepare_at_token;
  long long token_val;
  simpleBuf in;
  simpleBuf out;
  long long num_rqst;
  long long num_resp;
  long long num_reconn;
  //response time histogram
  long long start_usec;
  struct
  {
    long long count;
    long long sum;
  } histo[MAX_HISTO];
};
#define reset_worker_conn_state(s) do { \
  (s)->fd = -1;                         \
  (s)->reconn_after = 0LL;              \
  (s)->wprepared = 0;                   \
  (s)->wprepare_at_token = 0;           \
  (s)->token_val = 0;                   \
  (s)->in.cp = (s)->in.buf;             \
  (s)->out.cp = (s)->out.buf;           \
} while (0)

#define init_worker_state(s) do {       \
  int i;                                \
  init_simple_buf(&(s)->cin);           \
  (s)->el = NULL;                       \
  (s)->teid = 0LL;                      \
  (s)->arg = NULL;                      \
  (s)->size = 0;                        \
  (s)->tps = 0;                         \
  (s)->key = -1;                        \
  (s)->is_sync = 0;                     \
  (s)->fd = -1;                         \
  (s)->reconn_after = 0LL;              \
  (s)->wprepared = 0;                   \
  (s)->wprepare_at_token = 0;           \
  (s)->token_val = 0;                   \
  init_simple_buf(&(s)->in);            \
  init_simple_buf(&(s)->out);           \
  (s)->num_rqst = 0LL;                  \
  (s)->num_resp = 0LL;                  \
  (s)->num_reconn = 0LL;                \
  (s)->start_usec = 0LL;                \
  for(i=0;i<MAX_HISTO;i++) {            \
    (s)->histo[i].count = 0LL;          \
    (s)->histo[i].sum = 0LL;            \
  }                                     \
} while (0)

struct masterState_
{
  int used;
  int thread_running;		// thread running
  int workload_started;		// workload started
  pthread_t thr;
  int rfd;
  int wfd;
  workerArg arg;
};
#define init_master_state(s) do { \
  (s)->used = 0;                  \
  (s)->thread_running = 0;        \
  (s)->workload_started = 0;      \
  (s)->rfd = -1;                  \
  (s)->wfd = -1;                  \
  init_worker_arg(&(s)->arg);     \
} while(0)

/* -------------------------- */
/* Local function Declaration */
/* -------------------------- */

/* utilities */
static void set_error (char *ebuf, int ebuf_sz, char *format, ...);
static int nonblock_connect (workerState * ws, char *ebuf, int ebuf_sz);
static void free_connection (workerState * ws, int reconn);
static long long usec_from_tv (struct timeval *tv);
static long long currtime_usec (void);
static long long currtime_millis (void);
static void sb_reserve (simpleBuf * sb, int size);
static int sb_read (simpleBuf * sb, int fd);
static int sb_write (simpleBuf * sb, int fd);
static void sb_clear (simpleBuf * sb);

/* commands */
static int prepare_write (workerState * ws);
static int emit_get (workerState * ws);
static int emit_set (workerState * ws);
static int emit_command (workerState * ws);
static int command_start (workerState * ws, char *ebuf, int ebuf_sz);

/* event handlers */
static void request_handler (aeEventLoop * el, int fd, void *data, int mask);
static void response_handler (aeEventLoop * el, int fd, void *data, int mask);
static void command_handler (aeEventLoop * el, int fd, void *data, int mask);
static int worker_cron (aeEventLoop * el, long long id, void *data);
static void *worker_thread (void *arg);

/* main command handlers */
static void do_worker_cmd (masterState * ms, workerCmd * cmd);
static void user_cmd_add (masterState * ms_ary, char **tokens, int ntok);
static void do_cmd_del (masterState * ms_ary);
static void user_cmd_del (masterState * ms_ary, char **tokens, int ntok);
static void user_cmd_info (masterState * ms_ary, char **tokens, int ntok);
static void user_cmd_size (masterState * ms_ary, char **tokens, int ntok);
static void user_cmd_sleep (masterState * ms_ary, char **tokens, int ntok);
static void user_cmd_start (masterState * ms_ary, char **tokens, int ntok);
static void user_cmd_stat (masterState * ms_ary, char **tokens, int ntok);
static void do_cmd_stop (masterState * ms);
static void do_cmd_quit (masterState * ms);
static void user_cmd_stop (masterState * ms_ary, char **tokens, int ntok);
static void user_cmd_tps (masterState * ms_ary, char **tokens, int ntok);
static void user_cmd_quit (masterState * ms_ary, char **tokens, int ntok);

/* -------------- */
/* Local variable */
/* -------------- */
static int ks_crc[1024];
static int ks_crc_next[1024];

/* ------------------------- */
/* Local function Definition */
/* ------------------------- */

static void
set_error (char *ebuf, int ebuf_sz, char *format, ...)
{
  va_list args;

  assert (ebuf != NULL ? (ebuf_sz > 0) : 1);
  if (ebuf != NULL)
    {
      va_start (args, format);
      vsnprintf (ebuf, ebuf_sz, format, args);
      va_end (args);
    }
}

static int
nonblock_connect (workerState * ws, char *ebuf, int ebuf_sz)
{
  char *addr = ws->arg->host;
  int port = ws->arg->port;
  int socket_fd = -1;
  struct sockaddr_in sa;
  int one = 1;
  int flags;

  assert (addr != NULL);
  assert (port > 0);

  if ((socket_fd = socket (AF_INET, SOCK_STREAM, 0)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to create a socket");
      return -1;
    }

  if (setsockopt (socket_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof (one)) ==
      -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to setsockopt: SO_REUSEADDR: %s",
		 strerror (errno));
      return -1;
    }

  /* set options */
  if (setsockopt
      (socket_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof (int)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to set TCP_NODELAY option. errno:%d",
		 errno);
      goto error;
    }

  if (setsockopt
      (socket_fd, IPPROTO_TCP, SO_KEEPALIVE, &one, sizeof (int)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to set SO_KEEPALIVE option. errno:%d",
		 errno);
      goto error;
    }

  if ((flags = fcntl (socket_fd, F_GETFL)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to fcntl F_GETFL errno:%d", errno);
      goto error;
    }
  flags |= O_NONBLOCK;
  if (fcntl (socket_fd, F_SETFL, flags) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to fcntl F_GETFL errno:%d", errno);
      goto error;
    }

  /* connect */
  sa.sin_family = AF_INET;
  sa.sin_port = htons (port);

  if (inet_aton (addr, &sa.sin_addr) == 0)
    {
      struct hostent *he;

      /* try to convert host address from name notation */
      he = gethostbyname (addr);
      if (he == NULL)
	{
	  set_error (ebuf, ebuf_sz, "Failed to resolve address: %s", addr);
	  goto error;
	}
      memcpy (&sa.sin_addr, he->h_addr, sizeof (struct in_addr));
    }

  if (connect (socket_fd, (struct sockaddr *) &sa, sizeof (sa)) == -1)
    {
      if (errno != EINPROGRESS)
	{
	  set_error (ebuf, ebuf_sz, "Failed to connect: %s",
		     strerror (errno));
	  goto error;
	}
    }
  return socket_fd;

error:
  if (socket_fd >= 0)
    {
      close (socket_fd);
    }
  return -1;
}

static void
free_connection (workerState * ws, int reconn)
{
  assert (ws != NULL);

  if (ws->fd > 0)
    {
      aeDeleteFileEvent (ws->el, ws->fd, AE_WRITABLE);
      aeDeleteFileEvent (ws->el, ws->fd, AE_READABLE);
      close (ws->fd);
    }
  reset_worker_conn_state (ws);

  if (reconn)
    {
      ws->num_reconn++;
      ws->reconn_after = currtime_millis () + RECONNECT_INTERVAL;
    }
}

static long long
usec_from_tv (struct timeval *tv)
{
  long long usec;

  usec = ((long long) tv->tv_sec) * 1000000;
  usec += tv->tv_usec;

  return usec;
}

static long long
currtime_usec (void)
{
  struct timeval tv;

  gettimeofday (&tv, NULL);
  return usec_from_tv (&tv);
}

static long long
currtime_millis (void)
{
  struct timeval tv;

  gettimeofday (&tv, NULL);
  return usec_from_tv (&tv) / 1000;
}

static void
sb_reserve (simpleBuf * sb, int size)
{
  if (sb->buf == NULL || sb->cap - (sb->cp - sb->buf) < size)
    {
      int new_sz = sb->cap + size;
      char *tmp;

      if (sb->buf == NULL)
	{
	  tmp = calloc (1, new_sz);
	}
      else
	{
	  tmp = realloc (sb->buf, new_sz);
	}
      assert (tmp != NULL);
      sb->cap = new_sz;
      sb->cp = tmp + (sb->cp - sb->buf);
      sb->ep = tmp + new_sz;
      sb->buf = tmp;
    }
}


static int
sb_read (simpleBuf * sb, int fd)
{
  int tr = 0;

  while (1)
    {
      int nr;

      sb_reserve (sb, 7200);
      nr = read (fd, sb->cp, 7200);
      if (nr == -1)
	{
	  if (errno == EAGAIN)
	    {
	      break;
	    }
	  return -1;
	}
      else if (nr == 0)
	{
	  return -1;
	}
      else
	{
	  tr += nr;
	  sb->cp += nr;
	}
    }
  return tr;
}

static int
sb_write (simpleBuf * sb, int fd)
{
  int tw = 0;
  int avail;

  while ((avail = sb->ep - sb->cp) > 0)
    {
      int nr;

      nr = write (fd, sb->cp, avail);
      if (nr == -1)
	{
	  if (errno == EAGAIN)
	    {
	      break;
	    }
	  return -1;
	}
      else if (nr == 0)
	{
	  return -1;
	}
      else
	{
	  tw += nr;
	  sb->cp += nr;
	}
    }
  return tw;
}


static void
sb_clear (simpleBuf * sb)
{
  if (sb->buf != NULL)
    {
      free (sb->buf);
    }
  init_simple_buf (sb);
}

static int
prepare_write (workerState * ws)
{
  int ret;

  if (ws->wprepared == 0)
    {
      ret =
	aeCreateFileEvent (ws->el, ws->fd, AE_WRITABLE, request_handler, ws);
      if (ret == AE_ERR)
	{
	  return -1;
	}
      ws->wprepared = 1;
    }
  return 0;
}

static int
emit_get (workerState * ws)
{
  int nr;
  simpleBuf *sb;

  assert (ws->key >= 0);
  sb = &ws->out;
  assert (sb->cp == sb->buf);

  sb_reserve (sb, 128);
  nr = sprintf (sb->cp, "GET %d\r\n", ws->key);
  assert (nr > 0 && nr < 128);
  sb->ep = sb->buf + nr;

  return prepare_write (ws);;
}

static int
get_key (workerState * ws)
{
  int r;

  r = rand () % (1024 / MAX_ID);
  return r * MAX_ID + ws->arg->id;
}

static int
emit_set (workerState * ws)
{
  simpleBuf *sb;
  char *bp, *cp;
  int nr, i;
  static const char *data10 = "0123456789";

  sb = &ws->out;


  sb_reserve (sb, 128 + ws->size);
  cp = sb->cp;
  nr = sprintf (cp, "SET %d ", ws->key);
  assert (nr > 0);
  cp += nr;
  bp = cp;
  for (i = 0; i < ws->size; i++)
    {
      int idx = rand () % 10;
      *cp++ = data10[idx];
    }
  ks_crc_next[ws->key] = crc16 (bp, cp - bp, ks_crc[ws->key]);
  *cp++ = '\r';
  *cp++ = '\n';
  sb->ep = cp;
  ws->num_rqst++;

  return prepare_write (ws);
}

static int
emit_command (workerState * ws)
{
  simpleBuf *sb;

  sb = &ws->out;
  assert (sb->cp == sb->buf);
  sb->ep = sb->buf + sb->cap;

  // tps == 0 means no limit
  if (ws->tps > 0)
    {
      if (ws->token_val < SINGLE_TOKEN_VAL)
	{
	  ws->wprepare_at_token = 1;
	  return 0;
	}
      ws->token_val -= SINGLE_TOKEN_VAL;
    }

  ws->start_usec = currtime_usec ();
  ws->key = get_key (ws);
  if (ks_crc[ws->key] == -1 || ks_crc[ws->key] != ks_crc_next[ws->key])
    {
      ws->is_sync = 1;
      return emit_get (ws);
    }
  else
    {
      return emit_set (ws);
    }
}

static int
command_start (workerState * ws, char *ebuf, int ebuf_sz)
{
  int ret;

  assert (ws->fd == -1);
  //TODO need this? ws->reconn_after = currtime_millis () + RECONNECT_INTERVAL;

  ws->fd = nonblock_connect (ws, ebuf, ebuf_sz);
  if (ws->fd < 0)
    {
      return -1;
    }

  ret = aeCreateFileEvent (ws->el, ws->fd, AE_READABLE, response_handler, ws);
  assert (ret != AE_ERR);

  ret = emit_command (ws);
  assert (ret >= 0);

  return 0;
}

static void
request_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  workerState *ws = (workerState *) data;
  simpleBuf *sb = &ws->out;
  int tw;

  if (ws->reconn_after > 0LL)
    {
      ws->reconn_after = 0LL;
    }

  tw = sb_write (sb, fd);
  if (tw < 0)
    {
      fprintf (stderr, "[smr-client] failed to sb_write:%d\n", tw);
      free_connection (ws, 1);
      return;
    }

  if (sb->ep - sb->cp == 0)
    {
      sb->cp = sb->buf;		//reset sb
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      ws->wprepared = 0;
    }
  return;
}

static void
update_histo (workerState * ws)
{
  int i;
  long long scale = 1;
  long long diff;

  diff = currtime_usec () - ws->start_usec;
  assert (diff >= 0);
  for (i = 0; i < MAX_HISTO; i++)
    {
      if (diff <= scale)
	{
	  ws->histo[i].count++;
	  ws->histo[i].sum += diff;
	  return;
	}
      scale = scale * 10;
    }
  assert (0);
}

static void
make_histo (workerCmd * cmd, char *buf, int bufsz)
{
  int i, n;
  char *cp = buf;

  for (i = 0; i < MAX_HISTO; i++)
    {
      double avg;

      if (cmd->histo[i].count == 0)
	{
	  continue;
	}
      avg =
	(double) cmd->histo[i].sum / (double) cmd->histo[i].count / 1000.0;
      n =
	snprintf (cp, bufsz - (cp - buf), "[%d](%lld,%.2f)",
		  i, cmd->histo[i].count, avg);
      assert (n > 0);
      cp += n;
    }
}


static void
response_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  workerState *ws = (workerState *) data;
  simpleBuf *sb = &ws->in;
  int tr;
  int ret;

  tr = sb_read (sb, fd);
  if (tr < 0)
    {
      fprintf (stderr, "[smr-client] failed to sb_read:%d\n", tr);
      free_connection (ws, 1);
      return;
    }

  if (sb->cp - sb->buf > 2 && *(sb->cp - 2) == '\r' && *(sb->cp - 1) == '\n')
    {
      int crc = atoi (sb->buf);

      // update ks_crc
      ks_crc[ws->key] = crc;

      // check response
      if (ws->is_sync)
	{
	  ks_crc_next[ws->key] = crc;
	  ws->is_sync = 0;
	}
      else
	{
	  if (crc != ks_crc_next[ws->key])
	    {
	      fprintf (stdout,
		       "-ERR inconsistency detected. expected:%d received:%d\n",
		       ks_crc_next[ws->key], crc);
	      abort ();
	    }
	  // update stat

	  update_histo (ws);
	  ws->num_resp++;
	}

      // reset response
      sb->cp = sb->buf;
      ws->key = -1;

      ret = emit_command (ws);
      assert (ret >= 0);
    }
  return;
}

static void
command_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  int ret;
  workerState *ws = (workerState *) data;
  simpleBuf *sb = &ws->cin;
  char *cp;
  int is_quit = 0;
  int i;

  ret = sb_read (sb, fd);
  // master quits this fd instead
  assert (ret != -1);

  cp = sb->buf;
  while (!is_quit && sb->cp - cp >= sizeof (workerCmd *))
    {
      workerCmd *cmd = NULL;

      memcpy ((char *) &cmd, cp, sizeof (workerCmd *));
      switch (cmd->command)
	{
	case CMD_SIZE:
	  ws->size = cmd->ival;
	  cmd->ret = 0;
	  break;
	case CMD_TPS:
	  ws->tps = cmd->ival;
	  cmd->ret = 0;
	  break;
	case CMD_START:
	  cmd->ret = command_start (ws, cmd->ebuf, sizeof (cmd->ebuf));
	  break;
	case CMD_STOP:
	  free_connection (ws, 0);
	  cmd->ret = 0;
	  break;
	case CMD_STAT:
	  cmd->num_rqst = ws->num_rqst;
	  cmd->num_resp = ws->num_resp;
	  cmd->num_reconn = ws->num_reconn;
	  for (i = 0; i < MAX_HISTO; i++)
	    {
	      cmd->histo[i].count = ws->histo[i].count;
	      cmd->histo[i].sum = ws->histo[i].sum;
	    }
	  cmd->ret = 0;
	  break;
	case CMD_QUIT:
	  free_connection (ws, 0);
	  aeStop (ws->el);
	  cmd->ret = 0;
	  is_quit = 1;
	  break;
	default:
	  fprintf (stdout, "-ERR invalid command:%d\n", cmd->command);
	  abort ();
	}
      write (ws->arg->wfd, "k", 1);
      cp += sizeof (workerCmd *);
    }

  memmove (sb->buf, cp, sb->cp - cp);
  sb->cp = sb->buf + (sb->cp - cp);
}

static int
worker_cron (aeEventLoop * el, long long id, void *data)
{
  workerState *ws = (workerState *) data;
  long long ts = currtime_millis ();
  int ret;


  if (ws->reconn_after != 0LL && ts > ws->reconn_after)
    {
      if (ws->fd >= 0)
	{
	  free_connection (ws, 1);
	}
      else
	{
	  char ebuf[512];
	  (void) command_start (ws, ebuf, sizeof (ebuf));
	}
    }

  if (ws->tps > 0)
    {
      long long token_val;

      token_val = (ws->tps * SINGLE_TOKEN_VAL) / 1000 * WORKER_CRON_INTERVAL;
      ws->token_val += token_val;
      if (ws->token_val > ws->tps * SINGLE_TOKEN_VAL)
	{
	  ws->token_val = ws->tps * SINGLE_TOKEN_VAL;
	}
      if (ws->wprepare_at_token && ws->token_val >= SINGLE_TOKEN_VAL)
	{
	  ret = emit_command (ws);
	  assert (ret == 0);
	  ws->wprepare_at_token = 0;
	}
    }


  return WORKER_CRON_INTERVAL;
}

static void *
worker_thread (void *arg)
{
  workerArg *wa = (workerArg *) arg;
  int ret;
  int flags;
  workerState ws;

  // set read flag to be nonblock 
  flags = fcntl (wa->rfd, F_GETFL, 0);
  assert (flags != -1);
  ret = fcntl (wa->rfd, F_SETFL, flags | O_NONBLOCK);
  assert (ret != -1);

  init_worker_state (&ws);
  ws.arg = wa;
  ws.size = wa->size;
  ws.tps = wa->tps;
  ws.el = aeCreateEventLoop (2048);
  assert (ws.el != NULL);

  ret =
    aeCreateFileEvent (ws.el, ws.arg->rfd, AE_READABLE, command_handler, &ws);
  assert (ret != AE_ERR);

  ws.teid = aeCreateTimeEvent (ws.el, 1, worker_cron, &ws, NULL);
  assert (ws.teid != -1LL);

  aeMain (ws.el);

  sb_clear (&ws.cin);
  sb_clear (&ws.in);
  sb_clear (&ws.out);
  if (ws.teid != -1LL)
    {
      aeDeleteTimeEvent (ws.el, ws.teid);
    }
  if (ws.el != NULL)
    {
      aeDeleteEventLoop (ws.el);
    }
  return NULL;
}

static void
do_worker_cmd (masterState * ms, workerCmd * cmd)
{
  int ret;
  char c;

  ret = write (ms->wfd, &cmd, sizeof (workerCmd *));
  assert (ret == sizeof (workerCmd *));
  ret = read (ms->rfd, &c, 1);
  assert (ret == 1);
  assert (c == 'k');
}

static void
user_cmd_add (masterState * ms_ary, char **tokens, int ntok)
{
  int id;
  char *host;
  int port;
  int pipe_fd[2] = { -1, -1 };
  int pipe_fd2[2] = { -1, -1 };
  masterState *ms;
  int ret;

  if (ntok != 3)
    {
      fprintf (stdout, "-ERR ADD <ID> <host> <port>\n");
      return;
    }
  id = atoi (tokens[0]);
  host = tokens[1];
  port = atoi (tokens[2]);

  if (id < 0 || id > MAX_ID)
    {
      fprintf (stdout, "-ERR invalid ID:%d\n", id);
      return;
    }
  else if (port <= 0)
    {
      fprintf (stdout, "-ERR invalid PORT:%d\n", port);
      return;
    }
  else if (strlen (host) >= MAX_HOST_LEN)
    {
      fprintf (stdout, "-ERR host too long:%s\n", host);
      return;
    }

  ms = &ms_ary[id];
  if (ms->used)
    {
      fprintf (stdout, "-ERR ID:%d is used\n", id);
      return;
    }

  ms->arg.id = id;
  strcpy (ms->arg.host, host);
  ms->arg.port = port;
  ret = pipe (pipe_fd);
  if (ret == -1)
    {
      fprintf (stdout, "-ERR failed to create pipe:%d\n", errno);
      return;
    }
  ret = pipe (pipe_fd2);
  if (ret == -1)
    {
      close (pipe_fd[0]);
      close (pipe_fd[1]);
      fprintf (stdout, "-ERR failed to create pipe:%d\n", errno);
      return;
    }

  ms->arg.rfd = pipe_fd2[0];
  ms->arg.wfd = pipe_fd[1];
  ms->rfd = pipe_fd[0];
  ms->wfd = pipe_fd2[1];
  ms->used = 1;
  fprintf (stdout, "+OK\n");
  return;
}

static void
do_cmd_del (masterState * ms)
{
  if (ms->used)
    {
      if (ms->thread_running)
	{
	  do_cmd_quit (ms);
	}
      if (ms->arg.rfd != -1)
	{
	  close (ms->arg.rfd);
	}
      if (ms->arg.wfd != -1)
	{
	  close (ms->arg.wfd);
	}
      if (ms->rfd != -1)
	{
	  close (ms->rfd);
	}
      if (ms->wfd != -1)
	{
	  close (ms->wfd);
	}

      init_master_state (ms);
    }
}

static void
user_cmd_del (masterState * ms_ary, char **tokens, int ntok)
{
  int id;
  masterState *ms;

  if (ntok != 1)
    {
      fprintf (stdout, "-ERR DEL <ID>\n");
      return;
    }

  id = atoi (tokens[0]);
  if (id < 0 || id > MAX_ID)
    {
      fprintf (stdout, "-ERR invalid ID:%d\n", id);
      return;
    }

  ms = &ms_ary[id];
  if (ms->used)
    {
      do_cmd_del (&ms_ary[id]);
      fprintf (stdout, "+OK\n");
    }
  else
    {
      fprintf (stdout, "-ERR ID %d is not used\n", id);
    }

  return;
}

static void
user_cmd_info (masterState * ms_ary, char **tokens, int ntok)
{
  int i;

  if (ntok != 0)
    {
      fprintf (stdout, "-ERR INFO\n");
      return;
    }

  fprintf (stdout, "+OK ");
  for (i = 0; i < MAX_ID; i++)
    {
      masterState *ms = &ms_ary[i];
      if (ms->used)
	{
	  fprintf (stdout, " [%d]%s:%d(%s:%s)", i, ms->arg.host, ms->arg.port,
		   ms->thread_running ? "o" : "x",
		   ms->workload_started ? "o" : "x");
	}
    }
  fprintf (stdout, "\n");
  return;
}


static void
user_cmd_size (masterState * ms_ary, char **tokens, int ntok)
{
  int size;
  int id;
  masterState *ms;
  workerCmd cmd;


  if (ntok != 2)
    {
      fprintf (stdout, "-ERR SIZE <size> <ID>\n");
      return;
    }

  size = atoi (tokens[0]);
  id = atoi (tokens[1]);

  if (size <= 0)
    {
      fprintf (stdout, "-ERR invalid SIZE:%d\n", size);
      return;
    }
  else if (id < 0 || id > MAX_ID)
    {
      fprintf (stdout, "-ERR invalid ID:%d\n", id);
      return;
    }
  ms = &ms_ary[id];
  if (ms->used == 0)
    {
      fprintf (stdout, "-ERR id is not used:%d\n", id);
      return;
    }

  ms->arg.size = size;
  if (ms->thread_running)
    {
      cmd.command = CMD_SIZE;
      cmd.ival = size;
      do_worker_cmd (ms, &cmd);
    }

  fprintf (stdout, "+OK size is set to %d\n", size);
  return;
}

static void
user_cmd_sleep (masterState * ms_ary, char **tokens, int ntok)
{
  int msec;
  struct timespec ts;


  if (ntok != 1)
    {
      fprintf (stdout, "-ERR SLEEP <msec>\n");
      return;
    }

  msec = atoi (tokens[0]);
  if (msec < 0)
    {
      fprintf (stdout, "-ERR invalid MSEC:%d\n", msec);
      return;
    }

  ts.tv_sec = msec / 1000;
  ts.tv_nsec = (msec % 1000) * 1000000;
  (void) nanosleep (&ts, NULL);
  fprintf (stdout, "+OK\n");
}

static void
user_cmd_start (masterState * ms_ary, char **tokens, int ntok)
{
  int id;
  int ret;
  masterState *ms;
  workerCmd cmd;

  if (ntok != 1)
    {
      fprintf (stdout, "-ERR START <ID>\n");
      return;
    }

  id = atoi (tokens[0]);
  if (id < 0 || id > MAX_ID)
    {
      fprintf (stdout, "-ERR invalid ID:%d\n", id);
      return;
    }

  ms = &ms_ary[id];
  if (!ms->used)
    {
      fprintf (stdout, "-ERR ID:%d is not used\n", id);
      return;
    }
  else if (ms->workload_started)
    {
      fprintf (stdout, "-ERR ID:%d is already running\n", id);
      return;
    }

  if (!ms->thread_running)
    {
      ret = pthread_create (&ms->thr, NULL, worker_thread, &ms->arg);
      if (ret < 0)
	{
	  goto error;
	}
      ms->thread_running = 1;
    }

  cmd.command = CMD_START;
  do_worker_cmd (ms, &cmd);
  ms->workload_started = 1;
  fprintf (stdout, "+OK\n");
  return;

error:
  fprintf (stdout, "-ERR failed to create a thread errno:%d\n", errno);
  return;
}

static void
user_cmd_stat (masterState * ms_ary, char **tokens, int ntok)
{
  int id;
  masterState *ms;
  workerCmd cmd;
  char buf[1024];

  if (ntok != 1)
    {
      fprintf (stdout, "-ERR STAT <ID>\n");
      return;
    }

  id = atoi (tokens[0]);
  if (id < 0 || id > MAX_ID)
    {
      fprintf (stdout, "-ERR invalid ID:%d\n", id);
      return;
    }

  ms = &ms_ary[id];
  if (!ms->used)
    {
      fprintf (stdout, "-ERR ID:%d is not used\n", id);
      return;
    }

  cmd.command = CMD_STAT;
  do_worker_cmd (ms, &cmd);
  make_histo (&cmd, buf, sizeof (buf));
  fprintf (stdout,
	   "+OK rqst:%lld resp:%lld reconn:%lld histo_usec_log10:%s\n",
	   cmd.num_rqst, cmd.num_resp, cmd.num_reconn, buf);
  return;
}

static void
do_cmd_stop (masterState * ms)
{
  if (ms->workload_started)
    {
      workerCmd cmd;
      cmd.command = CMD_STOP;
      do_worker_cmd (ms, &cmd);
      ms->workload_started = 0;
    }
}

static void
do_cmd_quit (masterState * ms)
{
  if (ms->thread_running)
    {
      workerCmd cmd;
      cmd.command = CMD_QUIT;
      do_worker_cmd (ms, &cmd);
      pthread_join (ms->thr, NULL);
      ms->thread_running = 0;
      ms->workload_started = 0;
    }
}

static void
user_cmd_stop (masterState * ms_ary, char **tokens, int ntok)
{
  int id;
  masterState *ms;

  if (ntok != 1)
    {
      fprintf (stdout, "-ERR STOP <ID>\n");
      return;
    }

  id = atoi (tokens[0]);
  if (id < 0 || id > MAX_ID)
    {
      fprintf (stdout, "-ERR invalid ID:%d\n", id);
      return;
    }

  ms = &ms_ary[id];
  if (!ms->used)
    {
      fprintf (stdout, "-ERR ID:%d is not used\n", id);
      return;
    }
  else if (!ms->workload_started)
    {
      fprintf (stdout, "-ERR ID:%d is not started\n", id);
      return;
    }

  do_cmd_stop (ms);
  fprintf (stdout, "+OK\n");
  return;
}

static void
user_cmd_tps (masterState * ms_ary, char **tokens, int ntok)
{
  int tps;
  int id;
  masterState *ms;
  workerCmd cmd;


  if (ntok != 2)
    {
      fprintf (stdout, "-ERR TPS <tps> <ID>\n");
      return;
    }

  tps = atoi (tokens[0]);
  id = atoi (tokens[1]);

  if (tps < 0)
    {
      fprintf (stdout, "-ERR invalid TPS:%d\n", tps);
      return;
    }
  else if (id < 0 || id > MAX_ID)
    {
      fprintf (stdout, "-ERR invalid ID:%d\n", id);
      return;
    }
  ms = &ms_ary[id];
  if (ms->used == 0)
    {
      fprintf (stdout, "-ERR id is not used:%d\n", id);
      return;
    }

  ms->arg.tps = tps;
  if (ms->thread_running)
    {
      cmd.command = CMD_TPS;
      cmd.ival = tps;
      do_worker_cmd (ms, &cmd);
    }

  fprintf (stdout, "+OK tps is set to %d%s\n", tps,
	   tps == 0 ? "(unlimited)" : "");
  return;
}

static void
user_cmd_quit (masterState * ms_ary, char **tokens, int ntok)
{
  int i;
  int count = 0;

  if (ntok != 0)
    {
      fprintf (stdout, "-ERR QUIT\n");
      return;
    }

  for (i = 0; i < MAX_ID; i++)
    {
      if (ms_ary[i].used)
	{
	  do_cmd_del (&ms_ary[i]);
	  count++;
	}
    }
  fprintf (stdout, "+OK %d\n", count);
  return;
}

/* ---- */
/* MAIN */
/* ---- */
int
main (int argc, char *argv[])
{
  int i;
  char line_buf[8192];
  static masterState ms[MAX_ID];

  for (i = 0; i < 1024; i++)
    {
      ks_crc[i] = -1;
    }

  for (i = 0; i < MAX_ID; i++)
    {
      init_master_state (&ms[i]);
    }

  while (fgets (line_buf, sizeof (line_buf), stdin) != NULL)
    {
      char **tokens = NULL;
      int ntok = 0, ret;

      //below function modifies line_buf
      ret = smrmp_parse_msg (line_buf, sizeof (line_buf), &tokens);
      assert (ret == 0);

      while (tokens[ntok] != NULL)
	{
	  ntok++;
	}

      if (ntok == 0)
	{
	  ;
	}
      else if (strcasecmp (tokens[0], "ADD") == 0)
	{
	  user_cmd_add (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "DEL") == 0)
	{
	  user_cmd_del (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "HELP") == 0)
	{
	  fprintf (stdout,
		   "+OK ADD DEL HELP INFO SIZE SLEEP START STAT STOP TPS QUIT\n");
	}
      else if (strcasecmp (tokens[0], "INFO") == 0)
	{
	  user_cmd_info (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "SIZE") == 0)
	{
	  user_cmd_size (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "SLEEP") == 0)
	{
	  user_cmd_sleep (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "START") == 0)
	{
	  user_cmd_start (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "STAT") == 0)
	{
	  user_cmd_stat (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "STOP") == 0)
	{
	  user_cmd_stop (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "TPS") == 0)
	{
	  user_cmd_tps (ms, tokens + 1, ntok - 1);
	}
      else if (strcasecmp (tokens[0], "QUIT") == 0)
	{
	  user_cmd_quit (ms, tokens + 1, ntok - 1);
	  smrmp_free_msg (tokens);
	  break;
	}
      else
	{
	  fprintf (stdout, "-ERR unsupported command: %s\n", tokens[0]);
	}
      smrmp_free_msg (tokens);
      fflush (stdout);
    }
  return 0;
}

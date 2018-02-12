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

#include <sys/types.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <limits.h>
#include <getopt.h>
#include <signal.h>
#include <fcntl.h>

#include "sds.h"
#include "arcci.h"

#define DEFAULT_QUERY_BUF_SIZE 8192
#define REDIS_CLOSE_AFTER_REPLY 1
#define REDIS_REQ_INLINE 1
#define REDIS_REQ_MULTIBULK 2
#define REDIS_INLINE_MAX_SIZE   (1024*64)	/* Max size of inline reads */
#define MAX_CLIENT_THREAD 16384
#define MAX_LISTEN_BACKLOG 8192
#define PIDFILE "local_proxy.pid"

/* Structures */
typedef struct client_t
{
  int fd;

  sds querybuf;
  sds replybuf;
  int argc;
  size_t *argvlen;
  sds *argv;

  int reqtype;
  int multibulklen;
  long bulklen;

  arc_request_t *rqst;

  int flags;

  int total_append_command;
} client_t;

typedef struct conf_t
{
  char *cluster_name;
  char *zk_addr;
  int port;
  int daemonize;
  int query_timeout_millis;
  int max_num_clients;
  arc_conf_t capi_conf;
} conf_t;

typedef struct arc_ref_t
{
  arc_t *arc;
  int refcount;
} arc_ref_t;

typedef struct server_t
{
  /* Server accept FD */
  int fd;

  /* arc_t of CAPI globally accessible by clients */
  pthread_mutex_t reconfig_lock;
  arc_ref_t *arc_ref;

  /* thread id of reconfig thread */
  pthread_t reconfig_tid;
  /* array of thread id of client threads */
  pthread_t *client_tid;

  /* signal related variables */
  int reconfig_signal;
  int shutdown_signal;

  /* config file name */
  char *config_file;

  /* CAPI configuration */
  conf_t *conf;

  /* CAPI query timeout */
  int query_timeout_millis;

  /* maximun number of clients to accept */
  int max_num_clients;
  int num_clients;
} server_t;

server_t server;

/* Utils */
static void
usage ()
{
  fprintf (stderr, "\n" "Usage: local_proxy <config file>\n");
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
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

static void
createPidFile (void)
{
  /* Try to write the pid file in a best-effort way. */
  FILE *fp = fopen (PIDFILE, "w");
  if (fp)
    {
      fprintf (fp, "%d\n", (int) getpid ());
      fclose (fp);
    }
}

static void
daemonize ()
{
  int fd;

  if (fork () != 0)
    exit (0);
  setsid ();

  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1)
    {
      dup2 (fd, STDIN_FILENO);
      dup2 (fd, STDOUT_FILENO);
      dup2 (fd, STDERR_FILENO);
      if (fd > STDERR_FILENO)
	close (fd);
    }
}

static int
initFromConfigBuffer (sds config, conf_t * conf)
{
  char *err = NULL;
  int linenum = 0, totlines, i;
  sds *lines;

  lines = sdssplitlen (config, sdslen (config), "\n", 1, &totlines);

  for (i = 0; i < totlines; i++)
    {
      sds *argv;
      int argc;

      linenum = i + 1;
      lines[i] = sdstrim (lines[i], " \t\r\n");

      /* Skip comments and blank lines */
      if (lines[i][0] == '#' || lines[i][0] == '\0')
	continue;

      /* Split into arguments */
      argv = sdssplitargs (lines[i], &argc);
      if (argv == NULL)
	{
	  err = "Unbalanced quotes in configuration line";
	  goto err;
	}

      /* Skip this line if the resulting command vector is empty. */
      if (argc == 0)
	{
	  sdsfreesplitres (argv, argc);
	  continue;
	}
      sdstolower (argv[0]);

      if (!strcasecmp (argv[0], "zookeeper") && argc == 2)
	{
	  conf->zk_addr = strdup (argv[1]);
	}
      else if (!strcasecmp (argv[0], "cluster_name") && argc == 2)
	{
	  conf->cluster_name = strdup (argv[1]);
	}
      else if (!strcasecmp (argv[0], "port") && argc == 2)
	{
	  conf->port = atoi (argv[1]);
	  if (conf->port < 0 || conf->port > 65535)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid port";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "daemonize") && argc == 2)
	{
	  if (!strcasecmp (argv[1], "yes"))
	    {
	      conf->daemonize = 1;
	    }
	  else if (!strcasecmp (argv[1], "no"))
	    {
	      conf->daemonize = 0;
	    }
	  else
	    {
	      sdsfreesplitres (argv, argc);
	      err = "argument must be 'yes' or 'no'";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "num_conn_per_gw") && argc == 2)
	{
	  conf->capi_conf.num_conn_per_gw = atoi (argv[1]);
	  if (conf->capi_conf.num_conn_per_gw < 1
	      || conf->capi_conf.num_conn_per_gw > 32)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid num_conn_per_gw value";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "init_timeout_millis") && argc == 2)
	{
	  conf->capi_conf.init_timeout_millis = atoi (argv[1]);
	  if (conf->capi_conf.init_timeout_millis < 3000
	      || conf->capi_conf.init_timeout_millis > 600000)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid init_timeout_millis value";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "log_level") && argc == 2)
	{
	  if (!strcasecmp (argv[1], "NOLOG"))
	    {
	      conf->capi_conf.log_level = ARC_LOG_LEVEL_NOLOG;
	    }
	  else if (!strcasecmp (argv[1], "ERROR"))
	    {
	      conf->capi_conf.log_level = ARC_LOG_LEVEL_ERROR;
	    }
	  else if (!strcasecmp (argv[1], "WARN"))
	    {
	      conf->capi_conf.log_level = ARC_LOG_LEVEL_WARN;
	    }
	  else if (!strcasecmp (argv[1], "INFO"))
	    {
	      conf->capi_conf.log_level = ARC_LOG_LEVEL_INFO;
	    }
	  else if (!strcasecmp (argv[1], "DEBUG"))
	    {
	      conf->capi_conf.log_level = ARC_LOG_LEVEL_DEBUG;
	    }
	  else
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid log_level value";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "log_file_prefix") && argc == 2)
	{
	  if (argv[1][0] != '\0')
	    {
	      conf->capi_conf.log_file_prefix = strdup (argv[1]);
	    }
	}
      else if (!strcasecmp (argv[0], "max_fd") && argc == 2)
	{
	  conf->max_num_clients = atoi (argv[1]);
	  if (conf->max_num_clients < 1024 || conf->max_num_clients > 16000)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid max_fd value";
	      goto err;
	    }
	  // client conns + gw conns
	  conf->capi_conf.max_fd = conf->max_num_clients * 2;
	}
      else if (!strcasecmp (argv[0], "conn_reconnect_millis") && argc == 2)
	{
	  conf->capi_conf.conn_reconnect_millis = atoi (argv[1]);
	  if (conf->capi_conf.conn_reconnect_millis < 100
	      || conf->capi_conf.conn_reconnect_millis > 600000)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid conn_reconnect_millis value";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "zk_reconnect_millis") && argc == 2)
	{
	  conf->capi_conf.zk_reconnect_millis = atoi (argv[1]);
	  if (conf->capi_conf.zk_reconnect_millis < 100
	      || conf->capi_conf.zk_reconnect_millis > 600000)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid zk_reconnect_millis value";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "zk_session_timeout_millis")
	       && argc == 2)
	{
	  conf->capi_conf.zk_session_timeout_millis = atoi (argv[1]);
	  if (conf->capi_conf.zk_session_timeout_millis < 1000
	      || conf->capi_conf.zk_session_timeout_millis > 600000)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid zk_session_timeout_millis value";
	      goto err;
	    }
	}
      else if (!strcasecmp (argv[0], "local_proxy_query_timeout_millis")
	       && argc == 2)
	{
	  conf->query_timeout_millis = atoi (argv[1]);
	  if (conf->query_timeout_millis < 1000
	      || conf->query_timeout_millis > 600000)
	    {
	      sdsfreesplitres (argv, argc);
	      err = "Invalid local_proxy_query_timeout_millis";
	      goto err;
	    }
	}
      else
	{
	  sdsfreesplitres (argv, argc);
	  err = "Bad directive or wrong number of arguments";
	  goto err;
	}
      sdsfreesplitres (argv, argc);
    }
  sdsfreesplitres (lines, totlines);
  return 0;

err:
  fprintf (stderr, "\n*** FATAL CONFIG FILE ERROR ***\n");
  fprintf (stderr, "Reading the configuration file, at line %d\n", linenum);
  fprintf (stderr, ">>> '%s'\n", lines[i]);
  fprintf (stderr, "%s\n", err);
  sdsfreesplitres (lines, totlines);
  return -1;
}

static void
initializeConfigStructure (conf_t * conf)
{
  conf->cluster_name = NULL;
  conf->zk_addr = NULL;
  conf->port = -1;
  conf->daemonize = 0;
  conf->query_timeout_millis = 3000;
  conf->max_num_clients = 4096;
  arc_init_conf (&conf->capi_conf);
  conf->capi_conf.max_fd = conf->max_num_clients * 2;
}

static void
freeConfigStructure (conf_t * conf)
{
  if (conf == NULL)
    return;

  if (conf->cluster_name != NULL)
    {
      free (conf->cluster_name);
    }
  if (conf->zk_addr != NULL)
    {
      free (conf->zk_addr);
    }
  if (conf->capi_conf.log_file_prefix != NULL)
    {
      free (conf->capi_conf.log_file_prefix);
    }
  free (conf);
}

static conf_t *
initFromConfigFile ()
{
  FILE *fp;
  char buf[1024];
  sds config = sdsempty ();
  conf_t *conf;

  if ((fp = fopen (server.config_file, "r")) == NULL)
    {
      perror ("Can not open config file");
      sdsfree (config);
      return NULL;
    }

  while (fgets (buf, 1024, fp) != NULL)
    config = sdscat (config, buf);
  fclose (fp);

  conf = malloc (sizeof (conf_t));
  initializeConfigStructure (conf);

  if (initFromConfigBuffer (config, conf) == -1)
    {
      perror ("Invalid format of config file");
      sdsfree (config);
      freeConfigStructure (conf);
      return NULL;
    }

  sdsfree (config);

  if (conf->cluster_name == NULL || conf->zk_addr == NULL || conf->port == -1)
    {
      perror
	("Required properties are missing. (cluster_name, zookeeper addr, port)");
      freeConfigStructure (conf);
      return NULL;
    }

  return conf;
}

static arc_ref_t *
initializeArcRef ()
{
  arc_ref_t *arc_ref;

  arc_ref = malloc (sizeof (arc_ref_t));
  if (arc_ref == NULL)
    {
      perror ("Can not allocate memory");
      return NULL;
    }

  arc_ref->arc = NULL;
  arc_ref->refcount = 0;
  return arc_ref;
}

static arc_ref_t *
acquire_arc_ref ()
{
  arc_ref_t *arc_ref;

  pthread_mutex_lock (&server.reconfig_lock);
  server.arc_ref->refcount++;
  arc_ref = server.arc_ref;
  pthread_mutex_unlock (&server.reconfig_lock);

  return arc_ref;
}

static void
release_arc_ref (arc_ref_t * arc_ref)
{
  int delete_arc_ref = 0;

  pthread_mutex_lock (&server.reconfig_lock);
  arc_ref->refcount--;
  if (arc_ref->refcount == 0)
    {
      delete_arc_ref = 1;
    }
  pthread_mutex_unlock (&server.reconfig_lock);

  if (delete_arc_ref)
    {
      arc_destroy (arc_ref->arc);
      free (arc_ref);
    }
}

static void
initConfig (int argc, char **argv)
{
  /* Initialize */
  server.fd = -1;

  if (pthread_mutex_init (&server.reconfig_lock, NULL) != 0)
    {
      perror ("Can not initialize pthread_mutex");
      exit (EXIT_FAILURE);
    }
  server.arc_ref = initializeArcRef ();
  if (server.arc_ref == NULL)
    {
      perror ("Can not initialize arc_ref_t");
      exit (EXIT_FAILURE);
    }
  server.reconfig_tid = 0;
  server.client_tid = NULL;
  server.reconfig_signal = 0;
  server.shutdown_signal = 0;
  server.config_file = NULL;
  server.conf = NULL;

  /* Config file */
  if (argc != 2)
    {
      usage ();
      exit (EXIT_FAILURE);
    }

  server.config_file = strdup (argv[1]);
  server.conf = initFromConfigFile ();
  if (server.conf == NULL)
    {
      perror ("Can not initialize from config file");
      exit (EXIT_FAILURE);
    }
  server.query_timeout_millis = server.conf->query_timeout_millis;
  server.max_num_clients = server.conf->max_num_clients;
  server.num_clients = 0;
}

static arc_t *
initCapi (conf_t * conf)
{
  arc_t *arc;

  arc = arc_new_zk (conf->zk_addr, conf->cluster_name, &conf->capi_conf);

  return arc;
}

static void *
reconfigThread (arc_t * initial_arc)
{
  pthread_mutex_lock (&server.reconfig_lock);
  server.arc_ref->arc = initial_arc;
  server.arc_ref->refcount = 1;
  pthread_mutex_unlock (&server.reconfig_lock);

  while (!server.shutdown_signal)
    {
      /* Set reconfig arc_t */
      if (server.reconfig_signal)
	{
	  conf_t *new_conf, *tmp_conf;
	  arc_ref_t *new_arc_ref, *tmp_arc_ref;

	  /* initialize config setting from updated conf file */
	  new_conf = initFromConfigFile ();
	  if (new_conf == NULL
	      || strcmp (server.conf->cluster_name, new_conf->cluster_name)
	      || server.conf->port != new_conf->port
	      || server.conf->daemonize != new_conf->daemonize)
	    {
	      perror
		("Reconfig canceled. Can not change certain properties(cluster name, port, daemonize)");
	      freeConfigStructure (new_conf);
	      server.reconfig_signal = 0;
	      continue;
	    }

	  /* initialize new arc_t from new Configuration */
	  new_arc_ref = initializeArcRef ();
	  if (new_arc_ref == NULL)
	    {
	      perror
		("Reconfig canceled, Can not allocate memory for arc_ref_t");
	      freeConfigStructure (new_conf);
	      server.reconfig_signal = 0;
	      continue;
	    }

	  new_arc_ref->arc = initCapi (new_conf);
	  if (new_arc_ref->arc == NULL)
	    {
	      perror ("Reconfig canceled, Can not create new arc_t");
	      freeConfigStructure (new_conf);
	      free (new_arc_ref);
	      server.reconfig_signal = 0;
	      continue;
	    }

	  /* Set reconfiguration arc_t */
	  pthread_mutex_lock (&server.reconfig_lock);
	  tmp_arc_ref = server.arc_ref;
	  server.arc_ref = new_arc_ref;
	  server.arc_ref->refcount = 1;
	  tmp_conf = server.conf;
	  server.conf = new_conf;
	  server.query_timeout_millis = server.conf->query_timeout_millis;
	  server.max_num_clients = server.conf->max_num_clients;
	  pthread_mutex_unlock (&server.reconfig_lock);

	  release_arc_ref (tmp_arc_ref);
	  freeConfigStructure (tmp_conf);

	  server.reconfig_signal = 0;
	}
      sleep (1);
    }
  return NULL;
}

static int
initServer ()
{
  int sfd, on = 1;
  struct sockaddr_in sa;
  arc_t *arc;

  /* Daemonize */
  if (server.conf->daemonize)
    {
      daemonize ();
      createPidFile ();
    }

  /* Init CAPI */
  arc = initCapi (server.conf);
  if (arc == NULL)
    return -1;

  /* Init server socket */
  if ((sfd = socket (AF_INET, SOCK_STREAM, 0)) == -1)
    {
      perror ("creating socket");
      return -1;
    }

  if (setsockopt (sfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof (on)) == -1)
    {
      perror ("setsockopt SO_REUSEADDR");
      return -1;
    }

  memset (&sa, 0, sizeof (sa));
  sa.sin_family = AF_INET;
  sa.sin_port = htons (server.conf->port);
  sa.sin_addr.s_addr = htonl (INADDR_ANY);

  if (bind (sfd, (struct sockaddr *) &sa, sizeof (sa)) == -1)
    {
      perror ("bind");
      close (sfd);
      return -1;
    }

  if (listen (sfd, MAX_LISTEN_BACKLOG) == -1)
    {
      perror ("listen");
      close (sfd);
      return -1;
    }

  server.fd = sfd;

  /* Create arrays of client thread ids */
  server.client_tid = calloc (MAX_CLIENT_THREAD, sizeof (pthread_t));

  /* Initialize reconfig thread */
  pthread_create (&server.reconfig_tid, NULL, reconfigThread, arc);

  return 0;
}

static void
resetClient (client_t * c)
{
  int i;

  c->reqtype = 0;
  c->multibulklen = 0;
  c->bulklen = -1;

  if (c->argv)
    {
      for (i = 0; i < c->argc; i++)
	{
	  if (c->argv[i])
	    sdsfree (c->argv[i]);
	}
      free (c->argv);
      c->argv = NULL;
    }
  c->argc = 0;
  if (c->argvlen)
    {
      free (c->argvlen);
      c->argvlen = NULL;
    }
}

static void
freeClient (client_t * c)
{
  sdsfree (c->querybuf);
  sdsfree (c->replybuf);
  arc_free_request (c->rqst);
  resetClient (c);
  close (c->fd);
  free (c);
  pthread_mutex_lock (&server.reconfig_lock);
  server.num_clients--;
  pthread_mutex_unlock (&server.reconfig_lock);
  pthread_exit (0);
}

static void
addReplyStr (client_t * c, char *str)
{
  c->replybuf = sdscatlen (c->replybuf, str, strlen (str));
}

static void
addReplyStrLen (client_t * c, char *str, size_t len)
{
  c->replybuf = sdscatlen (c->replybuf, str, len);
}

static void
addReplyLongLongWithPrefix (client_t * c, long long ll, char prefix)
{
  c->replybuf = sdscatprintf (c->replybuf, "%c%lld\r\n", prefix, ll);
}

static void
addReplyFromCapi (client_t * c, arc_reply_t * reply)
{
  int i;

  switch (reply->type)
    {
    case ARC_REPLY_ERROR:
      addReplyStrLen (c, reply->d.error.str, reply->d.error.len);
      addReplyStrLen (c, "\r\n", 2);
      break;
    case ARC_REPLY_STATUS:
      addReplyStrLen (c, reply->d.status.str, reply->d.status.len);
      addReplyStrLen (c, "\r\n", 2);
      break;
    case ARC_REPLY_INTEGER:
      addReplyLongLongWithPrefix (c, reply->d.integer.val, ':');
      break;
    case ARC_REPLY_STRING:
      addReplyLongLongWithPrefix (c, reply->d.string.len, '$');
      addReplyStrLen (c, reply->d.string.str, reply->d.string.len);
      addReplyStrLen (c, "\r\n", 2);
      break;
    case ARC_REPLY_ARRAY:
      addReplyLongLongWithPrefix (c, reply->d.array.len, '*');
      for (i = 0; i < reply->d.array.len; i++)
	{
	  addReplyFromCapi (c, reply->d.array.elem[i]);
	}
      break;
    case ARC_REPLY_NIL:
      addReplyStrLen (c, "$-1\r\n", 5);
      break;
    }
}

static int
processReply (client_t * c, int *be_errno)
{
  arc_reply_t *reply;
  int ret;

  /* Make reply buffer */
  while (1)
    {
      ret = arc_get_reply (c->rqst, &reply, be_errno);
      if (ret == -1)
	{
	  return -1;
	}
      if (!reply)
	{
	  break;
	}
      addReplyFromCapi (c, reply);
      arc_free_reply (reply);
      c->total_append_command--;
    }
  return 0;
}

static int
processMultiBulk (client_t * c)
{
  char *newline = NULL;
  int pos = 0, ok;
  long long ll;

  if (c->multibulklen == 0)
    {
      newline = strchr (c->querybuf, '\r');
      if (newline == NULL)
	{
	  if (sdslen (c->querybuf) > REDIS_INLINE_MAX_SIZE)
	    {
	      addReplyStr (c,
			   "-ERR Protocol error: too big mbulk count string\r\n");
	      c->flags |= REDIS_CLOSE_AFTER_REPLY;
	    }
	  return -1;
	}
      if (newline - c->querybuf > sdslen (c->querybuf) - 2)
	return -1;

      ok = string2ll (c->querybuf + 1, newline - (c->querybuf + 1), &ll);
      if (!ok || ll > 1024 * 1024)
	{
	  addReplyStr (c,
		       "-ERR Protocol error: invalid multibulk length\r\n");
	  c->flags |= REDIS_CLOSE_AFTER_REPLY;
	  return -1;
	}
      c->multibulklen = ll;
      c->argv = malloc (sizeof (sds) * c->multibulklen);
      c->argvlen = malloc (sizeof (size_t) * c->multibulklen);
      pos = (newline - c->querybuf) + 2;
      if (ll <= 0)
	{
	  sdsrange (c->querybuf, pos, -1);
	  return 0;
	}
    }

  while (c->multibulklen)
    {
      if (c->bulklen == -1)
	{
	  newline = strchr (c->querybuf + pos, '\r');
	  if (newline == NULL)
	    {
	      if (sdslen (c->querybuf) > REDIS_INLINE_MAX_SIZE)
		{
		  addReplyStr (c,
			       "-ERR Protocol error: too big bulk count string\r\n");
		  c->flags |= REDIS_CLOSE_AFTER_REPLY;
		}
	      break;
	    }
	  if (newline - (c->querybuf) > (sdslen (c->querybuf) - 2))
	    break;

	  if (c->querybuf[pos] != '$')
	    {
	      addReplyStr (c, "-ERR Protocol error: expected '$', got '");
	      addReplyStrLen (c, c->querybuf + pos, 1);
	      addReplyStr (c, "'\r\n");
	      c->flags |= REDIS_CLOSE_AFTER_REPLY;
	      return -1;
	    }

	  ok =
	    string2ll (c->querybuf + pos + 1,
		       newline - (c->querybuf + pos + 1), &ll);
	  if (!ok || ll < 0 || ll > 512 * 1024 * 1024)
	    {
	      addReplyStr (c, "-ERR Protocol error: invalid bulk length\r\n");
	      c->flags |= REDIS_CLOSE_AFTER_REPLY;
	      return -1;
	    }

	  pos += newline - (c->querybuf + pos) + 2;
	  c->bulklen = ll;
	}

      if (sdslen (c->querybuf) - pos < (unsigned) (c->bulklen + 2))
	break;
      c->argvlen[c->argc] = c->bulklen;
      c->argv[c->argc++] = sdsnewlen (c->querybuf + pos, c->bulklen);
      pos += c->bulklen + 2;
      c->bulklen = -1;
      c->multibulklen--;
    }

  if (pos)
    sdsrange (c->querybuf, pos, -1);
  if (c->multibulklen)
    return -1;

  if (c->argc == 1 && sdslen (c->argv[0]) == 4
      && !strcasecmp (c->argv[0], "quit"))
    {
      addReplyStrLen (c, "+OK\r\n", 5);
      c->flags |= REDIS_CLOSE_AFTER_REPLY;
      return -1;
    }
  arc_append_commandcv (c->rqst, c->argc, (const char **) c->argv,
			c->argvlen);
  c->total_append_command++;
  return 0;
}

static int
processInline (client_t * c)
{
  char *newline;
  int count, argc, j, has_cr, ret;
  sds *split, *argv, aux;
  size_t querylen, *argvlen;

  newline = strchr (c->querybuf, '\n');
  has_cr = 0;

  if (newline == NULL)
    {
      if (sdslen (c->querybuf) > REDIS_INLINE_MAX_SIZE)
	{
	  addReplyStr (c, "-ERR Protocol error: too big inline request\r\n");
	  c->flags |= REDIS_CLOSE_AFTER_REPLY;
	}
      return -1;
    }

  if (newline != c->querybuf && *(newline - 1) == '\r')
    {
      newline--;
      has_cr = 1;
    }

  querylen = newline - (c->querybuf);
  aux = sdsnewlen (c->querybuf, querylen);
  split = sdssplitargs (aux, &count);
  sdsfree (aux);
  if (split == NULL)
    {
      addReplyStr (c,
		   "-ERR Protocol error: unbalanced quotes in request\r\n");
      c->flags |= REDIS_CLOSE_AFTER_REPLY;
      return -1;
    }

  argv = malloc (sizeof (sds) * count);
  argvlen = malloc (sizeof (size_t) * count);
  argc = 0;
  for (j = 0; j < count; j++)
    {
      if (sdslen (split[j]))
	{
	  argv[argc] = sdsdup (split[j]);
	  argvlen[argc] = sdslen (split[j]);
	  argc++;
	}
    }
  sdsfreesplitres (split, count);

  if (argc == 1 && !strcasecmp (argv[0], "quit"))
    {
      addReplyStrLen (c, "+OK\r\n", 5);
      c->flags |= REDIS_CLOSE_AFTER_REPLY;
      ret = -1;
      goto close_return;
    }

  ret = 0;
  if (argc > 0)
    {
      arc_append_commandcv (c->rqst, argc, (const char **) argv, argvlen);
      c->total_append_command++;
    }

close_return:

  while (argc--)
    {
      sdsfree (argv[argc]);
    }
  free (argv);
  free (argvlen);

  sdsrange (c->querybuf, newline - c->querybuf + 1 + has_cr, -1);
  return ret;
}

static void
processInputBuffer (client_t * c)
{
  while (sdslen (c->querybuf))
    {
      if (!c->reqtype)
	{
	  if (c->querybuf[0] == '*')
	    {
	      c->reqtype = REDIS_REQ_MULTIBULK;
	    }
	  else
	    {
	      c->reqtype = REDIS_REQ_INLINE;
	    }
	}

      if (c->reqtype == REDIS_REQ_MULTIBULK)
	{
	  if (processMultiBulk (c) == -1)
	    return;
	}
      else
	{
	  if (processInline (c) == -1)
	    return;
	}
      resetClient (c);
    }
}

static void *
clientThread (void *arg)
{
  client_t *c = (client_t *) arg;
  struct pollfd pfd;
  int ret;

  pthread_mutex_lock (&server.reconfig_lock);
  server.num_clients++;
  pthread_mutex_unlock (&server.reconfig_lock);

  c->querybuf = sdsMakeRoomFor (sdsempty (), DEFAULT_QUERY_BUF_SIZE);
  c->replybuf = sdsMakeRoomFor (sdsempty (), DEFAULT_QUERY_BUF_SIZE);
  c->argc = 0;
  c->argv = NULL;
  c->argvlen = NULL;
  c->reqtype = 0;
  c->multibulklen = 0;
  c->bulklen = -1;
  c->rqst = arc_create_request ();
  c->flags = 0;
  c->total_append_command = 0;

  pfd.fd = c->fd;

  while (1)
    {
      pfd.events = 0;
      if (!(c->flags & REDIS_CLOSE_AFTER_REPLY))
	{
	  pfd.events |= POLLIN;
	}
      if (sdslen (c->replybuf) > 0)
	{
	  pfd.events |= POLLOUT;
	}
      ret = poll (&pfd, 1, 1000);
      if (ret == -1)
	{
	  perror ("poll");
	  freeClient (c);
	}

      if (server.shutdown_signal)
	{
	  c->flags |= REDIS_CLOSE_AFTER_REPLY;
	}

      /* readable */
      if (pfd.revents & POLLIN)
	{
	  int pos = sdslen (c->querybuf);
	  int avail = sdsavail (c->querybuf);
	  ssize_t nread;

	  if (avail == 0)
	    {
	      c->querybuf =
		sdsMakeRoomFor (c->querybuf, sdslen (c->querybuf));
	      avail = sdsavail (c->querybuf);
	    }

	  nread = read (c->fd, c->querybuf + pos, avail);
	  if (nread > 0)
	    {
	      sdsIncrLen (c->querybuf, nread);
	      processInputBuffer (c);
	      if (c->total_append_command)
		{
		  int arc_errno, arc_be_errno, be_errno;
		  arc_ref_t *arc_ref;

		  arc_ref = acquire_arc_ref ();
		  ret =
		    arc_do_request (arc_ref->arc, c->rqst,
				    server.query_timeout_millis, &be_errno);
		  if (ret == -1)
		    {
		      arc_errno = errno;
		      arc_be_errno = be_errno;
		    }
		  else
		    {
		      ret = processReply (c, &be_errno);
		      if (ret == -1)
			{
			  arc_errno = errno;
			  arc_be_errno = be_errno;
			}
		    }
		  arc_free_request (c->rqst);
		  release_arc_ref (arc_ref);

		  c->rqst = arc_create_request ();

		  if (ret == -1)
		    {
		      if (arc_errno == ARC_ERR_TIMEOUT
			  || (arc_errno == ARC_ERR_BACKEND
			      && arc_be_errno == ARC_ERR_TIMEOUT))
			{
			  addReplyStr (c, "-ERR Redis Timeout\r\n");
			}
		      else
			{
			  addReplyStr (c, "-ERR Internal Error\r\n");
			}
		      c->flags |= REDIS_CLOSE_AFTER_REPLY;
		    }
		}
	    }
	  else
	    {
	      if (nread == -1 && errno == EAGAIN)
		{
		  /* Skip */
		}
	      else
		{
		  freeClient (c);
		}
	    }
	}

      /* writable */
      if (pfd.revents & POLLOUT)
	{
	  int pos = 0;
	  int avail = sdslen (c->replybuf);
	  ssize_t nwritten;

	  nwritten = write (c->fd, c->replybuf + pos, avail);
	  if (nwritten > 0)
	    {
	      avail -= nwritten;
	      pos += nwritten;
	      sdsrange (c->replybuf, pos, -1);
	    }
	  else
	    {
	      if (nwritten == -1 && errno == EAGAIN)
		{
		  /* Skip */
		}
	      else
		{
		  freeClient (c);
		}
	    }
	}

      if (sdslen (c->replybuf) == 0 && (c->flags & REDIS_CLOSE_AFTER_REPLY))
	{
	  freeClient (c);
	}
    }
  return NULL;
}

static int
setSocketNonBlock (int fd)
{
  int flags;

  if ((flags = fcntl (fd, F_GETFL)) == -1)
    {
      return -1;
    }
  if (fcntl (fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
      return -1;
    }
  return 0;
}

static int
setSocketTcpNoDelay (int fd)
{
  int yes = 1;

  if (setsockopt (fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof (yes)) == -1)
    {
      return -1;
    }
  return 0;
}

static int
runServer ()
{
  int i;

  while (!server.shutdown_signal)
    {
      int cfd, ret;
      struct sockaddr_in sa;
      socklen_t salen = sizeof (sa);;
      client_t *c;
      int can_accept;

      /* accpet */
      cfd = accept (server.fd, (struct sockaddr *) &sa, &salen);
      if (cfd == -1)
	{
	  if (errno == EINTR)
	    {
	      if (server.shutdown_signal)
		{
		  break;
		}
	      else
		{
		  continue;
		}
	    }
	  else
	    {
	      perror ("accept");
	      return -1;
	    }
	}

      /* pthread join */
      if (server.client_tid[cfd])
	{
	  ret = pthread_join (server.client_tid[cfd], NULL);
	  if (ret != 0)
	    {
	      perror ("pthread_join failed");
	      return -1;
	    }
	  server.client_tid[cfd] = 0;
	}

      /* limit client conn */
      pthread_mutex_lock (&server.reconfig_lock);
      can_accept = (server.num_clients < server.max_num_clients);
      pthread_mutex_unlock (&server.reconfig_lock);
      if (!can_accept)
	{
	  char *err = "-ERR max number of clients reached\r\n";
	  (void) write (cfd, err, strlen (err));
	  close (cfd);
	  continue;
	}

      /* set sock options */
      if (setSocketNonBlock (cfd) == -1)
	{
	  perror ("Set socket nonblock");
	  return -1;
	}
      if (setSocketTcpNoDelay (cfd) == -1)
	{
	  perror ("Set socket tcp_nodelay");
	  return -1;
	}

      /* create client thread */
      c = malloc (sizeof (client_t));
      c->fd = cfd;
      ret = pthread_create (&server.client_tid[cfd], NULL, clientThread, c);
      if (ret != 0)
	{
	  perror ("pthread_create failed");
	  return -1;
	}
    }
  close (server.fd);

  for (i = 0; i < MAX_CLIENT_THREAD; i++)
    {
      if (server.client_tid[i] != 0)
	{
	  pthread_join (server.client_tid[i], NULL);
	  server.client_tid[i] = 0;
	}
    }
  pthread_join (server.reconfig_tid, NULL);
  return 0;
}

static void
finalizeServer ()
{
  release_arc_ref (server.arc_ref);
  free (server.client_tid);
  free (server.config_file);
  freeConfigStructure (server.conf);
}

static void
sigtermHandler (int sig)
{
  if (server.conf->daemonize)
    {
      unlink (PIDFILE);
    }
  server.shutdown_signal = 1;
}

/* Reload Configuration */
static void
sighupHandler (int sig)
{
  server.reconfig_signal = 1;
}

static void
setupSignalHandlers ()
{
  struct sigaction act;

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sigtermHandler;
  sigaction (SIGTERM, &act, NULL);

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sighupHandler;
  sigaction (SIGHUP, &act, NULL);
}

int
main (int argc, char **argv)
{
  /* Signal 
   * -TERM : Gracefully shutdown 
   * -HUP : Reload configuration file */
  signal (SIGPIPE, SIG_IGN);
  setupSignalHandlers ();

  initConfig (argc, argv);
  if (initServer () == -1)
    exit (EXIT_FAILURE);
  if (runServer () == -1)
    exit (EXIT_FAILURE);
  finalizeServer ();

  exit (EXIT_SUCCESS);
}

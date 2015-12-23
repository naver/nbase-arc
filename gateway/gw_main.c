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

#include "gw_main.h"

global_conf_t global;
static master_t master;

static void
sigterm_handler (int sig)
{
  GW_NOTUSED (sig);
  global.shutdown_asap = 1;
}

static void
setup_signal_handler (void)
{
  struct sigaction act;

  signal (SIGHUP, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sigterm_handler;
  sigaction (SIGTERM, &act, NULL);
  sigaction (SIGINT, &act, NULL);

  // TODO Add signal handler for SIGSEGV which prints stack backtrace.
}

static const char *usage =
  "Usage: %s <options>                          \n"
  "option:                                      \n"
  "    -p: gateway port                         \n"
  "    -w: the number of workers                \n"
  "    -t: TCP listen backlog (min = 512)       \n"
  "    -D: daemonize (default false)            \n"
  "    -l: logfile prefix (default gateway)     \n"
  "    -c: conf master addr                     \n"
  "    -b: conf master port                     \n"
  "    -d: prefer domain socket                 \n"
  "    -n: cluster name                         \n";

static void
print_usage (char *prog)
{
  fprintf (stderr, usage, prog);
}

static void
print_cluster_configuration (cluster_conf * conf)
{
  sds buf = get_cluster_info_sds (conf);
  sds *lines;
  int nline, i;

  lines = sdssplitlen (buf, sdslen (buf), "\r\n", 2, &nline);

  for (i = 0; i < nline; i++)
    {
      gwlog (GW_NOTICE, "%s", lines[i]);
    }

  sdsfreesplitres (lines, nline);
  sdsfree (buf);
}

static void
daemonize (void)
{
  int fd;

  if (fork () != 0)
    {
      exit (0);
    }
  setsid ();

  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1)
    {
      dup2 (fd, STDIN_FILENO);
      dup2 (fd, STDOUT_FILENO);
      dup2 (fd, STDERR_FILENO);
      if (fd > STDERR_FILENO)
	{
	  close (fd);
	}
    }
}

static void
add_client (void *arg, void *privdata)
{
  int *cfd = arg;
  worker *w = privdata;

  cli_add_client (w->cli, *cfd);
  zfree (arg);
}

#define MAX_ACCEPTS_PER_CALL 1000
static void
user_accept_tcp_handler (aeEventLoop * el, int fd, void *privdata, int mask)
{
  int max = MAX_ACCEPTS_PER_CALL;
  char ip[INET6_ADDRSTRLEN];
  char ebuf[ANET_ERR_LEN];
  int cfd, cport;
  async_chan *async;
  int *data;
  int i, min_idx, min_conn, sum_conn;
  GW_NOTUSED (el);
  GW_NOTUSED (privdata);
  GW_NOTUSED (mask);

  while (max--)
    {
      cfd = anetTcpAccept (ebuf, fd, ip, sizeof (ip), &cport);
      if (cfd == ANET_ERR)
	{
	  if (errno != EWOULDBLOCK)
	    {
	      gwlog (GW_WARNING, "Accepting cleint connection: %s\n", ebuf);
	    }
	  return;
	}

      // Find worker with minimum connection and check that total connection
      // never exceeds limit
      min_idx = 0;
      min_conn = INT_MAX;
      sum_conn = 0;
      for (i = 0; i < ARRAY_N (&master.workers); i++)
	{
	  worker *wrk = ARRAY_GET (&master.workers, i);

	  if (wrk->cli->nclient < min_conn)
	    {
	      min_conn = wrk->cli->nclient;
	      min_idx = i;
	    }
	  sum_conn += wrk->cli->nclient;
	}

      if (sum_conn >= global.maxclients)
	{
	  const char *err = "-ERR max number of clients reached\r\n";
	  // That's a best effort error message, don't check write errors
	  if (write (cfd, err, strlen (err)) == -1)
	    {
	      // Nothing to do, Just to avoid the warning...
	    }
	  gwlog (GW_WARNING, "ERROR max number of clients reached");
	  close (cfd);
	  return;
	}

      async = ARRAY_GET (&master.worker_asyncs, min_idx);
      data = zmalloc (sizeof (int));
      *data = cfd;
      async_send_event (el, async, add_client, data);
    }
}

static void
admin_accept_tcp_handler (aeEventLoop * el, int fd, void *privdata, int mask)
{
  int max = MAX_ACCEPTS_PER_CALL;
  char ip[INET6_ADDRSTRLEN];
  char ebuf[ANET_ERR_LEN];
  int cfd, cport;
  GW_NOTUSED (el);
  GW_NOTUSED (privdata);
  GW_NOTUSED (mask);

  while (max--)
    {
      cfd = anetTcpAccept (ebuf, fd, ip, sizeof (ip), &cport);
      if (cfd == ANET_ERR)
	{
	  if (errno != EWOULDBLOCK)
	    {
	      gwlog (GW_WARNING, "Accepting cleint connection: %s\n", ebuf);
	    }
	  return;
	}
      cli_add_client (master.admin_cli, cfd);
    }
}

static int
master_cron (struct aeEventLoop *el, long long id, void *privdata)
{
  GW_NOTUSED (id);
  GW_NOTUSED (privdata);

  if (global.shutdown_asap)
    {
      gwlog (GW_NOTICE,
	     "Received SIGTERM, master shuts down and waits for all workers to terminate.");
      aeStop (el);
    }

  return 1000 / master.hz;
}

static void
init_config (int argc, char **argv)
{
  const char *confmaster_addr;
  char *logfile_prefix = NULL;
  int confmaster_port, ret;

  global.port = GW_DEFAULT_SERVERPORT;
  global.admin_port = global.port + 1;
  global.nworker = GW_DEFAULT_WORKERS;
  global.tcp_backlog = GW_DEFAULT_BACKLOG;
  global.maxclients = GW_DEFAULT_MAXCLIENTS;
  global.verbosity = GW_NOTICE;
  global.timeout_ms = GW_DEFAULT_TIMEOUT_MS;
  global.cluster_name = NULL;
  global.nbase_arc_home = NULL;
  global.conf = NULL;
  global.daemonize = 0;
  global.shutdown_asap = 0;
  global.prefer_domain_socket = 0;

  confmaster_addr = NULL;
  confmaster_port = 0;

  while ((ret = getopt (argc, argv, "p:w:t:Dl:c:b:n:hd")) >= 0)
    {
      switch (ret)
	{
	case 'p':
	  global.port = atoi (optarg);
	  global.admin_port = global.port + 1;
	  break;
	case 'w':
	  global.nworker = atoi (optarg);
	  break;
	case 't':
	  global.tcp_backlog = atoi (optarg);
	  break;
	case 'D':
	  global.daemonize = 1;
	  break;
	case 'l':
	  logfile_prefix = optarg;
	  break;
	case 'c':
	  confmaster_addr = optarg;
	  break;
	case 'b':
	  confmaster_port = atoi (optarg);
	  break;
	case 'd':
	  global.prefer_domain_socket = 1;
	  break;
	case 'n':
	  global.cluster_name = optarg;
	  break;
	case 'h':
	default:
	  print_usage (argv[0]);
	  exit (1);
	}
    }

  if (!global.cluster_name)
    {
      print_usage (argv[0]);
      exit (1);
    }

  if (global.daemonize)
    {
      gwlog_init (logfile_prefix, global.verbosity, 0, 1, 0);
      daemonize ();
    }
  else
    {
      gwlog_init (logfile_prefix, global.verbosity, 1, 1, 0);
    }

  if (global.tcp_backlog < GW_MIN_TCP_BACKLOG)
    {
      gwlog (GW_WARNING,
	     "TCP backlog size can't be below %d and is adjusted to minimum size, %d.",
	     GW_MIN_TCP_BACKLOG, GW_MIN_TCP_BACKLOG);
      global.tcp_backlog = GW_MIN_TCP_BACKLOG;
    }

  if (getenv ("NBASE_ARC_HOME"))
    {
      global.nbase_arc_home = sdsnew (getenv ("NBASE_ARC_HOME"));
    }
  else
    {
      global.nbase_arc_home = NULL;
    }

  if (global.prefer_domain_socket && !global.nbase_arc_home)
    {
      gwlog (GW_WARNING,
	     "To use domain socket, environment variable \"$NBASE_ARC_HOME\" should be set.");
      exit (1);
    }

  global.conf = create_conf_from_confmaster (confmaster_addr, confmaster_port,
					     global.cluster_name,
					     global.nbase_arc_home,
					     global.prefer_domain_socket);
  if (!global.conf)
    {
      gwlog (GW_WARNING, "Unable to initialize gateway. Exiting.");
      exit (1);
    }

  print_cluster_configuration (global.conf);
}

static void
create_master (void)
{
  master.el = aeCreateEventLoop (GW_DEFAULT_EVENTLOOP_SIZE);
  master.cron_teid =
    aeCreateTimeEvent (master.el, 0, master_cron, NULL, NULL);
  if (master.cron_teid == AE_ERR)
    {
      gwlog (GW_WARNING, "Unable to create time event for master cron.");
      exit (1);
    }
  master.hz = GW_DEFAULT_HZ;
  master.fd = -1;
  master.admin_fd = -1;
  master.admin_cli = NULL;
  master.admin_cmd_mgr = NULL;
  master.master_async = async_create (master.el, NULL);
  ARRAY_INIT (&master.worker_asyncs);
  ARRAY_INIT (&master.workers);
  master.conf = global.conf;
  master.mp_sbuf = sbuf_mempool_create (SBUF_DEFAULT_PAGESIZE);
  master.shared_stream =
    stream_hdr_create (SBUF_DEFAULT_PAGESIZE, master.mp_sbuf);

  // Admin command manager
  master.admin_cmd_mgr =
    cmd_mgr_admin_create (master.el, master.conf, master.master_async,
			  &master.worker_asyncs, master.shared_stream);

  // Admin client Pool
  master.admin_cli =
    cli_pool_create (master.el, master.admin_cmd_mgr, global.timeout_ms,
		     global.timeout_ms, master.shared_stream, master.mp_sbuf);
}

static void
start_master (void)
{
  char ebuf[ANET_ERR_LEN];

  // User request server
  master.fd = anetTcpServer (ebuf, global.port, NULL, global.tcp_backlog);
  if (master.fd == ANET_ERR)
    {
      gwlog (GW_WARNING, "Creating Server TCP listening socket. *:%d: %s",
	     global.port, ebuf);
      exit (1);
    }
  anetNonBlock (NULL, master.fd);
  if (aeCreateFileEvent
      (master.el, master.fd, AE_READABLE, user_accept_tcp_handler,
       NULL) == AE_ERR)
    {
      gwlog (GW_WARNING, "Unable to create file event for accept handler.");
      exit (1);
    }

  // Admin request server
  master.admin_fd =
    anetTcpServer (ebuf, global.admin_port, NULL, global.tcp_backlog);
  if (master.admin_fd == ANET_ERR)
    {
      gwlog (GW_WARNING, "Creating Server TCP listening socket. *:%d: %s",
	     global.admin_port, ebuf);
      exit (1);
    }
  anetNonBlock (NULL, master.admin_fd);
  if (aeCreateFileEvent
      (master.el, master.admin_fd, AE_READABLE, admin_accept_tcp_handler,
       NULL) == AE_ERR)
    {
      gwlog (GW_WARNING, "Unable to create file event for accept handler.");
      exit (1);
    }

  aeMain (master.el);
}

static void
create_workers (void)
{
  int i;

  for (i = 0; i < global.nworker; i++)
    {
      worker *wrk;

      wrk =
	create_worker (i, master.conf, master.master_async,
		       &master.worker_asyncs);
      ARRAY_PUSH (&master.workers, wrk);
    }
}

static void
finalize_master (void)
{
  destroy_conf (master.conf);
  cli_pool_destroy (master.admin_cli);
  cmd_mgr_destroy (master.admin_cmd_mgr);
  stream_hdr_free (master.shared_stream);
  ARRAY_FINALIZE (&master.workers);
  async_destroy (master.master_async);
  sbuf_mempool_destroy (master.mp_sbuf);
  aeDeleteTimeEvent (master.el, master.cron_teid);
  aeDeleteEventLoop (master.el);
  sdsfree (global.nbase_arc_home);
  gwlog_finalize ();
}

int
main (int argc, char **argv)
{
  setup_signal_handler ();

  // Initilize configuration from shell argument and configuration master server
  init_config (argc, argv);

  // Create master and worker structures
  create_master ();
  create_workers ();

  // Run eventloop
  start_workers (&master.workers);
  start_master ();

  // Finalize
  finalize_workers (&master.workers);
  finalize_master ();
}

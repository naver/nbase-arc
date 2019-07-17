#ifdef NBASE_ARC
#include "arc_internal.h"

#include <assert.h>
#include <stdlib.h>
#include <syslog.h>

#include "bio.h"
#include "slowlog.h"
#include "smr.h"

struct arcServer arc;

/* ------------------ */
/* Local declarations */
/* ------------------ */
/* initialization */
static int init_cron (struct aeEventLoop *eventLoop, long long id,
		      void *clientData);
static void init_smr_connect (void);
static void init_smr_disconnect (void);
static void init_check_smr_catchup (void);
static void init_config (void);
static void init_server_post (void);
static void init_arc (void);
static void init_server_pre (void);
static void ascii_art (void);

/* smr callbacks */
static void process_smr_callback (aeEventLoop * el, int fd, void *privdata,
				  int mask);
static int smrcb_close (void *arg, long long seq, short nid, int sid);
static int smrcb_data (void *arg, long long seq, long long timestamp,
		       short nid, int sid, int hash, smrData * smr_data,
		       int size);
static int smrcb_noti_rckpt (void *arg, char *be_host, int be_port);
static int smrcb_noti_ready (void *arg);

/* main */
static void redis_arc_main (int arc, char **argv);
static void trac_ops (void);
static void try_cronsave (void);
static void try_seqsave (void);
static void check_memory_hard_limit (unsigned long used_kb);
static void check_memory_limiting (unsigned long total_kb,
				   unsigned long used_kb, size_t rss_byte);
static void try_memory_limit (void);


/* ---------------- */
/* Local definition */
/* ---------------- */
static const char *nbase_arc_logo =
  "     _   ____                       ___    ____  ______                             \n"
  "    / | / / /_  ____ _________     /   |  / __ \\/ ____/     Redis %s (%s/%d) %s bit\n"
  "   /  |/ / __ \\/ __ `/ ___/ _ \\   / /| | / /_/ / /          Port: %d              \n"
  "  / /|  / /_/ / /_/ (__  )  __/  / ___ |/ _, _/ /___        SMR Port: %d            \n"
  " /_/ |_/_.___/\\__,_/____/\\___/  /_/  |_/_/ |_|\\____/        PID: %ld             \n\n";


static smrCallback smrCb = {
  smrcb_close,
  smrcb_data,
  smrcb_noti_rckpt,
  smrcb_noti_ready,
};

/* --------------- */
/* Local functions */
/* --------------- */
static int
init_cron (struct aeEventLoop *eventLoop, long long id, void *clientData)
{
  UNUSED (eventLoop);
  UNUSED (id);
  UNUSED (clientData);

  if (server.shutdown_asap)
    {
      if (prepareForShutdown (SHUTDOWN_NOSAVE) == C_OK)
	{
	  exit (0);
	}
      serverLog (LL_WARNING,
		 "SIGTERM received but errors trying to shut down the server, check the logs for more information");
    }
  return 1000 / server.hz;
}

static void
init_smr_connect (void)
{

  serverLog (LL_NOTICE,
	     "initialize SMR Connection, Local Dumpfile Seq Num:%lld",
	     arc.smr_seqnum);
  arc.smr_conn =
    smr_connect_tcp (arc.smr_lport, arc.smr_seqnum, &smrCb, NULL);

  if (arc.smr_conn == NULL)
    {
      serverLog (LL_WARNING, "Failed to connect to smr, errno(%d)", errno);
      exit (1);
    }

  arc.smr_fd = smr_get_poll_fd (arc.smr_conn);
  if (arc.smr_fd == -1)
    {
      serverLog (LL_WARNING, "Failed to get poll fd from smr");
      exit (1);
    }

  if (arc.smr_fd > 0
      && aeCreateFileEvent (server.el, arc.smr_fd, AE_READABLE,
			    process_smr_callback, NULL) == AE_ERR)
    {
      serverLog (LL_WARNING,
		 "Unrecoverable error creating smr.fd file event.");
      smr_disconnect (arc.smr_conn);
      exit (1);
    }
}

static void
init_smr_disconnect (void)
{
  serverLog (LL_NOTICE, "disconnect SMR Connection");
  aeDeleteFileEvent (server.el, arc.smr_fd, AE_READABLE);
  smr_disconnect (arc.smr_conn);
  arc.smr_conn = NULL;
  arc.smr_fd = 0;
  arc.smr_seqnum = 0LL;
}

static void
init_check_smr_catchup (void)
{
  long long cur_mstime = mstime ();

  serverLog (LL_NOTICE, "Check SMR catchup, time diff:%lldms",
	     cur_mstime - arc.last_catchup_check_mstime);
  if (cur_mstime - arc.last_catchup_check_mstime > 10)
    {
      char smr_cmd = ARC_SMR_CMD_CATCHUP_CHECK;

      arc.last_catchup_check_mstime = cur_mstime;
      smr_session_data (arc.smr_conn, 0, SMR_SESSION_DATA_HASH_NONE,
			&smr_cmd, 1);
    }
  else
    {
      serverLog (LL_NOTICE, "SMR Catchup Done");
      arc.smr_init_flags = ARC_SMR_INIT_DONE;
      init_server_post ();
    }
}

static void
init_config (void)
{
  int i;

  /* Stats */
  arc.stat_numcommands_replied = 0LL;
  arc.stat_numcommands_lcon = 0LL;
  arc.stat_bgdel_keys = 0LL;
  arc.ops_sec_idx = 0;
  arc.ops_sec_last_sample_time = 0LL;
  arc.replied_ops_sec_last_sample_ops = 0LL;
  for (i = 0; i < STATS_METRIC_SAMPLES; i++)
    {
      arc.replied_ops_sec_samples[i] = 0LL;
    }
  arc.lcon_ops_sec_last_sample_ops = 0LL;
  for (i = 0; i < STATS_METRIC_SAMPLES; i++)
    {
      arc.lcon_ops_sec_samples[i] = 0LL;
    }

  /* Config */
  arc.object_bio_delete_min_elems = ARC_OBJ_BIO_DELETE_MIN_ELEMS;
  arc.num_rdb_backups = 0;

  /* Server mode */
  arc.cluster_util_mode = 0;
  arc.dump_util_mode = 0;
  arc.cluster_mode = 0;

  /* Checkpoint */
  arc.checkpoint_filename = zstrdup ("checkpoint.rdb");
  arc.checkpoint_client = NULL;
  arc.checkpoint_seqnum = 0LL;
  arc.checkpoint_slots = NULL;

  /* Cron save */
  arc.cronsave_params = NULL;
  arc.cronsave_paramslen = 0;

  /* Seq save */
  arc.seqsave_gap = -1LL;

  /* Migrate state */
  arc.migrate_slot = NULL;
  arc.migclear_slot = NULL;

  /* SSS gc structure */
  arc.gc_idx = 0;
  arc.gc_num_line = 8192;	/* Note: this is nothing to do with NBASE_ARC_KS_SIZE */
  arc.gc_line = NULL;
  arc.gc_interval = 5000;
  arc.gc_obc = NULL;
  dlisth_init (&arc.gc_eager);
  arc.gc_eager_loops = 0;

  /* State machine replication (SMR) */
  arc.smr_conn = NULL;
  arc.smr_lport = 1900;
  arc.smr_fd = -1;
  arc.smr_seqnum = 0LL;
  arc.last_bgsave_seqnum = 0LL;
  arc.seqnum_before_bgsave = 0LL;
  arc.smr_init_flags = ARC_SMR_INIT_NONE;
  arc.last_catchup_check_mstime = 0LL;
  arc.smr_ts = 0LL;
  arc.smrlog_client = NULL;
  arc.smr_seqnum_reset = 0;

  /* Remote checkpoint */
  arc.need_rckpt = 0;
  arc.ckpt_host = NULL;
  arc.ckpt_port = 0;
  arc.is_ready = 0;

  /* Global callback infos */
  dlisth_init (&arc.global_callbacks);

  /* Memory limiting */
  arc.smr_oom_until = 0;
  arc.mem_limit_activated = 0;
  arc.mem_max_allowed_exceeded = 0;
  arc.mem_hard_limit_exceeded = 0;
  arc.meminfo_fd = -1;
  arc.mem_limit_active_perc = 100;
  arc.mem_max_allowed_perc = 100;
  arc.mem_hard_limit_perc = 100;
  arc.mem_limit_active_kb = 0;
  arc.mem_max_allowed_byte = 0;
  arc.mem_hard_limit_kb = 0;

  /* Local ip check */
  arc.local_ip_addrs = arcx_get_local_ip_addrs ();	//TODO why here?

#ifdef COVERAGE_TEST
  arc.debug_mem_usage_fixed = 0;
  arc.debug_total_mem_kb = 0LL;
  arc.debug_free_mem_kb = 0LL;
  arc.debug_cached_mem_kb = 0LL;
  arc.debug_redis_mem_rss_kb = 0LL;
#endif
}

static void
init_server_post (void)
{
  int j;

  /* Open the TCP listening socket for the user commands. */
  if (server.port != 0 &&
      listenToPort (server.port, server.ipfd, &server.ipfd_count) == C_ERR)
    {
      serverLog (LL_WARNING, "Opening port %d: %s", server.port,
		 server.neterr);
      exit (1);
    }

  /* Open the listening Unix domain socket. */
  if (server.unixsocket != NULL)
    {
      unlink (server.unixsocket);	/* don't care if this fails */
      server.sofd = anetUnixServer (server.neterr, server.unixsocket,
				    server.unixsocketperm,
				    server.tcp_backlog);
      if (server.sofd == ANET_ERR)
	{
	  serverLog (LL_WARNING, "Opening socket: %s", server.neterr);
	  exit (1);
	}
      anetNonBlock (NULL, server.sofd);
    }

  if (server.ipfd_count == 0 && server.sofd < 0)
    {
      serverLog (LL_WARNING, "Configured to not listen anywhere, exiting.");
      exit (1);
    }

  /* Create the serverCron() time event, that's our main way to process
   * background operations. */
  if (aeCreateTimeEvent (server.el, 1, serverCron, NULL, NULL) == AE_ERR)
    {
      serverPanic ("Can't create the serverCron time event.");
    }

  /* Create an event handler for accepting new connections in TCP and Unix
   * domain sockets. */
  for (j = 0; j < server.ipfd_count; j++)
    {
      if (aeCreateFileEvent (server.el, server.ipfd[j], AE_READABLE,
			     acceptTcpHandler, NULL) == AE_ERR)
	{
	  serverPanic
	    ("Unrecoverable error creating server.ipfd file event.");
	}
    }

  if (server.sofd > 0
      && aeCreateFileEvent (server.el, server.sofd, AE_READABLE,
			    acceptUnixHandler, NULL) == AE_ERR)
    {
      serverPanic ("Unrecoverable error creating server.sofd file event.");
    }

  if (server.ipfd_count > 0)
    {
      serverLog (LL_NOTICE,
		 "The server is now ready to accept connections on port %d",
		 server.port);
    }
  if (server.sofd > 0)
    {
      serverLog (LL_NOTICE,
		 "The server is now ready to accept connections at %s",
		 server.unixsocket);
    }
}

/* nbase-arc specific */
static void
init_arc (void)
{
  int j;

  arc.checkpoint_client = NULL;
  arc.checkpoint_seqnum = 0;
  arc.gc_line = zmalloc (sizeof (dlisth) * arc.gc_num_line);
  for (j = 0; j < arc.gc_num_line; j++)
    {
      dlisth_init (&arc.gc_line[j]);
    }
  arc.gc_obc = arcx_sss_obc_new ();
  serverAssert (arc.smrlog_client == NULL);
  arc.smrlog_client = createClient (-1);	/* make fake client for excuting smr log */
  dlisth_init (&arc.global_callbacks);
#ifdef COVERAGE_TEST
  /* initialize debugging value for injecting memory status */
  arc.debug_mem_usage_fixed = 0;
  arc.debug_total_mem_kb = 0;
  arc.debug_free_mem_kb = 0;
  arc.debug_cached_mem_kb = 0;
  arc.debug_redis_mem_rss_kb = 0;
#endif
  arcx_set_memory_limit_values ();
}


static void
init_server_pre (void)
{
  int j;

  signal (SIGHUP, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);
  setupSignalHandlers ();

  if (server.syslog_enabled)
    {
      openlog (server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
	       server.syslog_facility);
    }
  server.pid = getpid ();
  server.current_client = NULL;
  server.clients = listCreate ();
  server.clients_to_close = listCreate ();
  server.slaves = listCreate ();
  server.monitors = listCreate ();
  server.clients_pending_write = listCreate ();
  server.slaveseldb = -1;	/* Force to emit the first SELECT command. */
  server.unblocked_clients = listCreate ();
  server.ready_keys = listCreate ();
  server.clients_waiting_acks = listCreate ();
  server.get_ack_from_slaves = 0;
  server.clients_paused = 0;
  server.system_memory_size = zmalloc_get_memory_size ();

  createSharedObjects ();
  adjustOpenFilesLimit ();
  server.el = aeCreateEventLoop (server.maxclients + CONFIG_FDSET_INCR);
  server.db = zmalloc (sizeof (redisDb) * server.dbnum);

  /* Create the Redis databases, and initialize other internal state. */
  for (j = 0; j < server.dbnum; j++)
    {
      server.db[j].dict = dictCreate (&dbDictType, NULL);
      server.db[j].expires = dictCreate (&keyptrDictType, NULL);
      server.db[j].blocking_keys = dictCreate (&keylistDictType, NULL);
      server.db[j].ready_keys = dictCreate (&setDictType, NULL);
      server.db[j].watched_keys = dictCreate (&keylistDictType, NULL);
      server.db[j].eviction_pool = evictionPoolAlloc ();
      server.db[j].id = j;
      server.db[j].avg_ttl = 0;
    }
  server.pubsub_channels = dictCreate (&keylistDictType, NULL);
  server.pubsub_patterns = listCreate ();
  listSetFreeMethod (server.pubsub_patterns, freePubsubPattern);
  listSetMatchMethod (server.pubsub_patterns, listMatchPubsubPattern);
  server.cronloops = 0;
  server.rdb_child_pid = -1;
  server.aof_child_pid = -1;
  server.rdb_child_type = RDB_CHILD_TYPE_NONE;
  server.rdb_bgsave_scheduled = 0;
  aofRewriteBufferReset ();
  server.aof_buf = sdsempty ();
  server.lastsave = time (NULL);	/* At startup we consider the DB saved. */
  server.lastbgsave_try = 0;	/* At startup we never tried to BGSAVE. */
  server.rdb_save_time_last = -1;
  server.rdb_save_time_start = -1;
  server.dirty = 0;
  resetServerStats ();
  /* A few stats we don't want to reset: server startup time, and peak mem. */
  server.stat_starttime = time (NULL);
  server.stat_peak_memory = 0;
  server.resident_set_size = 0;
  server.lastbgsave_status = C_OK;
  server.aof_last_write_status = C_OK;
  server.aof_last_write_errno = 0;
  server.repl_good_slaves_count = 0;
  updateCachedTime ();

  init_arc ();

  /* 32 bit instances are limited to 4GB of address space, so if there is
   * no explicit limit in the user provided configuration we set a limit
   * at 3 GB using maxmemory with 'noeviction' policy'. This avoids
   * useless crashes of the Redis instance for out of memory. */
  if (server.arch_bits == 32 && server.maxmemory == 0)
    {
      serverLog (LL_WARNING,
		 "Warning: 32 bit instance detected but no memory limit set. Setting 3 GB maxmemory limit with 'noeviction' policy now.");
      server.maxmemory = 3072LL * (1024 * 1024);	/* 3 GB */
      server.maxmemory_policy = MAXMEMORY_NO_EVICTION;
    }

  if (server.cluster_enabled)
    {
      clusterInit ();
    }
  replicationScriptCacheInit ();
  scriptingInit (1);
  slowlogInit ();
  latencyMonitorInit ();
  bioInit ();
}

static void
ascii_art (void)
{
  char buf[16 * 1024];

  snprintf (buf, 1024 * 16, nbase_arc_logo,
	    REDIS_VERSION,
	    redisGitSHA1 (),
	    strtol (redisGitDirty (), NULL, 10) > 0,
	    (sizeof (long) == 8) ? "64" : "32",
	    server.port, arc.smr_lport, (long) getpid ());
  serverLogRaw (LL_NOTICE | LL_RAW, buf);
  return;
}

static void
process_smr_callback (aeEventLoop * el, int fd, void *privdata, int mask)
{
  int ret;
  UNUSED (el);
  UNUSED (fd);
  UNUSED (privdata);
  UNUSED (mask);

  ret = smr_process (arc.smr_conn);
  if (ret == -1)
    {
      serverLog (LL_NOTICE,
		 "SMR Replicator unreachable, smr_process retcode:%d, errno:%d, last_seqnum:%lld",
		 ret, errno, arc.smr_seqnum);
      (void) prepareForShutdown (0);	/* nothing to do but exiting */
      exit (0);
    }
}

static int
smrcb_close (void *arg, long long seq, short nid, int sid)
{
  client *c;
  callbackInfo *cb;
  dlisth *node;
  UNUSED (arg);
  UNUSED (seq);

  if (nid != smr_get_nid (arc.smr_conn)
      || !(arc.smr_init_flags & ARC_SMR_INIT_DONE))
    {
      return 0;
    }

  /* query from this server. get client from global callback list */
  assert (!dlisth_is_empty (&arc.global_callbacks));	/* global callback list shouldn't be empty */
  node = arc.global_callbacks.next;
  cb = (callbackInfo *) node;
  dlisth_delete (&cb->global_head);
  dlisth_delete (&cb->client_head);
  c = cb->client;
  zfree (cb);

  if (c->fd == -1)
    {
      return 0;			/* ignore if the client is already freed */
    }

  assert (c->fd == sid);
  addReplyError (c, c->smr->protocol_error_reply);
  c->flags |= CLIENT_CLOSE_AFTER_REPLY;

  return 0;
}


static int
smrcb_data (void *arg, long long seq, long long timestamp,
	    short nid, int sid, int hash, smrData * smr_data, int size)
{
  char *data = smr_data_get_data (smr_data);
  client *c;
  short cmdflags;
  dlisth *node;
  UNUSED (arg);

  /* Special commands */
  if (size == 1 && data[0] == ARC_SMR_CMD_CATCHUP_CHECK)
    {
      if (nid == smr_get_nid (arc.smr_conn)
	  && (arc.smr_init_flags & ARC_SMR_INIT_CATCHUP_PHASE2))
	{
	  init_check_smr_catchup ();
	}
      goto release_seq;
    }
  else if (size == 1 && data[0] == ARC_SMR_CMD_DELIVER_OOM)
    {
      arc.smr_oom_until = timestamp + ARC_OOM_DURATION_MS;
      goto release_seq;
    }

  /* Normal command */
  arc.smr_ts = timestamp;
  cmdflags = (0xFFFF0000 & sid) >> 16;

  /* Because we fixed the bug that causes timestamp be 0, 
   * this warning log is not necessary and just an assert statement is enough after all.
   * But currently, there are nbase-arc clusters which have the bug.
   * So, we don't assert for now. */
  if (timestamp == 0)
    {
      serverLog (LL_WARNING, "Timestamp of SMR callback is 0,"
		 "seq:%lld, nid:%d, sid:%d, hash:%d, size:%d",
		 seq, nid, sid, hash, size);
    }

  if ((arc.smr_init_flags & ARC_SMR_INIT_DONE)
      && nid == smr_get_nid (arc.smr_conn))
    {
      callbackInfo *cb;
      /* query from this server. get client from global callback list */
      assert (!dlisth_is_empty (&arc.global_callbacks));	/* global callback list shouldn't be empty */
      node = arc.global_callbacks.next;
      cb = (callbackInfo *) node;
      dlisth_delete (&cb->global_head);
      dlisth_delete (&cb->client_head);
      c = cb->client;
      assert (cb->hash == hash);
      assert (c->fd == -1 || c->fd == (0X0000FFFF & sid));

      /* We already parsed querybuf because the query is requested from this 
       * server before smr callback*/
      c->argc = cb->argc;
      c->argv = cb->argv;
      cb->argc = 0;
      cb->argv = NULL;
      zfree (cb);

      server.current_client = c;

      /* fake client doesn't need to execute non-write query(read-only or admin) */
      if (c->fd != -1 || cmdflags & CMD_WRITE)
	{
	  assert (!(c->flags & CLIENT_CLOSE_AFTER_REPLY));
	  processCommand (c);
	}
    }
  else
    {
      /* replicated query from other servers, or catchup query during initialize */
      c = arc.smrlog_client;
      server.current_client = c;

      /* fake client doesn't need to execute non-write query(read-only or admin) */
      if (cmdflags & CMD_WRITE)
	{
	  /* We need to parse querybuf because the query is from different server 
	   * or we are recovering without client */
	  sdsclear (c->querybuf);
	  c->querybuf = sdsMakeRoomFor (c->querybuf, size);
	  memcpy (c->querybuf, data, size);
	  sdsIncrLen (c->querybuf, size);
	  if (c->querybuf_peak < (size_t) size)
	    {
	      c->querybuf_peak = size;
	    }
	  processInputBuffer (c);
	}
    }

  resetClient (c);
  zfree (c->argv);
  c->argv = NULL;
  server.current_client = NULL;

release_seq:
  if (smr_release_seq_upto (arc.smr_conn, seq + size) == -1)
    {
      serverLog (LL_WARNING, "smr_release_seq_upto error");
      serverAssert (0);
      return -1;
    }
  arc.smr_seqnum = seq + size;

  return 0;
}


static int
smrcb_noti_rckpt (void *arg, char *be_host, int be_port)
{
  UNUSED (arg);

  if (arc.smr_init_flags != ARC_SMR_INIT_NONE)
    {
      serverLog (LL_WARNING, "rckpt command is called after initialization.");
      exit (1);
    }

  arc.need_rckpt = 1;
  arc.ckpt_host = zstrdup (be_host);
  arc.ckpt_port = be_port;
  aeStop (server.el);
  return 0;
}

static int
smrcb_noti_ready (void *arg)
{
  UNUSED (arg);

  assert (!arc.is_ready);
  arc.is_ready = 1;
  aeStop (server.el);
  return 0;
}

static void
redis_arc_main (int argc, char **argv)
{
  int background;
  long long teid;
  UNUSED (argc);
  UNUSED (argv);

  init_server_pre ();
  background = server.daemonize && !server.supervised;
  if (background || server.pidfile)
    {
      createPidFile ();
    }
  ascii_art ();
  serverLog (LL_WARNING, "Server started, Redis version " REDIS_VERSION);

#ifdef __linux__
  linuxMemoryWarnings ();
#endif
  loadDataFromDisk ();
  arc.last_bgsave_seqnum = arc.smr_seqnum;
  /* Warning the user about suspicious maxmemory setting. */
  if (server.maxmemory > 0 && server.maxmemory < 1024 * 1024)
    {
      serverLog (LL_WARNING,
		 "WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?",
		 server.maxmemory);
    }

  init_smr_connect ();

  /* initializeCron for handling sigterm */
  teid = aeCreateTimeEvent (server.el, 1, init_cron, NULL, NULL);
  aeMain (server.el);
  aeDeleteTimeEvent (server.el, teid);

  if (arc.need_rckpt)
    {
      serverLog (LL_NOTICE,
		 "Need more checkpoint from %s:%d", arc.ckpt_host,
		 arc.ckpt_port);

      init_smr_disconnect ();
      arc.smr_init_flags = ARC_SMR_INIT_RCKPT;
      if (arcx_get_dump (arc.ckpt_host, arc.ckpt_port, server.rdb_filename,
			 "0-8191", ARC_GETDUMP_DEFAULT_NET_LIMIT_MB) != C_OK)
	{
	  exit (1);
	}

      emptyDb (NULL);
      loadDataFromDisk ();
      arc.last_bgsave_seqnum = arc.smr_seqnum;
      init_smr_connect ();

      aeMain (server.el);
    }

  if (!arc.is_ready)
    {
      serverLog (LL_WARNING, "Invalid initialization state");
      exit (1);
    }

  if (arc.last_bgsave_seqnum)
    {
      if (smr_seq_ckpted (arc.smr_conn, arc.last_bgsave_seqnum) != 0)
	{
	  serverLog (LL_WARNING,
		     "Failed to notify checkpointed sequence to smr");
	  exit (1);
	}
      else
	{
	  serverLog (LL_NOTICE,
		     "Checkpointed sequence is sent to SMR, seqnum:%lld",
		     arc.last_bgsave_seqnum);
	}
    }

  arc.smr_seqnum_reset = 0;
  arc.last_catchup_check_mstime = mstime ();
  arc.smr_init_flags = ARC_SMR_INIT_CATCHUP_PHASE1;
  if (smr_catchup (arc.smr_conn) == -1)
    {
      serverLog (LL_WARNING, "Failed to catchup errno(%d)", errno);
      exit (1);
    }
  arc.smr_init_flags = ARC_SMR_INIT_CATCHUP_PHASE2;
  init_check_smr_catchup ();
  aeSetBeforeSleepProc (server.el, beforeSleep);
  aeMain (server.el);
  aeDeleteEventLoop (server.el);
  exit (0);
}

static void
trac_ops (void)
{
  long long t = mstime () - arc.ops_sec_last_sample_time;
  long long ops, ops_sec;

  /* Stat of operations of actual client except commands from replicated log */
  ops = arc.stat_numcommands_replied - arc.replied_ops_sec_last_sample_ops;
  ops_sec = t > 0 ? (ops * 1000 / t) : 0;
  arc.replied_ops_sec_samples[arc.ops_sec_idx] = ops_sec;
  arc.replied_ops_sec_last_sample_ops = arc.stat_numcommands_replied;

  /* Stat of operations from local connection */
  ops = arc.stat_numcommands_lcon - arc.lcon_ops_sec_last_sample_ops;
  ops_sec = t > 0 ? (ops * 1000 / t) : 0;
  arc.lcon_ops_sec_samples[arc.ops_sec_idx] = ops_sec;
  arc.lcon_ops_sec_last_sample_ops = arc.stat_numcommands_lcon;

  arc.ops_sec_last_sample_time = mstime ();
  arc.ops_sec_idx = (arc.ops_sec_idx + 1) % STATS_METRIC_SAMPLES;
}

static void
try_cronsave (void)
{
  struct tm cur, last;
  int j;

  localtime_r (&server.unixtime, &cur);
  localtime_r (&server.lastsave, &last);
  for (j = 0; j < arc.cronsave_paramslen; j++)
    {
      struct cronsaveParam *csp = arc.cronsave_params + j;
      if (last.tm_mday != cur.tm_mday)
	{
	  if (last.tm_hour < csp->hour
	      || (last.tm_hour == csp->hour && last.tm_min < csp->minute)
	      || csp->hour < cur.tm_hour
	      || (csp->hour == cur.tm_hour && csp->minute <= cur.tm_min))
	    {
	      serverLog (LL_NOTICE, "Cron save %dh:%dm daily. Saving...",
			 csp->hour, csp->minute);
	      rdbSaveBackground (server.rdb_filename);
	      break;
	    }
	}
      else
	{
	  /* last.tm_mday == cur.tm_mday */
	  if ((last.tm_hour < csp->hour
	       || (last.tm_hour == csp->hour && last.tm_min < csp->minute))
	      && (csp->hour < cur.tm_hour
		  || (csp->hour == cur.tm_hour && csp->minute <= cur.tm_min)))
	    {
	      serverLog (LL_NOTICE, "Cron save %dh:%dm daily. Saving...",
			 csp->hour, csp->minute);
	      rdbSaveBackground (server.rdb_filename);
	      break;
	    }
	}
    }
}

static void
try_seqsave (void)
{
  if (arc.seqsave_gap != -1
      && arc.smr_seqnum - arc.last_bgsave_seqnum > arc.seqsave_gap)
    {
      serverLog (LL_NOTICE,
		 "Sequence gap between last bgsave(%lld) and current(%lld) is more than %lld. Saving...",
		 arc.last_bgsave_seqnum, arc.smr_seqnum, arc.seqsave_gap);
      rdbSaveBackground (server.rdb_filename);
    }
}

static void
check_memory_hard_limit (unsigned long used_kb)
{
  if (!arc.mem_hard_limit_exceeded && used_kb >= arc.mem_hard_limit_kb)
    {
      arc.mem_hard_limit_exceeded = 1;
      serverLog (LL_NOTICE,
		 "Used memory is above hard limit. Machine used memory:%ldMB",
		 used_kb / 1024);
    }
  else if (arc.mem_hard_limit_exceeded && used_kb < arc.mem_hard_limit_kb)
    {
      serverLog (LL_NOTICE,
		 "Used memory is below hard limit. Machine used memory:%ldMB",
		 used_kb / 1024);
      arc.mem_hard_limit_exceeded = 0;
    }
}

static void
check_memory_limiting (unsigned long total_kb, unsigned long used_kb,
		       size_t rss_byte)
{
  if (!arc.mem_limit_activated && used_kb >= arc.mem_limit_active_kb)
    {
      /* Activate memory limiting */
      arc.mem_max_allowed_byte
	= total_kb * arc.mem_max_allowed_perc / 100 * rss_byte / used_kb;
      arc.mem_limit_activated = 1;

      serverLog (LL_NOTICE,
		 "Memory limiting activated. Current used memory(rss):%ldMB, Set max memory allowed:%ldMB",
		 rss_byte / 1024 / 1024,
		 arc.mem_max_allowed_byte / 1024 / 1024);
    }
  else if (arc.mem_limit_activated && used_kb < arc.mem_limit_active_kb)
    {
      /* Deactivate memory limiting */
      serverLog (LL_NOTICE, "Memory limiting deactivated.");
      arc.mem_limit_activated = 0;
      arc.mem_max_allowed_exceeded = 0;
    }
}

static void
try_memory_limit (void)
{
  if (arc.mem_limit_active_perc != 100 || arc.mem_hard_limit_perc != 100)
    {
      unsigned long total_kb, free_kb, cached_kb, used_kb;
      size_t rss_byte = zmalloc_get_rss ();

#ifdef COVERAGE_TEST
      if (arc.debug_mem_usage_fixed)
	{
	  rss_byte = arc.debug_redis_mem_rss_kb * 1024;
	}
#endif
      if (arcx_get_memory_usage (&total_kb, &free_kb, &cached_kb) == C_OK)
	{
	  free_kb += cached_kb;
	  used_kb = total_kb - free_kb;

	  if (arc.mem_hard_limit_perc != 100)
	    {
	      check_memory_hard_limit (used_kb);
	    }
	  if (arc.mem_limit_active_perc != 100)
	    {
	      check_memory_limiting (total_kb, used_kb, rss_byte);
	    }
	}

      /* Check maximum allowed memory for this redis */
      if (arc.mem_limit_activated && rss_byte >= arc.mem_max_allowed_byte)
	{
	  arc.mem_max_allowed_exceeded = 1;
	}
      else
	{
	  arc.mem_max_allowed_exceeded = 0;
	}

      /* Memory limiting 
       * If used memory exceeds hard limit or allowed limit, propagate oom message to all replicas */
      if ((arc.mem_limit_activated && arc.mem_max_allowed_exceeded)
	  || arc.mem_hard_limit_exceeded)
	{
	  char smr_cmd = ARC_SMR_CMD_DELIVER_OOM;
	  smr_session_data (arc.smr_conn, 0, SMR_SESSION_DATA_HASH_NONE,
			    &smr_cmd, 1);
	}
    }
}

/* ---------------------- */
/* Exported arcx function */
/* ---------------------- */
void
arcx_init_server_pre (void)
{
  init_server_pre ();
}

/* --------------------- */
/* Exported arc function */
/* --------------------- */

// following commands are not permitted when arc.cluster_mode = 1
// used commands are: ping, client list , lastsave , bgsave
static const char *non_cluster_commands[] = {
#ifndef COVERAGE_TEST
  "debug",
#endif
  "brpop", "brpoplpush", "blpop", "eval", "evalsha", "wait",
  "bgrewriteaof", "shutdown", "migrate", "move", "cluster", "asking",
  "restore-asking", "readonly", "readwrite", "psubscribe", "publish",
  "pubsub", "punsubscribe", "subscribe", "unsubscribe", "pfdebug",
  "pfselftest", "sync", "psync", "replconf", "save", "slaveof", NULL
};

void
arc_amend_command_table (void)
{
  int i = 0;
  char *name;

  while ((name = (char *) non_cluster_commands[i++]) != NULL)
    {
      struct redisCommand *cmd = lookupCommandByCString (name);
      cmd->flags |= CMD_NOCLUSTER;
    }
}

void
arc_tool_hook (int argc, char **argv)
{
  UNUSED (argc);

  if (strstr (argv[0], "cluster-util") != NULL)
    {
      arcx_cluster_util_main (argc, argv);
    }
  else if (strstr (argv[0], "dump-util") != NULL)
    {
      arcx_dump_util_main (argc, argv);
    }
}

void
arc_init_config (void)
{
  init_config ();
}

void
arc_init_arc (void)
{
  init_arc ();
}

void
arc_main_hook (int argc, char **argv)
{
  if (strstr (argv[0], "redis-arc") == NULL)
    {
      return;
    }

  arc.cluster_mode = 1;
  redis_arc_main (argc, argv);
}

int
arc_server_cron (void)
{
  int j;

  // higher rate gc
  if ((j = arcx_sss_gc_cron ()) > 0)
    {
      return j;
    }

  run_with_period (arc.gc_interval)
  {
    arcx_sss_garbage_collect (0);
  }

  run_with_period (100)
  {
    trac_ops ();
  }

  run_with_period (1000)
  {
    if (arc.cluster_mode && server.rdb_child_pid == -1)
      {
	try_cronsave ();
      }
    if (arc.cluster_mode && server.rdb_child_pid == -1)
      {
	try_seqsave ();
      }
  }

  run_with_period (10000)
  {
    try_memory_limit ();
  }

  // more to come... memory limit etc.
  return 0;
}

int
arc_expire_haveto_skip (sds key)
{
  if (arc.migrate_slot)
    {
      int slot = crc16 (key, sdslen (key)) % ARC_KS_SIZE;
      if (bitmapTestBit ((unsigned char *) arc.migrate_slot, slot))
	{
	  /* do not expire keys in migration */
	  return 1;
	}
    }
  return 0;
}

mstime_t
arc_mstime (void)
{
  if (arc.cluster_mode)
    {
      return arc.smr_ts;
    }
  return mstime ();
}

long long
arc_get_instantaneous_metric (long long *samples)
{
  int j;
  long long sum = 0;

  for (j = 0; j < STATS_METRIC_SAMPLES; j++)
    {
      sum += samples[j];
    }
  return sum / STATS_METRIC_SAMPLES;
}

int
arc_handle_command_rewrite (client * c)
{
  // When a request must be rejected by some reason such as oom handling or not-supported,
  // we rewrite the request with fake command (addreply_through_smr) and the error reason.
  // This modification is required to make the response in order w.r.t. replication stream
  // which contains the pipelined request.
  if (c->argv[0] == shared.addreply_through_smr)
    {
      serverAssert (c->argc == 2);
      addReply (c, c->argv[1]);
      return 1;
    }
  return 0;
}

// used to make short line
#define _gllorerrr getLongLongFromObjectOrReply
int
arc_debug_hook (client * c)
{
#if defined(COVERAGE_TEST)
  long long total, free, cached, redis_rss;

  if (strcasecmp (c->argv[1]->ptr, "memory") || c->argc != 6)
    {
      return 0;
    }

  GOTOIF (err, _gllorerrr (c, c->argv[2], &total, NULL) != C_OK);
  GOTOIF (err, _gllorerrr (c, c->argv[3], &free, NULL) != C_OK);
  GOTOIF (err, _gllorerrr (c, c->argv[4], &cached, NULL) != C_OK);
  GOTOIF (err, _gllorerrr (c, c->argv[5], &redis_rss, NULL) != C_OK);
  if (total == 0 && free == 0 && cached == 0)
    {
      arc.debug_mem_usage_fixed = 0;
    }
  else
    {
      arc.debug_mem_usage_fixed = 1;
    }

  if (arc.debug_total_mem_kb != (unsigned long long) total)
    {
      arc.debug_total_mem_kb = total;
      arcx_set_memory_limit_values ();
    }
  else
    {
      arc.debug_total_mem_kb = total;
    }
  arc.debug_free_mem_kb = free;
  arc.debug_cached_mem_kb = cached;
  arc.debug_redis_mem_rss_kb = redis_rss;
  addReply (c, shared.ok);
  return 1;

err:
  return 1;
#else
  UNUSED (c);
  return 0;
#endif
}

void
crc16Command (client * c)
{
  robj *o, *new;
  long long oldhash, newhash;
  sds val;

  val = c->argv[2]->ptr;
  o = lookupKeyWrite (c->db, c->argv[1]);
  if (o != NULL && checkType (c, o, OBJ_STRING))
    return;
  if (getLongLongFromObjectOrReply (c, o, &oldhash, NULL) != C_OK)
    return;

  newhash = crc16sd (val, sdslen (val), oldhash);
  new = createStringObjectFromLongLong (newhash);
  if (o)
    dbOverwrite (c->db, c->argv[1], new);
  else
    dbAdd (c->db, c->argv[1], new);
  signalModifiedKey (c->db, c->argv[1]);
  server.dirty++;
  addReply (c, shared.colon);
  addReply (c, new);
  addReply (c, shared.crlf);
}

#endif

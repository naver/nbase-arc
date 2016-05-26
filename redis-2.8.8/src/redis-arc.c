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

#include "fmacros.h"
#include <assert.h>
#include <fcntl.h>

#include "redis.h"

#ifdef NBASE_ARC
static void
createRedisSocket ()
{
  int j;

  if (server.port != 0 &&
      listenToPort (server.port, server.ipfd,
		    &server.ipfd_count) == REDIS_ERR)
    {
      redisLog (REDIS_WARNING, "Opening port %d: %s", server.port,
		server.neterr);
      exit (1);
    }
  if (server.unixsocket != NULL)
    {
      unlink (server.unixsocket);	/* don't care if this fails */
      server.sofd = anetUnixServer (server.neterr, server.unixsocket,
				    server.unixsocketperm,
				    server.tcp_backlog);
      if (server.sofd == ANET_ERR)
	{
	  redisLog (REDIS_WARNING, "Opening socket: %s", server.neterr);
	  exit (1);
	}
    }
  if (server.ipfd_count == 0 && server.sofd < 0)
    {
      redisLog (REDIS_WARNING, "Configured to not listen anywhere, exiting.");
      exit (1);
    }

  if (aeCreateTimeEvent (server.el, 1, serverCron, NULL, NULL) == AE_ERR)
    {
      redisPanic ("Can't create the serverCron time event.");
      exit (1);
    }

  /* Create an event handler for accepting new connections in TCP and Unix
   * domain sockets. */
  for (j = 0; j < server.ipfd_count; j++)
    {
      if (aeCreateFileEvent (server.el, server.ipfd[j], AE_READABLE,
			     acceptTcpHandler, NULL) == AE_ERR)
	{
	  redisPanic ("Unrecoverable error creating server.ipfd file event.");
	}
    }

  if (server.sofd > 0
      && aeCreateFileEvent (server.el, server.sofd, AE_READABLE,
			    acceptUnixHandler, NULL) == AE_ERR)
    redisPanic ("Unrecoverable error creating server.sofd file event.");

  if (server.aof_state == REDIS_AOF_ON)
    {
      server.aof_fd = open (server.aof_filename,
			    O_WRONLY | O_APPEND | O_CREAT, 0644);
      if (server.aof_fd == -1)
	{
	  redisLog (REDIS_WARNING, "Can't open the append-only file: %s",
		    strerror (errno));
	  exit (1);
	}
    }
  if (server.ipfd_count > 0)
    redisLog (REDIS_NOTICE,
	      "The server is now ready to accept connections on port %d",
	      server.port);
  if (server.sofd > 0)
    redisLog (REDIS_NOTICE,
	      "The server is now ready to accept connections at %s",
	      server.unixsocket);
}

static void
checkSmrCatchup ()
{
  long long cur_mstime = mstime ();
  redisLog (REDIS_NOTICE, "Check SMR catchup, time diff:%lldms",
	    cur_mstime - server.last_catchup_check_mstime);
  if (cur_mstime - server.last_catchup_check_mstime > 10)
    {
      char smr_cmd = REDIS_SMR_CMD_CATCHUP_CHECK;

      server.last_catchup_check_mstime = cur_mstime;
      smr_session_data (server.smr_conn, 0, SMR_SESSION_DATA_HASH_NONE,
			&smr_cmd, 1);
    }
  else
    {
      redisLog (REDIS_NOTICE, "SMR Catchup Done");
      server.smr_init_flags = SMR_INIT_DONE;
      createRedisSocket ();
    }
}

static int
smrCbClose (void *arg, long long seq, short nid, int sid)
{
  redisClient *c;
  callbackInfo *cb;
  dlisth *node;

  if (nid != smr_get_nid (server.smr_conn)
      || !(server.smr_init_flags & SMR_INIT_DONE))
    return REDIS_OK;

  /* query from this server. get client from global callback list */
  assert (!dlisth_is_empty (&server.global_callbacks));	/* global callback list shouldn't be empty */
  node = server.global_callbacks.next;
  cb = (callbackInfo *) node;
  dlisth_delete (&cb->global_head);
  dlisth_delete (&cb->client_head);
  c = cb->client;
  zfree (cb);

  if (c->fd == -1)
    return REDIS_OK;		/* ignore if the client is already freed */

  assert (c->fd == sid);

  addReplyError (c, c->smr.protocol_error_reply);
  c->flags |= REDIS_CLOSE_AFTER_REPLY;

  return REDIS_OK;
}

/* sid = |flags(4byte)|client fd(4byte)| */
static int
smrCbData (void *arg, long long seq, long long timestamp,
	   short nid, int sid, int hash, smrData * smr_data, int size)
{
  char *data = smr_data_get_data (smr_data);
  redisClient *c;
  short cmdflags;
  dlisth *node;

  /* Special commands */
  if (size == 1 && data[0] == REDIS_SMR_CMD_CATCHUP_CHECK)
    {
      if (nid == smr_get_nid (server.smr_conn)
	  && (server.smr_init_flags & SMR_INIT_CATCHUP_PHASE2))
	checkSmrCatchup ();
      goto release_seq;
    }
  else if (size == 1 && data[0] == REDIS_SMR_CMD_DELIVER_OOM)
    {
      server.smr_oom_until = timestamp + REDIS_OOM_DURATION_MS;
      goto release_seq;
    }

  /* Normal command */
  server.smr_mstime = timestamp;
  cmdflags = (0xFFFF0000 & sid) >> 16;

  /* Because we fixed the bug that causes timestamp be 0, 
   * this warning log is not necessary and just an assert statement is enough after all.
   * But currently, there are nbase-arc clusters which have the bug.
   * So, we don't assert for now. */
  if (timestamp == 0)
    {
      redisLog (REDIS_WARNING, "Timestamp of SMR callback is 0,"
		"seq:%lld, nid:%d, sid:%d, hash:%d, size:%d",
		seq, nid, sid, hash, size);
    }

  if ((server.smr_init_flags & SMR_INIT_DONE)
      && nid == smr_get_nid (server.smr_conn))
    {
      callbackInfo *cb;
      /* query from this server. get client from global callback list */
      assert (!dlisth_is_empty (&server.global_callbacks));	/* global callback list shouldn't be empty */
      node = server.global_callbacks.next;
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
      if (c->fd != -1 || cmdflags & REDIS_CMD_WRITE)
	{
	  assert (!(c->flags & REDIS_CLOSE_AFTER_REPLY));
	  processCommand (c);
	}
    }
  else
    {
      /* replicated query from other servers, or catchup query during initialize */
      c = server.smrlog_client;
      server.current_client = c;

      /* fake client doesn't need to execute non-write query(read-only or admin) */
      if (cmdflags & REDIS_CMD_WRITE)
	{
	  /* We need to parse querybuf because the query is from different server 
	   * or we are recovering without client */
	  sdsclear (c->querybuf);
	  c->querybuf = sdsMakeRoomFor (c->querybuf, size);
	  memcpy (c->querybuf, data, size);
	  sdsIncrLen (c->querybuf, size);
	  if (c->querybuf_peak < size)
	    c->querybuf_peak = size;
	  processInputBuffer (c);
	}
    }

  resetClient (c);
  zfree (c->argv);
  c->argv = NULL;
  server.current_client = NULL;

release_seq:
  if (smr_release_seq_upto (server.smr_conn, seq + size) == -1)
    {
      redisLog (REDIS_WARNING, "smr_release_seq_upto error");
      redisAssert (0);
      return REDIS_ERR;
    }
  server.smr_seqnum = seq + size;

  return REDIS_OK;
}

static int
smrCbNotiRckpt (void *arg, char *be_host, int be_port)
{
  if (server.smr_init_flags != SMR_INIT_NONE)
    {
      redisLog (REDIS_WARNING,
		"rckpt command is called after initialization.");
      exit (1);
    }

  server.need_rckpt = 1;
  server.ckpt_host = zstrdup (be_host);
  server.ckpt_port = be_port;
  aeStop (server.el);
  return 0;
}

static int
smrCbNotiReady (void *arg)
{
  assert (!server.is_ready);
  server.is_ready = 1;
  aeStop (server.el);
  return 0;
}

static smrCallback smrCb = {
  smrCbClose,
  smrCbData,
  smrCbNotiRckpt,
  smrCbNotiReady
};

static void
processSmrCallback (aeEventLoop * el, int fd, void *privdata, int mask)
{
  int ret;
  ret = smr_process (server.smr_conn);
  if (ret == -1)
    {
      redisLog (REDIS_NOTICE,
		"SMR Replicator unreachable, smr_process retcode:%d, errno:%d, last_seqnum:%lld",
		ret, errno, server.smr_seqnum);
      if (prepareForShutdown (0) == REDIS_OK)
	exit (0);
      redisLog (REDIS_WARNING,
		"SMR Replicator closed, but errors trying to shut down the server, check the logs for more information");
      exit (0);
    }
}

void
initClusterConfig ()
{
  /* check if cluster mode */
  if (!server.cluster_mode || server.cluster_util_mode
      || server.sentinel_mode)
    {
      redisLog (REDIS_WARNING, "Incompatible Redis Mode");
      exit (1);
    }

  server.migrate_slot = NULL;
  server.migclear_slot = NULL;
  server.smr_conn = NULL;
  server.smr_lport = 1900;
  server.smr_fd = 0;
  server.smr_seqnum = 0LL;
  server.smrlog_client = NULL;
  server.need_rckpt = 0;
  server.ckpt_host = NULL;
  server.ckpt_port = 0;
  server.is_ready = 0;
  server.cronsave_params = NULL;
  server.last_bgsave_seqnum = 0;
  resetServerCronSaveParams ();
  server.seqsave_gap = -1LL;
  server.smr_init_flags = SMR_INIT_NONE;
  server.smr_seqnum_reset = 0;
}

static void
smrConnect ()
{
  redisLog (REDIS_NOTICE,
	    "initialize SMR Connection, Local Dumpfile Seq Num:%lld",
	    server.smr_seqnum);
  server.smr_conn =
    smr_connect_tcp (server.smr_lport, server.smr_seqnum, &smrCb, NULL);

  if (server.smr_conn == NULL)
    {
      redisLog (REDIS_WARNING, "Failed to connect to smr, errno(%d)", errno);
      exit (1);
    }

  server.smr_fd = smr_get_poll_fd (server.smr_conn);
  if (server.smr_fd == -1)
    {
      redisLog (REDIS_WARNING, "Failed to get poll fd from smr");
      exit (1);
    }

  if (server.smr_fd > 0
      && aeCreateFileEvent (server.el, server.smr_fd, AE_READABLE,
			    processSmrCallback, NULL) == AE_ERR)
    {
      redisLog (REDIS_WARNING,
		"Unrecoverable error creating smr.fd file event.");
      smr_disconnect (server.smr_conn);
      exit (1);
    }
}

static void
smrDisconnect ()
{
  redisLog (REDIS_NOTICE, "disconnect SMR Connection");
  aeDeleteFileEvent (server.el, server.smr_fd, AE_READABLE);
  smr_disconnect (server.smr_conn);
  server.smr_conn = NULL;
  server.smr_fd = 0;
  server.smr_seqnum = 0LL;
}

static int
initializeCron (struct aeEventLoop *eventLoop, long long id, void *clientData)
{
  if (server.shutdown_asap)
    {
      if (prepareForShutdown (REDIS_SHUTDOWN_NOSAVE) == REDIS_OK)
	exit (0);
      redisLog (REDIS_WARNING,
		"SIGTERM received but errors trying to shut down the server, check the logs for more information");
    }
  return 1000 / server.hz;
}

/* Main */
int
clusterMain (int argc, char **argv)
{
  long long teid;
  redisLog (REDIS_WARNING, "Server started, Redis version " REDIS_VERSION);
#ifdef __linux__
  linuxOvercommitMemoryWarning ();
#endif
  loadDataFromDisk ();
  server.last_bgsave_seqnum = server.smr_seqnum;

  /* Warning the user about suspicious maxmemory setting. */
  if (server.maxmemory > 0 && server.maxmemory < 1024 * 1024)
    {
      redisLog (REDIS_WARNING,
		"WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?",
		server.maxmemory);
    }

  smrConnect ();

  /* initializeCron for handling sigterm */
  teid = aeCreateTimeEvent (server.el, 1, initializeCron, NULL, NULL);
  aeMain (server.el);
  aeDeleteTimeEvent (server.el, teid);

  if (server.need_rckpt)
    {
      redisLog (REDIS_NOTICE,
		"Need more checkpoint from %s:%d", server.ckpt_host,
		server.ckpt_port);
      smrDisconnect ();
      server.smr_init_flags = SMR_INIT_RCKPT;
      if (getdump
	  (server.ckpt_host, server.ckpt_port, server.rdb_filename,
	   "0-8191", REDIS_GETDUMP_DEFAULT_NET_LIMIT_MB) != REDIS_OK)
	{
	  exit (1);
	}

      emptyDb (NULL);
      loadDataFromDisk ();
      server.last_bgsave_seqnum = server.smr_seqnum;

      smrConnect ();
      aeMain (server.el);
    }

  if (!server.is_ready)
    {
      redisLog (REDIS_WARNING, "Invalid initialization state");
      exit (1);
    }

  if (server.last_bgsave_seqnum)
    {
      if (smr_seq_ckpted (server.smr_conn, server.last_bgsave_seqnum) != 0)
	{
	  redisLog (REDIS_WARNING,
		    "Failed to notify checkpointed sequence to smr");
	  exit (1);
	}
      else
	{
	  redisLog (REDIS_NOTICE,
		    "Checkpointed sequence is sent to SMR, seqnum:%lld",
		    server.last_bgsave_seqnum);
	}
    }

  server.smr_seqnum_reset = 0;
  server.last_catchup_check_mstime = mstime ();
  server.smr_init_flags = SMR_INIT_CATCHUP_PHASE1;
  if (smr_catchup (server.smr_conn) == -1)
    {
      redisLog (REDIS_WARNING, "Failed to catchup errno(%d)", errno);
      exit (1);
    }
  server.smr_init_flags = SMR_INIT_CATCHUP_PHASE2;
  checkSmrCatchup ();
  aeSetBeforeSleepProc (server.el, beforeSleep);
  aeMain (server.el);
  aeDeleteEventLoop (server.el);
  exit (0);
}
#endif /* NBASE_ARC */

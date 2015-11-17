#include "gw_cmd_mgr.h"
#include "gw_worker.h"

/*
 * Case 1) DELAY, PGS_DEL
 *
 *          [Master]            [Worker]
 *      cmd_send_command
 *      -> admin_xxx_command 
 *      -> parse_xxx_cmd 
 *      -> check_xxx_cmd 
 *      -> send_xxx_cmd 
 *      -> async_send_event     -> exec_xxx_cmd
 *                              -> check_xxx_cmd
 *                              -> post_xxx_cmd
 *                              -> <execute reconfiguration>
 *                              -> reply_xxx_cmd
 *      cmd_async_coro_invoker  <- async_send_event
 *      -> admin_xxx_command
 *      -> post_xxx_cmd
 *      -> <reply to client>
 *
 *
 * Case 2) REDIRECT, PG_ADD, PG_DEL, PGS_ADD
 *
 *          [Master]            [Worker]
 *      cmd_send_command
 *      -> admin_xxx_command 
 *      -> parse_xxx_cmd 
 *      -> check_xxx_cmd 
 *      -> send_xxx_cmd 
 *      -> async_send_event     -> exec_xxx_cmd
 *                              -> check_xxx_cmd
 *                              -> <execute reconfiguration>
 *      cmd_async_coro_invoker  <- async_send_event
 *      -> admin_xxx_command
 *      -> <reply to client>
 */

static dictType confDictType = {
  dictStrHash,
  dictStrDup,
  NULL,
  dictStrKeyCompare,
  dictStrDestructor,
  NULL
};

static command_async *
create_cmd_async (command_context * ctx, async_chan * callee, void *rqst_arg,
		  arg_free_proc * rqst_free)
{
  command_manager *mgr = ctx->my_mgr;
  command_async *cmd;

  cmd = zmalloc (sizeof (command_async));
  cmd->caller = mgr->my_async;
  cmd->callee = callee;
  cmd->return_exec = cmd_async_coro_invoker;
  cmd->rqst_arg = rqst_arg;
  cmd->rqst_free = rqst_free;
  cmd->resp_arg = NULL;
  cmd->resp_free = NULL;
  cmd->ctx = ctx;
  cmd->swallow = 0;
  cmd->finished = 0;

  return cmd;
}

/* ---------------------- CLUSTER INFO command ---------------------- */
void
admin_cluster_info_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;
  sds info;

  info = get_cluster_info_sds (mgr->conf);

  reply =
    stream_create_sbuf_printf (mgr->shared_stream, "$%ld\r\n%s\r\n",
			       sdslen (info), info);
  assert (reply);
  sdsfree (info);
  reply_and_free (ctx, reply);
}

/* ---------------------- DELAY command ---------------------- */
struct delay_arg
{
  long long slot_from;
  long long slot_to;
};

static void
free_delay_arg (void *free_arg)
{
  struct delay_arg *arg = free_arg;
  zfree (arg);
}

static int
parse_delay_cmd (command_context * ctx, long long *slot_from,
		 long long *slot_to)
{
  if (getParsedNumber (ctx->parse_ctx, 1, slot_from) == ERR)
    {
      return ERR;
    }
  if (getParsedNumber (ctx->parse_ctx, 2, slot_to) == ERR)
    {
      return ERR;
    }
  return OK;
}

static int
check_delay_cmd (cluster_conf * conf, long long slot_from, long long slot_to)
{
  block_range *range;
  int i;
  if (slot_from < 0 || slot_to >= conf->nslot || slot_from > slot_to)
    {
      return ERR;
    }
  for (i = slot_from; i <= slot_to; i++)
    {
      if (conf->slot_state[i] != SLOT_STATE_NORMAL)
	{
	  return ERR;
	}
    }

  range = zmalloc (sizeof (block_range));
  range->from = slot_from;
  range->to = slot_to;
  TAILQ_INSERT_TAIL (&conf->block_range_q, range, block_range_tqe);

  for (i = slot_from; i <= slot_to; i++)
    {
      conf->slot_state[i] = SLOT_STATE_BLOCKING;
    }
  return OK;
}

static void
post_delay_cmd (cluster_conf * conf, long long slot_from, long long slot_to)
{
  int i;
  for (i = slot_from; i <= slot_to; i++)
    {
      conf->slot_state[i] = SLOT_STATE_BLOCKED;
    }
}

static void
reply_delay_cmd (void *cbarg)
{
  command_async *cmd = cbarg;

  async_send_event (cmd->callee->el, cmd->caller, cmd->return_exec, cmd);
}

static void
exec_delay_cmd (void *arg, void *chan_data)
{
  command_async *cmd = arg;
  worker *wrk = chan_data;
  struct delay_arg *rqst_arg;
  int ret;

  rqst_arg = cmd->rqst_arg;
  ret = check_delay_cmd (wrk->conf, rqst_arg->slot_from, rqst_arg->slot_to);
  assert (ret == OK);
  post_delay_cmd (wrk->conf, rqst_arg->slot_from, rqst_arg->slot_to);

  ret =
    pool_block_slot (wrk->pool, rqst_arg->slot_from, rqst_arg->slot_to,
		     reply_delay_cmd, cmd);
  assert (ret == OK);
}

static command_async *
send_delay_cmd (command_context * ctx, async_chan * callee,
		long long slot_from, long long slot_to)
{
  command_async *cmd;
  struct delay_arg *rqst_arg;

  rqst_arg = zmalloc (sizeof (struct delay_arg));
  rqst_arg->slot_from = slot_from;
  rqst_arg->slot_to = slot_to;

  cmd = create_cmd_async (ctx, callee, rqst_arg, free_delay_arg);

  async_send_event (cmd->caller->el, callee, exec_delay_cmd, cmd);

  return cmd;
}

void
admin_delay_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  long long slot_from, slot_to;
  command_async *cmd;
  struct delay_arg *arg;
  sbuf *reply;
  int ret, i;

  COROUTINE_BEGIN (ctx->coro_line);

  // Parsing arguments
  if (parse_delay_cmd (ctx, &slot_from, &slot_to) == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream, "-ERR syntax error\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Validate delay range
  ret = check_delay_cmd (mgr->conf, slot_from, slot_to);
  if (ret == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR Slot is out of range or can't be blocked\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Set unable-to-cancel flag, because the command has post action after workers respond.
  ctx->unable_to_cancel = 1;

  gwlog (GW_NOTICE, "Begin DELAY admin command, slot:%lld - %lld", slot_from,
	 slot_to);

  // Send command to all workers
  for (i = 0; i < ARRAY_N (mgr->worker_asyncs); i++)
    {
      async_chan *callee;

      callee = ARRAY_GET (mgr->worker_asyncs, i);
      cmd = send_delay_cmd (ctx, callee, slot_from, slot_to);
      ARRAY_PUSH (&ctx->async_handles, cmd);
    }

  // Receive replies from all workers
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->async_handles); ctx->idx++)
    {
      while (!is_cmd_async_finished
	     (ARRAY_GET (&ctx->async_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
    }

  // Post processing cluster configuration
  cmd = ARRAY_GET (&ctx->async_handles, 0);
  arg = cmd->rqst_arg;
  post_delay_cmd (mgr->conf, arg->slot_from, arg->slot_to);

  gwlog (GW_NOTICE, "Success DELAY admin command");

  // Reply
  reply = stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

/* ---------------------- REDIRECT command ---------------------- */
struct redirect_arg
{
  long long slot_from;
  long long slot_to;
  char *redirect_to;
};

static void
free_redirect_arg (void *free_arg)
{
  struct redirect_arg *arg = free_arg;
  zfree (arg->redirect_to);
  zfree (arg);
}

static int
parse_redirect_cmd (command_context * ctx, long long *slot_from,
		    long long *slot_to, char **redirect_to)
{
  if (getParsedNumber (ctx->parse_ctx, 1, slot_from) == ERR)
    {
      return ERR;
    }
  if (getParsedNumber (ctx->parse_ctx, 2, slot_to) == ERR)
    {
      return ERR;
    }
  if (getParsedStr (ctx->parse_ctx, 3, redirect_to) == ERR)
    {
      return ERR;
    }
  return OK;
}

static int
check_redirect_cmd (cluster_conf * conf, long long slot_from,
		    long long slot_to, char *redirect_to)
{
  partition_conf *pg;
  block_range *range;
  int i;

  if (slot_from < 0 || slot_to >= conf->nslot || slot_from > slot_to)
    {
      return ERR;
    }
  pg = dictFetchValue (conf->partition, redirect_to);
  if (!pg || dictSize (pg->redis) == 0)
    {
      return ERR;
    }

  for (i = slot_from; i <= slot_to; i++)
    {
      if (conf->slot_state[i] != SLOT_STATE_BLOCKED)
	{
	  return ERR;
	}
    }

  range = NULL;
  TAILQ_FOREACH (range, &conf->block_range_q, block_range_tqe)
  {
    if (range->from == slot_from && range->to == slot_to)
      {
	break;
      }
  }
  if (range == NULL)
    {
      return ERR;
    }
  TAILQ_REMOVE (&conf->block_range_q, range, block_range_tqe);
  zfree (range);

  for (i = slot_from; i <= slot_to; i++)
    {
      conf->slot_state[i] = SLOT_STATE_NORMAL;
      conf->slots[i] = pg;
    }
  return OK;
}

static void
exec_redirect_cmd (void *arg, void *chan_data)
{
  command_async *cmd = arg;
  worker *wrk = chan_data;
  struct redirect_arg *rqst_arg;
  int ret;

  rqst_arg = cmd->rqst_arg;
  ret =
    check_redirect_cmd (wrk->conf, rqst_arg->slot_from, rqst_arg->slot_to,
			rqst_arg->redirect_to);
  assert (ret == OK);
  ret =
    pool_redirect_slot (wrk->pool, rqst_arg->slot_from, rqst_arg->slot_to,
			rqst_arg->redirect_to);
  assert (ret == OK);

  async_send_event (cmd->callee->el, cmd->caller, cmd->return_exec, cmd);
}

static command_async *
send_redirect_cmd (command_context * ctx, async_chan * callee,
		   long long slot_from, long long slot_to, char *redirect_to)
{
  command_async *cmd;
  struct redirect_arg *rqst_arg;

  rqst_arg = zmalloc (sizeof (struct redirect_arg));
  rqst_arg->slot_from = slot_from;
  rqst_arg->slot_to = slot_to;
  rqst_arg->redirect_to = zstrdup (redirect_to);

  cmd = create_cmd_async (ctx, callee, rqst_arg, free_redirect_arg);

  async_send_event (cmd->caller->el, callee, exec_redirect_cmd, cmd);

  return cmd;
}

void
admin_redirect_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  long long slot_from, slot_to;
  char *redirect_to;
  sbuf *reply;
  int ret, i;

  COROUTINE_BEGIN (ctx->coro_line);

  // Parsing arguments
  if (parse_redirect_cmd (ctx, &slot_from, &slot_to, &redirect_to) == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream, "-ERR syntax error\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Validate redirect range
  ret = check_redirect_cmd (mgr->conf, slot_from, slot_to, redirect_to);
  if (ret == ERR)
    {
      zfree (redirect_to);

      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR Slot is out of range or can't be redirected\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  gwlog (GW_NOTICE,
	 "Begin REDIRECT admin command, slot:%lld - %lld, redirect PG:%s",
	 slot_from, slot_to, redirect_to);

  // Send command to all workers
  for (i = 0; i < ARRAY_N (mgr->worker_asyncs); i++)
    {
      async_chan *callee;
      command_async *cmd;

      callee = ARRAY_GET (mgr->worker_asyncs, i);
      cmd = send_redirect_cmd (ctx, callee, slot_from, slot_to, redirect_to);
      ARRAY_PUSH (&ctx->async_handles, cmd);
    }
  zfree (redirect_to);

  // Receive replies from all workers
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->async_handles); ctx->idx++)
    {
      while (!is_cmd_async_finished
	     (ARRAY_GET (&ctx->async_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
    }

  gwlog (GW_NOTICE, "Success REDIRECT admin command");

  // Reply
  reply = stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

/* ---------------------- PG ADD command ---------------------- */
struct pg_add_arg
{
  char *pg_id;
};

static void
free_pg_add_arg (void *free_arg)
{
  struct pg_add_arg *arg = free_arg;
  zfree (arg->pg_id);
  zfree (arg);
}

static int
parse_pg_add_cmd (command_context * ctx, char **pg_id)
{
  if (getParsedStr (ctx->parse_ctx, 1, pg_id) == ERR)
    {
      return ERR;
    }
  return OK;
}

static int
check_pg_add_cmd (cluster_conf * conf, char *pg_id)
{
  partition_conf *pg;
  int ret;

  if (dictFetchValue (conf->partition, pg_id))
    {
      return ERR;
    }

  pg = zmalloc (sizeof (partition_conf));
  pg->pg_id = sdsnew (pg_id);
  pg->redis = dictCreate (&confDictType, NULL);
  ret = dictAdd (conf->partition, pg_id, pg);
  assert (ret == DICT_OK);
  return OK;
}

static void
exec_pg_add_cmd (void *arg, void *chan_data)
{
  command_async *cmd = arg;
  worker *wrk = chan_data;
  struct pg_add_arg *rqst_arg;
  int ret;

  rqst_arg = cmd->rqst_arg;
  ret = check_pg_add_cmd (wrk->conf, rqst_arg->pg_id);
  assert (ret == OK);
  ret = pool_add_partition (wrk->pool, rqst_arg->pg_id);
  assert (ret == OK);

  async_send_event (cmd->callee->el, cmd->caller, cmd->return_exec, cmd);
}

static command_async *
send_pg_add_cmd (command_context * ctx, async_chan * callee, char *pg_id)
{
  command_async *cmd;
  struct pg_add_arg *rqst_arg;

  rqst_arg = zmalloc (sizeof (struct pg_add_arg));
  rqst_arg->pg_id = zstrdup (pg_id);

  cmd = create_cmd_async (ctx, callee, rqst_arg, free_pg_add_arg);

  async_send_event (cmd->caller->el, callee, exec_pg_add_cmd, cmd);

  return cmd;
}

void
admin_pg_add_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  char *pg_id;
  sbuf *reply;
  int ret, i;

  COROUTINE_BEGIN (ctx->coro_line);

  // Parsing arguments
  if (parse_pg_add_cmd (ctx, &pg_id) == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream, "-ERR syntax error\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Validate pg id
  ret = check_pg_add_cmd (mgr->conf, pg_id);
  if (ret == ERR)
    {
      zfree (pg_id);

      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR PG already exists\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  gwlog (GW_NOTICE, "Begin PG_ADD admin command, PG:%s", pg_id);

  // Send command to all workers
  for (i = 0; i < ARRAY_N (mgr->worker_asyncs); i++)
    {
      async_chan *callee;
      command_async *cmd;

      callee = ARRAY_GET (mgr->worker_asyncs, i);
      cmd = send_pg_add_cmd (ctx, callee, pg_id);
      ARRAY_PUSH (&ctx->async_handles, cmd);
    }
  zfree (pg_id);

  // Receive replies from all workers
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->async_handles); ctx->idx++)
    {
      while (!is_cmd_async_finished
	     (ARRAY_GET (&ctx->async_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
    }

  gwlog (GW_NOTICE, "Success PG_ADD admin command");

  // Reply
  reply = stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

/* ---------------------- PG DEL command ---------------------- */
struct pg_del_arg
{
  char *pg_id;
};

static void
free_pg_del_arg (void *free_arg)
{
  struct pg_del_arg *arg = free_arg;
  zfree (arg->pg_id);
  zfree (arg);
}

static int
parse_pg_del_cmd (command_context * ctx, char **pg_id)
{
  if (getParsedStr (ctx->parse_ctx, 1, pg_id) == ERR)
    {
      return ERR;
    }
  return OK;
}

static int
check_pg_del_cmd (cluster_conf * conf, char *pg_id)
{
  partition_conf *pg;
  int i, ret;

  pg = dictFetchValue (conf->partition, pg_id);
  if (!pg || dictSize (pg->redis) > 0)
    {
      return ERR;
    }

  for (i = 0; i < conf->nslot; i++)
    {
      if (conf->slots[i] == pg)
	{
	  conf->slots[i] = NULL;
	}
    }

  sdsfree (pg->pg_id);
  dictRelease (pg->redis);
  ret = dictDelete (conf->partition, pg_id);
  assert (ret == DICT_OK);
  zfree (pg);
  return OK;
}

static void
exec_pg_del_cmd (void *arg, void *chan_data)
{
  command_async *cmd = arg;
  worker *wrk = chan_data;
  struct pg_del_arg *rqst_arg;
  int ret;

  rqst_arg = cmd->rqst_arg;
  ret = check_pg_del_cmd (wrk->conf, rqst_arg->pg_id);
  assert (ret == OK);
  ret = pool_delete_partition (wrk->pool, rqst_arg->pg_id);
  assert (ret == OK);

  async_send_event (cmd->callee->el, cmd->caller, cmd->return_exec, cmd);
}

static command_async *
send_pg_del_cmd (command_context * ctx, async_chan * callee, char *pg_id)
{
  command_async *cmd;
  struct pg_del_arg *rqst_arg;

  rqst_arg = zmalloc (sizeof (struct pg_del_arg));
  rqst_arg->pg_id = zstrdup (pg_id);

  cmd = create_cmd_async (ctx, callee, rqst_arg, free_pg_del_arg);

  async_send_event (cmd->caller->el, callee, exec_pg_del_cmd, cmd);

  return cmd;
}

void
admin_pg_del_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  char *pg_id;
  sbuf *reply;
  int ret, i;

  COROUTINE_BEGIN (ctx->coro_line);

  // Parsing arguments
  if (parse_pg_del_cmd (ctx, &pg_id) == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream, "-ERR syntax error\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Validate pg id
  ret = check_pg_del_cmd (mgr->conf, pg_id);
  if (ret == ERR)
    {
      zfree (pg_id);

      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR PG doesn't exist or can't be deleted\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  gwlog (GW_NOTICE, "Begin PG_DEL admin command, PG:%s", pg_id);

  // Send command to all workers
  for (i = 0; i < ARRAY_N (mgr->worker_asyncs); i++)
    {
      async_chan *callee;
      command_async *cmd;

      callee = ARRAY_GET (mgr->worker_asyncs, i);
      cmd = send_pg_del_cmd (ctx, callee, pg_id);
      ARRAY_PUSH (&ctx->async_handles, cmd);
    }
  zfree (pg_id);

  // Receive replies from all workers
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->async_handles); ctx->idx++)
    {
      while (!is_cmd_async_finished
	     (ARRAY_GET (&ctx->async_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
    }

  gwlog (GW_NOTICE, "Success PG_DEL admin command");

  // Reply
  reply = stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

/* ---------------------- PGS ADD command ---------------------- */
struct pgs_add_arg
{
  char *redis_id;
  char *pg_id;
  char *addr;
  long long port;
};

static void
free_pgs_add_arg (void *free_arg)
{
  struct pgs_add_arg *arg = free_arg;
  zfree (arg->redis_id);
  zfree (arg->pg_id);
  zfree (arg->addr);
  zfree (arg);
}

static int
parse_pgs_add_cmd (command_context * ctx, char **redis_id, char **pg_id,
		   char **addr, long long *port)
{
  if (getParsedStr (ctx->parse_ctx, 1, redis_id) == ERR)
    {
      return ERR;
    }
  if (getParsedStr (ctx->parse_ctx, 2, pg_id) == ERR)
    {
      zfree (*redis_id);
      return ERR;
    }
  if (getParsedStr (ctx->parse_ctx, 3, addr) == ERR)
    {
      zfree (*redis_id);
      zfree (*pg_id);
      return ERR;
    }
  if (getParsedNumber (ctx->parse_ctx, 4, port) == ERR)
    {
      zfree (*redis_id);
      zfree (*pg_id);
      zfree (*addr);
      return ERR;
    }
  return OK;
}

static int
check_pgs_add_cmd (cluster_conf * conf, char *redis_id, char *pg_id,
		   char *addr, long long port)
{
  partition_conf *pg;
  redis_conf *redis;
  int ret;

  if (dictFetchValue (conf->redis, redis_id))
    {
      return ERR;
    }

  pg = dictFetchValue (conf->partition, pg_id);
  if (!pg)
    {
      return ERR;
    }

  assert (!dictFetchValue (pg->redis, redis_id));

  redis = zmalloc (sizeof (redis_conf));
  redis->redis_id = sdsnew (redis_id);
  redis->pg_id = sdsnew (pg_id);
  redis->addr = sdsnew (addr);
  redis->port = port;
  redis->is_local = isLocalIpAddr (conf->local_ip_addrs, addr);
  redis->domain_socket_path =
    redis->is_local ? default_domain_path (conf->nbase_arc_home,
					   redis->port) : NULL;
  redis->deleting = 0;
  ret = dictAdd (conf->redis, redis_id, redis);
  assert (ret == DICT_OK);
  ret = dictAdd (pg->redis, redis_id, redis);
  assert (ret == DICT_OK);
  return OK;
}

static void
exec_pgs_add_cmd (void *arg, void *chan_data)
{
  command_async *cmd = arg;
  worker *wrk = chan_data;
  cluster_conf *conf = wrk->conf;
  redis_conf *redis;
  struct pgs_add_arg *rqst_arg;
  int ret;

  rqst_arg = cmd->rqst_arg;
  ret =
    check_pgs_add_cmd (conf, rqst_arg->redis_id, rqst_arg->pg_id,
		       rqst_arg->addr, rqst_arg->port);
  assert (ret == OK);

  redis = dictFetchValue (conf->redis, rqst_arg->redis_id);
  assert (redis != NULL);

  ret =
    pool_add_server (wrk->pool, redis->pg_id, redis->redis_id, redis->addr,
		     redis->port, redis->domain_socket_path, redis->is_local);
  assert (ret == OK);
  ret =
    pool_add_connection (wrk->pool, rqst_arg->redis_id,
			 conf->prefer_domain_socket);
  assert (ret == OK);

  async_send_event (cmd->callee->el, cmd->caller, cmd->return_exec, cmd);
}

static command_async *
send_pgs_add_cmd (command_context * ctx, async_chan * callee, char *redis_id,
		  char *pg_id, char *addr, long long port)
{
  command_async *cmd;
  struct pgs_add_arg *rqst_arg;

  rqst_arg = zmalloc (sizeof (struct pgs_add_arg));
  rqst_arg->redis_id = zstrdup (redis_id);
  rqst_arg->pg_id = zstrdup (pg_id);
  rqst_arg->addr = zstrdup (addr);
  rqst_arg->port = port;

  cmd = create_cmd_async (ctx, callee, rqst_arg, free_pgs_add_arg);

  async_send_event (cmd->caller->el, callee, exec_pgs_add_cmd, cmd);

  return cmd;
}

void
admin_pgs_add_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  char *redis_id, *pg_id, *addr;
  long long port;
  sbuf *reply;
  int ret, i;

  COROUTINE_BEGIN (ctx->coro_line);

  // Parsing arguments
  if (parse_pgs_add_cmd (ctx, &redis_id, &pg_id, &addr, &port) == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream, "-ERR syntax error\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Validate redis id
  ret = check_pgs_add_cmd (mgr->conf, redis_id, pg_id, addr, port);
  if (ret == ERR)
    {
      zfree (redis_id);
      zfree (pg_id);
      zfree (addr);

      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR Redis already exists or can't be added\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  gwlog (GW_NOTICE, "Begin PGS_ADD admin command, REDIS:%s, PG:%s, %s:%d",
	 redis_id, pg_id, addr, port);

  // Send command to all workers
  for (i = 0; i < ARRAY_N (mgr->worker_asyncs); i++)
    {
      async_chan *callee;
      command_async *cmd;

      callee = ARRAY_GET (mgr->worker_asyncs, i);
      cmd = send_pgs_add_cmd (ctx, callee, redis_id, pg_id, addr, port);
      ARRAY_PUSH (&ctx->async_handles, cmd);
    }
  zfree (redis_id);
  zfree (pg_id);
  zfree (addr);

  // Receive replies from all workers
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->async_handles); ctx->idx++)
    {
      while (!is_cmd_async_finished
	     (ARRAY_GET (&ctx->async_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
    }

  gwlog (GW_NOTICE, "Success PGS_ADD admin command");

  // Reply
  reply = stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

/* ---------------------- PGS DEL command ---------------------- */
struct pgs_del_arg
{
  char *redis_id;
  char *pg_id;
};

static void
free_pgs_del_arg (void *free_arg)
{
  struct pgs_del_arg *arg = free_arg;
  zfree (arg->redis_id);
  zfree (arg->pg_id);
  zfree (arg);
}

static int
parse_pgs_del_cmd (command_context * ctx, char **redis_id, char **pg_id)
{
  if (getParsedStr (ctx->parse_ctx, 1, redis_id) == ERR)
    {
      return ERR;
    }
  if (getParsedStr (ctx->parse_ctx, 2, pg_id) == ERR)
    {
      zfree (redis_id);
      return ERR;
    }
  return OK;
}

static int
check_pgs_del_cmd (cluster_conf * conf, char *redis_id, char *pg_id)
{
  partition_conf *pg;
  redis_conf *redis;

  redis = dictFetchValue (conf->redis, redis_id);
  if (!redis || redis->deleting)
    {
      return ERR;
    }
  pg = dictFetchValue (conf->partition, pg_id);
  if (!pg || strcmp (pg->pg_id, redis->pg_id))
    {
      return ERR;
    }
  assert (dictFetchValue (pg->redis, redis_id));
  redis->deleting = 1;
  return OK;
}

static void
post_pgs_del_cmd (cluster_conf * conf, char *redis_id, char *pg_id)
{
  partition_conf *pg;
  redis_conf *redis;
  int ret;

  redis = dictFetchValue (conf->redis, redis_id);
  pg = dictFetchValue (conf->partition, pg_id);
  ret = dictDelete (conf->redis, redis_id);
  assert (ret == DICT_OK);
  ret = dictDelete (pg->redis, redis_id);
  assert (ret == DICT_OK);
  sdsfree (redis->redis_id);
  sdsfree (redis->pg_id);
  sdsfree (redis->addr);
  sdsfree (redis->domain_socket_path);
  zfree (redis);
}

static void
reply_pgs_del_cmd (void *cbarg)
{
  command_async *cmd = cbarg;

  async_send_event (cmd->callee->el, cmd->caller, cmd->return_exec, cmd);
}

static void
exec_pgs_del_cmd (void *arg, void *chan_data)
{
  command_async *cmd = arg;
  worker *wrk = chan_data;
  struct pgs_del_arg *rqst_arg;
  int ret;

  rqst_arg = cmd->rqst_arg;
  ret = check_pgs_del_cmd (wrk->conf, rqst_arg->redis_id, rqst_arg->pg_id);
  assert (ret == OK);
  post_pgs_del_cmd (wrk->conf, rqst_arg->redis_id, rqst_arg->pg_id);

  ret =
    pool_delete_server (wrk->pool, rqst_arg->redis_id, reply_pgs_del_cmd,
			cmd);
  assert (ret == OK);
}

static command_async *
send_pgs_del_cmd (command_context * ctx, async_chan * callee, char *redis_id,
		  char *pg_id)
{
  command_async *cmd;
  struct pgs_del_arg *rqst_arg;

  rqst_arg = zmalloc (sizeof (struct pgs_del_arg));
  rqst_arg->redis_id = zstrdup (redis_id);
  rqst_arg->pg_id = zstrdup (pg_id);

  cmd = create_cmd_async (ctx, callee, rqst_arg, free_pgs_del_arg);

  async_send_event (cmd->caller->el, callee, exec_pgs_del_cmd, cmd);

  return cmd;
}

void
admin_pgs_del_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  char *redis_id, *pg_id;
  command_async *cmd;
  struct pgs_del_arg *arg;
  sbuf *reply;
  int ret, i;

  COROUTINE_BEGIN (ctx->coro_line);

  // Parsing arguments
  if (parse_pgs_del_cmd (ctx, &redis_id, &pg_id) == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream, "-ERR syntax error\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Validate redis id
  ret = check_pgs_del_cmd (mgr->conf, redis_id, pg_id);
  if (ret == ERR)
    {
      zfree (redis_id);
      zfree (pg_id);

      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR Redis doesn't exist or can't be deleted\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  // Set unable-to-cancel flag, because the command has post action after workers respond.
  ctx->unable_to_cancel = 1;

  gwlog (GW_NOTICE, "Begin PGS_DEL admin command, REDIS:%s, PG:%s", redis_id,
	 pg_id);

  // Send command to all workers
  for (i = 0; i < ARRAY_N (mgr->worker_asyncs); i++)
    {
      async_chan *callee;
      command_async *cmd;

      callee = ARRAY_GET (mgr->worker_asyncs, i);
      cmd = send_pgs_del_cmd (ctx, callee, redis_id, pg_id);
      ARRAY_PUSH (&ctx->async_handles, cmd);
    }
  zfree (redis_id);
  zfree (pg_id);

  // Receive replies from all workers
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->async_handles); ctx->idx++)
    {
      while (!is_cmd_async_finished
	     (ARRAY_GET (&ctx->async_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
    }

  // Post processing cluster configuration
  cmd = ARRAY_GET (&ctx->async_handles, 0);
  arg = cmd->rqst_arg;
  post_pgs_del_cmd (mgr->conf, arg->redis_id, arg->pg_id);

  gwlog (GW_NOTICE, "Success PGS_DEL admin command");

  // Reply
  reply = stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

/* ---------------------- HELP command ---------------------- */
static const char *admin_help =
  "==================== USAGE =====================\r\n"
  " cluster_info\r\n"
  " delay <slot from> <slot to>\r\n"
  " redirect <slot from> <slot to> <target pg_id>\r\n"
  " pg_add <pg_id>\r\n"
  " pg_del <pg_id>\r\n"
  " pgs_add <redis_id> <pg_id> <host> <redis port>\r\n"
  " pgs_del <redis_id> <pg_id>\r\n"
  " ping\r\n" " help\r\n" " quit\r\n"
  "================================================\r\n";

void
admin_help_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;

  reply =
    stream_create_sbuf_printf (mgr->shared_stream, "$%ld\r\n%s\r\n",
			       strlen (admin_help), admin_help);
  assert (reply);
  reply_and_free (ctx, reply);
}

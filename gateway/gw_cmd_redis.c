#include "gw_cmd_mgr.h"

static int
get_hash (ParseContext * ctx, int arg_idx)
{
  sbuf_pos start_pos;
  ssize_t len;
  int ret;

  if (getArgumentCount (ctx) <= arg_idx)
    {
      return ERR;
    }

  ret = getArgumentPosition (ctx, arg_idx, &start_pos, &len);
  assert (ret == OK);

  return sbuf_crc16 (start_pos, len, 0);
}

static int
get_slot_idx (command_manager * mgr, ParseContext * ctx, int arg_idx)
{
  int hash;
  hash = get_hash (ctx, arg_idx);
  if (hash == ERR)
    {
      return ERR;
    }
  return pool_slot_idx (mgr->pool, hash);
}

void
single_key_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  redis_command *cmd = ctx->cmd;
  redis_msg *handle;
  sbuf *reply;
  int slot_idx, ret;

  COROUTINE_BEGIN (ctx->coro_line);

  slot_idx = get_slot_idx (mgr, ctx->parse_ctx, cmd->firstkey);
  assert (slot_idx != ERR);

  ret =
    pool_send_query (mgr->pool, ctx->query, slot_idx, cmd_redis_coro_invoker,
		     ctx, &handle);
  ctx->query = NULL;
  ARRAY_PUSH (&ctx->msg_handles, handle);

  if (ret == ERR)
    {
      reply = pool_take_reply (handle);
      reply_and_free (ctx, reply);
      return;
    }

  COROUTINE_YIELD;

  handle = ARRAY_GET (&ctx->msg_handles, 0);
  if (pool_msg_fail (handle))
    {
      reply = pool_take_reply (handle);
      reply_and_free (ctx, reply);
      return;
    }

  if (pool_msg_local (handle))
    {
      ctx->local = 1;
    }
  reply = pool_take_reply (handle);
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

static void
set_split_seqs (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  int firstkey, lastkey, keystep, slot_idx, query_idx, count, i;

  firstkey = ctx->cmd->firstkey;
  lastkey = getArgumentCount (ctx->parse_ctx) + ctx->cmd->lastkey;
  keystep = ctx->cmd->keystep;

  for (i = firstkey; i <= lastkey; i += keystep)
    {
      slot_idx = get_slot_idx (ctx->my_mgr, ctx->parse_ctx, i);
      assert (slot_idx != ERR);

      index_helper_set (mgr->idx_helper, slot_idx, &query_idx, &count);
      ARRAY_PUSH (&ctx->msg_seqs, query_idx);
    }
}

static void
create_split_queries (command_context * ctx, array_query * ret_queries)
{
  command_manager *mgr;
  sbuf *query;
  int keystep, nquery, i, j, k;

  mgr = ctx->my_mgr;
  keystep = ctx->cmd->keystep;
  nquery = index_helper_total (mgr->idx_helper);

  for (i = 0; i < nquery; i++)
    {
      int slot_idx, count, ret;

      // Multibulk number and Command
      index_helper_get_by_idx (mgr->idx_helper, i, &slot_idx, &count);
      ret =
	stream_append_printf (mgr->shared_stream, "*%d\r\n$%ld\r\n%s\r\n",
			      count * keystep + 1, strlen (ctx->cmd->name),
			      ctx->cmd->name);
      assert (ret == OK);

      // Find bulk args and append
      for (j = 0; j < ARRAY_N (&ctx->msg_seqs); j++)
	{
	  if (ARRAY_GET (&ctx->msg_seqs, j) == i)
	    {
	      // Found bulk arg
	      for (k = 0; k < keystep; k++)
		{
		  sbuf_pos start;
		  ssize_t len;

		  getArgumentPosition (ctx->parse_ctx, 1 + j * keystep + k,
				       &start, &len);
		  ret =
		    stream_append_printf (mgr->shared_stream, "$%ld\r\n",
					  len);
		  assert (ret == OK);
		  ret =
		    stream_append_copy_sbuf_pos_len (mgr->shared_stream,
						     start, len);
		  assert (ret == OK);
		  ret =
		    stream_append_copy_buf (mgr->shared_stream,
					    (void *) "\r\n", 2);
		  assert (ret == OK);
		}
	    }
	}
      query =
	stream_create_sbuf (mgr->shared_stream,
			    stream_last_pos (mgr->shared_stream));
      assert (query);
      ARRAY_PUSH (ret_queries, query);
    }
}

static void
free_remain_queries (array_query * queries)
{
  int i;

  for (i = 0; i < ARRAY_N (queries); i++)
    {
      sbuf *query;

      query = ARRAY_GET (queries, i);
      if (query)
	{
	  sbuf_free (query);
	}
    }
}

static int
send_split_queries (command_context * ctx, sbuf ** err_reply)
{
  command_manager *mgr = ctx->my_mgr;
  redis_msg *handle;
  array_query queries;
  int i, ret;

  ARRAY_INIT (&queries);
  index_helper_clear (mgr->idx_helper);

  // find split seqs by slot_idx
  set_split_seqs (ctx);

  // create splited queries
  create_split_queries (ctx, &queries);

  // Send splited queries
  for (i = 0; i < ARRAY_N (&queries); i++)
    {
      int slot_idx, count;
      sbuf *query;

      index_helper_get_by_idx (mgr->idx_helper, i, &slot_idx, &count);

      query = ARRAY_GET (&queries, i);
      ARRAY_SET (&queries, i, NULL);

      ret =
	pool_send_query (ctx->my_mgr->pool, query, slot_idx,
			 cmd_redis_coro_invoker, ctx, &handle);
      ARRAY_PUSH (&ctx->msg_handles, handle);

      if (ret == ERR)
	{
	  *err_reply = pool_take_reply (handle);
	  free_remain_queries (&queries);
	  ARRAY_FINALIZE (&queries);

	  return ERR;
	}
    }
  ARRAY_FINALIZE (&queries);

  return OK;
}

static sbuf *
merge_reply_mset (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;

  return stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
}

static sbuf *
merge_reply_mget (command_context * ctx)
{
  command_manager *mgr;
  redis_msg *msg;
  int i, msg_idx, ret, tvar, count;
  sbuf_pos start;
  ParseContext *parse_ctx;
  ssize_t len;
  sbuf *reply;

  mgr = ctx->my_mgr;

  ret =
    stream_append_printf (mgr->shared_stream, "*%d\r\n",
			  getArgumentCount (ctx->parse_ctx) - 1);
  assert (ret == OK);

  index_helper_clear (mgr->idx_helper);
  for (i = 0; i < ARRAY_N (&ctx->msg_seqs); i++)
    {
      msg_idx = ARRAY_GET (&ctx->msg_seqs, i);
      msg = ARRAY_GET (&ctx->msg_handles, msg_idx);
      index_helper_set (mgr->idx_helper, msg_idx, &tvar, &count);
      parse_ctx = pool_get_parse_ctx (msg);

      getArgumentPosition (parse_ctx, count - 1, &start, &len);
      ret = stream_append_printf (mgr->shared_stream, "$%ld\r\n", len);
      assert (ret == OK);
      if (len < 0)
	{
	  continue;
	}
      ret = stream_append_copy_sbuf_pos_len (mgr->shared_stream, start, len);
      assert (ret == OK);
      ret = stream_append_copy_buf (mgr->shared_stream, (void *) "\r\n", 2);
      assert (ret == OK);
    }
  reply =
    stream_create_sbuf (mgr->shared_stream,
			stream_last_pos (mgr->shared_stream));
  assert (reply);
  return reply;
}

static sbuf *
merge_reply_del (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  redis_msg *msg;
  ssize_t len;
  sbuf_pos start;
  ParseContext *parse_ctx;
  long long del_count, ll;
  int i, ret;

  del_count = 0;
  for (i = 0; i < ARRAY_N (&ctx->msg_handles); i++)
    {
      msg = ARRAY_GET (&ctx->msg_handles, i);
      parse_ctx = pool_get_parse_ctx (msg);

      getArgumentPosition (parse_ctx, 0, &start, &len);
      sbuf_next_pos (&start);	// Skip ':' character
      ret = sbuf_string2ll (start, len - 1, &ll);
      assert (ret);		// Redis DEL command always returns in ':<number>\r\n' format.
      del_count += ll;
    }
  return stream_create_sbuf_printf (mgr->shared_stream, ":%lld\r\n",
				    del_count);
}

void
mset_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply, *err_reply;
  redis_msg *handle;
  int ret;

  COROUTINE_BEGIN (ctx->coro_line);

  // Arity check
  if (getArgumentCount (ctx->parse_ctx) % 2 == 0)
    {
      reply =
	stream_create_sbuf_printf (mgr->shared_stream,
				   "-ERR wrong number of arguments for '%s' command\r\n",
				   ctx->cmd->name);
      reply_and_free (ctx, reply);
      return;
    }
  // Send split queries
  ret = send_split_queries (ctx, &err_reply);
  if (ret == ERR)
    {
      reply_and_free (ctx, err_reply);
      return;
    }
  // Receive replies of queries
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->msg_handles); ctx->idx++)
    {
      while (!pool_msg_finished (ARRAY_GET (&ctx->msg_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}

      handle = ARRAY_GET (&ctx->msg_handles, ctx->idx);
      if (pool_msg_fail (handle))
	{
	  reply = pool_take_reply (handle);
	  reply_and_free (ctx, reply);
	  return;
	}
    }

  // Merge reply and send
  reply = merge_reply_mset (ctx);
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

void
mget_command (command_context * ctx)
{
  sbuf *reply, *err_reply;
  redis_msg *handle;
  int ret;

  COROUTINE_BEGIN (ctx->coro_line);

  // Send split queries
  ret = send_split_queries (ctx, &err_reply);
  if (ret == ERR)
    {
      reply_and_free (ctx, err_reply);
      return;
    }
  // Receive replies of queries
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->msg_handles); ctx->idx++)
    {
      while (!pool_msg_finished (ARRAY_GET (&ctx->msg_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}

      handle = ARRAY_GET (&ctx->msg_handles, ctx->idx);
      if (pool_msg_fail (handle))
	{
	  reply = pool_take_reply (handle);
	  reply_and_free (ctx, reply);
	  return;
	}
    }

  // Merge reply and send
  reply = merge_reply_mget (ctx);
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

void
del_command (command_context * ctx)
{
  sbuf *reply, *err_reply;
  redis_msg *handle;
  int ret;

  COROUTINE_BEGIN (ctx->coro_line);

  // Send split queries
  ret = send_split_queries (ctx, &err_reply);
  if (ret == ERR)
    {
      reply_and_free (ctx, err_reply);
      return;
    }
  // Receive replies of queries
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->msg_handles); ctx->idx++)
    {
      while (!pool_msg_finished (ARRAY_GET (&ctx->msg_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}

      handle = ARRAY_GET (&ctx->msg_handles, ctx->idx);
      if (pool_msg_fail (handle))
	{
	  reply = pool_take_reply (handle);
	  reply_and_free (ctx, reply);
	  return;
	}
    }

  // Merge reply and send
  reply = merge_reply_del (ctx);
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

void
ping_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;

  reply = stream_create_sbuf_str (mgr->shared_stream, "+PONG\r\n");
  reply_and_free (ctx, reply);
}

void
quit_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;

  ctx->close_after_reply = 1;
  reply = stream_create_sbuf_str (mgr->shared_stream, "+OK\r\n");
  reply_and_free (ctx, reply);
}

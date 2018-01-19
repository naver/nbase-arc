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

  return sbuf_crc16 (&start_pos, len, 0);
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
						     &start, len);
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
      ret = stream_append_copy_sbuf_pos_len (mgr->shared_stream, &start, len);
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
merge_int_replies (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  redis_msg *msg;
  ssize_t len;
  sbuf_pos start;
  ParseContext *parse_ctx;
  long long count, ll;
  int i, ret;

  count = 0;
  for (i = 0; i < ARRAY_N (&ctx->msg_handles); i++)
    {
      char ch;
      msg = ARRAY_GET (&ctx->msg_handles, i);
      parse_ctx = pool_get_parse_ctx (msg);

      getArgumentPosition (parse_ctx, 0, &start, &len);
      ch = sbuf_char (&start);
      if (ch == ':')
	{
	  sbuf_next_pos (&start);	// Skip ':' character
	  ret = sbuf_string2ll (&start, len - 1, &ll);
	  if (ret)
	    {
	      count += ll;
	      continue;
	    }
	}
      // bad integer response
      return stream_create_sbuf_printf (mgr->shared_stream,
					"-ERR bad integer response\r\n");
    }
  return stream_create_sbuf_printf (mgr->shared_stream, ":%lld\r\n", count);
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
multi_key_int_reply_command (command_context * ctx)
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
  reply = merge_int_replies (ctx);
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

/* buf = '<pg_idx>' or '<cursor><pg_idx>'
 * case 1) <pg_idx> = 1~4 byte length, cursor = 0
 * case 2) <cursor><pg_idx> = cursor length + fixed 4 byte pg_idx
 * */
//static int decode_cursor (char *buf, ssize_t len, unsigned long long *cursor, int *cursor_digits, int *pg_idx) {
static int
decode_cursor (command_context * ctx, unsigned long long *cursor,
	       int *cursor_digits, int *pg_idx)
{
  char pg_idx_buf[5], cursor_buf[128];
  char *buf, *eptr;
  int ret, ok, i;
  long long value;
  sbuf_pos start;
  ssize_t len;

  ret = getArgumentPosition (ctx->parse_ctx, 1, &start, &len);
  assert (ret == OK);
  ret = getParsedStr (ctx->parse_ctx, 1, &buf);
  assert (ret == OK);

  if (len <= 4)
    {
      ok = string2ll (buf, len, &value);
      if (!ok)
	{
	  zfree (buf);
	  return ERR;
	}
      *pg_idx = value;
      *cursor = 0;
      *cursor_digits = 1;
      zfree (buf);
      return OK;
    }

  memcpy (cursor_buf, buf, len - 4);
  cursor_buf[len - 4] = '\0';
  errno = 0;
  *cursor = strtoull (cursor_buf, &eptr, 10);
  if (isspace (cursor_buf[0]) || eptr[0] != '\0' || errno == ERANGE)
    {
      zfree (buf);
      return ERR;
    }
  *cursor_digits = len - 4;

  memcpy (pg_idx_buf, buf + len - 4, 4);
  pg_idx_buf[4] = '\0';
  value = 0;
  for (i = 0; i < 4; i++)
    {
      if (pg_idx_buf[i] < '0' || pg_idx_buf[i] > '9')
	{
	  zfree (buf);
	  return ERR;
	}
      value = value * 10 + pg_idx_buf[i] - '0';
    }
  *pg_idx = value;
  zfree (buf);
  return OK;
}

static sbuf *
create_modified_scan_query (command_context * ctx, unsigned long long cursor,
			    int cursor_digits, char skip_argc)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *query;
  int i, ret, argc;
  sbuf_pos start;
  ssize_t len;

  argc = getArgumentCount (ctx->parse_ctx);
  stream_append_printf (mgr->shared_stream, "*%d\r\n$4\r\nSCAN\r\n",
			argc - skip_argc + 1);
  stream_append_printf (mgr->shared_stream, "$%d\r\n%llu\r\n", cursor_digits,
			cursor);
  for (i = skip_argc + 1; i < argc; i++)
    {
      getArgumentPosition (ctx->parse_ctx, i, &start, &len);
      ret = stream_append_printf (mgr->shared_stream, "$%ld\r\n", len);
      assert (ret == OK);
      ret = stream_append_copy_sbuf_pos_len (mgr->shared_stream, &start, len);
      assert (ret == OK);
      ret = stream_append_copy_buf (mgr->shared_stream, (void *) "\r\n", 2);
      assert (ret == OK);
    }
  query =
    stream_create_sbuf (mgr->shared_stream,
			stream_last_pos (mgr->shared_stream));
  assert (query);
  return query;
}

static sbuf *
create_modified_scan_reply (command_context * ctx, redis_msg * handle,
			    int pg_idx)
{
  command_manager *mgr = ctx->my_mgr;
  ParseContext *parse_ctx;
  char pg_idx_buf[16], *redis_cursor;
  int i, ret, argc;
  sbuf *reply;
  sbuf_pos start;
  ssize_t len;

  parse_ctx = pool_get_parse_ctx (handle);
  ret = getParsedStr (parse_ctx, 0, &redis_cursor);
  assert (ret == OK);
  argc = getArgumentCount (parse_ctx);

  if (strcmp (redis_cursor, "0") == 0)
    {
      if (pg_idx + 1 >= pool_partition_count (mgr->pool))
	{
	  sprintf (pg_idx_buf, "0");
	}
      else
	{
	  sprintf (pg_idx_buf, "%d", pg_idx + 1);
	}
      stream_append_printf (mgr->shared_stream, "*2\r\n$%d\r\n%s\r\n",
			    strlen (pg_idx_buf), pg_idx_buf);
    }
  else
    {
      stream_append_printf (mgr->shared_stream, "*2\r\n$%d\r\n%s%04d\r\n",
			    strlen (redis_cursor) + 4, redis_cursor, pg_idx);
    }
  zfree (redis_cursor);

  stream_append_printf (mgr->shared_stream, "*%d\r\n", argc - 1);
  for (i = 1; i < argc; i++)
    {
      getArgumentPosition (parse_ctx, i, &start, &len);
      ret = stream_append_printf (mgr->shared_stream, "$%ld\r\n", len);
      assert (ret == OK);
      ret = stream_append_copy_sbuf_pos_len (mgr->shared_stream, &start, len);
      assert (ret == OK);
      ret = stream_append_copy_buf (mgr->shared_stream, (void *) "\r\n", 2);
      assert (ret == OK);
    }
  reply =
    stream_create_sbuf (mgr->shared_stream,
			stream_last_pos (mgr->shared_stream));
  return reply;
}

void
cscan_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  redis_msg *handle;
  sbuf *query, *reply;
  unsigned long long cursor;
  char *cursor_buf, *eptr;
  long long ll;
  int ret, skip_argc, ok, pg_idx, cursor_digits;
  const char *pg_id;
  sbuf_pos start;
  ssize_t len;

  COROUTINE_BEGIN (ctx->coro_line);

  getArgumentPosition (ctx->parse_ctx, 1, &start, &len);
  ok = sbuf_string2ll (&start, len, &ll);
  if (!ok)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }
  pg_idx = ll;

  if (pg_idx >= pool_partition_count (mgr->pool) || pg_idx < 0)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  ret = pool_nth_partition_id (mgr->pool, pg_idx, &pg_id);
  if (ret == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  ret = getArgumentPosition (ctx->parse_ctx, 2, &start, &len);
  assert (ret == OK);
  ret = getParsedStr (ctx->parse_ctx, 2, &cursor_buf);
  assert (ret == OK);

  cursor = strtoull (cursor_buf, &eptr, 10);
  if (isspace (cursor_buf[0]) || eptr[0] != '\0' || errno == ERANGE)
    {
      zfree (cursor_buf);
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }
  zfree (cursor_buf);
  cursor_digits = len;

  skip_argc = 2;
  query = create_modified_scan_query (ctx, cursor, cursor_digits, skip_argc);

  ret =
    pool_send_query_partition (mgr->pool, query, pg_id,
			       cmd_redis_coro_invoker, ctx, &handle);
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

void
cscanlen_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;
  int pg_count;

  pg_count = pool_partition_count (mgr->pool);
  reply = stream_create_sbuf_printf (mgr->shared_stream, ":%d\r\n", pg_count);
  reply_and_free (ctx, reply);
}

void
cscandigest_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;
  sds rle;
  size_t out_len;
  unsigned char *encoded;

  rle = get_pg_map_rle (mgr->conf);
  encoded =
    base64_encode ((const unsigned char *) rle, sdslen (rle), &out_len);
  assert (encoded != NULL);
  sdsfree (rle);
  encoded[out_len - 1] = '\0';

  reply =
    stream_create_sbuf_printf (mgr->shared_stream, "$%d\r\n%s\r\n",
			       out_len - 1, encoded);
  free (encoded);

  reply_and_free (ctx, reply);
}

void
scan_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  redis_msg *handle;
  sbuf *query, *reply;
  int ret, skip_argc, pg_idx, cursor_digits;
  unsigned long long cursor;
  const char *pg_id;


  COROUTINE_BEGIN (ctx->coro_line);

  ret = decode_cursor (ctx, &cursor, &cursor_digits, &pg_idx);
  if (ret == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  if (pg_idx >= pool_partition_count (mgr->pool) || pg_idx < 0)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  ret = pool_nth_partition_id (mgr->pool, pg_idx, &pg_id);
  if (ret == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  skip_argc = 1;
  query = create_modified_scan_query (ctx, cursor, cursor_digits, skip_argc);

  ret =
    pool_send_query_partition (mgr->pool, query, pg_id,
			       cmd_redis_coro_invoker, ctx, &handle);
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

  ret = decode_cursor (ctx, &cursor, &cursor_digits, &pg_idx);
  if (ret == ERR)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR invalid cursor\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  reply = create_modified_scan_reply (ctx, handle, pg_idx);
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

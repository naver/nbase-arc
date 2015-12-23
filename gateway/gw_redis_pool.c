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

#include "gw_redis_pool.h"

// Forward declaration
static int partition_select_conn (partition_group * pg,
				  int prefer_recent_conn,
				  redis_connection ** ret_conn);
static void msg_send_handler (aeEventLoop * el, int fd, void *privdata,
			      int mask);
static int enable_msg_send_handler (redis_connection * conn);
static int connect_redis (redis_pool * pool, redis_connection * conn);
static int server_select_conn (redis_server * server,
			       redis_connection ** ret_conn);
static int send_msg_slot_idx (redis_pool * pool, redis_msg * msg);

static dictType poolDictType = {
  dictStrHash,
  dictStrDup,
  NULL,
  dictStrKeyCompare,
  dictStrDestructor,
  NULL
};

static inline int
msg_has_slot (redis_msg * msg)
{
  return msg->slot_idx != MSG_SLOT_IDX_NONE;
}

static inline int
is_block_complete (block_info * block)
{
  return block->block_complete_cb && block->wait_count == 0;
}

static void
process_block_complete (block_info * block)
{
  assert (block->block_complete_cb && block->wait_count == 0);

  block->block_complete_cb (block->block_complete_cbarg);

  block->wait_count = 0;
  block->block_complete_cb = NULL;
  block->block_complete_cbarg = NULL;
}

static void
incr_slot_msg_count (redis_pool * pool, redis_msg * msg)
{
  cluster_slot *slot;

  if (!msg_has_slot (msg))
    {
      return;
    }

  slot = &pool->slots[msg->slot_idx];
  assert (slot->attached_conn == msg->my_conn
	  || (slot->attached_conn == NULL && slot->nmsg == 0));
  assert (!slot->blocked);

  if (!slot->attached_conn)
    {
      slot->attached_conn = msg->my_conn;
    }
  slot->nmsg++;
}

static void
decr_slot_msg_count (redis_pool * pool, redis_msg * msg)
{
  cluster_slot *slot;

  if (!msg_has_slot (msg))
    {
      return;
    }

  slot = &pool->slots[msg->slot_idx];
  assert (slot->attached_conn == msg->my_conn && slot->nmsg > 0);

  slot->nmsg--;
  if (slot->nmsg == 0)
    {
      slot->attached_conn = NULL;
    }
  if (slot->blocked)
    {
      assert (slot->block->wait_count > 0);
      slot->block->wait_count--;
      if (is_block_complete (slot->block))
	{
	  process_block_complete (slot->block);
	}
    }
}

static void
set_slot_attached_conn (redis_pool * pool, int slot_idx,
			redis_connection * new_conn)
{
  cluster_slot *slot;

  slot = &pool->slots[slot_idx];
  assert (slot->attached_conn != NULL && slot->nmsg > 0);
  slot->attached_conn = new_conn;
}

static void
free_redis_msg (redis_msg * msg)
{
  redis_pool *pool = msg->my_pool;

  sbuf_free (msg->query);
  msg->query = NULL;
  sbuf_free (msg->reply);
  msg->reply = NULL;
  if (msg->parse_ctx)
    {
      deleteParseContext (msg->parse_ctx);
      msg->parse_ctx = NULL;
    }
  mempool_free (pool->mp_redis_msg, msg);
}

static void
remove_msg_from_send_q (redis_connection * conn, redis_msg * msg)
{
  assert (msg->my_conn == conn);
  assert (msg->in_send_q);
  assert (conn->nsendq > 0);

  TAILQ_REMOVE (&conn->send_q, msg, msg_tqe);
  conn->nsendq--;
  msg->in_send_q = 0;

  if (conn->nsendq == 0 && conn->fd != -1)
    {
      aeDeleteFileEvent (conn->my_pool->el, conn->fd, AE_WRITABLE);
    }
}

static void
remove_msg_from_recv_q (redis_connection * conn, redis_msg * msg)
{
  assert (msg->my_conn == conn);
  assert (msg->in_recv_q);
  assert (conn->nrecvq > 0);

  TAILQ_REMOVE (&conn->recv_q, msg, msg_tqe);
  conn->nrecvq--;
  msg->in_recv_q = 0;
}

static void
remove_msg_from_block_q (redis_pool * pool, redis_msg * msg)
{
  assert (msg->my_conn == NULL);
  assert (msg->in_block_q);
  assert (pool->nblockq > 0);

  TAILQ_REMOVE (&pool->block_msg_q, msg, msg_tqe);
  pool->nblockq--;
  msg->in_block_q = 0;
}

static void
add_msg_to_send_q (redis_connection * conn, redis_msg * msg)
{
  assert (msg->my_conn == conn);
  assert (!msg->in_send_q);
  assert (msg->query != NULL);

  TAILQ_INSERT_TAIL (&conn->send_q, msg, msg_tqe);
  conn->nsendq++;
  msg->in_send_q = 1;
}

static void
add_msg_to_recv_q (redis_connection * conn, redis_msg * msg)
{
  assert (msg->my_conn == conn);
  assert (!msg->in_recv_q);

  TAILQ_INSERT_TAIL (&conn->recv_q, msg, msg_tqe);
  conn->nrecvq++;
  msg->in_recv_q = 1;
}

static void
add_msg_to_block_q (redis_pool * pool, redis_msg * msg)
{
  assert (msg->my_conn == NULL);
  assert (!msg->in_block_q);

  TAILQ_INSERT_TAIL (&pool->block_msg_q, msg, msg_tqe);
  pool->nblockq++;
  msg->in_block_q = 1;
}

static inline int
msg_gracefully_cancelable (redis_msg * msg)
{
  if (msg->in_send_q && sbuf_check_write_none (msg->query) == ERR)
    {
      return 0;
    }
  if (msg->in_recv_q)
    {
      return 0;
    }

  return 1;
}

static void
dequeue_msg (redis_msg * msg)
{
  if (msg->in_block_q)
    {
      remove_msg_from_block_q (msg->my_pool, msg);
    }
  else if (msg->in_send_q)
    {
      remove_msg_from_send_q (msg->my_conn, msg);
    }
  else if (msg->in_recv_q)
    {
      remove_msg_from_recv_q (msg->my_conn, msg);
    }
}

static sds
get_sds_conn_info (redis_connection * conn)
{
  redis_server *server;
  partition_group *pg;

  if (conn == NULL)
    {
      return sdsnew ("PG:Unallocated, Redis:Unallocated");
    }

  server = conn->my_server;
  pg = conn->my_pg;

  return sdscatprintf (sdsempty (), "PG:%s, Redis:%s, %s:%d", pg->id,
		       server->id, server->addr, server->port);
}

#define MSG_INFO_QUERY_LEN  120
static sds
get_sds_msg_info (redis_msg * msg)
{
  long long elapsed;
  char buf[MSG_INFO_QUERY_LEN + 1];
  size_t len, query_len;
  sds ret, query;

  assert (msg->query);

  query_len = sbuf_len (msg->query);
  len = query_len > MSG_INFO_QUERY_LEN ? MSG_INFO_QUERY_LEN : query_len;
  sbuf_copy_buf (buf, sbuf_start_pos (msg->query), len);
  query = sdscatrepr (sdsempty (), buf, len);

  elapsed = mstime () - msg->msg_enqueue_mstime;

  ret = sdscatprintf (sdsempty (), "elapsed:%lld, query:%s", elapsed, query);
  if (query_len - len > 0)
    {
      ret = sdscatprintf (ret, "... (%ld more bytes)", query_len - len);
    }
  sdsfree (query);
  return ret;
}

static void
process_msg_finish (redis_msg * msg)
{
  redis_pool *pool;

  pool = msg->my_pool;

  msg->finished = 1;
  if (msg->my_conn)
    {
      decr_slot_msg_count (pool, msg);
    }

  if (msg->swallow)
    {
      free_redis_msg (msg);
    }
  else
    {
      // If msg is not canceled, freeing msg is caller's responsibility.
      msg->reply_cb (msg, msg->cbarg);
    }
}

static void
process_msg_success (redis_msg * msg)
{
  msg->success = 1;
  process_msg_finish (msg);
}

/* A connection for executing message has been allocated, but message processing was
 * failed due to timeout or network problem. */
static void
process_msg_fail (redis_msg * msg)
{
  redis_pool *pool = msg->my_pool;
  redis_server *server;
  sds conn_info, msg_info;

  assert (msg->my_conn);

  conn_info = get_sds_conn_info (msg->my_conn);
  msg_info = get_sds_msg_info (msg);
  gwlog (GW_WARNING, "Query error or timeout. %s, %s", conn_info, msg_info);
  sdsfree (conn_info);
  sdsfree (msg_info);

  assert (!msg->reply);
  server = msg->my_conn->my_server;
  msg->reply =
    stream_create_sbuf_printf (pool->shared_stream,
			       "-ERR redis_%s unavailable\r\n", server->id);

  msg->fail = 1;
  process_msg_finish (msg);
}

/* There is no available connection for executing message after redirect admin command
 * because redis server or partition is unavailable. */
static void
process_redirected_msg_fail (redis_msg * msg)
{
  sds conn_info, msg_info;

  assert (!msg->my_conn);

  // Argument "msg->my_conn" is null, and it is valid to call get_sds_conn_info()
  // with null argument. get_sds_conn_info() will return "message unallocated" sds.
  conn_info = get_sds_conn_info (msg->my_conn);
  msg_info = get_sds_msg_info (msg);
  gwlog (GW_WARNING, "Query error or timeout. %s, %s", conn_info, msg_info);
  sdsfree (conn_info);
  sdsfree (msg_info);

  // Error reply was already made before this function has been called.
  assert (msg->reply);

  msg->fail = 1;
  process_msg_finish (msg);
}

static void
process_msg_free (redis_msg * msg)
{
  sds conn_info, msg_info;

  if (msg->finished)
    {
      free_redis_msg (msg);
      return;
    }

  // The function has been called with a unfinished message as a argument,
  // which means the message is canceled by client disconnection.
  conn_info = get_sds_conn_info (msg->my_conn);
  msg_info = get_sds_msg_info (msg);
  gwlog (GW_WARNING, "Query cancel by client closing. %s, %s", conn_info,
	 msg_info);
  sdsfree (conn_info);
  sdsfree (msg_info);

  msg->swallow = 1;
  if (msg_gracefully_cancelable (msg))
    {
      dequeue_msg (msg);
      process_msg_finish (msg);
    }
}

static void
move_conn_to_service (redis_connection * conn)
{
  redis_pool *pool;

  if (conn->in_service)
    {
      return;
    }
  conn->in_service = 1;

  pool = conn->my_pool;
  pool->nconn_in_disconnect--;
  TAILQ_REMOVE (&pool->conn_in_disconnect_q, conn, conn_tqe);
  pool->nconn_in_service++;
  TAILQ_INSERT_TAIL (&pool->conn_in_service_q, conn, conn_tqe);
}

static void
move_conn_to_disconnect (redis_connection * conn)
{
  redis_pool *pool;

  if (!conn->in_service)
    {
      return;
    }
  conn->in_service = 0;

  pool = conn->my_pool;
  pool->nconn_in_service--;
  TAILQ_REMOVE (&pool->conn_in_service_q, conn, conn_tqe);
  pool->nconn_in_disconnect++;
  TAILQ_INSERT_TAIL (&pool->conn_in_disconnect_q, conn, conn_tqe);
}

static void
conn_clear_msg_force (redis_connection * conn)
{
  redis_msg *msg, *tvar;

  TAILQ_FOREACH_SAFE (msg, &conn->recv_q, msg_tqe, tvar)
  {
    remove_msg_from_recv_q (conn, msg);
    process_msg_fail (msg);
  }
  TAILQ_FOREACH_SAFE (msg, &conn->send_q, msg_tqe, tvar)
  {
    remove_msg_from_send_q (conn, msg);
    process_msg_fail (msg);
  }
}

static void
conn_clear_msg_grace (redis_connection * conn)
{
  redis_connection *new_conn;
  redis_msg *msg, *tvar;
  int ret;

  assert (!conn->in_service);

  // Msg which already sends whole query to redis should fail.
  TAILQ_FOREACH_SAFE (msg, &conn->recv_q, msg_tqe, tvar)
  {
    remove_msg_from_recv_q (conn, msg);
    process_msg_fail (msg);
  }

  // Msg which partially sends query to redis should fail.
  TAILQ_FOREACH_SAFE (msg, &conn->send_q, msg_tqe, tvar)
  {
    if (sbuf_check_write_none (msg->query) == OK)
      {
	break;
      }
    remove_msg_from_send_q (conn, msg);
    process_msg_fail (msg);
  }
  if (conn->nsendq == 0)
    {
      return;
    }

  // Remaining msg can be re-routed to another connection.
  ret = partition_select_conn (conn->my_pg, 0, &new_conn);
  if (ret == ERR)
    {
      conn_clear_msg_force (conn);
      return;
    }
  assert (new_conn != conn);

  ret = enable_msg_send_handler (new_conn);
  if (ret == ERR)
    {
      conn_clear_msg_force (conn);
      return;
    }

  TAILQ_FOREACH_SAFE (msg, &conn->send_q, msg_tqe, tvar)
  {
    remove_msg_from_send_q (conn, msg);

    // We can't re-route msgs which don't have slot_idx, because these msgs
    // are specific to the connection and can't be re-routed to another 
    // connection in same partition.  (Example: PING, INFO, DBSIZE)
    if (!msg_has_slot (msg))
      {
	process_msg_fail (msg);
	continue;
      }

    msg->my_conn = new_conn;
    add_msg_to_send_q (new_conn, msg);
    set_slot_attached_conn (conn->my_pool, msg->slot_idx, new_conn);
  }

  // In case all remaining messages doesn't have slot and couldn't be re-routed,
  // we should check nsendq of new_conn and disable writable event if nsendq is 0.
  if (new_conn->nsendq == 0)
    {
      assert (new_conn->fd != -1);
      aeDeleteFileEvent (new_conn->my_pool->el, new_conn->fd, AE_WRITABLE);
    }
}

static inline int
is_conn_equal (redis_connection * a, redis_connection * b)
{
  return a == b;
}

static inline int
is_server_equal (redis_server * a, redis_server * b)
{
  return a == b;
}

static inline int
is_pg_equal (partition_group * a, partition_group * b)
{
  return a == b;
}

static void
delete_server (redis_pool * pool, redis_server * server)
{
  partition_group *pg;
  int ret;

  // We should delete all connections of this server before delete server.
  assert (ARRAY_N (&server->conns) == 0);

  // Delete from redis_pool
  ret = dictDelete (pool->redis_servers, server->id);
  assert (ret == DICT_OK);

  // Delete from partition
  pg = server->my_pg;
  ARRAY_DEL (&pg->servers, server, is_server_equal);

  if (server->delete_complete_cb)
    {
      server->delete_complete_cb (server->delete_complete_cbarg);
    }
  // Delete by itself
  zfree ((void *) server->id);
  zfree ((void *) server->addr);
  zfree ((void *) server->domain_socket_path);
  ARRAY_FINALIZE (&server->conns);
  zfree (server);
}

static void
delete_connection (redis_connection * conn)
{
  redis_pool *pool;
  redis_server *server;
  partition_group *pg;

  // We can only delete connection not in service.
  // We should close connection first before delete connection.
  assert (!conn->in_service);
  assert (conn->nrecvq == 0 && conn->nsendq == 0);

  pool = conn->my_pool;
  pg = conn->my_pg;
  server = conn->my_server;

  // Delete from redis_pool
  TAILQ_REMOVE (&pool->conn_in_disconnect_q, conn, conn_tqe);
  pool->nconn_in_disconnect--;

  // Delete from partition
  if (server->local)
    {
      ARRAY_DEL (&pg->local_conns, conn, is_conn_equal);
    }
  else
    {
      ARRAY_DEL (&pg->remote_conns, conn, is_conn_equal);
    }
  if (pg->recent_conn == conn)
    {
      pg->recent_conn = NULL;
    }
  // Delete from server
  ARRAY_DEL (&server->conns, conn, is_conn_equal);

  // Delete by itself
  stream_delegate_data_and_free (pool->shared_stream, conn->stream);
  deleteParseContext (conn->parse_ctx);
  zfree (conn);
}

static void
close_connection (redis_connection * conn)
{
  redis_pool *pool;

  if (conn->fd == -1)
    {
      return;
    }

  pool = conn->my_pool;
  aeDeleteFileEvent (pool->el, conn->fd, AE_WRITABLE | AE_READABLE);
  close (conn->fd);
  conn->fd = -1;
  move_conn_to_disconnect (conn);

  conn_clear_msg_grace (conn);
  assert (conn->nrecvq == 0 && conn->nsendq == 0);

  stream_discard_appended (conn->stream);
  resetParseContext (conn->parse_ctx, conn->stream);
}

static int
conn_gracefully_closeable (redis_connection * conn)
{
  redis_msg *msg;

  if (!conn->in_service)
    {
      return 1;
    }
  if (conn->nrecvq > 0)
    {
      return 0;
    }
  if (conn->nsendq == 0)
    {
      return 1;
    }
  msg = TAILQ_FIRST (&conn->send_q);
  if (sbuf_check_write_none (msg->query) == OK)
    {
      return 1;
    }
  return 0;
}

static void
close_conn_and_reconfigure_pool (redis_connection * conn)
{
  redis_pool *pool;
  redis_server *server;

  pool = conn->my_pool;
  server = conn->my_server;

  close_connection (conn);
  if (conn->delete_asap)
    {
      delete_connection (conn);
    }
  if (server->delete_asap && ARRAY_N (&server->conns) == 0)
    {
      delete_server (pool, server);
    }
}

static inline int
is_wait_init_time_expired (redis_pool * pool)
{
  return (pool->initialize_wait_until != POOL_WAIT_INIT_FOREVER
	  && pool->initialize_wait_until < mstime ());
}

static inline int
all_conn_in_service (redis_pool * pool)
{
  return (pool->nconn_in_disconnect == 0);
}

static void
stop_wait_init (redis_pool * pool)
{
  pool->stop_after_init = 0;
  pool->initialize_wait_until = 0;
  aeStop (pool->el);
}

static void
init_slot (cluster_slot * slot, partition_group * pg)
{
  slot->my_pg = pg;
  slot->nmsg = 0LL;
  slot->attached_conn = NULL;
  slot->block = NULL;
  slot->blocked = 0;
  slot->has_pg = 1;
}

static void
track_latency_histogram (latency_histo_t * histo)
{
  int i;

  histo->sample_idx = (histo->sample_idx + 1) % LATENCY_HISTOGRAM_SAMPLES;
  for (i = 0; i < LATENCY_HISTOGRAM_COLUMNS; i++)
    {
      histo->latency_histogram_samples[histo->sample_idx][i] = 0;
    }
}

static void
track_latency_stat (latency_stat_t * stat)
{
  stat->sample_idx = (stat->sample_idx + 1) % LATENCY_STAT_SAMPLES;
  stat->max_latency_samples[stat->sample_idx] = 0;
}

static int
redis_pool_cron (struct aeEventLoop *el, long long id, void *privdata)
{
  redis_pool *pool = privdata;
  redis_connection *conn, *tvar;
  int *hz = &pool->hz;
  int *cronloops = &pool->cronloops;
  GW_NOTUSED (el);
  GW_NOTUSED (id);

  run_with_period (1000)
  {
    TAILQ_FOREACH_SAFE (conn, &pool->conn_in_disconnect_q, conn_tqe, tvar)
    {
      close_connection (conn);
      connect_redis (pool, conn);
    }
  }

  run_with_period (100)
  {
    dictIterator *di;
    dictEntry *de;

    // Latency Histogram
    track_latency_histogram (&pool->latency_histo);

    // Latency Statistics
    di = dictGetIterator (pool->redis_servers);
    while ((de = dictNext (di)) != NULL)
      {
	redis_server *server = dictGetVal (de);
	track_latency_stat (&server->latency_stat);
      }
    dictReleaseIterator (di);
  }

  run_with_period (100)
  {
    long long curtime = mstime ();
    TAILQ_FOREACH_SAFE (conn, &pool->conn_in_service_q, conn_tqe, tvar)
    {
      // connection list is sorted in assending order by last active time.
      // So, we can break loop as soon as we find first active connection
      // in list.
      if (curtime - conn->last_active_mstime < pool->conn_inactive_timeout)
	{
	  break;
	}

      // If there are waiting msgs in the queue and the connection is inactive,
      // we should close the connection.
      if (conn->nsendq + conn->nrecvq > 0)
	{
	  sds conn_info = get_sds_conn_info (conn);

	  gwlog (GW_WARNING, "Redis is not responding. elapsed:%lld, %s",
		 curtime - conn->last_active_mstime, conn_info);
	  sdsfree (conn_info);
	  close_conn_and_reconfigure_pool (conn);
	}
    }
  }

  if (pool->stop_after_init && is_wait_init_time_expired (pool))
    {
      stop_wait_init (pool);
    }

  (*cronloops)++;
  return 1000 / *hz;
}

redis_pool *
pool_create (aeEventLoop * el, int nslot, long long msg_timeout,
	     long long conn_inactive_timeout, sbuf_hdr * shared_stream,
	     sbuf_mempool * mp_sbuf)
{
  redis_pool *pool;

  pool = zmalloc (sizeof (redis_pool));

  // Eventloop
  pool->el = el;
  pool->cron_teid = aeCreateTimeEvent (el, 1000, redis_pool_cron, pool, NULL);
  pool->hz = GW_DEFAULT_HZ;
  pool->cronloops = 0;
  // Slot mapping
  pool->nslot = nslot;
  pool->slots = zcalloc (sizeof (cluster_slot) * nslot);
  // Partition group
  pool->partition_groups = dictCreate (&poolDictType, NULL);
  ARRAY_INIT (&pool->idx_to_pg);
  // Redis server
  pool->redis_servers = dictCreate (&poolDictType, NULL);
  // Redis connection
  pool->nconn_in_service = 0;
  TAILQ_INIT (&pool->conn_in_service_q);
  pool->nconn_in_disconnect = 0;
  TAILQ_INIT (&pool->conn_in_disconnect_q);
  // Timeout setting
  pool->msg_timeout = msg_timeout;
  pool->conn_inactive_timeout = conn_inactive_timeout;
  pool->initialize_wait_until = 0;
  // slot reconfigure
  TAILQ_INIT (&pool->block_info_q);
  // Block msg
  pool->nblockq = 0;
  TAILQ_INIT (&pool->block_msg_q);
  // Global stream buffer
  pool->shared_stream = shared_stream;
  // Stat
  memset (&pool->latency_histo, 0, sizeof (pool->latency_histo));
  // Flag
  pool->stop_after_init = 0;
  // Memory pool
  pool->mp_redis_msg =
    mempool_create (sizeof (struct redis_msg), MEMPOOL_DEFAULT_POOL_SIZE);
  pool->mp_parse_ctx = createParserMempool ();
  pool->mp_sbuf = mp_sbuf;

  return pool;
}

int
pool_add_partition (redis_pool * pool, const char *pg_id)
{
  partition_group *pg;
  int ret;

  pg = zmalloc (sizeof (partition_group));
  ret = dictAdd (pool->partition_groups, (void *) pg_id, pg);
  if (ret == DICT_ERR)
    {
      // PG ID already exists.
      zfree (pg);
      return ERR;
    }
  ARRAY_PUSH (&pool->idx_to_pg, pg);

  pg->id = zstrdup (pg_id);
  pg->idx = ARRAY_N (&pool->idx_to_pg) - 1;
  ARRAY_INIT (&pg->servers);
  ARRAY_INIT (&pg->remote_conns);
  ARRAY_INIT (&pg->local_conns);
  pg->recent_conn = NULL;

  return OK;
}

int
pool_add_server (redis_pool * pool, const char *pg_id, const char *redis_id,
		 const char *addr, int port, const char *domain_socket_path,
		 int is_local)
{
  redis_server *server;
  partition_group *pg;
  int ret;

  pg = dictFetchValue (pool->partition_groups, pg_id);
  if (!pg)
    {
      return ERR;
    }

  server = zmalloc (sizeof (redis_server));
  ret = dictAdd (pool->redis_servers, (void *) redis_id, server);
  if (ret == DICT_ERR)
    {
      // REDIS ID already exists.
      zfree (server);
      return ERR;
    }
  ARRAY_PUSH (&pg->servers, server);

  server->my_pool = pool;
  server->my_pg = pg;
  server->id = zstrdup (redis_id);
  server->addr = zstrdup (addr);
  server->port = port;
  server->domain_socket_path =
    domain_socket_path ? zstrdup (domain_socket_path) : NULL;
  ARRAY_INIT (&server->conns);
  server->delete_complete_cb = NULL;
  server->delete_complete_cbarg = NULL;
  memset (&server->latency_stat, 0, sizeof (server->latency_stat));
  server->local = is_local;
  server->delete_asap = 0;

  return OK;
}

int
pool_add_connection (redis_pool * pool, const char *redis_id,
		     int prefer_domain_socket)
{
  redis_server *server;
  redis_connection *conn;
  partition_group *pg;

  server = dictFetchValue (pool->redis_servers, redis_id);
  if (!server)
    {
      return ERR;
    }
  pg = server->my_pg;

  conn = zmalloc (sizeof (redis_connection));
  // Add to pool
  TAILQ_INSERT_TAIL (&pool->conn_in_disconnect_q, conn, conn_tqe);
  pool->nconn_in_disconnect++;
  // Add to partition
  if (server->local)
    {
      ARRAY_PUSH (&pg->local_conns, conn);
    }
  else
    {
      ARRAY_PUSH (&pg->remote_conns, conn);
    }
  // Add to server
  ARRAY_PUSH (&server->conns, conn);

  conn->my_pool = pool;
  conn->my_pg = pg;
  conn->my_server = server;
  conn->fd = -1;
  conn->stream = stream_hdr_create (SBUF_DEFAULT_PAGESIZE, pool->mp_sbuf);
  conn->parse_ctx = createParseContext (pool->mp_parse_ctx, conn->stream);
  conn->nsendq = 0;
  TAILQ_INIT (&conn->send_q);
  conn->nrecvq = 0;
  TAILQ_INIT (&conn->recv_q);
  conn->last_active_mstime = 0;
  conn->domain_socket = (server->domain_socket_path
			 && prefer_domain_socket) ? 1 : 0;
  conn->in_service = 0;
  conn->delete_asap = 0;

  connect_redis (pool, conn);

  return OK;
}

int
pool_set_slot (redis_pool * pool, int from, int to, const char *pg_id)
{
  partition_group *pg;
  int i;

  pg = dictFetchValue (pool->partition_groups, pg_id);
  if (!pg)
    {
      return ERR;
    }
  if (from < 0 || to >= pool->nslot || from > to)
    {
      return ERR;
    }
  for (i = from; i <= to; i++)
    {
      init_slot (&pool->slots[i], pg);
    }

  return OK;
}

int
pool_wait_init (redis_pool * pool, long long wait_mstime)
{
  if (wait_mstime == POOL_WAIT_INIT_FOREVER)
    {
      pool->initialize_wait_until = POOL_WAIT_INIT_FOREVER;
    }
  else
    {
      pool->initialize_wait_until = mstime () + wait_mstime;
    }
  pool->stop_after_init = 1;

  aeMain (pool->el);
  if (all_conn_in_service (pool))
    {
      return OK;
    }
  else
    {
      return ERR;
    }
}

void
pool_destroy (redis_pool * pool)
{
  redis_connection *conn, *tvar;
  redis_server *server;
  partition_group *pg;
  redis_msg *msg;
  block_info *block, *block_tvar;
  dictIterator *di;
  dictEntry *de;

  // Delete cron
  aeDeleteTimeEvent (pool->el, pool->cron_teid);

  // Delete connection
  TAILQ_FOREACH_SAFE (conn, &pool->conn_in_service_q, conn_tqe, tvar)
  {
    close_connection (conn);
  }
  TAILQ_FOREACH_SAFE (conn, &pool->conn_in_disconnect_q, conn_tqe, tvar)
  {
    close_connection (conn);
    delete_connection (conn);
  }

  // Delete servers
  di = dictGetIterator (pool->redis_servers);
  while ((de = dictNext (di)) != NULL)
    {
      server = dictGetVal (de);
      assert (ARRAY_N (&server->conns) == 0);

      pg = server->my_pg;
      ARRAY_DEL (&pg->servers, server, is_server_equal);

      zfree ((void *) server->id);
      zfree ((void *) server->addr);
      zfree ((void *) server->domain_socket_path);
      ARRAY_FINALIZE (&server->conns);
      zfree (server);
    }
  dictReleaseIterator (di);
  dictRelease (pool->redis_servers);

  // Delete partition
  ARRAY_FINALIZE (&pool->idx_to_pg);
  di = dictGetIterator (pool->partition_groups);
  while ((de = dictNext (di)) != NULL)
    {
      pg = dictGetVal (de);
      assert (ARRAY_N (&pg->servers) == 0);
      assert (ARRAY_N (&pg->remote_conns) == 0
	      && ARRAY_N (&pg->local_conns) == 0);

      zfree ((void *) pg->id);
      ARRAY_FINALIZE (&pg->servers);
      ARRAY_FINALIZE (&pg->remote_conns);
      ARRAY_FINALIZE (&pg->local_conns);
      zfree (pg);
    }
  dictReleaseIterator (di);
  dictRelease (pool->partition_groups);

  // Delete blocked msg
  TAILQ_FOREACH (msg, &pool->block_msg_q, msg_tqe)
  {
    free_redis_msg (msg);
  }

  // Delete block info
  TAILQ_FOREACH_SAFE (block, &pool->block_info_q, block_info_tqe, block_tvar)
  {
    zfree (block);
  }

  // Delete slot mapping
  zfree (pool->slots);

  mempool_destroy (pool->mp_redis_msg);
  destroyParserMempool (pool->mp_parse_ctx);
  zfree (pool);
}

int
pool_delete_partition (redis_pool * pool, const char *pg_id)
{
  partition_group *pg;
  cluster_slot *slot;
  int i, ret;

  pg = dictFetchValue (pool->partition_groups, pg_id);
  if (!pg)
    {
      return ERR;
    }
  if (ARRAY_N (&pg->servers) > 0)
    {
      return ERR;
    }
  assert (ARRAY_N (&pg->remote_conns) == 0
	  && ARRAY_N (&pg->local_conns) == 0);

  // Delete from redis_pool
  ret = dictDelete (pool->partition_groups, pg_id);
  assert (ret == DICT_OK);
  ARRAY_DEL (&pool->idx_to_pg, pg, is_pg_equal);

  for (i = 0; i < pool->nslot; i++)
    {
      slot = &pool->slots[i];

      if (slot->my_pg == pg)
	{
	  assert (slot->nmsg == 0LL);
	  assert (slot->attached_conn == NULL);

	  slot->my_pg = NULL;
	  slot->has_pg = 0;
	}
    }

  zfree ((void *) pg->id);
  ARRAY_FINALIZE (&pg->servers);
  ARRAY_FINALIZE (&pg->remote_conns);
  ARRAY_FINALIZE (&pg->local_conns);
  zfree (pg);
  return 0;
}

int
pool_delete_server (redis_pool * pool, const char *redis_id,
		    delete_complete_proc * cb, void *cbarg)
{
  redis_server *server;
  array_conn tconns;
  int i;

  server = dictFetchValue (pool->redis_servers, redis_id);
  if (!server)
    {
      // Server not found
      return ERR;
    }
  if (server->delete_asap)
    {
      // Already deleted
      return ERR;
    }

  server->delete_complete_cb = cb;
  server->delete_complete_cbarg = cbarg;
  server->delete_asap = 1;

  // We need to iterate array safely, because ARRAY_DEL will be called inside
  // close_conn_and_reconfigure_pool()
  ARRAY_INIT (&tconns);
  for (i = 0; i < ARRAY_N (&server->conns); i++)
    {
      ARRAY_PUSH (&tconns, ARRAY_GET (&server->conns, i));
    }
  for (i = 0; i < ARRAY_N (&tconns); i++)
    {
      redis_connection *conn;

      conn = ARRAY_GET (&tconns, i);
      conn->delete_asap = 1;
      if (conn_gracefully_closeable (conn))
	{
	  close_conn_and_reconfigure_pool (conn);
	}
    }
  ARRAY_FINALIZE (&tconns);

  return OK;
}

int
pool_block_slot (redis_pool * pool, int from, int to,
		 block_complete_proc * cb, void *cbarg)
{
  cluster_slot *slot;
  block_info *block;
  int i;

  if (from < 0 || to >= pool->nslot || from > to)
    {
      return ERR;
    }

  for (i = from; i <= to; i++)
    {
      slot = &pool->slots[i];
      if (slot->blocked)
	{
	  return ERR;
	}
    }

  block = zmalloc (sizeof (block_info));
  block->my_pool = pool;
  block->from = from;
  block->to = to;
  block->wait_count = 0;
  block->block_complete_cb = cb;
  block->block_complete_cbarg = cbarg;

  for (i = from; i <= to; i++)
    {
      slot = &pool->slots[i];

      assert (!slot->blocked);
      slot->blocked = 1;
      slot->block = block;

      block->wait_count += slot->nmsg;
    }

  TAILQ_INSERT_TAIL (&pool->block_info_q, block, block_info_tqe);

  if (is_block_complete (block))
    {
      process_block_complete (block);
    }
  return OK;
}

int
pool_redirect_slot (redis_pool * pool, int from, int to,
		    const char *dst_pg_id)
{
  partition_group *pg;
  cluster_slot *slot;
  block_info *block;
  redis_msg *msg, *tvar;
  int i, ret;

  // Slot should be blocked before redirecting
  block = NULL;
  TAILQ_FOREACH (block, &pool->block_info_q, block_info_tqe)
  {
    if (block->from == from && block->to == to && block->wait_count == 0)
      {
	break;
      }
  }
  if (block == NULL)
    {
      return ERR;
    }

  pg = dictFetchValue (pool->partition_groups, dst_pg_id);
  if (!pg)
    {
      return ERR;
    }

  for (i = from; i <= to; i++)
    {
      slot = &pool->slots[i];
      assert (slot->blocked && slot->nmsg == 0);
      assert (slot->block == block);

      slot->my_pg = pg;
      slot->block = NULL;
      slot->blocked = 0;
      slot->has_pg = 1;
    }

  TAILQ_REMOVE (&pool->block_info_q, block, block_info_tqe);
  zfree (block);

  TAILQ_FOREACH_SAFE (msg, &pool->block_msg_q, msg_tqe, tvar)
  {
    if (msg->slot_idx < from || msg->slot_idx > to)
      {
	continue;
      }

    remove_msg_from_block_q (pool, msg);
    ret = send_msg_slot_idx (pool, msg);
    assert (!msg->in_block_q);

    if (ret == ERR)
      {
	process_redirected_msg_fail (msg);
      }
  }

  return OK;
}

static redis_msg *
msg_create (redis_pool * pool, sbuf * query, msg_reply_proc * cb, void *cbarg)
{
  redis_msg *msg;

  msg = mempool_alloc (pool->mp_redis_msg);
  msg->my_pool = pool;
  msg->my_pg = NULL;
  msg->my_conn = NULL;
  msg->slot_idx = MSG_SLOT_IDX_NONE;
  msg->query = query;
  msg->reply = NULL;
  msg->parse_ctx = NULL;
  msg->reply_cb = cb;
  msg->cbarg = cbarg;
  // TODO Periodically caching mstime() may be possible and will improve
  // performance by reducing gettimeofday() call.
  msg->msg_enqueue_mstime = mstime ();
  msg->local = 0;
  msg->in_send_q = 0;
  msg->in_recv_q = 0;
  msg->in_block_q = 0;
  msg->finished = 0;
  msg->success = 0;
  msg->fail = 0;
  msg->swallow = 0;

  return msg;
}

static inline int
is_conn_valid (redis_connection * conn)
{
  return (conn->in_service && !conn->delete_asap);
}

static redis_connection *
array_select_conn (array_conn * conns)
{
  redis_connection *conn, *min_conn;
  int i, min_q;

  min_q = INT_MAX;
  min_conn = NULL;
  for (i = 0; i < ARRAY_N (conns); i++)
    {
      conn = ARRAY_GET (conns, i);
      if (is_conn_valid (conn))
	{
	  if (!min_conn)
	    {
	      min_q = conn->nsendq + conn->nrecvq;
	      min_conn = conn;
	    }
	  else if (min_q > conn->nsendq + conn->nrecvq)
	    {
	      min_q = conn->nsendq + conn->nrecvq;
	      min_conn = conn;
	    }
	}
    }

  return min_conn;
}

static int
partition_select_conn (partition_group * pg, int prefer_recent_conn,
		       redis_connection ** ret_conn)
{
  redis_connection *conn;

  // TODO Need benchmark test for prefer_recent_conn option. Currently, this
  // option is not used, but enabling this option may be useful because it
  // increases pipeline effect.
  if (prefer_recent_conn && pg->recent_conn
      && is_conn_valid (pg->recent_conn))
    {
      *ret_conn = pg->recent_conn;
      return OK;
    }

  // Check local connection first
  conn = array_select_conn (&pg->local_conns);
  if (!conn)
    {
      // Check remote connection
      conn = array_select_conn (&pg->remote_conns);
    }

  if (conn)
    {
      pg->recent_conn = conn;
      *ret_conn = conn;
      return OK;
    }

  return ERR;
}

static int
server_select_conn (redis_server * server, redis_connection ** ret_conn)
{
  redis_connection *conn;

  conn = array_select_conn (&server->conns);
  if (conn)
    {
      *ret_conn = conn;
      return OK;
    }

  return ERR;
}

static int
slot_select_conn (redis_pool * pool, int slot_idx,
		  redis_connection ** ret_conn)
{
  redis_connection *conn;
  cluster_slot *slot;
  int ret;

  assert (slot_idx >= 0 && slot_idx < pool->nslot);

  slot = &pool->slots[slot_idx];

  if (!slot->has_pg)
    {
      return SLOT_EMPTY;
    }
  if (slot->blocked)
    {
      return SLOT_BLOCKED;
    }

  if (slot->attached_conn)
    {
      *ret_conn = slot->attached_conn;
      return SLOT_NORMAL;
    }

  ret = partition_select_conn (slot->my_pg, 0, &conn);
  if (ret == OK)
    {
      *ret_conn = conn;
      return SLOT_NORMAL;
    }
  else
    {
      return SLOT_EMPTY;
    }
}

static void
update_conn_last_active (redis_connection * conn)
{
  redis_pool *pool = conn->my_pool;

  conn->last_active_mstime = mstime ();
  // keep connection list in order by last active time
  if (conn->in_service)
    {
      TAILQ_REMOVE (&pool->conn_in_service_q, conn, conn_tqe);
      TAILQ_INSERT_TAIL (&pool->conn_in_service_q, conn, conn_tqe);
    }
}

static int
enable_msg_send_handler (redis_connection * conn)
{
  redis_pool *pool;

  pool = conn->my_pool;
  if (!(aeGetFileEvents (pool->el, conn->fd) & AE_WRITABLE))
    {
      if (aeCreateFileEvent
	  (pool->el, conn->fd, AE_WRITABLE, msg_send_handler, conn) == AE_ERR)
	{
	  return ERR;
	}
      update_conn_last_active (conn);
    }
  return OK;
}

static int
send_msg_conn (redis_pool * pool, redis_connection * conn, redis_msg * msg)
{
  if (enable_msg_send_handler (conn) == ERR)
    {
      redis_server *server = conn->my_server;

      assert (!msg->reply);
      msg->reply =
	stream_create_sbuf_printf (pool->shared_stream,
				   "-ERR redis_%s unavailable\r\n",
				   server->id);
      msg->finished = 1;
      msg->fail = 1;
      return ERR;
    }

  msg->my_pg = conn->my_pg;
  msg->my_conn = conn;
  msg->local = conn->my_server->local;
  incr_slot_msg_count (pool, msg);
  add_msg_to_send_q (conn, msg);
  return OK;
}

static int
send_msg_slot_idx (redis_pool * pool, redis_msg * msg)
{
  redis_connection *conn;
  int ret;

  assert (msg->slot_idx != MSG_SLOT_IDX_NONE);

  ret = slot_select_conn (pool, msg->slot_idx, &conn);
  // Slot blocked
  if (ret == SLOT_BLOCKED)
    {
      add_msg_to_block_q (pool, msg);
      return OK;
    }
  // Slot unavailable
  if (ret == SLOT_EMPTY)
    {
      assert (!msg->reply);
      msg->reply =
	stream_create_sbuf_printf (pool->shared_stream,
				   "-ERR slot_%d unavailable\r\n",
				   msg->slot_idx);
      msg->finished = 1;
      msg->fail = 1;
      return ERR;
    }
  // Slot avail
  return send_msg_conn (pool, conn, msg);
}

static int
pool_send_query_conn (redis_pool * pool, redis_connection * conn,
		      sbuf * query, msg_reply_proc * cb, void *cbarg,
		      redis_msg ** ret_handle)
{
  redis_msg *msg;

  msg = msg_create (pool, query, cb, cbarg);
  *ret_handle = msg;

  return send_msg_conn (pool, conn, msg);
}

int
pool_send_query (redis_pool * pool, sbuf * query, int slot_idx,
		 msg_reply_proc * cb, void *cbarg, redis_msg ** ret_handle)
{
  redis_msg *msg;

  msg = msg_create (pool, query, cb, cbarg);
  *ret_handle = msg;

  msg->slot_idx = slot_idx;
  return send_msg_slot_idx (pool, msg);
}

int
pool_send_query_all (redis_pool * pool, const char *query_str,
		     msg_reply_proc * cb, void *cbarg,
		     array_msg * ret_handles)
{
  redis_connection *conn;
  redis_server *server;
  redis_msg *msg;
  sbuf *query;
  dictIterator *di;
  dictEntry *de;
  int ret, msg_count;

  msg_count = 0;

  di = dictGetIterator (pool->redis_servers);
  while ((de = dictNext (di)) != NULL)
    {
      server = dictGetVal (de);

      query = stream_create_sbuf_str (pool->shared_stream, query_str);
      msg = msg_create (pool, query, cb, cbarg);
      ARRAY_PUSH (ret_handles, msg);

      ret = server_select_conn (server, &conn);
      if (ret == ERR)
	{
	  assert (!msg->reply);
	  msg->reply =
	    stream_create_sbuf_printf (pool->shared_stream,
				       "-ERR redis_%s unavailable\r\n",
				       server->id);
	  msg->finished = 1;
	  msg->fail = 1;
	  continue;
	}

      ret = send_msg_conn (pool, conn, msg);
      if (ret == ERR)
	{
	  // Error reply is already made in send_msg_conn()
	  continue;
	}

      msg_count++;
    }
  dictReleaseIterator (di);

  if (msg_count == 0)
    {
      return ERR;
    }
  return OK;
}

void
pool_free_msg (redis_msg * handle)
{
  process_msg_free (handle);
}

void
pool_free_msgs (array_msg * handles)
{
  redis_msg *msg;
  int i;

  for (i = 0; i < ARRAY_N (handles); i++)
    {
      msg = ARRAY_GET (handles, i);
      process_msg_free (msg);
    }
}

sbuf *
pool_take_reply (redis_msg * handle)
{
  sbuf *reply;
  reply = handle->reply;
  handle->reply = NULL;
  return reply;
}

sbuf *
pool_get_reply (redis_msg * handle)
{
  return handle->reply;
}

ParseContext *
pool_get_parse_ctx (redis_msg * handle)
{
  return handle->parse_ctx;
}

int
pool_msg_finished (redis_msg * handle)
{
  return handle->finished;
}

int
pool_msg_success (redis_msg * handle)
{
  return handle->success;
}

int
pool_msg_fail (redis_msg * handle)
{
  return handle->fail;
}

int
pool_msg_local (redis_msg * handle)
{
  return handle->local;
}

int
pool_slot_idx (redis_pool * pool, int keyhash)
{
  return keyhash % pool->nslot;
}

static void
msg_send_handler (aeEventLoop * el, int fd, void *privdata, int mask)
{
  redis_connection *conn = privdata;
  redis_msg *msg, *tvar;
  sbuf *bufv[SBUF_IOV_MAX];
  int nbuf;
  ssize_t nwritten;
  GW_NOTUSED (mask);

  nbuf = 0;
  if (conn->delete_asap)
    {
      // Case 1. We can delete connection now.
      if (conn_gracefully_closeable (conn))
	{
	  goto close_conn;
	}
      // Case 2. We can stop writable event of msg_send_handler now. But, we
      // should keep readable event until all msgs in recv_q are received.
      msg = TAILQ_FIRST (&conn->send_q);
      if (sbuf_check_write_none (msg->query) == OK)
	{
	  aeDeleteFileEvent (el, fd, AE_WRITABLE);
	  return;
	}
      // Case 3. We should finish sending first msg in send_q, because the msg
      // is partially sent to redis.
      bufv[nbuf++] = msg->query;
    }
  else
    {
      TAILQ_FOREACH (msg, &conn->send_q, msg_tqe)
      {
	if (nbuf == SBUF_IOV_MAX)
	  {
	    break;
	  }
	bufv[nbuf++] = msg->query;
      }
    }
  assert (nbuf > 0);

  nwritten = sbuf_writev (bufv, nbuf, fd);
  if (nwritten <= 0)
    {
      sds conn_info;

      if (nwritten == -1 && errno == EAGAIN)
	{
	  return;
	}
      conn_info = get_sds_conn_info (conn);
      gwlog (GW_WARNING, "Writing to redis: %s, %s", strerror (errno),
	     conn_info);
      sdsfree (conn_info);
      goto close_conn;
    }

  update_conn_last_active (conn);

  TAILQ_FOREACH_SAFE (msg, &conn->send_q, msg_tqe, tvar)
  {
    if (sbuf_check_write_finish (msg->query) == ERR)
      {
	break;
      }
    remove_msg_from_send_q (conn, msg);
    add_msg_to_recv_q (conn, msg);
  }

  return;

close_conn:
  close_conn_and_reconfigure_pool (conn);

}

static void
update_latency_histogram (latency_histo_t * histo, unsigned int v)
{
  static const unsigned int b[] = { 0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000 };
  static const unsigned int S[] = { 1, 2, 4, 8, 16 };
  register unsigned int r = 0;
  int idx = histo->sample_idx;
  int i;

  if (v <= 1)
    {
      r = 0;
    }
  else
    {
      v--;
      for (i = 4; i >= 0; i--)
	{
	  if (v & b[i])
	    {
	      v >>= S[i];
	      r |= S[i];
	    }
	}
      r++;
    }

  r = r < LATENCY_HISTOGRAM_COLUMNS ? r : LATENCY_HISTOGRAM_COLUMNS - 1;
  histo->latency_histogram_samples[idx][r]++;
}

static void
update_latency_stats (latency_stat_t * stat, unsigned int v)
{
  int idx = stat->sample_idx;

  if (stat->max_latency_samples[idx] < v)
    {
      stat->max_latency_samples[idx] = v;
    }
}

static void
msg_read_handler (aeEventLoop * el, int fd, void *privdata, int mask)
{
  redis_connection *conn = privdata;
  redis_pool *pool = conn->my_pool;
  sbuf *reply;
  redis_msg *msg;
  ssize_t nread;
  sds err;
  int ret;
  GW_NOTUSED (el);
  GW_NOTUSED (mask);

  nread = stream_append_read (conn->stream, fd);
  if (nread <= 0)
    {
      sds conn_info;

      if (nread == -1 && errno == EAGAIN)
	{
	  return;
	}

      conn_info = get_sds_conn_info (conn);
      if (nread == 0)
	{
	  gwlog (GW_WARNING, "Redis connection closed. %s", conn_info);
	}
      else
	{
	  gwlog (GW_WARNING, "Reading from redis: %s, %s", strerror (errno),
		 conn_info);
	}
      sdsfree (conn_info);
      goto close_conn;
    }

  update_conn_last_active (conn);

  do
    {
      ret = replyParser (conn->parse_ctx, conn->stream, &reply, &err);
      if (ret == PARSE_COMPLETE)
	{
	  unsigned int latency;

	  assert (!TAILQ_EMPTY (&conn->recv_q));
	  msg = TAILQ_FIRST (&conn->recv_q);
	  remove_msg_from_recv_q (conn, msg);

	  latency = mstime () - msg->msg_enqueue_mstime;
	  update_latency_histogram (&pool->latency_histo, latency);
	  update_latency_stats (&conn->my_server->latency_stat, latency);

	  msg->reply = reply;
	  msg->parse_ctx = conn->parse_ctx;
	  conn->parse_ctx =
	    createParseContext (pool->mp_parse_ctx, conn->stream);

	  process_msg_success (msg);

	  if (conn->delete_asap && conn_gracefully_closeable (conn))
	    {
	      goto close_conn;
	    }
	}
      else if (ret == PARSE_ERROR)
	{
	  gwlog (GW_WARNING, "Incompatiable reply from redis_%s, %s",
		 conn->my_server->id, err);
	  sdsfree (err);
	  goto close_conn;
	}
    }
  while (ret != PARSE_INSUFFICIENT_DATA);

  return;

close_conn:
  close_conn_and_reconfigure_pool (conn);
}

static void
connect_handler (redis_msg * msg, void *arg)
{
  redis_connection *conn = arg;
  redis_pool *pool;
  sds conn_info;

  pool = conn->my_pool;
  conn_info = get_sds_conn_info (conn);
  if (pool_msg_success (msg))
    {
      gwlog (GW_NOTICE, "Connection established to redis. %s", conn_info);
      move_conn_to_service (conn);
      if (pool->stop_after_init && all_conn_in_service (pool))
	{
	  stop_wait_init (pool);
	}
    }
  else if (pool_msg_fail (msg))
    {
      gwlog (GW_NOTICE, "Unable to connect to redis. %s", conn_info);
    }
  sdsfree (conn_info);
  pool_free_msg (msg);
}

static int
connect_redis (redis_pool * pool, redis_connection * conn)
{
  redis_server *server;
  redis_msg *msg;
  sbuf *query;
  char ebuf[ANET_ERR_LEN];
  int fd, ret;

  server = conn->my_server;

  if (conn->fd != -1 || conn->in_service)
    {
      return ERR;
    }
  if (conn->domain_socket)
    {
      fd =
	anetUnixNonBlockConnect (ebuf, (char *) server->domain_socket_path);
      if (fd == ANET_ERR)
	{
	  sds conn_info = get_sds_conn_info (conn);
	  gwlog (GW_WARNING,
		 "Unable to connect to redis via domain socket. %s",
		 conn_info);
	  sdsfree (conn_info);
	  fd =
	    anetTcpNonBlockConnect (ebuf, (char *) server->addr,
				    server->port);
	}
    }
  else
    {
      fd = anetTcpNonBlockConnect (ebuf, (char *) server->addr, server->port);
    }
  if (fd == ANET_ERR)
    {
      sds conn_info = get_sds_conn_info (conn);
      gwlog (GW_WARNING, "Unable to connect to redis. %s", conn_info);
      sdsfree (conn_info);
      return ERR;
    }

  anetEnableTcpNoDelay (NULL, fd);
  anetKeepAlive (NULL, fd, GW_DEFAULT_TCP_KEEPALIVE);

  if (aeCreateFileEvent (pool->el, fd, AE_READABLE, msg_read_handler, conn) ==
      AE_ERR)
    {
      close (fd);
      return ERR;
    }

  conn->fd = fd;

  query = stream_create_sbuf_str (pool->shared_stream, "PING\r\n");
  ret = pool_send_query_conn (pool, conn, query, connect_handler, conn, &msg);
  if (ret == ERR)
    {
      pool_free_msg (msg);
      close (fd);
      conn->fd = -1;
      return ERR;
    }

  return fd;
}

void
pool_get_latency_histogram (redis_pool * pool, long long *histo)
{
  int i, j;

  for (i = 0; i < LATENCY_HISTOGRAM_COLUMNS; i++)
    {
      histo[i] = 0;
      for (j = 0; j < LATENCY_HISTOGRAM_SAMPLES; j++)
	{
	  histo[i] += pool->latency_histo.latency_histogram_samples[j][i];
	}
    }
}

static int
is_redis_avail (redis_server * server)
{
  redis_connection *conn;
  int i;

  for (i = 0; i < ARRAY_N (&server->conns); i++)
    {
      conn = ARRAY_GET (&server->conns, i);
      if (conn->in_service)
	{
	  return 1;
	}
    }
  return 0;
}

int
pool_available_redis_count (redis_pool * pool)
{
  dictIterator *di;
  dictEntry *de;
  int count;

  count = 0;

  di = dictGetIterator (pool->redis_servers);
  while ((de = dictNext (di)) != NULL)
    {
      redis_server *server = dictGetVal (de);
      if (is_redis_avail (server))
	{
	  count++;
	}
    }
  dictReleaseIterator (di);

  return count;
}

static int
is_pg_avail (partition_group * pg)
{
  redis_connection *conn;
  int i;

  for (i = 0; i < ARRAY_N (&pg->local_conns); i++)
    {
      conn = ARRAY_GET (&pg->local_conns, i);
      if (conn->in_service)
	{
	  return 1;
	}
    }
  for (i = 0; i < ARRAY_N (&pg->remote_conns); i++)
    {
      conn = ARRAY_GET (&pg->remote_conns, i);
      if (conn->in_service)
	{
	  return 1;
	}
    }
  return 0;

}

int
pool_available_partition_count (redis_pool * pool)
{
  dictIterator *di;
  dictEntry *de;
  int count;

  count = 0;

  di = dictGetIterator (pool->partition_groups);
  while ((de = dictNext (di)) != NULL)
    {
      partition_group *pg = dictGetVal (de);
      if (is_pg_avail (pg))
	{
	  count++;
	}
    }
  dictReleaseIterator (di);

  return count;
}

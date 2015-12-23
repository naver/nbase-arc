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

#ifndef _GW_REDIS_POOL_H_
#define _GW_REDIS_POOL_H_

#include <sys/types.h>
#include <limits.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>

#include "gw_config.h"
#include "gw_stream_buf.h"
#include "gw_redis_parser.h"
#include "gw_mem_pool.h"
#include "gw_array.h"
#include "gw_util.h"
#include "gw_log.h"
#include "queue.h"
#include "zmalloc.h"
#include "dict.h"
#include "anet.h"
#include "ae.h"

/* --------------- PUBLIC --------------- */
// Typedef
typedef struct latency_histo_s latency_histo_t;
typedef struct latency_stat_s latency_stat_t;
typedef struct redis_pool redis_pool;
typedef struct partition_group partition_group;
typedef struct redis_server redis_server;
typedef struct redis_connection redis_connection;
typedef struct cluster_slot cluster_slot;
typedef struct block_info block_info;
typedef struct redis_msg redis_msg;

typedef struct array_pg array_pg;
typedef struct array_server array_server;
typedef struct array_conn array_conn;
typedef struct array_msg array_msg;

typedef struct msg_tqh msg_tqh;
typedef struct conn_tqh conn_tqh;
typedef struct block_info_tqh block_info_tqh;

// Callback
typedef void msg_reply_proc (redis_msg * handle, void *cbarg);
typedef void delete_complete_proc (void *cbarg);
typedef void block_complete_proc (void *cbarg);

// Initialize function
redis_pool *pool_create (aeEventLoop * el, int nslot, long long msg_timeout,
			 long long conn_inactive_timeout,
			 sbuf_hdr * shared_stream, sbuf_mempool * mp_sbuf);
int pool_add_partition (redis_pool * pool, const char *pg_id);
int pool_add_server (redis_pool * pool, const char *pg_id,
		     const char *redis_id, const char *addr, int port,
		     const char *domain_socket_path, int is_local);
int pool_add_connection (redis_pool * pool, const char *redis_id,
			 int prefer_domain_socket);
int pool_set_slot (redis_pool * pool, int from, int to, const char *pg_id);
#define POOL_WAIT_INIT_FOREVER 0
int pool_wait_init (redis_pool * pool, long long wait_mstime);
void pool_destroy (redis_pool * pool);

// Reconfigure function
int pool_delete_partition (redis_pool * pool, const char *pg_id);
int pool_delete_server (redis_pool * pool, const char *redis_id,
			delete_complete_proc * cb, void *cbarg);
int pool_block_slot (redis_pool * pool, int from, int to,
		     block_complete_proc * cb, void *cbarg);
int pool_redirect_slot (redis_pool * pool, int from, int to,
			const char *dst_pg_id);

// Msg function
int pool_send_query (redis_pool * pool, sbuf * query, int slot_idx,
		     msg_reply_proc * cb, void *cbarg,
		     redis_msg ** ret_handle);
int pool_send_query_all (redis_pool * pool, const char *query,
			 msg_reply_proc * cb, void *cbarg,
			 array_msg * ret_handles);
void pool_free_msg (redis_msg * handle);
void pool_free_msgs (array_msg * handles);
sbuf *pool_take_reply (redis_msg * handle);
sbuf *pool_get_reply (redis_msg * handle);
ParseContext *pool_get_parse_ctx (redis_msg * handle);
int pool_msg_finished (redis_msg * handle);
int pool_msg_success (redis_msg * handle);
int pool_msg_fail (redis_msg * handle);
int pool_msg_local (redis_msg * handle);
int pool_slot_idx (redis_pool * pool, int keyhash);

// Stat
void pool_get_latency_histogram (redis_pool * pool, long long *histo);
int pool_available_redis_count (redis_pool * pool);
int pool_available_partition_count (redis_pool * pool);

/* --------------- PRIVATE --------------- */
#define SLOT_NORMAL     0
#define SLOT_EMPTY      (-1)
#define SLOT_BLOCKED    (-2)

#define MSG_SLOT_IDX_NONE   (-1)

#define LATENCY_HISTOGRAM_COLUMNS 12
#define LATENCY_HISTOGRAM_SAMPLES 16
#define LATENCY_STAT_SAMPLES 16

#define ARRAY_INIT_SIZE_PG          100
#define ARRAY_INIT_SIZE_SERVER      16
#define ARRAY_INIT_SIZE_CONN        16
#define ARRAY_INIT_SIZE_MSG         100

ARRAY_HEAD (array_pg, partition_group *, ARRAY_INIT_SIZE_PG);
ARRAY_HEAD (array_server, redis_server *, ARRAY_INIT_SIZE_SERVER);
ARRAY_HEAD (array_conn, redis_connection *, ARRAY_INIT_SIZE_CONN);
ARRAY_HEAD (array_msg, redis_msg *, ARRAY_INIT_SIZE_MSG);

TAILQ_HEAD (msg_tqh, redis_msg);
TAILQ_HEAD (conn_tqh, redis_connection);
TAILQ_HEAD (block_info_tqh, block_info);

struct latency_histo_s
{
  int sample_idx;
  long long
    latency_histogram_samples[LATENCY_HISTOGRAM_SAMPLES]
    [LATENCY_HISTOGRAM_COLUMNS];
};

struct latency_stat_s
{
  int sample_idx;
  /* TODO Implement more latency stats
     long long last_latency_sum;
     long long last_latency_count;
     long long avg_latency_samples[LATENCY_STAT_SAMPLES];
   */
  unsigned int max_latency_samples[LATENCY_STAT_SAMPLES];
};

struct redis_pool
{
  // Eventloop
  aeEventLoop *el;
  long long cron_teid;
  int hz;
  int cronloops;

  // Slot mapping
  int nslot;
  cluster_slot *slots;

  // Partition group
  dict *partition_groups;
  array_pg idx_to_pg;

  // Redis server
  dict *redis_servers;

  // Redis connection
  int nconn_in_service;
  conn_tqh conn_in_service_q;	// conn is sorted in order by last active time
  int nconn_in_disconnect;
  conn_tqh conn_in_disconnect_q;

  // Timeout setting
  long long msg_timeout;
  long long conn_inactive_timeout;
  long long initialize_wait_until;

  // Slot reconfigure
  block_info_tqh block_info_q;

  // Block msg
  int nblockq;
  msg_tqh block_msg_q;

  // per-thread shared stream buffer
  sbuf_hdr *shared_stream;

  // Stat
  latency_histo_t latency_histo;

  // Memory pool
  mempool_hdr_t *mp_redis_msg;
  mempool_hdr_t *mp_parse_ctx;
  sbuf_mempool *mp_sbuf;

  // Flag
  unsigned stop_after_init:1;
};

struct partition_group
{
  const char *id;
  int idx;

  array_server servers;
  array_conn remote_conns;
  array_conn local_conns;
  redis_connection *recent_conn;
};

struct redis_server
{
  redis_pool *my_pool;
  partition_group *my_pg;
  const char *id;

  const char *addr;
  int port;
  const char *domain_socket_path;

  array_conn conns;

  delete_complete_proc *delete_complete_cb;
  void *delete_complete_cbarg;

  // Stat
  latency_stat_t latency_stat;

  unsigned local:1;
  unsigned delete_asap:1;
};

struct redis_connection
{
  TAILQ_ENTRY (redis_connection) conn_tqe;

  redis_pool *my_pool;
  partition_group *my_pg;
  redis_server *my_server;

  int fd;
  sbuf_hdr *stream;
  ParseContext *parse_ctx;

  int nsendq;
  msg_tqh send_q;
  int nrecvq;
  msg_tqh recv_q;

  long long last_active_mstime;

  unsigned domain_socket:1;
  unsigned in_service:1;
  unsigned delete_asap:1;
};

struct cluster_slot
{
  partition_group *my_pg;

  long long nmsg;
  // To keep per-slot consistency of pipelining queries, we maintain attached
  // connection for every slot. At first, there is no attached connection and
  // the value is NULL. When first query which belongs to the slot arrives,
  // attached connection for that slot is set. After that, every query belongs
  // to the slot go to attached connection. If every query is processed and no
  // more query is left in the slot, attached connection is set back to NULL.
  redis_connection *attached_conn;

  block_info *block;

  unsigned blocked:1;
  unsigned has_pg:1;
};

struct block_info
{
  TAILQ_ENTRY (block_info) block_info_tqe;

  redis_pool *my_pool;
  int from;
  int to;
  long long wait_count;
  block_complete_proc *block_complete_cb;
  void *block_complete_cbarg;
};

struct redis_msg
{
  TAILQ_ENTRY (redis_msg) msg_tqe;

  redis_pool *my_pool;
  partition_group *my_pg;
  redis_connection *my_conn;
  int slot_idx;

  sbuf *query;
  sbuf *reply;
  ParseContext *parse_ctx;

  msg_reply_proc *reply_cb;
  void *cbarg;

  long long msg_enqueue_mstime;

  unsigned local:1;
  unsigned in_send_q:1;
  unsigned in_recv_q:1;
  unsigned in_block_q:1;
  unsigned finished:1;
  unsigned success:1;
  unsigned fail:1;
  unsigned swallow:1;
};
#endif

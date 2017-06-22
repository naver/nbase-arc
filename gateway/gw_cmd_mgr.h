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

#ifndef _GW_CMD_MGR_H_
#define _GW_CMD_MGR_H_

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <assert.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>

#include "gw_config.h"
#include "gw_redis_pool.h"
#include "gw_async_chan.h"
#include "gw_stream_buf.h"
#include "gw_redis_parser.h"
#include "gw_mem_pool.h"
#include "gw_util.h"
#include "gw_log.h"
#include "queue.h"
#include "coroutine.h"
#include "zmalloc.h"
#include "dict.h"
#include "util.h"
#include "base64.h"
#include "ae.h"
#include "sds.h"

/* --------------- PUBLIC --------------- */
// Typedef
typedef struct command_stat command_stat;
typedef struct command_async command_async;
typedef struct command_context command_context;
typedef struct command_manager command_manager;
typedef struct redis_command redis_command;

typedef struct array_seq array_seq;
typedef struct array_query array_query;
typedef struct array_cmd_async array_cmd_async;

// Callback
typedef void cmd_reply_proc (sbuf * reply, void *cbarg);
typedef void cmd_process_proc (command_context * ctx);

// Initialize function
command_manager *cmd_mgr_user_create (aeEventLoop * el, redis_pool * pool,
				      cluster_conf * conf,
				      async_chan * my_async,
				      array_async_chans * worker_asyncs,
				      sbuf_hdr * shared_stream);
command_manager *cmd_mgr_admin_create (aeEventLoop * el, cluster_conf * conf,
				       async_chan * my_async,
				       array_async_chans * worker_asyncs,
				       sbuf_hdr * shared_stream);
void cmd_mgr_destroy (command_manager * mgr);

// Command function
command_context *cmd_create_ctx (command_manager * mgr, sbuf * query,
				 ParseContext * parse_ctx,
				 cmd_reply_proc * cb, void *cbarg);
void cmd_send_command (command_context * ctx);
void cmd_cancel (command_context * ctx);
int cmd_is_close_after_reply (command_context * ctx);

// Command stats
long long cmd_stat_total_commands (command_manager * mgr);
long long cmd_stat_total_commands_local (command_manager * mgr);
long long cmd_stat_ops (command_manager * mgr);
long long cmd_stat_local_ops (command_manager * mgr);

/* --------------- PRIVATE --------------- */
#define COMMAND_MANAGER_TYPE_USER   1
#define COMMAND_MANAGER_TYPE_ADMIN  2
#define REDIS_CMD_MAX_LEN 32

#define COMMAND_STAT_SAMPLES 16

/* Command flags. Please check the command table defined in the redis.c file
 * for more information about the meaning of every flag. */
#define REDIS_CMD_WRITE 1	/* "w" flag */
#define REDIS_CMD_READONLY 2	/* "r" flag */
#define REDIS_CMD_DENYOOM 4	/* "m" flag */
#define REDIS_CMD_FORCE_REPLICATION 8	/* "f" flag */
#define REDIS_CMD_ADMIN 16	/* "a" flag */
#define REDIS_CMD_PUBSUB 32	/* "p" flag */
#define REDIS_CMD_NOSCRIPT  64	/* "s" flag */
#define REDIS_CMD_RANDOM 128	/* "R" flag */
#define REDIS_CMD_SORT_FOR_SCRIPT 256	/* "S" flag */
#define REDIS_CMD_LOADING 512	/* "l" flag */
#define REDIS_CMD_STALE 1024	/* "t" flag */
#define REDIS_CMD_SKIP_MONITOR 2048	/* "M" flag */

#define GW_CMD_NOT_SUPPORT 4096	/* "N" flag */
#define GW_CMD_ADMIN 8192	/* "A" flag */

#define ARRAY_MSG_SEQ_INIT_SIZE 100
#define ARRAY_QUERY_INIT_SIZE 100
#define ARRAY_CMD_ASYNC_INIT_SIZE 100

ARRAY_HEAD (array_seq, int, ARRAY_MSG_SEQ_INIT_SIZE);
ARRAY_HEAD (array_query, sbuf *, ARRAY_QUERY_INIT_SIZE);
ARRAY_HEAD (array_cmd_async, command_async *, ARRAY_CMD_ASYNC_INIT_SIZE);

TAILQ_HEAD (admin_cmd_tqh, redis_msg);

void cmd_redis_coro_invoker (redis_msg * handle, void *cbarg);
void cmd_async_coro_invoker (void *arg, void *privdata);
void cmd_free_async (command_async * cmd_async);
void reply_and_free (command_context * ctx, sbuf * reply);
int is_cmd_async_finished (command_async * cmd);

// User Command function
void single_key_command (command_context * ctx);
void mget_command (command_context * ctx);
void mset_command (command_context * ctx);
void del_command (command_context * ctx);
void dbsize_command (command_context * ctx);
void info_command (command_context * ctx);
void ping_command (command_context * ctx);
void quit_command (command_context * ctx);
void cscan_command (command_context * ctx);
void cscanlen_command (command_context * ctx);
void cscandigest_command (command_context * ctx);
void scan_command (command_context * ctx);

// Admin Command function
void admin_cluster_info_command (command_context * ctx);
void admin_delay_command (command_context * ctx);
void admin_redirect_command (command_context * ctx);
void admin_pg_add_command (command_context * ctx);
void admin_pg_del_command (command_context * ctx);
void admin_pgs_add_command (command_context * ctx);
void admin_pgs_del_command (command_context * ctx);
void admin_help_command (command_context * ctx);

typedef void arg_free_proc (void *arg);

struct command_stat
{
  long long numcommands;
  long long numcommands_local;

  int sample_idx;
  long long last_sample_time;

  long long ops_sec_last_sample_count;
  long long ops_sec_samples[COMMAND_STAT_SAMPLES];

  long long local_ops_sec_last_sample_count;
  long long local_ops_sec_samples[COMMAND_STAT_SAMPLES];
};

struct command_async
{
  async_chan *caller;
  async_chan *callee;
  async_exec_proc *return_exec;

  void *rqst_arg;
  arg_free_proc *rqst_free;
  void *resp_arg;
  arg_free_proc *resp_free;

  command_context *ctx;

  unsigned swallow:1;
  unsigned finished:1;
};

struct command_context
{
  int coro_line;
  command_manager *my_mgr;

  redis_command *cmd;
  ParseContext *parse_ctx;
  sbuf *query;

  cmd_reply_proc *reply_cb;
  void *cbarg;

  array_msg msg_handles;
  array_seq msg_seqs;
  array_cmd_async async_handles;

  int idx;

  unsigned close_after_reply:1;
  unsigned local:1;
  unsigned unable_to_cancel:1;
  unsigned swallow:1;
};

struct command_manager
{
  aeEventLoop *el;
  long long cron_teid;
  int hz;
  int cronloops;

  redis_pool *pool;
  dict *commands;

  cluster_conf *conf;
  async_chan *my_async;
  array_async_chans *worker_asyncs;

  // Global stream buffer
  sbuf_hdr *shared_stream;

  // Memory pool
  mempool_hdr_t *mp_cmdctx;

  // Index helper for splitting keys while executing multi key commands.
  index_helper *idx_helper;

  command_stat stat;
};

struct redis_command
{
  const char *name;
  cmd_process_proc *proc;
  int arity;
  const char *sflags;		/* Flags as string represenation, one char per flag. */
  int flags;			/* The actual flags, obtained from the 'sflags' field. */
  int firstkey;			/* The first argument that's a key (0 = no keys) */
  int lastkey;			/* THe last argument that's a key */
  int keystep;			/* The step between first and last key */
  long long microseconds, calls;
};
#endif

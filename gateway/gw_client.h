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

#ifndef _GW_CLIENT_H_
#define _GW_CLIENT_H_

#include <sys/time.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>

#include "gw_config.h"
#include "gw_cmd_mgr.h"
#include "gw_stream_buf.h"
#include "gw_redis_parser.h"
#include "gw_mem_pool.h"
#include "gw_util.h"
#include "queue.h"
#include "anet.h"
#include "ae.h"

/* --------------- PUBLIC --------------- */
// Typedef
typedef struct client_request client_request;
typedef struct client client;
typedef struct client_pool client_pool;

typedef struct request_tqh request_tqh;
typedef struct client_tqh client_tqh;

// Initialize function
client_pool *cli_pool_create (aeEventLoop * el, command_manager * cmd_mgr,
			      long long rqst_timeout,
			      long long conn_inactive_timeout,
			      sbuf_hdr * shared_stream,
			      sbuf_mempool * mp_sbuf);
void cli_pool_destroy (client_pool * pool);

// client function
int cli_add_client (client_pool * pool, int fd);

/* --------------- PRIVATE --------------- */
TAILQ_HEAD (request_tqh, client_request);
TAILQ_HEAD (client_tqh, client);

struct client_pool
{
  aeEventLoop *el;
  long long cron_teid;
  int hz;
  int cronloops;

  command_manager *cmd_mgr;

  int nclient;
  struct client_tqh clients;

  // Global stream buffer
  sbuf_hdr *shared_stream;

  // Memory pool
  mempool_hdr_t *mp_client_request;
  mempool_hdr_t *mp_parse_ctx;
  sbuf_mempool *mp_sbuf;

  long long rqst_timeout;
  long long conn_inactive_timeout;
};

struct client
{
  TAILQ_ENTRY (client) client_tqe;

  client_pool *my_pool;

  int fd;
  sbuf_hdr *stream;
  ParseContext *parse_ctx;

  int nrqstq;
  struct request_tqh rqst_q;

  // TODO This variable is not used currently. Timeout for inactive client
  // may be needed but isn't implemented yet.
  long long last_active_mstime;

  unsigned close_after_reply:1;
};

struct client_request
{
  TAILQ_ENTRY (client_request) request_tqe;

  client *my_client;
  command_context *cmd_ctx;

  sbuf *reply;

  // TODO This variable is not used currently. Client side query timeout may
  // be needed but isn't implemented yet.
  long long rqst_enqueue_mstime;

  unsigned reply_received:1;
};
#endif

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

#ifndef _GW_WORKER_H_
#define _GW_WORKER_H_

#include <sys/types.h>
#include <pthread.h>
#include <assert.h>

#include "gw_config.h"
#include "gw_redis_pool.h"
#include "gw_client.h"
#include "gw_cmd_mgr.h"
#include "gw_async_chan.h"
#include "zmalloc.h"
#include "dict.h"
#include "ae.h"

#define ARRAY_INIT_SIZE_WORKER 64

typedef struct worker worker;
typedef struct array_worker array_worker;

ARRAY_HEAD (array_worker, worker *, ARRAY_INIT_SIZE_WORKER);

struct worker
{
  int id;
  pthread_t thr;

  aeEventLoop *el;
  long long cron_teid;
  int hz;

  redis_pool *pool;
  client_pool *cli;
  command_manager *cmd_mgr;

  async_chan *my_async;
  async_chan *master_async;
  array_async_chans *worker_asyncs;

  cluster_conf *conf;
  sbuf_mempool *mp_sbuf;
  sbuf_hdr *shared_stream;
};

worker *create_worker (int worker_id, cluster_conf * conf,
		       async_chan * master_async,
		       array_async_chans * worker_asyncs);
void start_workers (array_worker * workers);
void finalize_workers (array_worker * workers);
#endif

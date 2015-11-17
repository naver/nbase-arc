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

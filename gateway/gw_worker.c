#include "gw_worker.h"

static int
worker_cron (struct aeEventLoop *el, long long id, void *privdata)
{
  worker *wrk = privdata;
  GW_NOTUSED (id);

  if (global.shutdown_asap)
    {
      aeStop (el);
    }

  return 1000 / wrk->hz;
}

static void
create_redis_pool (worker * wrk)
{
  redis_pool *pool;
  aeEventLoop *el = wrk->el;
  cluster_conf *conf = wrk->conf;
  dictIterator *di;
  dictEntry *de;
  int i;

  pool =
    pool_create (el, conf->nslot, global.timeout_ms, global.timeout_ms,
		 wrk->shared_stream, wrk->mp_sbuf);

  // Add Partition
  di = dictGetIterator (conf->partition);
  while ((de = dictNext (di)) != NULL)
    {
      partition_conf *pg = dictGetVal (de);
      pool_add_partition (pool, pg->pg_id);
    }
  dictReleaseIterator (di);

  // Add redis server
  di = dictGetIterator (conf->redis);
  while ((de = dictNext (di)) != NULL)
    {
      redis_conf *redis = dictGetVal (de);
      pool_add_server (pool, redis->pg_id, redis->redis_id, redis->addr,
		       redis->port, redis->domain_socket_path,
		       redis->is_local);
    }
  dictReleaseIterator (di);

  // Set slot mapping
  for (i = 0; i < conf->nslot; i++)
    {
      partition_conf *pg = conf->slots[i];
      if (pg)
	{
	  pool_set_slot (pool, i, i, pg->pg_id);
	}
    }

  // Connect redis server
  di = dictGetIterator (conf->redis);
  while ((de = dictNext (di)) != NULL)
    {
      redis_conf *redis = dictGetVal (de);
      pool_add_connection (pool, redis->redis_id, conf->prefer_domain_socket);
    }
  dictReleaseIterator (di);

  wrk->pool = pool;
}

static void
create_cmd_mgr (worker * wrk)
{
  command_manager *cmd_mgr;
  aeEventLoop *el = wrk->el;
  cluster_conf *conf = wrk->conf;

  assert (wrk->pool);

  cmd_mgr =
    cmd_mgr_user_create (el, wrk->pool, conf, wrk->my_async,
			 wrk->worker_asyncs, wrk->shared_stream);

  wrk->cmd_mgr = cmd_mgr;
}

static void
create_client_pool (worker * wrk)
{
  client_pool *cli;
  aeEventLoop *el = wrk->el;

  assert (wrk->pool);
  assert (wrk->cmd_mgr);

  cli =
    cli_pool_create (el, wrk->cmd_mgr, global.timeout_ms, global.timeout_ms,
		     wrk->shared_stream, wrk->mp_sbuf);

  wrk->cli = cli;
}

worker *
create_worker (int worker_id, cluster_conf * conf, async_chan * master_async,
	       array_async_chans * worker_asyncs)
{
  worker *wrk;

  wrk = zmalloc (sizeof (worker));
  wrk->id = worker_id;
  wrk->el = aeCreateEventLoop (GW_DEFAULT_EVENTLOOP_SIZE);
  wrk->cron_teid = aeCreateTimeEvent (wrk->el, 0, worker_cron, wrk, NULL);
  wrk->hz = GW_DEFAULT_HZ;
  wrk->pool = NULL;
  wrk->cli = NULL;
  wrk->cmd_mgr = NULL;
  wrk->my_async = async_create (wrk->el, wrk);
  wrk->master_async = master_async;
  wrk->worker_asyncs = worker_asyncs;
  ARRAY_PUSH (wrk->worker_asyncs, wrk->my_async);
  wrk->conf = duplicate_conf (conf);
  wrk->mp_sbuf = sbuf_mempool_create (SBUF_DEFAULT_PAGESIZE);
  wrk->shared_stream =
    stream_hdr_create (SBUF_DEFAULT_PAGESIZE, wrk->mp_sbuf);

  create_redis_pool (wrk);
  create_cmd_mgr (wrk);
  create_client_pool (wrk);

  return wrk;
}

static void *
init_worker_thread (void *arg)
{
  worker *wrk = arg;

  pool_wait_init (wrk->pool, global.timeout_ms);

  return NULL;
}

static void *
start_worker_thread (void *arg)
{
  worker *wrk = arg;

  aeMain (wrk->el);

  return NULL;
}

void
start_workers (array_worker * workers)
{
  int i, ret;

  // Make sure that all redis connections are established for workers. This step
  // blocks until every connection is established or timeout passes.
  for (i = 0; i < ARRAY_N (workers); i++)
    {
      worker *wrk = ARRAY_GET (workers, i);

      ret = pthread_create (&wrk->thr, NULL, init_worker_thread, wrk);
      assert (ret == 0);
    }

  for (i = 0; i < ARRAY_N (workers); i++)
    {
      worker *wrk = ARRAY_GET (workers, i);

      ret = pthread_join (wrk->thr, NULL);
      assert (ret == 0);
    }

  // Run eventloop of workers
  for (i = 0; i < ARRAY_N (workers); i++)
    {
      worker *wrk = ARRAY_GET (workers, i);

      ret = pthread_create (&wrk->thr, NULL, start_worker_thread, wrk);
      assert (ret == 0);
    }
}

static void
free_worker (worker * wrk)
{
  cli_pool_destroy (wrk->cli);
  cmd_mgr_destroy (wrk->cmd_mgr);
  pool_destroy (wrk->pool);
  async_destroy (wrk->my_async);
  aeDeleteTimeEvent (wrk->el, wrk->cron_teid);
  aeDeleteEventLoop (wrk->el);
  destroy_conf (wrk->conf);
  stream_hdr_free (wrk->shared_stream);
  sbuf_mempool_destroy (wrk->mp_sbuf);

  zfree (wrk);
}

void
finalize_workers (array_worker * workers)
{
  int i, ret;

  for (i = 0; i < ARRAY_N (workers); i++)
    {
      worker *wrk = ARRAY_GET (workers, i);

      ret = pthread_join (wrk->thr, NULL);
      assert (ret == 0);

      free_worker (wrk);
    }
}

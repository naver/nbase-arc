#include "gw_cmd_mgr.h"
#include "gw_worker.h"

/* ---------------------- INFO command ---------------------- */
typedef struct redis_stat_tqh redis_stat_tqh;
TAILQ_HEAD (redis_stat_tqh, redis_stat_t);

struct redis_stat_t
{
  TAILQ_ENTRY (redis_stat_t) redis_stat_tqe;

  char *redis_id;
  unsigned int max_latency;
};

struct info_ret
{
  // Redis
  redis_stat_tqh redis_stat_q;

  // Gateway
  long long gateway_total_commands;
  long long gateway_ops;
  long long gateway_total_commands_local;
  long long gateway_ops_local;
  int gateway_connected_clients;
  int gateway_disconnected_redis;

  // Latency
  long long latency_histogram[LATENCY_HISTOGRAM_COLUMNS];
};

static void
free_info_ret (void *free_arg)
{
  struct info_ret *arg = free_arg;
  struct redis_stat_t *stat, *tvar;

  TAILQ_FOREACH_SAFE (stat, &arg->redis_stat_q, redis_stat_tqe, tvar)
  {
    zfree (stat->redis_id);
    zfree (stat);
  }
  zfree (arg);
}

static void
get_redis_stat (redis_pool * pool, redis_stat_tqh * q)
{
  dictIterator *di;
  dictEntry *de;
  int i;

  di = dictGetIterator (pool->redis_servers);
  while ((de = dictNext (di)) != NULL)
    {
      redis_server *server = dictGetVal (de);
      struct redis_stat_t *stat;

      stat = zmalloc (sizeof (struct redis_stat_t));
      stat->redis_id = zstrdup (server->id);
      stat->max_latency = 0;
      for (i = 0; i < LATENCY_STAT_SAMPLES; i++)
	{
	  if (stat->max_latency < server->latency_stat.max_latency_samples[i])
	    {
	      stat->max_latency = server->latency_stat.max_latency_samples[i];
	    }
	}
      TAILQ_INSERT_TAIL (q, stat, redis_stat_tqe);
    }
  dictReleaseIterator (di);
}

static void
exec_info_cmd (void *arg, void *chan_data)
{
  command_async *cmd = arg;
  worker *wrk = chan_data;
  struct info_ret *resp_arg;

  resp_arg = cmd->resp_arg;

  // Redis info
  get_redis_stat (wrk->pool, &resp_arg->redis_stat_q);

  // Gateway info
  resp_arg->gateway_total_commands = cmd_stat_total_commands (wrk->cmd_mgr);
  resp_arg->gateway_ops = cmd_stat_ops (wrk->cmd_mgr);
  resp_arg->gateway_total_commands_local =
    cmd_stat_total_commands_local (wrk->cmd_mgr);
  resp_arg->gateway_ops_local = cmd_stat_local_ops (wrk->cmd_mgr);
  resp_arg->gateway_connected_clients = wrk->cli->nclient;
  resp_arg->gateway_disconnected_redis = wrk->pool->nconn_in_disconnect;

  // Latency info
  pool_get_latency_histogram (wrk->pool, resp_arg->latency_histogram);

  async_send_event (cmd->callee->el, cmd->caller, cmd->return_exec, cmd);
}

static command_async *
send_info_cmd (command_context * ctx, async_chan * callee)
{
  command_manager *mgr = ctx->my_mgr;
  command_async *cmd;
  struct info_ret *resp_arg;

  resp_arg = zmalloc (sizeof (struct info_ret));
  TAILQ_INIT (&resp_arg->redis_stat_q);

  cmd = zmalloc (sizeof (command_async));
  cmd->caller = mgr->my_async;
  cmd->callee = callee;
  cmd->return_exec = cmd_async_coro_invoker;
  cmd->rqst_arg = NULL;
  cmd->rqst_free = NULL;
  cmd->resp_arg = resp_arg;
  cmd->resp_free = free_info_ret;
  cmd->ctx = ctx;
  cmd->swallow = 0;
  cmd->finished = 0;

  async_send_event (cmd->caller->el, callee, exec_info_cmd, cmd);

  return cmd;
}

static sds
sdscatinfo_cluster (sds info, command_context * ctx)
{
  cluster_conf *conf = ctx->my_mgr->conf;
  redis_pool *pool = ctx->my_mgr->pool;
  sds rle;

  rle = get_pg_map_rle (conf);
  info = sdscatprintf (info, "# Cluster\r\n"
		       "cluster_name:%s\r\n"
		       "total_redis_instances:%ld\r\n"
		       "redis_instances_available:%d\r\n"
		       "total_partition_groups:%ld\r\n"
		       "partition_groups_available:%d\r\n"
		       "cluster_slot_size:%d\r\n"
		       "pg_map_rle:%s\r\n",
		       conf->cluster_name,
		       dictSize (conf->redis),
		       pool_available_redis_count (pool),
		       dictSize (conf->partition),
		       pool_available_partition_count (pool),
		       conf->nslot, rle);
  sdsfree (rle);
  return info;
}

static sds
sdscatinfo_redis (sds info, command_context * ctx)
{
  cluster_conf *conf = ctx->my_mgr->conf;
  struct redis_stat_t *stat;
  struct info_ret *resp;
  int i, ret;
  sds redis_id_list;
  dictType statDictType = { dictStrHash, dictStrDup, NULL, dictStrKeyCompare,
    dictStrDestructor, NULL
  };
  dictIterator *di;
  dictEntry *de;
  dict *aggr;


  // Aggregate max latency
  aggr = dictCreate (&statDictType, NULL);
  for (i = 0; i < ARRAY_N (&ctx->async_handles); i++)
    {
      command_async *async = ARRAY_GET (&ctx->async_handles, i);

      resp = async->resp_arg;
      TAILQ_FOREACH (stat, &resp->redis_stat_q, redis_stat_tqe)
      {
	unsigned int *max_latency;

	max_latency = dictFetchValue (aggr, stat->redis_id);
	if (!max_latency)
	  {
	    max_latency = zmalloc (sizeof (unsigned int));
	    *max_latency = stat->max_latency;
	    ret = dictAdd (aggr, stat->redis_id, max_latency);
	  }
	else
	  {
	    if (*max_latency < stat->max_latency)
	      {
		*max_latency = stat->max_latency;
	      }
	  }
      }
    }

  // Print stats
  redis_id_list = get_redis_id_list (conf);
  info = sdscatprintf (info, "# REDIS\r\n"
		       "redis_id_list:%s\r\n", redis_id_list);
  sdsfree (redis_id_list);

  di = dictGetIterator (aggr);
  while ((de = dictNext (di)) != NULL)
    {
      char *redis_id = dictGetKey (de);
      unsigned int *max_latency = dictGetVal (de);
      redis_conf *redis;

      redis = dictFetchValue (conf->redis, redis_id);
      if (!redis)
	{
	  continue;
	}

      info =
	sdscatprintf (info,
		      "redis_%s:addr=%s port=%d pg_id=%s max_latency=%d\r\n",
		      redis_id, redis->addr, redis->port, redis->pg_id,
		      *max_latency);
    }
  dictReleaseIterator (di);

  // Release aggregate date
  di = dictGetIterator (aggr);
  while ((de = dictNext (di)) != NULL)
    {
      unsigned int *max_latency = dictGetVal (de);

      zfree (max_latency);
    }
  dictReleaseIterator (di);
  dictRelease (aggr);

  return info;
}

static sds
sdscatinfo_gateway (sds info, command_context * ctx)
{
  cluster_conf *conf = ctx->my_mgr->conf;
  struct info_ret *resp;
  long long total_cmd = 0, total_cmd_local = 0;
  long long ops = 0, ops_local = 0;
  int conn_clients = 0, disconn_redis = 0;
  sds delay_filter;
  int i;

  // Aggregate gateway stats
  for (i = 0; i < ARRAY_N (&ctx->async_handles); i++)
    {
      command_async *async = ARRAY_GET (&ctx->async_handles, i);

      resp = async->resp_arg;
      total_cmd += resp->gateway_total_commands;
      total_cmd_local += resp->gateway_total_commands_local;
      ops += resp->gateway_ops;
      ops_local += resp->gateway_ops_local;
      conn_clients += resp->gateway_connected_clients;
      disconn_redis += resp->gateway_disconnected_redis;
    }

  delay_filter = get_delay_filter (conf);
  info = sdscatprintf (info, "# Gateway\r\n"
		       "gateway_process_id:%d\r\n"
		       "gateway_tcp_port:%d\r\n"
		       "gateway_total_commands_processed:%lld\r\n"
		       "gateway_instantaneous_ops_per_sec:%lld\r\n"
		       "gateway_total_commands_lcon:%lld\r\n"
		       "gateway_instantaneous_lcon_ops_per_sec:%lld\r\n"
		       "gateway_connected_clients:%d\r\n"
		       "gateway_prefer_domain_socket:%d\r\n"
		       "gateway_disconnected_redis:%d\r\n"
		       "gateway_delay_filter_rle:%s\r\n",
		       getpid (),
		       global.port,
		       total_cmd,
		       ops,
		       total_cmd_local,
		       ops_local,
		       conn_clients,
		       global.prefer_domain_socket,
		       disconn_redis, delay_filter);
  sdsfree (delay_filter);

  return info;
}

static sds
sdscatinfo_latency (sds info, command_context * ctx)
{
  long long histo[LATENCY_HISTOGRAM_COLUMNS];
  struct info_ret *resp;
  int i, j, interval;

  // Aggregate latency histogram
  memset (&histo, 0, sizeof (histo));
  for (i = 0; i < ARRAY_N (&ctx->async_handles); i++)
    {
      command_async *async = ARRAY_GET (&ctx->async_handles, i);

      resp = async->resp_arg;
      for (j = 0; j < LATENCY_HISTOGRAM_COLUMNS; j++)
	{
	  histo[j] += resp->latency_histogram[j];
	}
    }

  // Print
  info = sdscat (info, "# Latency\r\n");
  interval = 1;
  for (i = 0; i < LATENCY_HISTOGRAM_COLUMNS - 1; i++)
    {
      info =
	sdscatprintf (info, "less_than_or_equal_to_%dms:%lld\r\n",
		      interval, histo[i]);
      interval *= 2;
    }
  info =
    sdscatprintf (info, "more_than_%dms:%lld\r\n", interval / 2,
		  histo[LATENCY_HISTOGRAM_COLUMNS - 1]);

  return info;
}

struct redis_info_stat_s
{
  long long connected_clients;

  long long used_memory;
  long long used_memory_rss;
  long long used_memory_peak;

  long long total_connections_received;
  long long total_commands_processed;
  long long instantaneous_ops_per_sec;
  long long total_commands_lcon;
  long long instantaneous_lcon_ops_per_sec;
  long long rejected_connections;
  long long expired_keys;
  long long evicted_keys;
  long long keyspace_hits;
  long long keyspace_misses;

  long long keys;
  long long expires;
  long long avg_ttl;
};

static int
number_after_equal_char (sds s, long long *ll)
{
  int argc, ret;
  sds *argv;

  argv = sdssplitlen (s, sdslen (s), "=", 1, &argc);
  if (!argv || argc != 2)
    {
      if (argv)
	{
	  sdsfreesplitres (argv, argc);
	}
      return 0;
    }

  ret = string2ll (argv[1], sdslen (argv[1]), ll);
  sdsfreesplitres (argv, argc);
  if (!ret)
    {
      return 0;
    }

  return 1;
}

static void
parse_info (sds data, struct redis_info_stat_s *stat)
{
  sds *lines;
  int nline, i;

  lines = sdssplitlen (data, sdslen (data), "\r\n", 2, &nline);
  if (!lines)
    {
      return;
    }

  for (i = 0; i < nline; i++)
    {
      sds *args;
      int narg;
      long long ll;

      args = sdssplitlen (lines[i], sdslen (lines[i]), ":", 1, &narg);
      if (!args || narg != 2)
	{
	  if (args)
	    {
	      sdsfreesplitres (args, narg);
	    }
	  continue;
	}

      if (!strcmp (args[0], "db0"))
	{
	  sds *items;
	  int nitem, j;

	  items = sdssplitlen (args[1], sdslen (args[1]), ",", 1, &nitem);
	  if (!items)
	    {
	      sdsfreesplitres (args, narg);
	      continue;
	    }

	  for (j = 0; j < nitem; j++)
	    {
	      number_after_equal_char (items[j], &ll);
	      if (!strncmp (items[j], "keys", 4))
		{
		  stat->keys = ll;
		}
	      else if (!strncmp (items[j], "expires", 7))
		{
		  stat->expires = ll;
		}
	      else if (!strncmp (items[j], "avg_ttl", 7))
		{
		  stat->avg_ttl = ll;
		}
	    }
	  sdsfreesplitres (items, nitem);
	  sdsfreesplitres (args, narg);
	  continue;
	}

      ll = 0;
      string2ll (args[1], sdslen (args[1]), &ll);

      if (!strcmp (args[0], "connected_clients"))
	{
	  stat->connected_clients = ll;
	}
      else if (!strcmp (args[0], "used_memory"))
	{
	  stat->used_memory = ll;
	}
      else if (!strcmp (args[0], "used_memory_rss"))
	{
	  stat->used_memory_rss = ll;
	}
      else if (!strcmp (args[0], "used_memory_peak"))
	{
	  stat->used_memory_peak = ll;
	}
      else if (!strcmp (args[0], "total_connections_received"))
	{
	  stat->total_connections_received = ll;
	}
      else if (!strcmp (args[0], "total_commands_replied"))
	{
	  // Sum of "total_commands_replied" is actual stat of 
	  // cluster without replicated commands.
	  stat->total_commands_processed = ll;
	}
      else if (!strcmp (args[0], "instantaneous_replied_ops_per_sec"))
	{
	  // Same as "total_command_replied"
	  stat->instantaneous_ops_per_sec = ll;
	}
      else if (!strcmp (args[0], "total_commands_lcon"))
	{
	  stat->total_commands_lcon = ll;
	}
      else if (!strcmp (args[0], "instantaneous_lcon_ops_per_sec"))
	{
	  stat->instantaneous_lcon_ops_per_sec = ll;
	}
      else if (!strcmp (args[0], "rejected_connections"))
	{
	  stat->rejected_connections = ll;
	}
      else if (!strcmp (args[0], "expired_keys"))
	{
	  stat->expired_keys = ll;
	}
      else if (!strcmp (args[0], "evicted_keys"))
	{
	  stat->evicted_keys = ll;
	}
      else if (!strcmp (args[0], "keyspace_hits"))
	{
	  stat->keyspace_hits = ll;
	}
      else if (!strcmp (args[0], "keyspace_misses"))
	{
	  stat->keyspace_misses = ll;
	}

      sdsfreesplitres (args, narg);
    }
  sdsfreesplitres (lines, nline);
}

static void
aggregate_redis_stat (command_context * ctx, struct redis_info_stat_s *aggr)
{
  dictType pgDictType = { dictStrHash,
    dictStrDup,
    NULL,
    dictStrKeyCompare,
    dictStrDestructor,
    NULL
  };
  dict *pg_dict;
  int i;

  pg_dict = dictCreate (&pgDictType, NULL);

  for (i = 0; i < ARRAY_N (&ctx->msg_handles); i++)
    {
      redis_msg *msg = ARRAY_GET (&ctx->msg_handles, i);
      ParseContext *parse_ctx = pool_get_parse_ctx (msg);
      struct redis_info_stat_s stat;
      sbuf_pos start;
      ssize_t len;
      int dup_pg;
      sds buf;

      if (pool_msg_fail (msg))
	{
	  continue;
	}

      getArgumentPosition (parse_ctx, 0, &start, &len);
      buf = sdsMakeRoomFor (sdsempty (), len + 1);
      sbuf_copy_buf (buf, start, len);
      sdsIncrLen (buf, len);

      memset (&stat, 0, sizeof (stat));
      parse_info (buf, &stat);
      sdsfree (buf);

      if (!dictFind (pg_dict, msg->my_pg->id))
	{
	  dup_pg = 0;
	  dictAdd (pg_dict, (void *) msg->my_pg->id, NULL);
	}
      else
	{
	  dup_pg = 1;
	}

      aggr->connected_clients += stat.connected_clients;

      aggr->used_memory += stat.used_memory;
      aggr->used_memory_rss += stat.used_memory_rss;
      aggr->used_memory_peak += stat.used_memory_peak;

      aggr->total_connections_received += stat.total_connections_received;
      aggr->total_commands_processed += stat.total_commands_processed;
      aggr->instantaneous_ops_per_sec += stat.instantaneous_ops_per_sec;
      aggr->total_commands_lcon += stat.total_commands_lcon;
      aggr->instantaneous_lcon_ops_per_sec +=
	stat.instantaneous_lcon_ops_per_sec;
      aggr->rejected_connections += stat.rejected_connections;
      if (!dup_pg)
	{
	  aggr->expired_keys += stat.expired_keys;
	  aggr->evicted_keys += stat.evicted_keys;
	}
      aggr->keyspace_hits += stat.keyspace_hits;
      aggr->keyspace_misses += stat.keyspace_misses;

      if (!dup_pg)
	{
	  aggr->keys += stat.keys;
	  aggr->expires += stat.expires;
	  aggr->avg_ttl += stat.avg_ttl;
	}
    }
  dictRelease (pg_dict);
}

static void
bytes_to_human (char *s, long long n)
{
  double d;

  if (n < 1024)
    {
      /* Bytes */
      sprintf (s, "%lluB", n);
      return;
    }
  else if (n < (1024 * 1024))
    {
      d = (double) n / (1024);
      sprintf (s, "%.2fK", d);
    }
  else if (n < (1024LL * 1024 * 1024))
    {
      d = (double) n / (1024 * 1024);
      sprintf (s, "%.2fM", d);
    }
  else if (n < (1024LL * 1024 * 1024 * 1024))
    {
      d = (double) n / (1024LL * 1024 * 1024);
      sprintf (s, "%.2fG", d);
    }
  else
    {
      d = (double) n / (1024LL * 1024 * 1024 * 1024);
      sprintf (s, "%.2fT", d);
    }
}


static sbuf *
merge_reply_info (command_context * ctx, char *section,
		  int allsections, int defsections)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;
  sds info;
  struct redis_info_stat_s aggr;
  int sections = 0;

  if (ARRAY_N (&ctx->msg_handles) > 0)
    {
      memset (&aggr, 0, sizeof (aggr));
      aggregate_redis_stat (ctx, &aggr);
    }

  info = sdsMakeRoomFor (sdsempty (), 4096);

  if (allsections || defsections
      || (section && !strcasecmp (section, "cluster")))
    {
      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}
      info = sdscatinfo_cluster (info, ctx);
    }

  if (allsections || (section && !strcasecmp (section, "redis")))
    {
      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}
      info = sdscatinfo_redis (info, ctx);
    }

  if (allsections || (section && !strcasecmp (section, "gateway")))
    {
      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}
      info = sdscatinfo_gateway (info, ctx);
    }

  if (allsections || (section && !strcasecmp (section, "latency")))
    {
      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}
      info = sdscatinfo_latency (info, ctx);
    }

  if (allsections || (section && !strcasecmp (section, "clients")))
    {
      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}
      info = sdscatprintf (info, "# Clients\r\n"
			   "connected_clients:%lld\r\n",
			   aggr.connected_clients);
    }

  if (allsections || defsections
      || (section && !strcasecmp (section, "memory")))
    {
      char hmem[64];
      char peak_hmem[64];

      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}
      bytes_to_human (hmem, aggr.used_memory);
      bytes_to_human (peak_hmem, aggr.used_memory_peak);

      info = sdscatprintf (info, "# Memory\r\n"
			   "used_memory:%lld\r\n"
			   "used_memory_human:%s\r\n"
			   "used_memory_rss:%lld\r\n"
			   "used_memory_peak:%lld\r\n"
			   "used_memory_peak_human:%s\r\n"
			   "mem_fragmentation_ratio:%.2f\r\n",
			   aggr.used_memory,
			   hmem,
			   aggr.used_memory_rss,
			   aggr.used_memory_peak,
			   peak_hmem,
			   (aggr.used_memory ==
			    0) ? 0 : (float) aggr.used_memory_rss /
			   aggr.used_memory);
    }

  if (allsections || defsections
      || (section && !strcasecmp (section, "stats")))
    {
      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}
      info = sdscatprintf (info, "# Stats\r\n"
			   "total_connections_received:%lld\r\n"
			   "total_commands_processed:%lld\r\n"
			   "instantaneous_ops_per_sec:%lld\r\n"
			   "total_commands_lcon:%lld\r\n"
			   "instantaneous_lcon_ops_per_sec:%lld\r\n"
			   "rejected_connections:%lld\r\n"
			   "expired_keys:%lld\r\n"
			   "evicted_keys:%lld\r\n"
			   "keyspace_hits:%lld\r\n"
			   "keyspace_misses:%lld\r\n",
			   aggr.total_connections_received,
			   aggr.total_commands_processed,
			   aggr.instantaneous_ops_per_sec,
			   aggr.total_commands_lcon,
			   aggr.instantaneous_lcon_ops_per_sec,
			   aggr.rejected_connections,
			   aggr.expired_keys,
			   aggr.evicted_keys,
			   aggr.keyspace_hits, aggr.keyspace_misses);
    }

  if (allsections || defsections
      || (section && !strcasecmp (section, "keyspace")))
    {
      int pg_count;
      if (sections++)
	{
	  info = sdscat (info, "\r\n");
	}

      pg_count = pool_available_partition_count (mgr->pool);

      info = sdscatprintf (info, "# Keyspace\r\n"
			   "db0:keys=%lld,expires=%lld,avg_ttl=%lld\r\n",
			   aggr.keys,
			   aggr.expires,
			   (pg_count == 0) ? 0 : aggr.avg_ttl / pg_count);
    }

  reply =
    stream_create_sbuf_printf (mgr->shared_stream, "$%ld\r\n%s\r\n",
			       sdslen (info), info);
  sdsfree (info);
  return reply;
}

static int
is_redis_data (char *section)
{
  return section && ((strcasecmp (section, "clients") == 0)
		     || (strcasecmp (section, "memory") == 0)
		     || (strcasecmp (section, "stats") == 0)
		     || (strcasecmp (section, "keyspace") == 0));
}

static int
is_worker_data (char *section)
{
  return section && ((strcasecmp (section, "redis") == 0)
		     || (strcasecmp (section, "gateway") == 0)
		     || (strcasecmp (section, "latency") == 0));
}

static int
get_section (command_context * ctx, char **section,
	     int *allsections, int *defsections)
{
  *section = NULL;
  *allsections = 0;
  *defsections = 1;

  if (getArgumentCount (ctx->parse_ctx) == 2)
    {
      if (getParsedStr (ctx->parse_ctx, 1, section) == ERR)
	{
	  return ERR;
	}
    }

  if (*section)
    {
      *allsections = strcasecmp (*section, "all") == 0;
      *defsections = strcasecmp (*section, "default") == 0;
    }

  return OK;
}

void
info_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  int allsections, defsections;
  char *section;
  sbuf *reply;
  int ret, i;

  COROUTINE_BEGIN (ctx->coro_line);

  // Arity Check
  if (getArgumentCount (ctx->parse_ctx) != 1
      && getArgumentCount (ctx->parse_ctx) != 2)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream, "-ERR syntax error\r\n");
      reply_and_free (ctx, reply);
      return;
    }
  // Parsing arguments
  ret = get_section (ctx, &section, &allsections, &defsections);
  assert (ret == OK);

  // Send queries to all servers
  if (allsections || defsections || is_redis_data (section))
    {
      char buf[1024];

      if (section)
	{
	  snprintf (buf, 1024, "INFO %s\r\n", section);
	}
      else
	{
	  snprintf (buf, 1024, "INFO\r\n");
	}

      ret =
	pool_send_query_all (mgr->pool, buf, cmd_redis_coro_invoker, ctx,
			     &ctx->msg_handles);
      // Ignore failed msgs and try best to get information from success msgs.
    }
  zfree (section);

  // Receive replies of queries
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->msg_handles); ctx->idx++)
    {
      while (!pool_msg_finished (ARRAY_GET (&ctx->msg_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
      // Ignore failed msgs and try best to get information from success msgs.
    }

  // Parsing arguments
  ret = get_section (ctx, &section, &allsections, &defsections);
  assert (ret == OK);

  // Send command to all workers
  if (allsections || is_worker_data (section))
    {
      for (i = 0; i < ARRAY_N (mgr->worker_asyncs); i++)
	{
	  async_chan *callee;
	  command_async *cmd;

	  callee = ARRAY_GET (mgr->worker_asyncs, i);
	  cmd = send_info_cmd (ctx, callee);
	  ARRAY_PUSH (&ctx->async_handles, cmd);
	}
    }
  zfree (section);

  // Receive replies from all workers
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->async_handles); ctx->idx++)
    {
      while (!is_cmd_async_finished
	     (ARRAY_GET (&ctx->async_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
    }

  // Parsing arguments
  ret = get_section (ctx, &section, &allsections, &defsections);
  assert (ret == OK);

  // Merge reply and send
  reply = merge_reply_info (ctx, section, allsections, defsections);
  zfree (section);
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

static sbuf *
merge_reply_dbsize (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  redis_msg *msg;
  ParseContext *parse_ctx;
  sbuf_pos start;
  ssize_t len;
  int i, ret;
  long long count, ll;
  dict *pg_dict;
  dictType pgDictType = { dictStrHash,
    dictStrDup,
    NULL,
    dictStrKeyCompare,
    dictStrDestructor,
    NULL
  };

  pg_dict = dictCreate (&pgDictType, NULL);

  count = 0;
  for (i = 0; i < ARRAY_N (&ctx->msg_handles); i++)
    {
      int dup_pg;
      msg = ARRAY_GET (&ctx->msg_handles, i);
      parse_ctx = pool_get_parse_ctx (msg);

      if (pool_msg_fail (msg))
	{
	  continue;
	}

      if (!dictFind (pg_dict, msg->my_pg->id))
	{
	  dup_pg = 0;
	  dictAdd (pg_dict, (void *) msg->my_pg->id, NULL);
	}
      else
	{
	  dup_pg = 1;
	}

      if (!dup_pg)
	{
	  getArgumentPosition (parse_ctx, 0, &start, &len);
	  sbuf_next_pos (&start);	// Skip ':' character
	  ret = sbuf_string2ll (start, len - 1, &ll);
	  assert (ret);		// Redis DBSIZE command always returns in ':<number>\r\n' format.
	  count += ll;
	}
    }
  dictRelease (pg_dict);

  return stream_create_sbuf_printf (mgr->shared_stream, ":%lld\r\n", count);
}

void
dbsize_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;
  int ret;

  COROUTINE_BEGIN (ctx->coro_line);

  // Send queries to all servers
  ret =
    pool_send_query_all (mgr->pool, "DBSIZE\r\n", cmd_redis_coro_invoker, ctx,
			 &ctx->msg_handles);
  // Ignore failed msgs and try best to get information from success msgs.

  // Receive replies of queries
  for (ctx->idx = 0; ctx->idx < ARRAY_N (&ctx->msg_handles); ctx->idx++)
    {
      while (!pool_msg_finished (ARRAY_GET (&ctx->msg_handles, ctx->idx)))
	{
	  COROUTINE_YIELD;
	}
      // Ignore failed msgs and try best to get information from success msgs.
    }

  // Merge reply and send
  reply = merge_reply_dbsize (ctx);
  reply_and_free (ctx, reply);

  COROUTINE_END;
}

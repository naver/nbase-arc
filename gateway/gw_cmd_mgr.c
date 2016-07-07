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

struct redis_command redis_command_table[] = {
/* SSS Commands */
  {"s3lget", single_key_command, 5, "r", 0, 2, 2, 1, 0, 0},
  {"s3lmget", single_key_command, -4, "r", 0, 2, 2, 1, 0, 0},
  {"s3lkeys", single_key_command, -3, "r", 0, 2, 2, 1, 0, 0},
  {"s3lvals", single_key_command, 4, "r", 0, 2, 2, 1, 0, 0},
  {"s3ladd", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3laddat", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3lmadd", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3lrem", single_key_command, -4, "w", 0, 2, 2, 1, 0, 0},
  {"s3lmrem", single_key_command, -5, "w", 0, 2, 2, 1, 0, 0},
  {"s3lset", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3lreplace", single_key_command, 8, "wm", 0, 2, 2, 1, 0, 0},
  {"s3lcount", single_key_command, -3, "r", 0, 2, 2, 1, 0, 0},
  {"s3lexists", single_key_command, -5, "r", 0, 2, 2, 1, 0, 0},
  {"s3lexpire", single_key_command, -4, "w", 0, 2, 2, 1, 0, 0},
  {"s3lmexpire", single_key_command, -6, "w", 0, 2, 2, 1, 0, 0},
  {"s3lttl", single_key_command, -5, "r", 0, 2, 2, 1, 0, 0},
  {"s3sget", single_key_command, 5, "r", 0, 2, 2, 1, 0, 0},
  {"s3smget", single_key_command, -4, "r", 0, 2, 2, 1, 0, 0},
  {"s3skeys", single_key_command, -3, "r", 0, 2, 2, 1, 0, 0},
  {"s3svals", single_key_command, 4, "r", 0, 2, 2, 1, 0, 0},
  {"s3sadd", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3saddat", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3smadd", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3srem", single_key_command, -4, "w", 0, 2, 2, 1, 0, 0},
  {"s3smrem", single_key_command, -5, "w", 0, 2, 2, 1, 0, 0},
  {"s3sset", single_key_command, -7, "wm", 0, 2, 2, 1, 0, 0},
  {"s3sreplace", single_key_command, 8, "wm", 0, 2, 2, 1, 0, 0},
  {"s3scount", single_key_command, -3, "r", 0, 2, 2, 1, 0, 0},
  {"s3sexists", single_key_command, -5, "r", 0, 2, 2, 1, 0, 0},
  {"s3sexpire", single_key_command, -4, "w", 0, 2, 2, 1, 0, 0},
  {"s3smexpire", single_key_command, -6, "w", 0, 2, 2, 1, 0, 0},
  {"s3sttl", single_key_command, -5, "r", 0, 2, 2, 1, 0, 0},
  {"s3keys", single_key_command, 3, "r", 0, 2, 2, 1, 0, 0},
  {"s3count", single_key_command, 3, "r", 0, 2, 2, 1, 0, 0},
  {"s3expire", single_key_command, 4, "w", 0, 2, 2, 1, 0, 0},
  {"s3rem", single_key_command, 3, "w", 0, 2, 2, 1, 0, 0},
  {"s3mrem", single_key_command, -4, "w", 0, 2, 2, 1, 0, 0},
/* Default Redis Commands */
  {"get", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"set", single_key_command, -3, "wm", 0, 1, 1, 1, 0, 0},
  {"setnx", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"setex", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"psetex", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"append", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"strlen", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"del", del_command, -2, "w", 0, 1, -1, 1, 0, 0},
  {"exists", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"setbit", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"getbit", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"setrange", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"getrange", single_key_command, 4, "r", 0, 1, 1, 1, 0, 0},
  {"substr", single_key_command, 4, "r", 0, 1, 1, 1, 0, 0},
  {"incr", single_key_command, 2, "wm", 0, 1, 1, 1, 0, 0},
  {"decr", single_key_command, 2, "wm", 0, 1, 1, 1, 0, 0},
  {"mget", mget_command, -2, "r", 0, 1, -1, 1, 0, 0},
  {"rpush", single_key_command, -3, "wm", 0, 1, 1, 1, 0, 0},
  {"lpush", single_key_command, -3, "wm", 0, 1, 1, 1, 0, 0},
  {"rpushx", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"lpushx", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"linsert", single_key_command, 5, "wm", 0, 1, 1, 1, 0, 0},
  {"rpop", single_key_command, 2, "w", 0, 1, 1, 1, 0, 0},
  {"lpop", single_key_command, 2, "w", 0, 1, 1, 1, 0, 0},
  {"brpop", NULL, -3, "Nws", 0, 1, 1, 1, 0, 0},
  {"brpoplpush", NULL, 4, "Nwms", 0, 1, 2, 1, 0, 0},
  {"blpop", NULL, -3, "Nws", 0, 1, -2, 1, 0, 0},
  {"llen", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"lindex", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"lset", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"lrange", single_key_command, 4, "r", 0, 1, 1, 1, 0, 0},
  {"ltrim", single_key_command, 4, "w", 0, 1, 1, 1, 0, 0},
  {"lrem", single_key_command, 4, "w", 0, 1, 1, 1, 0, 0},
  {"rpoplpush", NULL, 3, "Nwm", 0, 1, 2, 1, 0, 0},
  {"sadd", single_key_command, -3, "wm", 0, 1, 1, 1, 0, 0},
  {"srem", single_key_command, -3, "w", 0, 1, 1, 1, 0, 0},
  {"smove", NULL, 4, "Nw", 0, 1, 2, 1, 0, 0},
  {"sismember", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"scard", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"spop", NULL, 2, "NwRs", 0, 1, 1, 1, 0, 0},
  {"srandmember", single_key_command, -2, "rR", 0, 1, 1, 1, 0, 0},
  {"sinter", NULL, -2, "NrS", 0, 1, -1, 1, 0, 0},
  {"sinterstore", NULL, -3, "Nwm", 0, 1, -1, 1, 0, 0},
  {"sunion", NULL, -2, "NrS", 0, 1, -1, 1, 0, 0},
  {"sunionstore", NULL, -3, "Nwm", 0, 1, -1, 1, 0, 0},
  {"sdiff", NULL, -2, "NrS", 0, 1, -1, 1, 0, 0},
  {"sdiffstore", NULL, -3, "Nwm", 0, 1, -1, 1, 0, 0},
  {"smembers", single_key_command, 2, "rS", 0, 1, 1, 1, 0, 0},
  {"sscan", single_key_command, -3, "rR", 0, 1, 1, 1, 0, 0},
  {"zadd", single_key_command, -4, "wm", 0, 1, 1, 1, 0, 0},
  {"zincrby", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"zrem", single_key_command, -3, "w", 0, 1, 1, 1, 0, 0},
  {"zremrangebyscore", single_key_command, 4, "w", 0, 1, 1, 1, 0, 0},
  {"zremrangebyrank", single_key_command, 4, "w", 0, 1, 1, 1, 0, 0},
  {"zunionstore", NULL, -4, "Nwm", 0, 0, 0, 0, 0, 0},
  {"zinterstore", NULL, -4, "Nwm", 0, 0, 0, 0, 0, 0},
  {"zrange", single_key_command, -4, "r", 0, 1, 1, 1, 0, 0},
  {"zrangebyscore", single_key_command, -4, "r", 0, 1, 1, 1, 0, 0},
  {"zrevrangebyscore", single_key_command, -4, "r", 0, 1, 1, 1, 0, 0},
  {"zcount", single_key_command, 4, "r", 0, 1, 1, 1, 0, 0},
  {"zrevrange", single_key_command, -4, "r", 0, 1, 1, 1, 0, 0},
  {"zcard", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"zscore", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"zrank", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"zrevrank", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"zscan", single_key_command, -3, "rR", 0, 1, 1, 1, 0, 0},
  {"hset", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"hsetnx", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"hget", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"hmset", single_key_command, -4, "wm", 0, 1, 1, 1, 0, 0},
  {"hmget", single_key_command, -3, "r", 0, 1, 1, 1, 0, 0},
  {"hincrby", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"hincrbyfloat", single_key_command, 4, "wm", 0, 1, 1, 1, 0, 0},
  {"hdel", single_key_command, -3, "w", 0, 1, 1, 1, 0, 0},
  {"hlen", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"hkeys", single_key_command, 2, "rS", 0, 1, 1, 1, 0, 0},
  {"hvals", single_key_command, 2, "rS", 0, 1, 1, 1, 0, 0},
  {"hgetall", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"hexists", single_key_command, 3, "r", 0, 1, 1, 1, 0, 0},
  {"hscan", single_key_command, -3, "rR", 0, 1, 1, 1, 0, 0},
  {"incrby", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"decrby", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"incrbyfloat", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"getset", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
  {"mset", mset_command, -3, "wm", 0, 1, -1, 2, 0, 0},
  {"msetnx", NULL, -3, "Nwm", 0, 1, -1, 2, 0, 0},
  {"randomkey", NULL, 1, "NrR", 0, 0, 0, 0, 0, 0},
  {"select", NULL, 2, "Nrl", 0, 0, 0, 0, 0, 0},
  {"move", NULL, 3, "Nw", 0, 1, 1, 1, 0, 0},
  {"rename", NULL, 3, "Nw", 0, 1, 2, 1, 0, 0},
  {"renamenx", NULL, 3, "Nw", 0, 1, 2, 1, 0, 0},
  {"expire", single_key_command, 3, "w", 0, 1, 1, 1, 0, 0},
  {"expireat", single_key_command, 3, "w", 0, 1, 1, 1, 0, 0},
  {"pexpire", single_key_command, 3, "w", 0, 1, 1, 1, 0, 0},
  {"pexpireat", single_key_command, 3, "w", 0, 1, 1, 1, 0, 0},
  {"keys", NULL, 2, "NrS", 0, 0, 0, 0, 0, 0},
  {"scan", NULL, -2, "NrR", 0, 0, 0, 0, 0, 0},
  {"dbsize", dbsize_command, 1, "r", 0, 0, 0, 0, 0, 0},
  {"auth", NULL, 2, "Nrslt", 0, 0, 0, 0, 0, 0},
  {"echo", NULL, 2, "Nr", 0, 0, 0, 0, 0, 0},
  {"save", NULL, 1, "Nars", 0, 0, 0, 0, 0, 0},
  {"bgsave", NULL, 1, "Nar", 0, 0, 0, 0, 0, 0},
  {"bgrewriteaof", NULL, 1, "Nar", 0, 0, 0, 0, 0, 0},
  {"shutdown", NULL, -1, "Narl", 0, 0, 0, 0, 0, 0},
  {"lastsave", NULL, 1, "NrR", 0, 0, 0, 0, 0, 0},
  {"type", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"multi", NULL, 1, "Nrs", 0, 0, 0, 0, 0, 0},
  {"exec", NULL, 1, "NsM", 0, 0, 0, 0, 0, 0},
  {"discard", NULL, 1, "Nrs", 0, 0, 0, 0, 0, 0},
  {"sync", NULL, 1, "Nars", 0, 0, 0, 0, 0, 0},
  {"psync", NULL, 3, "Nars", 0, 0, 0, 0, 0, 0},
  {"replconf", NULL, -1, "Narslt", 0, 0, 0, 0, 0, 0},
  {"flushdb", NULL, 1, "Nw", 0, 0, 0, 0, 0, 0},
  {"flushall", NULL, 1, "Nw", 0, 0, 0, 0, 0, 0},
  {"sort", NULL, -2, "Nwm", 0, 1, 1, 1, 0, 0},
  {"info", info_command, -1, "rlt", 0, 0, 0, 0, 0, 0},
  {"monitor", NULL, 1, "Nars", 0, 0, 0, 0, 0, 0},
  {"ttl", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"pttl", single_key_command, 2, "r", 0, 1, 1, 1, 0, 0},
  {"persist", single_key_command, 2, "w", 0, 1, 1, 1, 0, 0},
  {"slaveof", NULL, 3, "Nast", 0, 0, 0, 0, 0, 0},
  {"debug", NULL, -2, "Nas", 0, 0, 0, 0, 0, 0},
  {"config", NULL, -2, "Nar", 0, 0, 0, 0, 0, 0},
  {"subscribe", NULL, -2, "Nrpslt", 0, 0, 0, 0, 0, 0},
  {"unsubscribe", NULL, -1, "Nrpslt", 0, 0, 0, 0, 0, 0},
  {"psubscribe", NULL, -2, "Nrpslt", 0, 0, 0, 0, 0, 0},
  {"punsubscribe", NULL, -1, "Nrpslt", 0, 0, 0, 0, 0, 0},
  {"publish", NULL, 3, "Npltr", 0, 0, 0, 0, 0, 0},
  {"pubsub", NULL, -2, "NpltrR", 0, 0, 0, 0, 0, 0},
  {"watch", NULL, -2, "Nrs", 0, 1, -1, 1, 0, 0},
  {"unwatch", NULL, 1, "Nrs", 0, 0, 0, 0, 0, 0},
  {"restore", single_key_command, 4, "awm", 0, 1, 1, 1, 0, 0},
  {"migrate", NULL, 6, "Naw", 0, 0, 0, 0, 0, 0},
  {"dump", single_key_command, 2, "ar", 0, 1, 1, 1, 0, 0},
  {"object", single_key_command, -2, "r", 0, 2, 2, 2, 0, 0},
  {"client", NULL, -2, "Nar", 0, 0, 0, 0, 0, 0},
  {"eval", NULL, -3, "Ns", 0, 0, 0, 0, 0, 0},
  {"evalsha", NULL, -3, "Ns", 0, 0, 0, 0, 0, 0},
  {"slowlog", NULL, -2, "Nr", 0, 0, 0, 0, 0, 0},
  {"script", NULL, -2, "Nras", 0, 0, 0, 0, 0, 0},
  {"time", NULL, 1, "NrR", 0, 0, 0, 0, 0, 0},
  {"bitop", NULL, -4, "Nwm", 0, 2, -1, 1, 0, 0},
  {"bitcount", single_key_command, -2, "r", 0, 1, 1, 1, 0, 0},
  {"bitpos", single_key_command, -3, "r", 0, 1, 1, 1, 0, 0},
/* Gateway Commands */
  {"ping", ping_command, 1, "rt", 0, 0, 0, 0, 0, 0},
  {"quit", quit_command, 1, "r", 0, 0, 0, 0, 0, 0},
/* For Test */
  {"crc16", single_key_command, 3, "wm", 0, 1, 1, 1, 0, 0},
};

struct redis_command admin_command_table[] = {
/* Admin Commands */
  {"cluster_info", admin_cluster_info_command, 1, "A", 0, 0, 0, 0, 0, 0},
  {"delay", admin_delay_command, 3, "A", 0, 0, 0, 0, 0, 0},
  {"redirect", admin_redirect_command, 4, "A", 0, 0, 0, 0, 0, 0},
  {"pg_add", admin_pg_add_command, 2, "A", 0, 0, 0, 0, 0, 0},
  {"pg_del", admin_pg_del_command, 2, "A", 0, 0, 0, 0, 0, 0},
  {"pgs_add", admin_pgs_add_command, 5, "A", 0, 0, 0, 0, 0, 0},
  {"pgs_del", admin_pgs_del_command, 3, "A", 0, 0, 0, 0, 0, 0},
  {"ping", ping_command, 1, "rt", 0, 0, 0, 0, 0, 0},
  {"quit", quit_command, 1, "r", 0, 0, 0, 0, 0, 0},
  {"help", admin_help_command, 1, "A", 0, 0, 0, 0, 0, 0}
};

static dictType commandTableDictType = {
  dictStrCaseHash,		/* hash function */
  NULL,				/* key dup */
  NULL,				/* val dup */
  dictStrKeyCaseCompare,	/* key compare */
  dictStrDestructor,		/* key destructor */
  NULL				/* val destructor */
};

static void
populate_command_table (dict * commands, int type)
{
  int j;
  int numcommands;
  struct redis_command *command_table;

  switch (type)
    {
    case COMMAND_MANAGER_TYPE_USER:
      numcommands =
	sizeof (redis_command_table) / sizeof (struct redis_command);
      command_table = redis_command_table;
      break;
    case COMMAND_MANAGER_TYPE_ADMIN:
      numcommands =
	sizeof (admin_command_table) / sizeof (struct redis_command);
      command_table = admin_command_table;
      break;
    default:
      assert (0);
    }


  for (j = 0; j < numcommands; j++)
    {
      struct redis_command *c = command_table + j;
      const char *f = c->sflags;
      int retval;

      while (*f != '\0')
	{
	  switch (*f)
	    {
	    case 'w':
	      c->flags |= REDIS_CMD_WRITE;
	      break;
	    case 'r':
	      c->flags |= REDIS_CMD_READONLY;
	      break;
	    case 'm':
	      c->flags |= REDIS_CMD_DENYOOM;
	      break;
	    case 'a':
	      c->flags |= REDIS_CMD_ADMIN;
	      break;
	    case 'p':
	      c->flags |= REDIS_CMD_PUBSUB;
	      break;
	    case 'f':
	      c->flags |= REDIS_CMD_FORCE_REPLICATION;
	      break;
	    case 's':
	      c->flags |= REDIS_CMD_NOSCRIPT;
	      break;
	    case 'R':
	      c->flags |= REDIS_CMD_RANDOM;
	      break;
	    case 'S':
	      c->flags |= REDIS_CMD_SORT_FOR_SCRIPT;
	      break;
	    case 'l':
	      c->flags |= REDIS_CMD_LOADING;
	      break;
	    case 't':
	      c->flags |= REDIS_CMD_STALE;
	      break;
	    case 'M':
	      c->flags |= REDIS_CMD_SKIP_MONITOR;
	      break;
	    case 'N':
	      c->flags |= GW_CMD_NOT_SUPPORT;
	      break;
	    case 'A':
	      c->flags |= GW_CMD_ADMIN;
	      break;
	    default:
	      gwlog (GW_WARNING, "Unsupported command flag");
	      assert (0);
	      break;
	    }
	  f++;
	}
      retval = dictAdd (commands, zstrdup (c->name), c);
      assert (retval == DICT_OK);
    }
}

static struct redis_command *
lookup_command (dict * commands, const void *name)
{
  return dictFetchValue (commands, name);
}

static redis_command *
get_redis_command (command_manager * mgr, ParseContext * ctx)
{
  char cmdbuf[REDIS_CMD_MAX_LEN + 1];
  sbuf_pos start_pos;
  ssize_t len;
  int ret;

  if (getArgumentCount (ctx) < 1)
    {
      return NULL;
    }

  ret = getArgumentPosition (ctx, 0, &start_pos, &len);
  assert (ret == OK);

  if (len > REDIS_CMD_MAX_LEN)
    {
      return NULL;
    }

  ret = sbuf_copy_buf (cmdbuf, start_pos, len);
  assert (ret == OK);
  cmdbuf[len] = '\0';

  return lookup_command (mgr->commands, cmdbuf);
}

static void
track_ops (command_stat * stat)
{
  long long curtime = mstime ();
  long long t = curtime - stat->last_sample_time;
  long long ops = stat->numcommands - stat->ops_sec_last_sample_count;
  long long local_ops =
    stat->numcommands_local - stat->local_ops_sec_last_sample_count;
  long long ops_sec, local_ops_sec;

  ops_sec = t > 0 ? (ops * 1000 / t) : 0;
  stat->ops_sec_samples[stat->sample_idx] = ops_sec;
  stat->ops_sec_last_sample_count = stat->numcommands;

  local_ops_sec = t > 0 ? (local_ops * 1000 / t) : 0;
  stat->local_ops_sec_samples[stat->sample_idx] = local_ops_sec;
  stat->local_ops_sec_last_sample_count = stat->numcommands_local;

  stat->sample_idx = (stat->sample_idx + 1) % COMMAND_STAT_SAMPLES;
  stat->last_sample_time = curtime;
}

static long long
sample_ops_sec (long long *samples)
{
  int i;
  long long sum = 0;

  for (i = 0; i < COMMAND_STAT_SAMPLES; i++)
    {
      sum += samples[i];
    }
  return sum / COMMAND_STAT_SAMPLES;
}

long long
cmd_stat_total_commands (command_manager * mgr)
{
  return mgr->stat.numcommands;
}

long long
cmd_stat_total_commands_local (command_manager * mgr)
{
  return mgr->stat.numcommands_local;
}

long long
cmd_stat_ops (command_manager * mgr)
{
  return sample_ops_sec (mgr->stat.ops_sec_samples);
}

long long
cmd_stat_local_ops (command_manager * mgr)
{
  return sample_ops_sec (mgr->stat.local_ops_sec_samples);
}

static int
cmd_cron (aeEventLoop * el, long long id, void *privdata)
{
  command_manager *mgr = privdata;
  int *hz = &mgr->hz;
  int *cronloops = &mgr->cronloops;
  GW_NOTUSED (el);
  GW_NOTUSED (id);

  run_with_period (100) track_ops (&mgr->stat);

  (*cronloops)++;
  return 1000 / *hz;
}

static command_manager *
cmd_mgr_create (aeEventLoop * el, redis_pool * pool, cluster_conf * conf,
		async_chan * my_async, array_async_chans * worker_asyncs,
		sbuf_hdr * shared_stream, int type)
{
  command_manager *mgr;

  mgr = zmalloc (sizeof (command_manager));
  mgr->el = el;
  mgr->cron_teid = aeCreateTimeEvent (el, 1000, cmd_cron, mgr, NULL);
  mgr->hz = GW_DEFAULT_HZ;
  mgr->cronloops = 0;
  mgr->pool = pool;
  mgr->commands = dictCreate (&commandTableDictType, NULL);
  populate_command_table (mgr->commands, type);
  mgr->conf = conf;
  mgr->my_async = my_async;
  mgr->worker_asyncs = worker_asyncs;
  mgr->shared_stream = shared_stream;
  mgr->idx_helper = index_helper_create (INDEX_HELPER_INIT_SIZE);
  memset (&mgr->stat, 0, sizeof (mgr->stat));

  // Memory pool
  mgr->mp_cmdctx =
    mempool_create (sizeof (struct command_context),
		    MEMPOOL_DEFAULT_POOL_SIZE);

  return mgr;
}

command_manager *
cmd_mgr_user_create (aeEventLoop * el, redis_pool * pool, cluster_conf * conf,
		     async_chan * my_async, array_async_chans * worker_asyncs,
		     sbuf_hdr * shared_stream)
{
  return cmd_mgr_create (el, pool, conf, my_async, worker_asyncs,
			 shared_stream, COMMAND_MANAGER_TYPE_USER);
}

command_manager *
cmd_mgr_admin_create (aeEventLoop * el, cluster_conf * conf,
		      async_chan * my_async,
		      array_async_chans * worker_asyncs,
		      sbuf_hdr * shared_stream)
{
  return cmd_mgr_create (el, NULL, conf, my_async, worker_asyncs,
			 shared_stream, COMMAND_MANAGER_TYPE_ADMIN);
}

void
cmd_mgr_destroy (command_manager * mgr)
{
  aeDeleteTimeEvent (mgr->el, mgr->cron_teid);
  dictRelease (mgr->commands);
  index_helper_destroy (mgr->idx_helper);
  mempool_destroy (mgr->mp_cmdctx);
  zfree (mgr);
}

command_context *
cmd_create_ctx (command_manager * mgr, sbuf * query,
		ParseContext * parse_ctx, cmd_reply_proc * cb, void *cbarg)
{
  command_context *ctx;

  ctx = mempool_alloc (mgr->mp_cmdctx);
  ctx->coro_line = 0;
  ctx->my_mgr = mgr;
  ctx->cmd = get_redis_command (mgr, parse_ctx);
  ctx->parse_ctx = parse_ctx;
  ctx->query = query;
  ctx->reply_cb = cb;
  ctx->cbarg = cbarg;
  ARRAY_INIT (&ctx->msg_handles);
  ARRAY_INIT (&ctx->msg_seqs);
  ARRAY_INIT (&ctx->async_handles);
  ctx->close_after_reply = 0;
  ctx->local = 0;
  ctx->unable_to_cancel = 0;
  ctx->swallow = 0;

  return ctx;
}

void
cmd_redis_coro_invoker (redis_msg * handle, void *cbarg)
{
  command_context *ctx = cbarg;
  GW_NOTUSED (handle);

  ctx->cmd->proc (ctx);
}

void
cmd_async_coro_invoker (void *arg, void *privdata)
{
  command_async *cmd = arg;
  command_context *ctx = cmd->ctx;
  GW_NOTUSED (privdata);

  cmd->finished = 1;
  if (cmd->swallow)
    {
      cmd_free_async (cmd);
      return;
    }
  ctx->cmd->proc (ctx);
}

void
cmd_free_async (command_async * cmd_async)
{
  if (cmd_async->finished)
    {
      if (cmd_async->rqst_free && cmd_async->rqst_arg)
	{
	  cmd_async->rqst_free (cmd_async->rqst_arg);
	}
      if (cmd_async->resp_free && cmd_async->resp_arg)
	{
	  cmd_async->resp_free (cmd_async->resp_arg);
	}
      zfree (cmd_async);
    }
  else
    {
      cmd_async->swallow = 1;
    }
}

static void
async_free_handles (array_cmd_async * async_handles)
{
  command_async *cmd_async;
  int i;

  for (i = 0; i < ARRAY_N (async_handles); i++)
    {
      cmd_async = ARRAY_GET (async_handles, i);
      cmd_free_async (cmd_async);
    }
}

static void
cmd_free (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;

  deleteParseContext (ctx->parse_ctx);
  ctx->parse_ctx = NULL;
  sbuf_free (ctx->query);
  ctx->query = NULL;

  pool_free_msgs (&ctx->msg_handles);
  ARRAY_FINALIZE (&ctx->msg_handles);
  ARRAY_FINALIZE (&ctx->msg_seqs);
  async_free_handles (&ctx->async_handles);
  ARRAY_FINALIZE (&ctx->async_handles);

  mempool_free (mgr->mp_cmdctx, ctx);
}

void
cmd_cancel (command_context * ctx)
{
  if (ctx->unable_to_cancel)
    {
      ctx->swallow = 1;
      return;
    }
  cmd_free (ctx);
}

void
reply_and_free (command_context * ctx, sbuf * reply)
{
  command_manager *mgr = ctx->my_mgr;

  // "delay" and "pgs_del" commands are not cancelable, because these commands
  // have post action after receiving responses from workers. If cmd_cancel()
  // function is called due to client disconnection, these commands with 
  // unable-to-cancel flag continue the execution and just swallow the reply
  // after execution finishes and reply_and_free() is called.
  if (ctx->swallow)
    {
      assert (ctx->unable_to_cancel);
      cmd_free (ctx);
      return;
    }

  ctx->reply_cb (reply, ctx->cbarg);

  mgr->stat.numcommands++;
  if (ctx->local)
    {
      mgr->stat.numcommands_local++;
    }

  cmd_free (ctx);
}

int
is_cmd_async_finished (command_async * cmd)
{
  return cmd->finished;
}

int
cmd_is_close_after_reply (command_context * ctx)
{
  if (ctx->close_after_reply)
    {
      return 1;
    }
  return 0;
}

void
cmd_send_command (command_context * ctx)
{
  command_manager *mgr = ctx->my_mgr;
  sbuf *reply;

  if (!ctx->cmd)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR Unknown command\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  if (ctx->cmd->flags & GW_CMD_NOT_SUPPORT)
    {
      reply =
	stream_create_sbuf_str (mgr->shared_stream,
				"-ERR Unsupported command\r\n");
      reply_and_free (ctx, reply);
      return;
    }

  if ((ctx->cmd->arity > 0
       && ctx->cmd->arity != getArgumentCount (ctx->parse_ctx))
      || (getArgumentCount (ctx->parse_ctx) < -ctx->cmd->arity))
    {
      reply =
	stream_create_sbuf_printf (mgr->shared_stream,
				   "-ERR wrong number of arguments for '%s' command\r\n",
				   ctx->cmd->name);
      reply_and_free (ctx, reply);
      return;
    }

  ctx->cmd->proc (ctx);
}

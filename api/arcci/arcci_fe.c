#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#pragma warning(push)
#pragma warning(disable : 4996)	// strdup
#pragma warning(disable : 4267)	// for converting datatype from size_t to int
#endif
# include "fmacros.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#ifdef _WIN32
#include "win32fixes.h"
#include <stdlib.h>
#include <crtdbg.h>
#else
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#endif

#include "arcci_common.h"
#include "crc16.h"
#include "arcci.h"

#define FE_ERROR(e) errno < 0 ? errno : (errno = (e))

typedef struct stk_item_s stk_item_t;
typedef struct fe_parse_ctx_s fe_parse_ctx_t;


/* from redis-3.0.0-beta8 */
#define REDIS_CMD_WRITE 1	/* "w" flag */
#define REDIS_CMD_READONLY 2	/* "r" flag */
#define REDIS_CMD_DENYOOM 4	/* "m" flag */
#define REDIS_CMD_NOT_USED_1 8	/* no longer used flag */
#define REDIS_CMD_ADMIN 16	/* "a" flag */
#define REDIS_CMD_PUBSUB 32	/* "p" flag */
#define REDIS_CMD_NOSCRIPT  64	/* "s" flag */
#define REDIS_CMD_RANDOM 128	/* "R" flag */
#define REDIS_CMD_SORT_FOR_SCRIPT 256	/* "S" flag */
#define REDIS_CMD_LOADING 512	/* "l" flag */
#define REDIS_CMD_STALE 1024	/* "t" flag */
#define REDIS_CMD_SKIP_MONITOR 2048	/* "M" flag */
#define REDIS_CMD_ASKING 4096	/* "k" flag */
#define REDIS_CMD_FAST 8192	/* "F" flag */

struct redisCommand
{
  char *name;
  void *unused1;
  int arity;
  char *sflags;
  int flags;
  void *getkeys_proc;
  int firstkey;
  int lastkey;
  int keystep;
  long long unused3, unused4;
};
#define _D ((void *)-1)

struct stk_item_s
{
  arc_reply_t *reply;
  int idx;
};

#define FE_PARSE_MAX_DEPTH 9
struct fe_parse_ctx_s
{
  int stk_idx;
  stk_item_t stk[FE_PARSE_MAX_DEPTH];
};
#define init_fe_parse_ctx(c) do {              \
  memset(c, 0, sizeof(fe_parse_ctx_t));        \
} while (0)

/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */
/* formatting */
static int redisFormatCommandArgv (cmdcb_t cb, void *cbarg, char **target,
				   int argc, const char **argv,
				   const size_t * argvlen);
static int redisvFormatCommand (cmdcb_t cb, void *cbarg, char **target,
				const char *format, va_list ap);
static int redis_append_command (arc_job_t * aj, char *cmd, int len,
				 cmd_peek_t * peek);
static void redis_cmdcb (int pos, int off, const char *val, size_t vallen,
			 void *ctx);

/* gateway */
static int parse_gw_spec (const char *gw, int *ngw, gw_t *** ra);
static gw_t *create_gw (char *host, int port);
static void destroy_gw (gw_t * gw);

/* redis parser callback implementation */
static int fe_parse_cb_return (fe_parse_ctx_t * c, arc_reply_t * reply);
static int fe_on_string (void *ctx, char *s, int len, int is_null);
static int fe_on_status (void *ctx, char *s, int len);
static int fe_on_error (void *ctx, char *s, int len);
static int fe_on_integer (void *ctx, long long val);
static int fe_on_array (void *ctx, int len, int is_null);

/* redis command table */
static unsigned int dict_case_hash (const void *key);
static int dict_case_compare (void *priv_data, const void *key1,
			      const void *key2);
static void populateCommandTable (void);
static dict *create_command_table (void);

/* conditional job */
static be_job_t *create_cond_be_job (be_job_type_t type);
static void destroy_cond (void *r);
static int fe_do_cond (fe_t * fe, be_job_type_t type, int timeout_millis);

/* arc job */
static be_job_t *create_noop_be_job (void);
static void destroy_noop_job (void *d);
static be_job_t *create_arc_be_job (void);
static void destroy_arc_job (void *d);
static int fe_do_arc_job (fe_t * fe, be_job_t * job, int *be_errno);
static int cmd_is_singlekey (struct redisCommand *cmd, int argc);
static void get_cmd_info (fe_t * fe, arc_job_t * aj, cmd_peek_t * peek,
			  int *found, int *is_write, int *key_crc);
static int fe_preproc_job (fe_t * fe, be_job_t * job);
static void destroy_arc_reply (arc_reply_t * reply);

/* backend job */
static int submit_be_job (fe_t * fe, be_job_t * r);

/* backend */
static be_t *create_be (int use_zk, int max_fd);
static void destroy_be_raw (be_t * be);

/* front end */
static fe_t *create_fe (void);
static void destroy_fe (fe_t * fe);

static int check_conf (const arc_conf_t * conf, int *log_prefix_size);
static arc_t *arc_new_common (int use_zk, const char *hosts,
			      const char *cluster_name,
			      const arc_conf_t * conf);

/* --------------- */
/* STATIC VARIABLE */
/* --------------- */

static pthread_mutex_t command_table_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static int command_table_initialized = 0;

static struct redisCommand redisCommandTable[] = {
  /* from nBase-ARC 1.2 */
  {"s3lget", NULL, 5, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lmget", NULL, -4, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lkeys", NULL, -3, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lvals", NULL, 4, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3ladd", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3laddat", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lmadd", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lrem", NULL, -4, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lmrem", NULL, -5, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lset", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lreplace", NULL, 8, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lcount", NULL, -3, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lexists", NULL, -5, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lexpire", NULL, -4, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lmexpire", NULL, -6, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3lttl", NULL, -5, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3sget", NULL, 5, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3smget", NULL, -4, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3skeys", NULL, -3, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3svals", NULL, 4, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3sadd", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3saddat", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3smadd", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3srem", NULL, -4, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3smrem", NULL, -5, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3sset", NULL, -7, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3sreplace", NULL, 8, "wm", 0, NULL, 2, 2, 1, 0, 0},
  {"s3scount", NULL, -3, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3sexists", NULL, -5, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3sexpire", NULL, -4, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3smexpire", NULL, -6, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3sttl", NULL, -5, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3keys", NULL, 3, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3count", NULL, 3, "r", 0, NULL, 2, 2, 1, 0, 0},
  {"s3expire", NULL, 4, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3rem", NULL, 3, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3mrem", NULL, -4, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"s3gc", NULL, 2, "w", 0, NULL, 0, 0, 0, 0, 0},
  {"checkpoint", NULL, 2, "ars", 0, NULL, 0, 0, 0, 0, 0},
  {"migstart", NULL, 2, "aw", 0, NULL, 0, 0, 0, 0, 0},
  {"migend", NULL, 1, "aw", 0, NULL, 0, 0, 0, 0, 0},
  {"migconf", NULL, -2, "aw", 0, NULL, 0, 0, 0, 0, 0},
  {"migpexpireat", NULL, 3, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"crc16", NULL, 3, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"bping", NULL, 1, "r", 0, NULL, 0, 0, 0, 0, 0},
  {"quit", NULL, 1, "r", 0, NULL, 0, 0, 0, 0, 0},
  /* from redis-3.0.0-beta8 */
  {"get", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"set", _D, -3, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"setnx", _D, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"setex", _D, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"psetex", _D, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"append", _D, 3, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"strlen", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"del", _D, -2, "w", 0, NULL, 1, -1, 1, 0, 0},
  {"exists", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"setbit", _D, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"getbit", _D, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"setrange", _D, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"getrange", _D, 4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"substr", _D, 4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"incr", _D, 2, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"decr", _D, 2, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"mget", _D, -2, "r", 0, NULL, 1, -1, 1, 0, 0},
  {"rpush", _D, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"lpush", _D, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"rpushx", _D, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"lpushx", _D, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"linsert", _D, 5, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"rpop", _D, 2, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"lpop", _D, 2, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"brpop", _D, -3, "ws", 0, NULL, 1, 1, 1, 0, 0},
  {"brpoplpush", _D, 4, "wms", 0, NULL, 1, 2, 1, 0, 0},
  {"blpop", _D, -3, "ws", 0, NULL, 1, -2, 1, 0, 0},
  {"llen", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"lindex", _D, 3, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"lset", _D, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"lrange", _D, 4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"ltrim", _D, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"lrem", _D, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"rpoplpush", _D, 3, "wm", 0, NULL, 1, 2, 1, 0, 0},
  {"sadd", _D, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"srem", _D, -3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"smove", _D, 4, "wF", 0, NULL, 1, 2, 1, 0, 0},
  {"sismember", _D, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"scard", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"spop", _D, 2, "wRsF", 0, NULL, 1, 1, 1, 0, 0},
  {"srandmember", _D, -2, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"sinter", _D, -2, "rS", 0, NULL, 1, -1, 1, 0, 0},
  {"sinterstore", _D, -3, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"sunion", _D, -2, "rS", 0, NULL, 1, -1, 1, 0, 0},
  {"sunionstore", _D, -3, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"sdiff", _D, -2, "rS", 0, NULL, 1, -1, 1, 0, 0},
  {"sdiffstore", _D, -3, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"smembers", _D, 2, "rS", 0, NULL, 1, 1, 1, 0, 0},
  {"sscan", _D, -3, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"zadd", _D, -4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"zincrby", _D, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrem", _D, -3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"zremrangebyscore", _D, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"zremrangebyrank", _D, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"zremrangebylex", _D, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"zunionstore", _D, -4, "wm", 0, _D, 0, 0, 0, 0, 0},
  {"zinterstore", _D, -4, "wm", 0, _D, 0, 0, 0, 0, 0},
  {"zrange", _D, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zrangebyscore", _D, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrangebyscore", _D, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zrangebylex", _D, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrangebylex", _D, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zcount", _D, 4, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zlexcount", _D, 4, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrange", _D, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zcard", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zscore", _D, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrank", _D, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrank", _D, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zscan", _D, -3, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"hset", _D, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hsetnx", _D, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hget", _D, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hmset", _D, -4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"hmget", _D, -3, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"hincrby", _D, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hincrbyfloat", _D, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hdel", _D, -3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"hlen", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hkeys", _D, 2, "rS", 0, NULL, 1, 1, 1, 0, 0},
  {"hvals", _D, 2, "rS", 0, NULL, 1, 1, 1, 0, 0},
  {"hgetall", _D, 2, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"hexists", _D, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hscan", _D, -3, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"incrby", _D, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"decrby", _D, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"incrbyfloat", _D, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"getset", _D, 3, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"mset", _D, -3, "wm", 0, NULL, 1, -1, 2, 0, 0},
  {"msetnx", _D, -3, "wm", 0, NULL, 1, -1, 2, 0, 0},
  {"randomkey", _D, 1, "rR", 0, NULL, 0, 0, 0, 0, 0},
  {"select", _D, 2, "rlF", 0, NULL, 0, 0, 0, 0, 0},
  {"move", _D, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"rename", _D, 3, "w", 0, NULL, 1, 2, 1, 0, 0},
  {"renamenx", _D, 3, "wF", 0, NULL, 1, 2, 1, 0, 0},
  {"expire", _D, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"expireat", _D, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"pexpire", _D, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"pexpireat", _D, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"keys", _D, 2, "rS", 0, NULL, 0, 0, 0, 0, 0},
  {"scan", _D, -2, "rR", 0, NULL, 0, 0, 0, 0, 0},
  {"dbsize", _D, 1, "rF", 0, NULL, 0, 0, 0, 0, 0},
  {"auth", _D, 2, "rsltF", 0, NULL, 0, 0, 0, 0, 0},
  {"ping", _D, -1, "rtF", 0, NULL, 0, 0, 0, 0, 0},
  {"echo", _D, 2, "rF", 0, NULL, 0, 0, 0, 0, 0},
  {"save", _D, 1, "ars", 0, NULL, 0, 0, 0, 0, 0},
  {"bgsave", _D, 1, "ar", 0, NULL, 0, 0, 0, 0, 0},
  {"bgrewriteaof", _D, 1, "ar", 0, NULL, 0, 0, 0, 0, 0},
  {"shutdown", _D, -1, "arlt", 0, NULL, 0, 0, 0, 0, 0},
  {"lastsave", _D, 1, "rRF", 0, NULL, 0, 0, 0, 0, 0},
  {"type", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"multi", _D, 1, "rsF", 0, NULL, 0, 0, 0, 0, 0},
  {"exec", _D, 1, "sM", 0, NULL, 0, 0, 0, 0, 0},
  {"discard", _D, 1, "rsF", 0, NULL, 0, 0, 0, 0, 0},
  {"sync", _D, 1, "ars", 0, NULL, 0, 0, 0, 0, 0},
  {"psync", _D, 3, "ars", 0, NULL, 0, 0, 0, 0, 0},
  {"replconf", _D, -1, "arslt", 0, NULL, 0, 0, 0, 0, 0},
  {"flushdb", _D, 1, "w", 0, NULL, 0, 0, 0, 0, 0},
  {"flushall", _D, 1, "w", 0, NULL, 0, 0, 0, 0, 0},
  {"sort", _D, -2, "wm", 0, _D, 1, 1, 1, 0, 0},
  {"info", _D, -1, "rlt", 0, NULL, 0, 0, 0, 0, 0},
  {"monitor", _D, 1, "ars", 0, NULL, 0, 0, 0, 0, 0},
  {"ttl", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"pttl", _D, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"persist", _D, 2, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"slaveof", _D, 3, "ast", 0, NULL, 0, 0, 0, 0, 0},
  {"role", _D, 1, "last", 0, NULL, 0, 0, 0, 0, 0},
  {"debug", _D, -2, "as", 0, NULL, 0, 0, 0, 0, 0},
  {"config", _D, -2, "art", 0, NULL, 0, 0, 0, 0, 0},
  {"subscribe", _D, -2, "rpslt", 0, NULL, 0, 0, 0, 0, 0},
  {"unsubscribe", _D, -1, "rpslt", 0, NULL, 0, 0, 0, 0, 0},
  {"psubscribe", _D, -2, "rpslt", 0, NULL, 0, 0, 0, 0, 0},
  {"punsubscribe", _D, -1, "rpslt", 0, NULL, 0, 0, 0, 0, 0},
  {"publish", _D, 3, "pltrF", 0, NULL, 0, 0, 0, 0, 0},
  {"pubsub", _D, -2, "pltrR", 0, NULL, 0, 0, 0, 0, 0},
  {"watch", _D, -2, "rsF", 0, NULL, 1, -1, 1, 0, 0},
  {"unwatch", _D, 1, "rsF", 0, NULL, 0, 0, 0, 0, 0},
  {"cluster", _D, -2, "ar", 0, NULL, 0, 0, 0, 0, 0},
  {"restore", _D, -4, "awm", 0, NULL, 1, 1, 1, 0, 0},
  {"restore-asking", _D, -4, "awmk", 0, NULL, 1, 1, 1, 0, 0},
  {"migrate", _D, -6, "aw", 0, NULL, 0, 0, 0, 0, 0},
  {"asking", _D, 1, "r", 0, NULL, 0, 0, 0, 0, 0},
  {"readonly", _D, 1, "rF", 0, NULL, 0, 0, 0, 0, 0},
  {"readwrite", _D, 1, "rF", 0, NULL, 0, 0, 0, 0, 0},
  {"dump", _D, 2, "ar", 0, NULL, 1, 1, 1, 0, 0},
  {"object", _D, 3, "r", 0, NULL, 2, 2, 2, 0, 0},
  {"client", _D, -2, "ar", 0, NULL, 0, 0, 0, 0, 0},
  {"eval", _D, -3, "s", 0, _D, 0, 0, 0, 0, 0},
  {"evalsha", _D, -3, "s", 0, _D, 0, 0, 0, 0, 0},
  {"slowlog", _D, -2, "r", 0, NULL, 0, 0, 0, 0, 0},
  {"script", _D, -2, "ras", 0, NULL, 0, 0, 0, 0, 0},
  {"time", _D, 1, "rRF", 0, NULL, 0, 0, 0, 0, 0},
  {"bitop", _D, -4, "wm", 0, NULL, 2, -1, 1, 0, 0},
  {"bitcount", _D, -2, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"bitpos", _D, -3, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"wait", _D, 3, "rs", 0, NULL, 0, 0, 0, 0, 0},
  {"command", _D, 0, "rlt", 0, NULL, 0, 0, 0, 0, 0},
  {"pfselftest", _D, 1, "r", 0, NULL, 0, 0, 0, 0, 0},
  {"pfadd", _D, -2, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"pfcount", _D, -2, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"pfmerge", _D, -2, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"pfdebug", _D, -3, "w", 0, NULL, 0, 0, 0, 0, 0},
  {"latency", _D, -2, "arslt", 0, NULL, 0, 0, 0, 0, 0}
};

static dictType commandTableDictType = {
  dict_case_hash,		// hash function 
  NULL,				// key dup            
  NULL,				// val dup
  dict_case_compare,		// key compare
  NULL,				// key destructor
  NULL				// val destructor
};

static redis_parse_cb_t feParseCb = {
  fe_on_string,
  fe_on_status,
  fe_on_error,
  fe_on_integer,
  fe_on_array
};

#ifdef FI_ENABLED
int fi_Eanbled = 0;
fi_t *fe_Fi = NULL;
#endif

/* ------------------------- */
/* LOCAL FUNCTION DEFINITION */
/* ------------------------- */
/* verbatim inclusion of formatting function */
#include "from_hiredis.c"
static int
redis_append_command (arc_job_t * aj, char *cmd, int len, cmd_peek_t * peek)
{
  sds newbuf;
  sds *target;

  target = &aj->rqst.obuf;

  newbuf = *target;
  if (newbuf == NULL)
    {
      FE_FI (newbuf, NULL, 0);
      newbuf = sdsnewlen (cmd, len);
      FI_END ();
      if (newbuf == NULL)
	{
	  FE_ERROR (ARC_ERR_NOMEM);
	  return -1;
	}
      *target = newbuf;
      goto done;
    }

  FE_FI (newbuf, NULL, 0);
  newbuf = sdscatlen (newbuf, cmd, len);
  FI_END ();
  if (newbuf == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }
  *target = newbuf;
done:
  dlisth_insert_before (&peek->head, &aj->rqst.peeks);
  aj->rqst.ncmd++;
  return 0;
}

static void
redis_cmdcb (int pos, int off, const char *val, size_t vallen, void *ctx)
{
  cmd_peek_t *peek;

  peek = (cmd_peek_t *) ctx;
  if (peek == NULL)
    {
      return;
    }

  peek->argc++;
  if (pos >= MAX_CMD_PEEK)
    {
      return;
    }
  assert (peek->num_peek == pos);
  peek->off[pos] = off;
  peek->len[pos] = vallen;
  peek->num_peek++;
  return;
}

/* parse comma separated host:port pair */
static int
parse_gw_spec (const char *gw, int *ngw, gw_t *** ra)
{
  char *hosts = NULL;
  int n_term;
  char *cp;
  gw_t **gw_ary = NULL;
  int gw_idx;
  char *term;
  char *end_ptr = NULL;
  int i;

  /* check the number of gateways and make gateway array in advance */
  n_term = 1;
  cp = (char *) gw;
  while ((cp = strchr (cp, ',')) != NULL)
    {
      n_term++;
      cp++;
    }

  FE_FI (gw_ary, NULL, 0);
  gw_ary = calloc (n_term, sizeof (gw_t *));
  FI_END ();
  if (gw_ary == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }
  gw_idx = 0;

  /* parse the gateway hosts */
  FE_FI (hosts, NULL, 0);
  hosts = strdup (gw);
  FI_END ();
  if (hosts == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      goto fail_return;
    }

#ifndef _WIN32
  term = strtok_r (hosts, ",", &end_ptr);
#else
  term = strtok_s (hosts, ",", &end_ptr);
#endif
  while (term != NULL)
    {
      char *colon_p;
      char *host_part;
      char *port_part;
      char *ep = NULL;
      long port;

      FE_FI (colon_p, NULL, 0);
      colon_p = strchr (term, ':');
      FI_END ();
      if (colon_p == NULL)
	{
	  FE_ERROR (ARC_ERR_BAD_ADDRFMT);
	  goto fail_return;
	}
      *colon_p = '\0';
      host_part = term;
      port_part = colon_p + 1;

      FE_FI (port, -1, 0);
      port = strtol (port_part, &ep, 10);
      FI_END ();
      if (*ep != '\0' || (port < 0 || port >= INT_MAX))
	{
	  FE_ERROR (ARC_ERR_BAD_ADDRFMT);
	  goto fail_return;
	}

      gw_ary[gw_idx] = create_gw (host_part, (int) port);
      if (gw_ary[gw_idx] == NULL)
	{
	  FE_ERROR (ARC_ERR_GENERIC);
	  goto fail_return;
	}

      gw_idx++;
#ifndef _WIN32
      term = strtok_r (NULL, ",", &end_ptr);
#else
      term = strtok_s (NULL, ",", &end_ptr);
#endif
    }

  free (hosts);
  *ngw = gw_idx;
  *ra = gw_ary;
  return 0;

fail_return:
  if (gw_ary != NULL)
    {
      for (i = 0; i < n_term; i++)
	{
	  if (gw_ary[i] != NULL)
	    {
	      destroy_gw (gw_ary[i]);
	    }
	}
      free (gw_ary);
    }
  if (hosts != NULL)
    {
      free (hosts);
    }

  FE_ERROR (ARC_ERR_GENERIC);
  return -1;
}

static gw_t *
create_gw (char *host, int port)
{
  gw_t *gw;

  FE_FI (gw, NULL, 0);
  gw = malloc (sizeof (gw_t));
  FI_END ();
  if (gw == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_gw (gw);
  gw->state = GW_STATE_USED;

  FE_FI (gw->host, NULL, 0);
  gw->host = strdup (host);
  FI_END ();
  if (gw->host == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  gw->port = port;

  return gw;
}

static void
destroy_gw (gw_t * gw)
{

  if (gw == NULL)
    {
      return;
    }

  assert (dlisth_is_empty (&gw->conns));

  if (gw->host != NULL)
    {
      free (gw->host);
    }

  free (gw);
}

static arc_reply_t *
fe_parse_make_reply (fe_parse_ctx_t * c)
{
  arc_reply_t *reply = NULL;
  int stk_too_deep;

  /* check stack overflow */
  FE_FI (stk_too_deep, 1, 0);
  stk_too_deep = (c->stk_idx >= FE_PARSE_MAX_DEPTH);
  FI_END ();
  if (stk_too_deep)
    {
      FE_ERROR (ARC_ERR_TOO_DEEP_RESP);
      return NULL;
    }

  FE_FI (reply, NULL, 0);
  reply = calloc (1, sizeof (arc_reply_t));
  FI_END ();
  if (reply == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  c->stk[c->stk_idx].reply = reply;
  return reply;
}

static int
fe_parse_cb_return (fe_parse_ctx_t * c, arc_reply_t * reply)
{
  stk_item_t *me;
  stk_item_t *parent = NULL;

  if (reply == NULL)
    {
      int i;
      /* reply == NULL means error. cleanup context and return */
      for (i = 0; i < FE_PARSE_MAX_DEPTH; i++)
	{
	  if (c->stk[i].reply != NULL)
	    {
	      destroy_arc_reply (c->stk[i].reply);
	      c->stk[i].reply = NULL;
	    }
	}
      FE_ERROR (ARC_ERR_GENERIC);
      return -1;
    }

  me = &c->stk[c->stk_idx];
  assert (me->reply == reply);
  if (c->stk_idx >= 1)
    {
      parent = &c->stk[c->stk_idx - 1];
    }

  if (reply->type == ARC_REPLY_ARRAY && reply->d.array.len > me->idx)
    {
      /* shift */
      c->stk_idx++;
      return PARSE_CB_OK_CONTINUE;
    }
  else
    {
      /* reduce */
      if (parent == NULL)
	{
	  return PARSE_CB_OK_DONE;
	}
      else
	{
	  assert (parent->reply->type == ARC_REPLY_ARRAY);
	  parent->reply->d.array.elem[parent->idx++] = reply;
	  me->reply = NULL;
	  me->idx = 0;
	  c->stk_idx--;
	  return fe_parse_cb_return (c, parent->reply);
	}
    }
}

#define MAKE_REPLY() do {                   \
  reply = fe_parse_make_reply(c);           \
  if(reply == NULL)                         \
    {                                       \
      FE_ERROR (ARC_ERR_NOMEM);          \
      return fe_parse_cb_return (c, NULL);  \
    }                                       \
} while (0)

static int
fe_on_string (void *ctx, char *s, int len, int is_null)
{
  fe_parse_ctx_t *c = (fe_parse_ctx_t *) ctx;	//MAKE_REPLY use this 
  arc_reply_t *reply;		//MAKE_REPLY use this

  MAKE_REPLY ();
  if (is_null)
    {
      reply->type = ARC_REPLY_NIL;
      return fe_parse_cb_return (c, reply);
    }
  else
    {
      reply->type = ARC_REPLY_STRING;
      reply->d.string.len = len;
      reply->d.string.str = s;
      *(s + len) = '\0';	// safe by \r\n
      return fe_parse_cb_return (c, reply);
    }
}

static int
fe_on_status (void *ctx, char *s, int len)
{
  fe_parse_ctx_t *c = (fe_parse_ctx_t *) ctx;
  arc_reply_t *reply;

  MAKE_REPLY ();
  reply->type = ARC_REPLY_STATUS;
  reply->d.status.len = len;
  reply->d.status.str = s;
  *(s + len) = '\0';		// safe by \r\n
  return fe_parse_cb_return (c, reply);
}

static int
fe_on_error (void *ctx, char *s, int len)
{
  fe_parse_ctx_t *c = (fe_parse_ctx_t *) ctx;
  arc_reply_t *reply;

  MAKE_REPLY ();
  reply->type = ARC_REPLY_ERROR;
  reply->d.error.len = len;
  reply->d.error.str = s;
  *(s + len) = '\0';		// safe by \r\n
  return fe_parse_cb_return (c, reply);
}

static int
fe_on_integer (void *ctx, long long val)
{
  fe_parse_ctx_t *c = (fe_parse_ctx_t *) ctx;
  arc_reply_t *reply;

  MAKE_REPLY ();
  reply->type = ARC_REPLY_INTEGER;
  reply->d.integer.val = val;
  return fe_parse_cb_return (c, reply);
}

static int
fe_on_array (void *ctx, int len, int is_null)
{
  fe_parse_ctx_t *c = (fe_parse_ctx_t *) ctx;
  arc_reply_t *reply;
  arc_reply_t **replies;

  if (is_null)
    {
      MAKE_REPLY ();
      reply->type = ARC_REPLY_NIL;
      return fe_parse_cb_return (c, reply);
    }

  MAKE_REPLY ();
  reply->type = ARC_REPLY_ARRAY;
  reply->d.array.len = len;
  if (len == 0)
    {
      return fe_parse_cb_return (c, reply);
    }

  FE_FI (replies, NULL, 0);
  replies = calloc (len, sizeof (arc_reply_t *));
  FI_END ();
  if (replies == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return fe_parse_cb_return (c, NULL);
    }
  reply->d.array.elem = replies;
  return fe_parse_cb_return (c, reply);
}

static unsigned int
dict_case_hash (const void *key)
{
  return dictGenCaseHashFunction ((unsigned char *) key,
				  strlen ((char *) key));
}

static int
dict_case_compare (void *priv_data, const void *key1, const void *key2)
{
  return strcasecmp (key1, key2) == 0;
}

static void
populateCommandTable (void)
{
  int j;
  int numcommands = sizeof (redisCommandTable) / sizeof (struct redisCommand);

  for (j = 0; j < numcommands; j++)
    {
      struct redisCommand *c = redisCommandTable + j;
      char *f = c->sflags;

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
	    case 'k':
	      c->flags |= REDIS_CMD_ASKING;
	      break;
	    case 'F':
	      c->flags |= REDIS_CMD_FAST;
	      break;
	    default:
	      break;
	    }
	  f++;
	}
    }
}

static dict *
create_command_table (void)
{
  dict *command_table;
  int i, ret;

  FE_FI (command_table, NULL, 0);
  command_table = dictCreate (&commandTableDictType, NULL);
  FI_END ();
  if (command_table == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }

  // initialize command table
  pthread_mutex_lock (&command_table_init_mutex);
  if (!command_table_initialized)
    {
      populateCommandTable ();
      command_table_initialized = 1;
    }
  pthread_mutex_unlock (&command_table_init_mutex);

  // make dict
  for (i = 0; i < sizeof (redisCommandTable) / sizeof (struct redisCommand);
       i++)
    {
      struct redisCommand *cmd = &redisCommandTable[i];
      FE_FI (ret, -99, 0);
      ret = dictAdd (command_table, cmd->name, cmd);
      FI_END ();
      if (ret != DICT_OK)
	{
	  dictRelease (command_table);
	  FE_ERROR (ARC_ERR_NOMEM);
	  return NULL;
	}
    }
  /* 
   * Note
   * Although command table is read-only, rehashing step can be taken at each dictFind call.
   * So we need below consolidation steps.
   */
  while (dictIsRehashing (command_table))
    {
      (void) dictFind (command_table, "dummy_x");
    }

  return command_table;
}

#ifndef _WIN32
static be_job_t *
create_cond_be_job (be_job_type_t type)
{
  cond_t *d;
  be_job_t *r;

  FE_FI (d, NULL, 0);
  d = malloc (sizeof (cond_t));
  FI_END ();
  if (d == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_cond (d);

  FE_FI (r, NULL, 0);
  r = create_be_job (type, destroy_cond, d, 0);
  FI_END ();
  if (r == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      destroy_cond (d);
      return NULL;
    }
  return r;
}
#else
static be_job_t *
create_cond_be_job (be_job_type_t type)
{
  HANDLE *d;
  be_job_t *r;

  FE_FI (d, NULL, 0);
  d = malloc (sizeof (HANDLE));
  FI_END ();
  if (d == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  *d = CreateEvent (NULL, FALSE, FALSE, NULL);

  FE_FI (r, NULL, 0);
  r = create_be_job (type, destroy_cond, d, 0);
  FI_END ();
  if (r == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      destroy_cond (d);
      return NULL;
    }
  return r;
}
#endif

#ifndef WIN32
static void
destroy_cond (void *r)
{
  if (r != NULL)
    {
      cond_t *job = (cond_t *) r;

      pthread_mutex_destroy (&job->mutex);
      pthread_cond_destroy (&job->cond);
      free (job);
    }
}
#else
static void
destroy_cond (void *r)
{
  if (r != NULL)
    {
      HANDLE *job = (HANDLE *) r;

      CloseHandle (*job);
      free (job);
    }
}
#endif

static int
fe_do_cond (fe_t * fe, be_job_type_t type, int timeout_millis)
{
  be_job_t *job = NULL;
  int ret = 0;
#ifndef _WIN32
  struct timespec ts;
  cond_t *ir = NULL;
#else
  HANDLE *ir = INVALID_HANDLE_VALUE;
#endif

  job = create_cond_be_job (type);
  if (job == NULL)
    {
      FE_ERROR (ARC_ERR_GENERIC);
      return -1;
    }
  job->to = currtime_millis () + timeout_millis;

#ifndef _WIN32
  get_timespec_from_millis (&ts, job->to);
  ir = (cond_t *) job->data;
  pthread_mutex_lock (&ir->mutex);

  if ((ret = submit_be_job (fe, job)) == -1)
    {
      pthread_mutex_unlock (&ir->mutex);
      goto fail_return;
    }

  ret = 0;
  while (ir->pred == 0 && ret == 0)
    {
      ret = pthread_cond_timedwait (&ir->cond, &ir->mutex, &ts);
    }

  pthread_mutex_unlock (&ir->mutex);
#else
  if ((ret = submit_be_job (fe, job)) == -1)
    {
      goto fail_return;
    }

  ir = (HANDLE *) job->data;
  if (WaitForSingleObject (*ir, timeout_millis) == WAIT_OBJECT_0)
    {
      ret = 0;
    }
  else
    {
      ret = -1;
    }
#endif

  FE_FI (ret, -1, 0);
  FI_END ();
  if (ret == 0)
    {
      decref_be_job (job);
      return 0;
    }
  else
    {
      ret = -1;
      FE_ERROR (ARC_ERR_TIMEOUT);
    }

fail_return:
  if (job != NULL)
    {
      decref_be_job (job);
    }
  return ret;
}

static be_job_t *
create_noop_be_job (void)
{
  noop_job_t *d;
  be_job_t *r;

  FE_FI (d, NULL, 0);
  d = malloc (sizeof (noop_job_t));
  FI_END ();
  if (d == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_noop_job (d);

  FE_FI (r, NULL, 0);
  r = create_be_job (BE_JOB_NOOP, destroy_noop_job, d, 0);
  FI_END ();
  if (r == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      destroy_noop_job (d);
      return NULL;
    }
  return r;
}

static void
destroy_noop_job (void *d)
{
  if (d != NULL)
    {
      free (d);
    }
}

static be_job_t *
create_arc_be_job (void)
{
  arc_job_t *d = NULL;
  be_job_t *noop = NULL;
  be_job_t *r = NULL;

  FE_FI (d, NULL, 0);
  d = malloc (sizeof (arc_job_t));
  FI_END ();
  if (d == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_arc_job (d);

  /* 
   * NOOP job is used when a ARC job is aborted by timeout but some data
   * must be retained to keep connection processing correctly.
   * Because it is very hard to handle memory allocation error at backend side,
   * it is allocated here (client side)
   */
  noop = create_noop_be_job ();
  if (noop == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      goto error_return;
    }
  d->noop = noop;
  noop = NULL;

  r = create_be_job (BE_JOB_ARC, destroy_arc_job, d, 0);
  if (r == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      goto error_return;
    }

  return r;

error_return:
  if (d != NULL)
    {
      destroy_arc_job (d);
    }
  if (noop != NULL)
    {
      dlisth_delete (&noop->head);
      decref_be_job (noop);
    }
  return NULL;
}

static void
destroy_arc_job (void *d)
{
  arc_job_t *job = (arc_job_t *) d;

  if (job == NULL)
    {
      return;
    }

  if (job->noop != NULL)
    {
      dlisth_delete (&job->noop->head);
      decref_be_job (job->noop);
    }

  if (job->rqst.obuf != NULL)
    {
      sdsfree (job->rqst.obuf);
    }

  while (!dlisth_is_empty (&job->rqst.peeks))
    {
      dlisth *h = job->rqst.peeks.next;
      cmd_peek_t *peek = (cmd_peek_t *) h;
      dlisth_delete (h);
      free (peek);
    }

  if (job->resp.ibuf != NULL)
    {
      sdsfree (job->resp.ibuf);
    }

  if (job->resp.replies != NULL)
    {
      int i;
      for (i = 0; i < job->resp.ncmd; i++)
	{
	  if (job->resp.replies[i] != NULL)
	    {
	      destroy_arc_reply (job->resp.replies[i]);
	    }
	}
      free (job->resp.replies);
    }

  pthread_mutex_destroy (&job->mutex);
  pthread_cond_destroy (&job->cond);
  free (job);
}

static int
fe_do_arc_job (fe_t * fe, be_job_t * job, int *be_errno)
{
  int ret;
  arc_job_t *aj;

  aj = (arc_job_t *) job->data;

  pthread_mutex_lock (&aj->mutex);

  /* set state in advance because submit_be_job transfers ownership asynchronously */
  aj->state = ARCJOB_STATE_SUBMITTED;
  ret = submit_be_job (fe, job);
  if (ret == -1)
    {
      FE_ERROR (ARC_ERR_GENERIC);
      aj->err = errno;
      /* restore state (it's mine) */
      aj->state = ARCJOB_STATE_NONE;
      pthread_mutex_unlock (&aj->mutex);
      goto fail_return;
    }

  ret = 0;
  while (aj->pred == 0 && ret == 0)
    {
      ret = pthread_cond_wait (&aj->cond, &aj->mutex);
    }
  pthread_mutex_unlock (&aj->mutex);

  if (ret == 0)
    {
      assert (aj->state == ARCJOB_STATE_COMPLETED);
      /* reference count is decreased at the time of destruction */
      if (aj->err != 0)
	{
	  int aj_resp_ncmd;
	  *be_errno = aj->err;

	  /* Note: no fault injection, set by backend */
	  aj_resp_ncmd = aj->resp.ncmd;
	  if (aj->resp.ncmd > 0)
	    {
	      FE_ERROR (ARC_ERR_PARTIAL);
	    }
	  else
	    {
	      FE_ERROR (ARC_ERR_BACKEND);
	    }
	  return -1;
	}
      else
	{
	  return 0;
	}
    }
  else
    {
      FE_ERROR (ARC_ERR_SYSCALL);
      return -1;
    }

fail_return:
  return -1;
}

static int
cmd_is_singlekey (struct redisCommand *cmd, int argc)
{
  int last;
  int numkeys = 0;
  int i;

  if (cmd->firstkey == 0)
    {
      //no key
      return 0;
    }
  else if (cmd->getkeys_proc)
    {
      //multiple key
      return 0;
    }

  last = cmd->lastkey;
  if (last < 0)
    {
      last = argc + last;
    }

  for (i = cmd->firstkey; i <= last; i += cmd->keystep)
    {
      numkeys++;
    }

  return (numkeys == 1);
}

static void
get_cmd_info (fe_t * fe, arc_job_t * aj, cmd_peek_t * peek, int *found,
	      int *is_write, int *key_crc)
{
  char *cmd, *cmd_ep, *pos;
  char saved;
  dictEntry *de;
  struct redisCommand *command;
  int keypos;

  *found = 0;
  *is_write = 0;
  *key_crc = -1;

  /* 
   * get command information 
   * tempoaryily overwrite command end position (it is safe by \r\n)
   */
  cmd = aj->rqst.obuf + peek->cmd_off + peek->off[0];
  cmd_ep = cmd + peek->len[0];
  saved = *cmd_ep;
  *cmd_ep = 0;
  de = dictFind (fe->commands, cmd);
  *cmd_ep = saved;
  if (de == NULL)
    {
      /* 
       * no such command in my dictionary. it is not a error
       * because my command table may be insufficient
       */
      return;
    }
  *found = 1;

  command = de->v.val;
  *is_write = ((command->flags & REDIS_CMD_WRITE) > 0);

  keypos = command->firstkey;
  if (!cmd_is_singlekey (command, peek->argc) || keypos >= peek->num_peek)
    {
      return;
    }

  pos = aj->rqst.obuf + peek->cmd_off + peek->off[keypos];
  *key_crc = crc16 (pos, peek->len[keypos], 0);
  return;
}

static int fe_preproc_job (fe_t * fe, be_job_t * job)
{
  arc_job_t *aj;
  dlisth *h;
  int count = 0;
  int pn = -1;

  assert (job->type == BE_JOB_ARC);
  aj = (arc_job_t *) job->data;
  assert (aj->rqst.ncmd >= 1);

  for (h = aj->rqst.peeks.next; h != &aj->rqst.peeks; h = h->next)
    {
      cmd_peek_t *peek = (cmd_peek_t *) h;
      int found = 0, is_write = 0, crc = -1;

      get_cmd_info (fe, aj, peek, &found, &is_write, &crc);
      if (!found || crc == -1)
	{
	  aj->rqst.diff_pn = 1;
	  return 0;
	}

      count++;
      if (count == 1)
	{
	  pn = crc % CLUSTER_SIZE;
	  aj->rqst.is_write = is_write;
	  aj->rqst.crc = crc;
	}
      else
	{
	  if (pn != (crc % CLUSTER_SIZE))
	    {
	      aj->rqst.diff_pn = 1;
	      return 0;
	    }
	}
    }
  return 0;
}

static void
destroy_arc_reply (arc_reply_t * reply)
{
  int i;

  if (reply == NULL)
    {
      return;
    }

  switch (reply->type)
    {
      /* Note: arc reply peeks data from resp.ibuf */
    case ARC_REPLY_ERROR:
    case ARC_REPLY_STATUS:
    case ARC_REPLY_INTEGER:
    case ARC_REPLY_STRING:
    case ARC_REPLY_NIL:
      break;
    case ARC_REPLY_ARRAY:
      if (reply->d.array.elem != NULL)
	{
	  for (i = 0; i < reply->d.array.len; i++)
	    {
	      destroy_arc_reply (reply->d.array.elem[i]);
	    }
	  free (reply->d.array.elem);
	}
      break;
    default:
      assert (0);
      break;
    }
  free (reply);
}

static int
submit_be_job (fe_t * fe, be_job_t * r)
{
  int ret;

  addref_be_job (r);		// +1 for pipe
  FE_FI (ret, -1, 0);
#ifndef _WIN32
  ret = write (fe->pipe_fd, &r, sizeof (be_job_t *));
#else
  if (r->type == BE_JOB_NOOP)
    assert (0);
  EnterCriticalSection (&fe->pipe_lock);
  if (!WriteFile (fe->pipe_fd, &r, sizeof (be_job_t *), &ret, NULL))
    {
      LeaveCriticalSection (&fe->pipe_lock);
      errno = WSAGetLastError ();
      ret = -1;
    }
  LeaveCriticalSection (&fe->pipe_lock);
#endif
  FI_END ();
  if (ret != sizeof (be_job_t *))
    {
      FE_ERROR (ARC_ERR_BE_DISCONNECTED);
      decref_be_job (r);
      return -1;
    }
  return 0;
}

static be_t *
create_be (int use_zk, int max_fd)
{
  be_t *be = NULL;

  FE_FI (be, NULL, 0);
  be = malloc (sizeof (be_t));
  FI_END ();
  if (be == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_be (be);

  be->use_zk = use_zk;

  /* make permanent zk_to_job */
  be->zk_to_job.type = BE_JOB_ZK_TO;
  addref_be_job (&be->zk_to_job);

  /* {FDLIMIT}: clients can have many file descriptos (e.g. web server) */
  FE_FI (be->el, NULL, 0);
  be->el = aeCreateEventLoop (max_fd);
  FI_END ();
  if (be->el == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      goto fail_return;
    }
  return be;

fail_return:
  if (be != NULL)
    {
      destroy_be_raw (be);
    }
  return NULL;
}

static void
destroy_be_raw (be_t * be)
{
  int i;

  if (be == NULL)
    {
      return;
    }

  assert (be->zh == NULL);
  assert (dlisth_is_empty (&be->retry_arc_jobs));
  assert (dlisth_is_empty (&be->garbage_conns));
  assert (be->app_log_fp == NULL);

  if (be->pipe_fd != INVALID_PIPE)
    {
      close_pipe (be->pipe_fd);
    }

  if (be->el != NULL)
    {
      aeDeleteEventLoop (be->el);
    }

  if (be->zk_host != NULL)
    {
      free (be->zk_host);
    }

  if (be->cluster_name != NULL)
    {
      free (be->cluster_name);
    }

  if (be->gw_ary != NULL)
    {
      for (i = 0; i < be->n_gw; i++)
	{
	  if (be->gw_ary[i] != NULL)
	    {
	      destroy_gw (be->gw_ary[i]);
	    }
	}
      free (be->gw_ary);
    }

  if (be->gw_hosts != NULL)
    {
      free (be->gw_hosts);
    }

  free (be);
}

static fe_t *
create_fe (void)
{
  fe_t *fe = NULL;

  FE_FI (fe, NULL, 0);
  fe = malloc (sizeof (fe_t));
  FI_END ();
  if (fe == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_fe (fe);

  FE_FI (fe->commands, NULL, 0);
  fe->commands = create_command_table ();
  FI_END ();
  if (fe->commands == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      free (fe);
      return NULL;
    }
  return fe;
}

static void
destroy_fe (fe_t * fe)
{
  if (fe == NULL)
    {
      return;
    }

// If it runs in a windows than, a fe->pipe_fd is closed by be_thread.
#ifndef _WIN32
  if (fe->pipe_fd != INVALID_PIPE)
    {
      close_pipe (fe->pipe_fd);
    }
#else
  DeleteCriticalSection (&fe->pipe_lock);
#endif

  if (fe->commands != NULL)
    {
      dictRelease (fe->commands);
    }

  free (fe);
}

static int
check_conf (const arc_conf_t * conf, int *log_prefix_size)
{
  if (conf == NULL)
    {
      return 0;
    }

  if (conf->num_conn_per_gw < CONF_NUM_CONN_PER_GW_MIN ||
      conf->init_timeout_millis < CONF_INIT_TIMEOUT_MILLIS_MIN ||
      (conf->log_level < ARC_LOG_LEVEL_NOLOG
       || conf->log_level > ARC_LOG_LEVEL_DEBUG)
      || conf->max_fd < CONF_MAX_FD_MIN
      || conf->conn_reconnect_millis < CONF_CONN_RECONNECT_MILLIS_MIN
      || conf->zk_reconnect_millis < CONF_ZK_RECONNECT_MILLIS_MIN
      || conf->zk_session_timeout_millis < CONF_ZK_SESSION_TIMEOUT_MILLIS_MIN)
    {
      FE_ERROR (ARC_ERR_BAD_CONF);
      return -1;
    }

  *log_prefix_size = 0;
  if (conf->log_level > ARC_LOG_LEVEL_NOLOG)
    {
      if (conf->log_file_prefix != NULL)
	{
	  int prefix_size;

	  FE_FI (prefix_size, PATH_MAX + 1, 0);
	  prefix_size = strlen (conf->log_file_prefix);
	  FI_END ();
	  if (prefix_size + APP_LOG_MAX_SUFFIX_LEN > PATH_MAX)
	    {
	      FE_ERROR (ARC_ERR_BAD_CONF);
	      return -1;
	    }
	  *log_prefix_size = prefix_size;
	}
    }
  return 0;
}

static arc_t *
arc_new_common (int use_zk, const char *hosts, const char *cluster_name,
		const arc_conf_t * conf)
{
  fe_t *fe = NULL;
  be_t *be = NULL;
  pipe_t pipe_fd[2] = { INVALID_PIPE, INVALID_PIPE };
  int ret;
  arc_conf_t my_conf;
  int log_prefix_size = 0;

  if (conf != NULL)
    {
      FE_FI (ret, -1, 0);
      ret = check_conf (conf, &log_prefix_size);
      FI_END ();
      if (ret == -1)
	{
	  FE_ERROR (ARC_ERR_BAD_CONF);
	  return NULL;
	}
      my_conf = *conf;
    }
  else
    {
      init_arc_conf (&my_conf);
    }

  /* create pipe for communication between works and backend thread */
  FE_FI (ret, -1, 0);
#ifndef WIN32
  ret = pipe (pipe_fd);
#else
#define PIPE_BUFSIZE 1024
  DWORD pid;
  HANDLE pipe;
  char pipe_name[64];
  static long uid_seed = 0;

  pid = GetCurrentProcessId ();
  long uid = InterlockedIncrement (&uid_seed);
  sprintf_s (pipe_name, 64, "\\\\.\\pipe\\arcci_pipe_%d_%d", pid, uid);
  pipe = CreateNamedPipe (pipe_name,
			  PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
			  PIPE_TYPE_MESSAGE |
			  PIPE_READMODE_MESSAGE,
			  PIPE_UNLIMITED_INSTANCES,
			  PIPE_BUFSIZE, PIPE_BUFSIZE, 0, NULL);
  if (pipe == INVALID_HANDLE_VALUE)
    {
      ret = -1;
    }
  else
    {
      DWORD dwMode = PIPE_READMODE_MESSAGE | PIPE_WAIT;
      if (!SetNamedPipeHandleState (pipe, &dwMode, NULL, NULL))
	{
	  FE_ERROR (ARC_ERR_SYSCALL);
	  goto fail_return;
	}
      pipe_fd[0] = pipe;
    }
#endif
  FI_END ();
  if (ret == -1)
    {
      FE_ERROR (ARC_ERR_SYSCALL);
      goto fail_return;
    }

  /* make read end pipe nonblock */
  FE_FI (ret, -1, 0);
#ifndef _WIN32
  ret = fcntl (pipe_fd[0], F_SETFL, O_NONBLOCK);
#endif
  FI_END ();
  if (ret == -1)
    {
      FE_ERROR (ARC_ERR_SYSCALL);
      goto fail_return;
    }

  /* initialize front end */
  FE_FI (fe, NULL, 0);
  fe = create_fe ();
  FI_END ();
  if (fe == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
#ifndef _WIN32
  fe->pipe_fd = pipe_fd[1];
#endif
  pipe_fd[1] = INVALID_PIPE;

  /* intialize back end and copy configuration */
  FE_FI (be, NULL, 0);
  be = create_be (use_zk, my_conf.max_fd);
  FI_END ();
  if (be == NULL)
    {
      goto fail_return;
    }
  be->pipe_fd = pipe_fd[0];
  pipe_fd[0] = INVALID_PIPE;
  if (log_prefix_size > 0)
    {
      strcpy (be->app_log_buf, my_conf.log_file_prefix);
      be->app_log_prefix_size = log_prefix_size;
    }
  my_conf.log_file_prefix = NULL;
  be->conf = my_conf;

#ifdef WIN32
  if (aeWinPipeAttach (be->pipe_fd) != 0)
    {
      goto fail_return;
    }

  pipe_fd[1] = CreateFile (pipe_name,
			   GENERIC_WRITE,
			   0,
			   NULL, OPEN_EXISTING, FILE_FLAG_OVERLAPPED, NULL);
  if (pipe_fd[1] == INVALID_HANDLE_VALUE)
    {
      FE_ERROR (ARC_ERR_SYSCALL);
      goto fail_return;
    }

  // Pipe connected; change to message-read mode. 
  DWORD dwMode = PIPE_READMODE_MESSAGE;
  if (!SetNamedPipeHandleState (pipe_fd[1], &dwMode, NULL, NULL))
    {
      FE_ERROR (ARC_ERR_SYSCALL);
      goto fail_return;
    }
  fe->pipe_fd = pipe_fd[1];
  be->pipe_fd_fe = fe->pipe_fd;
  pipe_fd[1] = INVALID_PIPE;
#endif

  /* do gateway lookup specific jobs */
  if (use_zk)
    {
      FE_FI (be->zk_host, NULL, 0);
      be->zk_host = strdup (hosts);
      FI_END ();
      if (be->zk_host == NULL)
	{
	  FE_ERROR (ARC_ERR_NOMEM);
	  goto fail_return;
	}

      FE_FI (be->cluster_name, NULL, 0);
      be->cluster_name = strdup (cluster_name);
      FI_END ();
      if (be->cluster_name == NULL)
	{
	  FE_ERROR (ARC_ERR_NOMEM);
	  goto fail_return;
	}
    }
  else
    {
      ret = parse_gw_spec (hosts, &be->n_gw, &be->gw_ary);
      if (ret == -1)
	{
	  FE_ERROR (ARC_ERR_BAD_ADDRFMT);
	  goto fail_return;
	}
      FE_FI (be->gw_hosts, NULL, 0);
      be->gw_hosts = strdup (hosts);
      FI_END ();
      if (be->gw_hosts == NULL)
	{
	  FE_ERROR (ARC_ERR_NOMEM);
	  goto fail_return;
	}
    }

#ifdef _WIN32
  /* these data are stored in thread-local-storage */
  be->iocph = aeWinGetIOCP ();
  be->iocp_state = aeWinGetIOCPState ();
#endif

  /* launch backend thread */
  FE_FI (ret, -1, 0);
  ret = pthread_create (&fe->be_thread, NULL, be_thread, be);
  FI_END ();
  if (ret != 0)
    {
      FE_ERROR (ARC_ERR_SYSCALL);
      goto fail_return;
    }
  fe->be = be;
  be = NULL;			// be instance is owned by the be_thread

  /* make init request */
  ret = fe_do_cond (fe, BE_JOB_INIT, my_conf.init_timeout_millis);
  if (ret == 0)
    {
      return fe;
    }

fail_return:
  if (pipe_fd[0] != INVALID_PIPE)
    {
      close_pipe (pipe_fd[0]);
    }

  if (pipe_fd[1] != INVALID_PIPE)
    {
      close_pipe (pipe_fd[1]);
    }

  if (fe != NULL)
    {
      if (fe->be != NULL)
	{
	  /* this means that backend thread running */
	  fe->be->abrupt_exit = 1;
	  pthread_join (fe->be_thread, NULL);
	}
      destroy_fe (fe);
    }

  if (be != NULL)
    {
      destroy_be_raw (be);
    }
  return NULL;
}

/* ------------------ */
/* EXPORTED FUNCTIONS */
/* ------------------ */
void
arc_init_conf (arc_conf_t * conf)
{
  if (conf == NULL)
    {
      return;
    }
  init_arc_conf (conf);
}

arc_t *
arc_new_zk (const char *hosts, const char *cluster_name,
	    const arc_conf_t * conf)
{
  int bad_argument;
  errno = 0;

  FE_FI (bad_argument, 1, 0);
  bad_argument = (hosts == NULL || cluster_name == NULL);
  FI_END ();
  if (bad_argument)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return NULL;
    }

  return arc_new_common (1, hosts, cluster_name, conf);
}

arc_t *
arc_new_gw (const char *gw_spec, const arc_conf_t * conf)
{
  int bad_argument;
  errno = 0;

  FE_FI (bad_argument, 1, 0);
  bad_argument = (gw_spec == NULL || gw_spec[0] == '\0');
  FI_END ();
  if (bad_argument)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return NULL;
    }

  return arc_new_common (0, gw_spec, NULL, conf);
}

void
arc_destroy (arc_t * arc)
{
  fe_t *fe;

  if (arc == NULL)
    {
      return;
    }
  errno = 0;
  fe = (fe_t *) arc;

  fe->be->abrupt_exit = 1;
  pthread_join (fe->be_thread, NULL);
  destroy_fe (fe);
  return;
}

arc_request_t *
arc_create_request (void)
{
  be_job_t *job;

  errno = 0;
  FE_FI (job, NULL, 0);
  job = create_arc_be_job ();
  FI_END ();
  if (job == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }

  return job;
}

void
arc_free_request (arc_request_t * rqst)
{
  be_job_t *job;

  errno = 0;
  job = (be_job_t *) rqst;

  if (rqst == NULL)
    {
      return;
    }

  if (job->type != BE_JOB_ARC)
    {
      return;
    }

  decref_be_job (job);
}

int
arc_append_command (arc_request_t * rqst, const char *format, ...)
{
  va_list ap;
  int ret;
  int bad_argument;

  errno = 0;
  FE_FI (bad_argument, 1, 0);
  bad_argument = (rqst == NULL || format == NULL);
  FI_END ();
  if (bad_argument)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }

  va_start (ap, format);
  ret = arc_append_commandv (rqst, format, ap);
  va_end (ap);
  return ret;
}

int
arc_append_commandv (arc_request_t * rqst, const char *format, va_list ap)
{
  char *cmd = NULL;
  cmd_peek_t *peek;
  int len, ret;
  be_job_t *job;
  arc_job_t *arc_job;
  int bad_argument, job_type, arc_job_state;

  errno = 0;
  FE_FI (bad_argument, 0, 0);
  bad_argument = (rqst == NULL || format == NULL);
  FI_END ();
  if (bad_argument)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }

  job = (be_job_t *) rqst;
  FE_FI (job_type, BE_JOB_ARC + 1, 0);
  job_type = job->type;
  FI_END ();
  if (job_type != BE_JOB_ARC)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }

  arc_job = (arc_job_t *) job->data;
  FE_FI (arc_job_state, ARCJOB_STATE_NONE + 1, 0);
  arc_job_state = arc_job->state;
  FI_END ();
  if (arc_job_state != ARCJOB_STATE_NONE)
    {
      FE_ERROR (ARC_ERR_BAD_RQST_STATE);
      return -1;
    }

  FE_FI (peek, NULL, 0);
  peek = malloc (sizeof (cmd_peek_t));
  FI_END ();
  if (peek == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }
  init_cmd_peek (peek);
  peek->cmd_off = arc_job->rqst.obuf ? sdslen (arc_job->rqst.obuf) : 0;

  FE_FI (len, -1, 0);
  len = redisvFormatCommand (redis_cmdcb, peek, &cmd, format, ap);
  FI_END ();
  if (len == -1)
    {
      free (peek);
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }

  ret = redis_append_command (arc_job, cmd, len, peek);
  free (cmd);
  if (ret == -1)
    {
      free (peek);
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }
  return 0;
}

int
arc_append_commandcv (arc_request_t * rqst, int argc,
		      const char **argv, const size_t * argvlen)
{
  char *cmd = NULL;
  cmd_peek_t *peek;
  int len, ret;
  be_job_t *job;
  arc_job_t *arc_job;
  int bad_argument;
  int job_type, arc_job_state;

  errno = 0;
  FE_FI (bad_argument, 1, 0);
  bad_argument = (rqst == NULL || argc < 1 || argv == NULL
		  || argvlen == NULL);
  FI_END ();
  if (bad_argument)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }

  job = (be_job_t *) rqst;
  FE_FI (job_type, BE_JOB_NOOP, 0);
  job_type = job->type;
  FI_END ();
  if (job_type != BE_JOB_ARC)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }

  arc_job = (arc_job_t *) job->data;
  FE_FI (arc_job_state, ARCJOB_STATE_NONE + 1, 0);
  arc_job_state = arc_job->state;
  FI_END ();
  if (arc_job_state != ARCJOB_STATE_NONE)
    {
      FE_ERROR (ARC_ERR_BAD_RQST_STATE);
      return -1;
    }

  FE_FI (peek, NULL, 0);
  peek = malloc (sizeof (cmd_peek_t));
  FI_END ();
  if (peek == NULL)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }
  init_cmd_peek (peek);
  peek->cmd_off = arc_job->rqst.obuf ? sdslen (arc_job->rqst.obuf) : 0;

  FE_FI (len, -1, 0);
  len = redisFormatCommandArgv (redis_cmdcb, peek, &cmd, argc, argv, argvlen);
  FI_END ();
  if (len == -1)
    {
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }

  ret = redis_append_command (arc_job, cmd, len, peek);
  free (cmd);
  if (ret == -1)
    {
      free (peek);
      FE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }
  return 0;
}

int
arc_do_request (arc_t * arc, arc_request_t * rqst, int timeout_millis,
		int *be_errno)
{
  fe_t *fe;
  be_job_t *job;
  arc_job_t *aj;
  int bad_argument;
  int aj_state, aj_rqst_ncmd;
  int ret;

  errno = 0;
  fe = (fe_t *) arc;
  job = (be_job_t *) rqst;

  FE_FI (bad_argument, 1, 0);
  bad_argument = (fe == NULL || job == NULL || timeout_millis < 0
		  || be_errno == NULL);
  FI_END ();
  if (bad_argument)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }
  *be_errno = 0;

  aj = (arc_job_t *) job->data;
  FE_FI (aj_state, ARCJOB_STATE_NONE + 1, 0);
  aj_state = aj->state;
  FI_END ();
  if (aj_state != ARCJOB_STATE_NONE)
    {
      FE_ERROR (ARC_ERR_BAD_RQST_STATE);
      return -1;
    }

  FE_FI (aj_rqst_ncmd, 0, 0);
  aj_rqst_ncmd = aj->rqst.ncmd;
  FI_END ();
  if (aj_rqst_ncmd == 0)
    {
      FE_ERROR (ARC_ERR_BAD_RQST_STATE);
      return -1;
    }

  if (timeout_millis == 0)
    {
      timeout_millis = INT_MAX;
    }
  job->to = currtime_millis () + timeout_millis;

  ret = fe_preproc_job (fe, job);
  if (ret < 0)
    {
      return -1;
    }
  return fe_do_arc_job (fe, job, be_errno);
}

int
arc_get_reply (arc_request_t * rqst, arc_reply_t ** reply, int *be_errno)
{
  be_job_t *job;
  arc_job_t *aj;
  int bad_argument, job_type, aj_err;

  errno = 0;

  FE_FI (bad_argument, -1, 0);
  bad_argument = (rqst == NULL || reply == NULL || be_errno == NULL);
  FI_END ();
  if (bad_argument)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }
  *reply = NULL;
  *be_errno = 0;

  job = (be_job_t *) rqst;

  FE_FI (job_type, BE_JOB_ARC + 1, 0);
  job_type = job->type;
  FI_END ();
  if (job_type != BE_JOB_ARC)
    {
      FE_ERROR (ARC_ERR_ARGUMENT);
      return -1;
    }

  aj = (arc_job_t *) job->data;

  /* make reply vector */
  if (aj->resp.replies == NULL && aj->resp.ncmd > 0)
    {
      int ret, i, rsz, off;
      arc_reply_t **replies = NULL;

      FE_FI (replies, NULL, 0);
      replies = calloc (aj->resp.ncmd, sizeof (arc_reply_t *));
      FI_END ();
      if (replies == NULL)
	{
	  FE_ERROR (ARC_ERR_NOMEM);
	  return -1;
	}

      off = 0;
      for (i = 0; i < aj->resp.ncmd; i++)
	{
	  fe_parse_ctx_t ctx;
	  /*
	   * Note: callback functions PEEKs and modifies data in resp.ibuf.
	   * this means parser call is not idempotent
	   */
	  init_fe_parse_ctx (&ctx);
	  /* Note: no fault injection (not idempotent) */
	  ret =
	    parse_redis_response (aj->resp.ibuf + off, aj->resp.off - off,
				  &rsz, &feParseCb, &ctx);
	  if (ret == -1 || ctx.stk_idx != 0)
	    {
	      int j;

	      FE_ERROR (ARC_ERR_GENERIC);
	      /* release temporary results */
	      for (j = 0; j < i; j++)
		{
		  destroy_arc_reply (replies[j]);
		}
	      free (replies);
	      /* readjust resp fields */
	      aj->resp.ncmd = 0;
	      aj->resp.off = 0;
	      aj->err = errno;
	      /* errno is set above */
	      return -1;
	    }
	  off += rsz;
	  replies[i] = ctx.stk[0].reply;
	}
      aj->resp.replies = replies;
    }

  if (aj->resp.reply_off < aj->resp.ncmd)
    {
      *reply = aj->resp.replies[aj->resp.reply_off];
      aj->resp.replies[aj->resp.reply_off] = NULL;
      aj->resp.reply_off++;
      return 0;
    }

  /* partial error or errror */
  FE_FI (aj_err, ARC_ERR_NOMEM, 0);
  aj_err = aj->err;
  FI_END ();
  if (aj_err)
    {
      *be_errno = aj_err;
      FE_ERROR (ARC_ERR_BACKEND);
      return -1;
    }
  return 0;
}

void
arc_free_reply (arc_reply_t * reply)
{
  if (reply != NULL)
    {
      destroy_arc_reply (reply);
    }
}


#ifdef _WIN32
  BOOL __stdcall DllMain (void *module, unsigned long reason_for_call,
			  void *reserved) 
{
  if (reason_for_call == DLL_PROCESS_ATTACH)
    {
      
#ifdef CHECK_LEAK
	_CrtSetDbgFlag (_CRTDBG_ALLOC_MEM_DF | _CRTDBG_CHECK_ALWAYS_DF |
			_CRTDBG_LEAK_CHECK_DF | _CRTDBG_DELAY_FREE_MEM_DF);
      
#endif
	if (aeInitWinSock () != 0)
	{
	  FE_ERROR (ARC_ERR_SYSCALL);
	  return FALSE;
	}
    }
  return TRUE;
}


#endif
#ifdef _WIN32
#pragma warning(pop)
#endif

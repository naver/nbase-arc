#ifndef _GW_CONFIG_H_
#define _GW_CONFIG_H_

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#include "gw_util.h"
#include "gw_log.h"
#include "queue.h"
#include "zmalloc.h"
#include "sds.h"
#include "dict.h"
#include "util.h"
#include "anet.h"

#define GW_NOTUSED(V) ((void) V)

#define OK 0
#define ERR -1

#define GW_DEFAULT_EVENTLOOP_SIZE  16000
#define GW_DEFAULT_HZ 10
#define GW_DEFAULT_SERVERPORT 6000
#define GW_DEFAULT_WORKERS 4
#define GW_DEFAULT_MAXCLIENTS 8192
#define GW_DEFAULT_BACKLOG 8192
#define GW_MIN_TCP_BACKLOG 512
#define GW_DEFAULT_TCP_KEEPALIVE 60
#define GW_DEFAULT_TIMEOUT_MS 5000

#define SLOT_STATE_NORMAL 0
#define SLOT_STATE_BLOCKING 1
#define SLOT_STATE_BLOCKED 2

typedef struct global_conf_t global_conf_t;
typedef struct redis_conf redis_conf;
typedef struct partition_conf partition_conf;
typedef struct cluster_conf cluster_conf;

typedef struct block_range block_range;
typedef struct block_range_tqh block_range_tqh;
TAILQ_HEAD (block_range_tqh, block_range);

// Master can modify global_conf, but workers are only allowed to read.
extern global_conf_t global;

struct global_conf_t
{
  int port;
  int admin_port;

  int nworker;
  int tcp_backlog;
  int maxclients;
  int verbosity;
  int timeout_ms;

  const char *cluster_name;
  sds nbase_arc_home;

  cluster_conf *conf;

  unsigned daemonize:1;
  unsigned shutdown_asap:1;
  unsigned prefer_domain_socket:1;
};

struct redis_conf
{
  sds redis_id;
  sds pg_id;
  sds addr;
  int port;
  sds domain_socket_path;
  unsigned is_local:1;
  unsigned deleting:1;
};

struct partition_conf
{
  sds pg_id;
  dict *redis;
};

struct cluster_conf
{
  sds cluster_name;
  sds nbase_arc_home;
  block_range_tqh block_range_q;
  char **local_ip_addrs;

  int nslot;
  partition_conf **slots;
  int *slot_state;
  dict *partition;
  dict *redis;

  unsigned prefer_domain_socket:1;
};

struct block_range
{
  TAILQ_ENTRY (block_range) block_range_tqe;
  int from;
  int to;
};

cluster_conf *create_conf_from_cm_data (const char *cluster_name,
					const char *nbase_arc_home,
					int prefer_domain_socket, sds data);
cluster_conf *create_conf_from_confmaster (const char *cm_addr, int cm_port,
					   const char *cluster_name,
					   const char *nbase_arc_home,
					   int prefer_domain_socket);
cluster_conf *duplicate_conf (cluster_conf * conf);
void destroy_conf (cluster_conf * conf);
sds default_domain_path (sds nbase_arc_home, int port);
sds get_cluster_info_sds (cluster_conf * conf);
sds get_pg_map_rle (cluster_conf * conf);
sds get_redis_id_list (cluster_conf * conf);
sds get_delay_filter (cluster_conf * conf);
#endif

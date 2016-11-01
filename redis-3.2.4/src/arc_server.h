#ifndef _ARC_H_
#define _ARC_H_

#include "smr_be.h"

/* ---------------------- */
/* Redis header extension */
/* ---------------------- */
/* server.h */
#define CMD_NOCLUSTER               16384
//      OBJ_HASH                    4
#define OBJ_SSS                     5
//      OBJ_ENCODING_QUICKLIST      9
#define OBJ_ENCODING_SSS            10
//      CLIENT_LUA_DEBUG_SYNC       (1<<26)
#define CLIENT_LOCAL_CONN           (1<<27)
/* rdb.h */
//      RDB_TYPE_HASH               4
#define RDB_TYPE_SSS                5

/* ----------- */
/* Definitions */
/* ----------- */
#define ARC_KS_SIZE                      8192	/* keyspace size */
#define ARC_CLUSTER_DB                   0
#define ARC_OOM_DURATION_MS              (1000*11)
#define ARC_MAX_RDB_BACKUPS              24
#define ARC_RDB_MAX_HEADERS              16
#define ARC_OBJ_BIO_DELETE_MIN_ELEMS     100
#define ARC_GETDUMP_DEFAULT_NET_LIMIT_MB 30
#define ARC_SMR_CMD_CATCHUP_CHECK        'C'
#define ARC_SMR_CMD_DELIVER_OOM          'D'
#define ARC_SMR_CLIENT_CLOSING           (1<<0)
#define ARC_SMR_REPL_WAIT_CHECKPOINT_END 10

/* SMR initialize flags */
#define ARC_SMR_INIT_NONE                0
#define ARC_SMR_INIT_RCKPT               1
#define ARC_SMR_INIT_CATCHUP_PHASE1      2
#define ARC_SMR_INIT_CATCHUP_PHASE2      4
#define ARC_SMR_INIT_DONE                8


/* -------- */
/* Struture */
/* -------- */
typedef struct dlisth_s dlisth;
struct dlisth_s
{
  dlisth *next;
  dlisth *prev;
};
#define dlisth_init(h)                 \
do {                                   \
  dlisth *__h = (h);                   \
  (__h)->next = (__h)->prev = (__h);   \
} while (0)

#define dlisth_is_empty(h) ((h)->next == (h) && (h)->prev == (h))

#define dlisth_delete(h_)              \
do {                                   \
  dlisth *__h = (h_);                  \
  (__h)->next->prev = (__h)->prev;     \
  (__h)->prev->next = (__h)->next;     \
  (__h)->next = (__h)->prev = (__h);   \
} while(0)

#define dlisth_insert_before(ih, bh)   \
do {                                   \
  dlisth *__ih = (ih);                 \
  dlisth *__bh = (bh);                 \
  (__ih)->next = (__bh);               \
  (__ih)->prev = (__bh)->prev;         \
  (__bh)->prev->next = (__ih);         \
  (__bh)->prev = (__ih);               \
} while (0)

#define dlisth_insert_after(ih, bh)    \
do {                                   \
  dlisth *__ih = (ih);                 \
  dlisth *__bh = (bh);                 \
  (__ih)->prev = (__bh);               \
  (__ih)->next = (__bh)->next;         \
  (__bh)->next->prev = (__ih);         \
  (__bh)->next = (__ih);               \
} while (0)

struct cronsaveParam
{
  int minute;
  int hour;
};

typedef struct callbackInfo
{
  dlisth global_head;		/* global list of callback infos */
  dlisth client_head;		/* per-client list of callback infos */
  struct client *client;
  int argc;
  robj **argv;			/* The parsed argv is reserved for resuing after smr callback is invoked. */
  int hash;			/* reserve hash for checking consistency */
} callbackInfo;

struct arcServer
{
  /* Stats */
  long long stat_numcommands_replied;	/* Number of processed commands with reply */
  long long stat_numcommands_lcon;	/* Number of processed commands from local connection */
  long long stat_bgdel_keys;	/* Number of keys deleted by background thread */
  long long replied_ops_sec_last_sample_ops;	/* numcommands in last sample */
  long long replied_ops_sec_samples[STATS_METRIC_SAMPLES];
  long long lcon_ops_sec_last_sample_ops;	/* numcommands in last sample */
  long long lcon_ops_sec_samples[STATS_METRIC_SAMPLES];

  /* Config */
  long long object_bio_delete_min_elems;	/* minimum elements count for deleting object with bio thread */
  int num_rdb_backups;		/* Total number of rdb file backups */

  /* Server mode */
  int cluster_util_mode;	/* True if this instance is a Cluster-util. */
  int dump_util_mode;		/* True if this instance is a Dump-util. */
  int cluster_mode;		/* cluster mode */

  /* Checkpoint */
  char *checkpoint_filename;
  client *checkpoint_client;
  long long checkpoint_seqnum;
  sds checkpoint_slots;

  /* Cron save */
  struct cronsaveParam *cronsave_params;	/* Cron style Save points array for RDB */
  int cronsave_paramslen;	/* Number of cron saving points */

  /* Seq save */
  long long seqsave_gap;	/* Max sequence gap before bgsave */

  /* Migration state */
  sds migrate_slot;		/* bitarray represents hashslot of migration in progress. */
  sds migclear_slot;		/* bitarray represents hashslot of migclear in progress. */

  /* SSS gc structure */
  int gc_idx;
  int gc_num_line;
  int gc_interval;		/* Background gc interval in milliseconds */
  dlisth *gc_line;		/* fixed rate gc object headers */
  dlisth gc_eager;		/* s3 object header for eager mode gc */
  void *gc_obc;			/* s3 object cursor for incremental purge */
  int gc_eager_loops;		/* event loop count in eager mode */

  /* State machine replicator (SMR) */
  smrConnector *smr_conn;
  int smr_lport;
  int smr_fd;
  long long smr_seqnum;
  long long last_bgsave_seqnum;
  long long seqnum_before_bgsave;
  int smr_init_flags;
  long long last_catchup_check_mstime;
  long long smr_ts;		/* globally identical mstime */
  client *smrlog_client;	/* The "fake client" to executing smrlog without replying */
  int smr_seqnum_reset;

  /* Remote checkpoint */
  int need_rckpt;
  char *ckpt_host;
  int ckpt_port;
  int is_ready;

  /* Global callback infos */
  dlisth global_callbacks;	/* global list of callback infos */

  /* memory limiting */
  long long smr_oom_until;
  int mem_limit_activated;
  int mem_max_allowed_exceeded;
  int mem_hard_limit_exceeded;
  int meminfo_fd;
  int mem_limit_active_perc;
  int mem_max_allowed_perc;
  int mem_hard_limit_perc;
  unsigned long mem_limit_active_kb;
  unsigned long mem_max_allowed_byte;
  unsigned long mem_hard_limit_kb;

  /* local ip check */
  char **local_ip_addrs;

#ifdef COVERAGE_TEST
  /* debugging value for injecting memory states */
  int debug_mem_usage_fixed;
  unsigned long debug_total_mem_kb;
  unsigned long debug_free_mem_kb;
  unsigned long debug_cached_mem_kb;
  unsigned long debug_redis_mem_rss_kb;
#endif
};

struct arcClient
{
  sds querybuf;
  size_t querybuf_peak;
  size_t querylen;
  int argc;
  robj **argv;			/* temporary pointer used in parsing phase and will 
				   be added to reserved_args. */
  dlisth client_callbacks;	/* per-client list of callback infos */
  struct redisCommand *cmd;
  int reqtype;
  int multibulklen;
  long bulklen;
  int flags;
  sds protocol_error_reply;

  // checkpoint related
  int ckptdbfd;
  off_t ckptdboff;
  off_t ckptdbsize;
};

/* -------- */
/* Exported */
/* -------- */
extern struct arcServer arc;

/* arc_config.c */
#define arc_config_get() do {                                                                   \
    config_get_numerical_field("sss-gc-lines",arc.gc_num_line);                                 \
    config_get_numerical_field("sss-gc-interval", arc.gc_interval);                             \
    config_get_numerical_field("smr-local-port",arc.smr_lport);                                 \
    config_get_numerical_field("number-of-rdb-backups",arc.num_rdb_backups);                    \
    config_get_numerical_field("memory-limit-activation-percentage",arc.mem_limit_active_perc); \
    config_get_numerical_field("memory-max-allowed-percentage",arc.mem_max_allowed_perc);       \
    config_get_numerical_field("memory-hard-limit-percentage",arc.mem_hard_limit_perc);         \
    config_get_numerical_field("object-bio-delete-min-elems",arc.object_bio_delete_min_elems);  \
} while(0);

#define arc_rewrite_config() do {                                                                            \
    rewriteConfigNumericalOption(state, "sss-gc-lines", arc.gc_num_line, 8192);                              \
    rewriteConfigNumericalOption(state, "sss-gc-interval", arc.gc_interval, 5000);                           \
    rewriteConfigNumericalOption(state, "smr-local-port",arc.smr_lport,1900);                                \
    rewriteConfigNumericalOption(state, "number-of-rdb-backups",arc.num_rdb_backups,1900);                   \
    rewriteConfigNumericalOption(state, "memory-limit-activation-percentage",arc.mem_limit_active_perc,100); \
    rewriteConfigNumericalOption(state, "memory-max-allowed-percentage",arc.mem_max_allowed_perc,100);       \
    rewriteConfigNumericalOption(state, "memory-hard-limit-percentage",arc.mem_hard_limit_perc,100);        \
    rewriteConfigNumericalOption(state, "object-bio-delete-min-elems",arc.object_bio_delete_min_elems,ARC_OBJ_BIO_DELETE_MIN_ELEMS);  \
} while(0)

extern int arc_config_set (client * c);
extern int arc_config_cmp_load (int argc, sds * argv, char **err_ret);

/* arc_server.c */
#define arc_shared_init() do {                                                         \
    /* this is special system attribute for used for sharded db dump */                \
    shared.db_version = createStringObject("\001\002\003db_version", 13);              \
    /* key for saving global smr_mstime */                                             \
    shared.db_smr_mstime = createStringObject("\001\002\003db_smr_mstime", 16);        \
    /* key for saving migration state */                                               \
    shared.db_migrate_slot = createStringObject("\001\002\003db_migrate_slot", 18);    \
    /* key for saving migration state */                                               \
    shared.db_migclear_slot = createStringObject("\001\002\003db_migclear_slot", 19);  \
    /* Reserved object in order to reply through replication stream. */                \
    shared.addreply_through_smr = createStringObject("dummyobject_notused", 19);       \
} while(0)

//Note: last command must not ends with comma(,)
#define ARC_REDIS_COMMAND_TBL                                         \
    {"s3lget",s3lgetCommand,5,"r",0,NULL,2,2,1,0,0},                  \
    {"s3lmget",s3lmgetCommand,-4,"r",0,NULL,2,2,1,0,0},               \
    {"s3lkeys",s3lkeysCommand,-3,"r",0,NULL,2,2,1,0,0},               \
    {"s3lvals",s3lvalsCommand,4,"r",0,NULL,2,2,1,0,0},                \
    {"s3ladd",s3laddCommand,-7,"wm",0,NULL,2,2,1,0,0},                \
    {"s3laddat",s3laddatCommand,-7,"wm",0,NULL,2,2,1,0,0},            \
    {"s3lmadd",s3lmaddCommand,-7,"wm",0,NULL,2,2,1,0,0},              \
    {"s3lrem",s3lremCommand,-4,"w",0,NULL,2,2,1,0,0},                 \
    {"s3lmrem",s3lmremCommand,-5,"w",0,NULL,2,2,1,0,0},               \
    {"s3lset",s3lsetCommand,-7,"wm",0,NULL,2,2,1,0,0},                \
    {"s3lreplace",s3lreplaceCommand,8,"wm",0,NULL,2,2,1,0,0},         \
    {"s3lcount",s3lcountCommand,-3,"r",0,NULL,2,2,1,0,0},             \
    {"s3lexists",s3lexistsCommand,-5,"r",0,NULL,2,2,1,0,0},           \
    {"s3lexpire",s3lexpireCommand,-4,"w",0,NULL,2,2,1,0,0},           \
    {"s3lmexpire",s3lmexpireCommand,-6,"w",0,NULL,2,2,1,0,0},         \
    {"s3lttl",s3lttlCommand,-5,"r",0,NULL,2,2,1,0,0},                 \
    {"s3sget",s3sgetCommand,5,"r",0,NULL,2,2,1,0,0},                  \
    {"s3smget",s3smgetCommand,-4,"r",0,NULL,2,2,1,0,0},               \
    {"s3skeys",s3skeysCommand,-3,"r",0,NULL,2,2,1,0,0},               \
    {"s3svals",s3svalsCommand,4,"r",0,NULL,2,2,1,0,0},                \
    {"s3sadd",s3saddCommand,-7,"wm",0,NULL,2,2,1,0,0},                \
    {"s3saddat",s3saddatCommand,-7,"wm",0,NULL,2,2,1,0,0},            \
    {"s3smadd",s3smaddCommand,-7,"wm",0,NULL,2,2,1,0,0},              \
    {"s3srem",s3sremCommand,-4,"w",0,NULL,2,2,1,0,0},                 \
    {"s3smrem",s3smremCommand,-5,"w",0,NULL,2,2,1,0,0},               \
    {"s3sset",s3ssetCommand,-7,"wm",0,NULL,2,2,1,0,0},                \
    {"s3sreplace",s3sreplaceCommand,8,"wm",0,NULL,2,2,1,0,0},         \
    {"s3scount",s3scountCommand,-3,"r",0,NULL,2,2,1,0,0},             \
    {"s3sexists",s3sexistsCommand,-5,"r",0,NULL,2,2,1,0,0},           \
    {"s3sexpire",s3sexpireCommand,-4,"w",0,NULL,2,2,1,0,0},           \
    {"s3smexpire",s3smexpireCommand,-6,"w",0,NULL,2,2,1,0,0},         \
    {"s3sttl",s3sttlCommand,-5,"r",0,NULL,2,2,1,0,0},                 \
    {"s3keys",s3keysCommand,3,"r",0,NULL,2,2,1,0,0},                  \
    {"s3count",s3countCommand,3,"r",0,NULL,2,2,1,0,0},                \
    {"s3expire",s3expireCommand,4,"w",0,NULL,2,2,1,0,0},              \
    {"s3rem",s3remCommand,3,"w",0,NULL,2,2,1,0,0},                    \
    {"s3mrem",s3mremCommand,-4,"w",0,NULL,2,2,1,0,0},                 \
    {"s3gc",s3gcCommand,2,"w",0,NULL,0,0,0,0,0},                       \
    {"checkpoint",checkpointCommand,2,"ars",0,NULL,0,0,0,0,0},        \
    {"migstart",migstartCommand,2,"aw",0,NULL,0,0,0,0,0},             \
    {"migend",migendCommand,1,"aw",0,NULL,0,0,0,0,0},                 \
    {"migconf",migconfCommand,-2,"aw",0,NULL,0,0,0,0,0},              \
    {"migpexpireat",migpexpireatCommand,3,"w",0,NULL,1,1,1,0,0},      \
    {"bping",bpingCommand,1,"r",0,NULL,0,0,0,0,0},                    \
    {"quit",quitCommand,1,"r",0,NULL,0,0,0,0,0}                       \

extern void arc_init_config (void);
extern void arc_tool_hook (int argc, char **argv);
extern void arc_init_arc (void);
extern void arc_main_hook (int argc, char **argv);
// return 0 to continue, hz otherwise
extern int arc_server_cron (void);
extern int arc_expire_haveto_skip (sds key);
extern mstime_t arc_mstime (void);

/* arc_networking.c */
extern void arc_smrc_create (client * c);
extern void arc_smrc_free (client * c);
extern void arc_smrc_accept_bh (client * c);
extern void arc_smrc_set_protocol_error (client * c);
extern void arc_smrc_try_process (client * c);

/* arc_sss.c */
extern int arc_rewrite_sss_object (rio * r, robj * key, robj * o);
extern int arc_sss_type_value_count (robj * o);
extern void arc_sss_compute_dataset_digest (robj * o, unsigned char *digest);
extern robj *arc_create_sss_object (robj * key);
extern void arc_free_sss_object (robj * o);
extern int arc_rdb_save_sss_type (rio * rdb, robj * o);
extern int arc_rdb_save_sss_object (rio * rdb, robj * o);
extern robj *arc_rdb_load_sss_object (rio * rdb);
extern int arc_rio_peek_key (rio * rdb, int rdbtype, robj * key);

/* arc_checkpoint.c */
extern void arc_bgsave_done_handler (int ok);
extern int arc_rdb_save_rio_with_file (rio * rdb, FILE * fp, int *error);
extern int arc_rdb_save_onwrite (rio * rdb, int *error);
extern int arc_rdb_save_skip (sds keystr);
extern int arc_rdb_save_aux_fields (rio * rdb);
extern int arc_rdb_load_aux_fields_hook (robj * auxkey, robj * auxval,
					 long long *now);

/* ------------------------- */
/* Redis commands extensions */
/* ------------------------- */
/* arc_networking.c */
extern void bpingCommand (client * c);
extern void quitCommand (client * c);

/* arc_t_sss.c */
// list semantic s3 commands 
extern void s3lgetCommand (client * c);
extern void s3lmgetCommand (client * c);
extern void s3lkeysCommand (client * c);
extern void s3lvalsCommand (client * c);
extern void s3laddCommand (client * c);
extern void s3laddatCommand (client * c);
extern void s3lmaddCommand (client * c);
extern void s3lremCommand (client * c);
extern void s3lmremCommand (client * c);
extern void s3lsetCommand (client * c);
extern void s3lreplaceCommand (client * c);
extern void s3lcountCommand (client * c);
extern void s3lexistsCommand (client * c);
extern void s3lexpireCommand (client * c);
extern void s3lmexpireCommand (client * c);
extern void s3lttlCommand (client * c);
// set semantic s3 commands
extern void s3sgetCommand (client * c);
extern void s3smgetCommand (client * c);
extern void s3skeysCommand (client * c);
extern void s3svalsCommand (client * c);
extern void s3saddCommand (client * c);
extern void s3saddatCommand (client * c);
extern void s3smaddCommand (client * c);
extern void s3sremCommand (client * c);
extern void s3smremCommand (client * c);
extern void s3ssetCommand (client * c);
extern void s3sreplaceCommand (client * c);
extern void s3scountCommand (client * c);
extern void s3sexistsCommand (client * c);
extern void s3sexpireCommand (client * c);
extern void s3smexpireCommand (client * c);
extern void s3sttlCommand (client * c);
// generic s3 commands
extern void s3keysCommand (client * c);
extern void s3countCommand (client * c);
extern void s3expireCommand (client * c);
extern void s3remCommand (client * c);
extern void s3mremCommand (client * c);
// s3 gc
extern void s3gcCommand (client * c);

/* arc_checkpoint.c */
void checkpointCommand (client * c);
void migstartCommand (client * c);
void migendCommand (client * c);
void migconfCommand (client * c);
void migpexpireatCommand (client * c);

#endif

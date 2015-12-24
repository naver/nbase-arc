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

#ifndef _ARCCI_COMMON_H_
#define _ARCCI_COMMON_H_

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#include "os.h"
#include "arcci.h"
#include "dlist.h"
#include "dict.h"
#include "sds.h"
#include "ae.h"
#include "win32fixes.h"

#define HASHED_TIMER_WHEEL_SIZE 500
#define BACKEND_DEFAULT_CRON_INTERVAL (1000/HASHED_TIMER_WHEEL_SIZE)
#define BACKEND_DEFAULT_ZK_CRON_INTERVAL 10
#define CLUSTER_SIZE 8192
#define MAX_CMD_PEEK 8
#define IBUF_LEN (16*1024)
#define MAX_BUF_LEN (1024*1024*1024)
#define APP_LOG_MAX_SUFFIX_LEN 128
#define APP_LOG_ROTATE_LIMIT (64*1024*1024)

#ifdef FI_ENABLED
#define MAX_ERRNO_INJECTION 32
typedef struct fi_s fi_t;
typedef struct fi_entry_s fi_entry_t;

struct fi_s
{
  dict *tbl;
  long long seq;
  int err;
};
#define init_fi(e) do {                          \
  (e)->tbl = NULL;                               \
  (e)->seq = 0LL;                                \
  (e)->err = 0LL;                                \
} while(0)

struct fi_entry_s
{
  char *file;
  int line;
  int err_idx;
  int err_cnt;
  int err_nums[MAX_ERRNO_INJECTION];
};
#define init_fi_entry(e) do {                    \
  (e)->file = NULL;                              \
  (e)->line = 0;                                 \
  (e)->err_idx = 0;                              \
  (e)->err_cnt = 0;                              \
} while (0)

extern int fi_Enabled;
extern fi_t *fe_Fi;
extern fi_t *be_Fi;

extern int arc_fault_inject (fi_t * fi, const char *file, int line, ...);
extern fi_t *arc_fi_new (void);
extern void arc_fi_destroy (fi_t * fi);

/* 
 * NOTE: FI functions must at lease 1 errno
 */
#define FE_FI(r,v,...) do {                                                           \
    if(fi_Enabled && arc_fault_inject(fe_Fi,__FILE__,__LINE__,__VA_ARGS__)) {         \
	r = v;                                                                        \
    } else {

#define BE_FI(r,v,...) do {                                                           \
    if(fi_Enabled && arc_fault_inject(be_Fi,__FILE__,__LINE__,__VA_ARGS__)) {         \
	r = v;                                                                        \
    } else {

#define FI_END() }} while(0)

#else
#define FE_FI(r,v,...)
#define BE_FI(r,v,...)
#define FI_END()
#endif /* FI_ENABLED */

typedef struct be_job_s be_job_t;
typedef struct cond_s cond_t;
typedef struct cmd_peek_s cmd_peek_t;
typedef struct arc_job_s arc_job_t;
typedef struct noop_job_s noop_job_t;
typedef struct be_s be_t;
typedef struct fe_s fe_t;

typedef struct pn_gw_aff_s pn_gw_aff_t;
typedef struct gw_s gw_t;
typedef struct gw_conn_s gw_conn_t;
typedef struct redis_parse_cb_s redis_parse_cb_t;


/* --------------- */
/* backend job */
/* --------------- */
typedef enum
{
  /* create by client (job->ref_mutex is used) */
  BE_JOB_INIT = 1,
  BE_JOB_ARC = 2,
  BE_JOB_NEED_MUTEX_LIMIT = 99,
  /* create by backend itself */
  BE_JOB_NOOP = 100,
  BE_JOB_RECONNECT = 101,
  BE_JOB_ZK_TO = 102,
} be_job_type_t;

#define is_job_need_mutex(job) ((job)->type <= BE_JOB_NEED_MUTEX_LIMIT)

struct be_job_s
{
  dlisth head;			// must be the first member
  be_job_type_t type;		// job type
  dlisth htwhead;		// timeout link header
  long long to;			// timeout value for this job
  pthread_mutex_t ref_mutex;	// for reference counting
  int ref_count;		// reference count
  void (*free_func) (void *);	// data free function
  void *data;			// type specific data
  int cost;			// request cost
  long long conn_ts;		// time that this request is bound to the conn
  gw_conn_t *conn;		// connection that this request is bound (may be null)
};
#define init_be_job(b) do {                      \
  dlisth_init(&(b)->head);                       \
  (b)->type = BE_JOB_NOOP;                       \
  dlisth_init(&(b)->htwhead);                    \
  (b)->to = 0LL;                                 \
  pthread_mutex_init(&(b)->ref_mutex, NULL);     \
  (b)->ref_count = 0;                            \
  (b)->free_func = NULL;                         \
  (b)->data = NULL;                              \
  (b)->cost = 0;                                 \
  (b)->conn_ts = 0LL;                            \
  (b)->conn = NULL;                              \
} while (0)

#define htwhead_to_be_job(h)                     \
  ((be_job_t *) ((char *)h - offsetof(be_job_t, htwhead)))

struct cond_s
{
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int pred;
  int err;
};
#define init_cond(r) do {                        \
  pthread_mutex_init(&(r)->mutex, NULL);         \
  pthread_cond_init(&(r)->cond, NULL);           \
  (r)->pred = 0;                                 \
  (r)->err = 0;                                  \
} while (0)

typedef enum
{
  ARCJOB_STATE_NONE = 0,	// associated with nothing
  ARCJOB_STATE_SUBMITTED = 1,	// job is sent to the BE
  ARCJOB_STATE_CONN = 2,	// wait for connection allocation
  ARCJOB_STATE_WAITING = 3,	// wait in the connection request queue
  ARCJOB_STATE_SENDING = 4,	// some bytes are sent
  ARCJOB_STATE_SENT = 5,	// all bytes are sent
  ARCJOB_STATE_COMPLETED = 6,	// success or abort
} arc_job_state_t;

struct cmd_peek_s
{
  dlisth head;
  int cmd_off;			// command offset w.r.t rqst.obuf
  int argc;			// number of arguments of a command (temp)
  int num_peek;			// number of argument peek over the obuf (temp)
  int off[MAX_CMD_PEEK];	// argument offset in obuf (temp)
  int len[MAX_CMD_PEEK];	// argument length in obuf (temp)
};
#define init_cmd_peek(p) do {      \
  int _i;                          \
  dlisth_init(&(p)->head);         \
  (p)->cmd_off = 0;                \
  (p)->argc = 0;                   \
  (p)->num_peek = 0;               \
  for(_i=0;_i<MAX_CMD_PEEK;_i++) { \
    (p)->off[_i] = 0;              \
  }                                \
  for(_i=0;_i<MAX_CMD_PEEK;_i++) { \
    (p)->len[_i] = 0;              \
  }                                \
} while(0)


struct arc_job_s
{
  arc_job_state_t state;	// request state
  be_job_t *noop;		// see detailed explanation about this field at create_arc_be_job
  /* out state for request */
  struct
  {
    sds obuf;			// request raw buffer (sds)
    int ncmd;			// number of commands in obuf (>1 when requests are pipelined)
    dlisth peeks;		// list of cmd_peek_t
    int diff_pn;		// has keys to different partitions
    int is_write;		// is wite command (first command)
    int crc;			// crc of the key of the first command (-1 means no information) 
    int sent_cnt;		// number of sent bytes
  } rqst;
  /* in state for response */
  struct
  {
    sds ibuf;			// response raw buffer (sds)
    int off;			// offset from ibuf upto which responses are received
    int ncmd;			// number of command upto offset position
    int reply_off;		// offset used by arc_get_reply function
    arc_reply_t **replies;	// array size equals ncmd
  } resp;
  /* signallig */
  pthread_mutex_t mutex;	// conditional variable mutex
  pthread_cond_t cond;		// conditional variable
  int pred;			// conditiolan variable predicate
  int err;			// 0 means (rs.ncmd = cmd), otherwise it is errno
};
#define init_arc_job(r) do {                     \
  (r)->state = ARCJOB_STATE_NONE;                \
  (r)->noop = NULL;                              \
  (r)->rqst.obuf = NULL;                         \
  (r)->rqst.ncmd = 0;                            \
  dlisth_init(&(r)->rqst.peeks);                 \
  (r)->rqst.diff_pn = 0;                         \
  (r)->rqst.is_write = 0;                        \
  (r)->rqst.crc = -1;                            \
  (r)->rqst.sent_cnt = 0;                        \
  (r)->resp.ibuf = NULL;                         \
  (r)->resp.off = 0;                             \
  (r)->resp.ncmd = 0;                            \
  (r)->resp.reply_off = 0;                       \
  (r)->resp.replies = 0;                         \
  pthread_mutex_init(&(r)->mutex, NULL);         \
  pthread_cond_init(&(r)->cond, NULL);           \
  (r)->pred = 0;                                 \
  (r)->err = 0;                                  \
} while (0)

struct noop_job_s
{
  arc_job_state_t job_state;
  int rqst_ncmd;
  int recv_off;
  int resp_ncmd;
};
#define init_noop_job(n) do {                    \
  (n)->job_state = ARCJOB_STATE_NONE;            \
  (n)->rqst_ncmd = 0;                            \
  (n)->recv_off = 0;                             \
  (n)->resp_ncmd = 0;                            \
} while (0)

/* ------- */
/* gateway */
/* ------- */
typedef enum
{
  GW_STATE_NONE = 0,		// nothing is assoficated with this gateway
  GW_STATE_USED = 1,		// gateway is in use
  GW_STATE_UNUSED = 2,		// gateway is not used
} gw_state_t;

/* 
 * per partition gateway affinity
 */
#define GW_AFF_NONE   'N'
#define GW_AFF_READ   'R'
#define GW_AFF_WRITE  'W'
#define GW_AFF_ALL    'A'

#define MAX_READ_GW_AFF_IDX  8
#define MAX_WRITE_GW_AFF_IDX 8
struct pn_gw_aff_s
{
  short w_ids[MAX_WRITE_GW_AFF_IDX];	// gateway id that has write affinity
  short r_ids[MAX_READ_GW_AFF_IDX];	// gateway ids that has read affinity
};
#define init_pn_gw_aff(p) do {             \
  int i_;                                  \
  for(i_=0;i_<MAX_WRITE_GW_AFF_IDX;i_++) { \
    (p)->w_ids[i_]=-1;                     \
  }                                        \
  for(i_=0;i_<MAX_READ_GW_AFF_IDX;i_++) {  \
    (p)->r_ids[i_]=-1;                     \
  }                                        \
} while (0)

struct gw_s
{
  gw_state_t state;		// gateway status
  char *host;			// host
  int port;			// port
  int n_active;			// usable connections
  short id;			// gateway id 
  long gwl_version;		// gateway list version
  dlisth conns;			// connection to the gateway
};
#define init_gw(g) do {                          \
  (g)->state = GW_STATE_NONE;                    \
  (g)->host = NULL;                              \
  (g)->port = 0;                                 \
  (g)->n_active = 0;                             \
  (g)->id = -1;                                  \
  (g)->gwl_version = 0;                          \
  dlisth_init(&(g)->conns);                      \
} while (0)

/* ------------------ */
/* gateway connection */
/* ------------------ */
typedef enum
{
  GW_CONN_NONE = 0,		// nothing is associated with this structure
  GW_CONN_CONNECTING = 1,	// connection in progress
  GW_CONN_OK = 2,		// application level ping/pong successful
  GW_CONN_CLOSING = 3,		// will go to 'closed' if all pending requests are handled
  GW_CONN_CLOSED = 4,		// in garbage_conns head and will be garbage collected
} gw_conn_state_t;

struct gw_conn_s
{
  dlisth head;			// list header
  dlisth gc_head;		// list header for be->garbage_conns
  be_t *be;			// backpointer
  gw_t *gw;			// backpointer
  be_job_t *conn_to_job;	// connection setup timeout job (actually it is BE_JOB_RECONNECT)
  socket_t fd;			// connedtion fd
  int w_prepared;		// write handler set flag
  int state;			// connection state
  int dedicated;		// is this connection is dedicated for request that has affinity to the gateway
  long long total_cost;		// total cost of jobs in this conn
  long long recent_peek_cnt;	// number of peek after every reset
  dlisth jobs;			// jobs in sending
  dlisth sent_jobs;		// jobs sent and wait for response
  sds ibuf;			// input buffer
  /* temporary buffer for ping/pong handshake */
  char buf[16];			// buffer
  char *bp;			// current buffer position
  char *ep;			// buffer end position
};
#define init_gw_conn(c) do {                     \
  dlisth_init(&(c)->head);                       \
  dlisth_init(&(c)->gc_head);                    \
  (c)->be = NULL;                                \
  (c)->gw = NULL;                                \
  (c)->conn_to_job = NULL;                       \
  (c)->fd = -1;                                  \
  (c)->w_prepared = 0;                           \
  (c)->state = GW_CONN_NONE;                     \
  (c)->dedicated = 0;                            \
  (c)->total_cost = 0LL;                         \
  (c)->recent_peek_cnt = 0LL;                    \
  dlisth_init(&(c)->jobs);                       \
  dlisth_init(&(c)->sent_jobs);                  \
  (c)->ibuf = NULL;                              \
  (c)->buf[0] = '\0';                            \
  (c)->bp = &(c)->buf[0];                        \
  (c)->ep = &(c)->buf[0];                        \
} while (0)

#define gc_head_to_gw_conn(h)                    \
  ((gw_conn_t *) ((char *)h - offsetof(gw_conn_t, gc_head)))

/* --------------------- */
/* zookeeper integration */
/* --------------------- */
typedef enum
{
  ZK_STATE_NONE = 0,
  ZK_STATE_CONNECTED = 1,
} zk_state_t;

/* ------------- */
/* Configuration */

/* ------------- */
#define CONF_NUM_CONN_PER_GW_DEFAULT                  3
#define CONF_NUM_CONN_PER_GW_MIN                      2
#define CONF_INIT_TIMEOUT_MILLIS_DEFAULT              10000
#define CONF_INIT_TIMEOUT_MILLIS_MIN                  3000
#define CONF_LOG_LEVEL_DEFAULT                        ARC_LOG_LEVEL_NOLOG
#define CONF_MAX_FD_MIN                               1024
#define CONF_MAX_FD_DEFAULT                           4096
#define CONF_CONN_RECONNECT_MILLIS_MIN                100
#define CONF_CONN_RECONNECT_MILLIS_DEFAULT            1000
#define CONF_ZK_RECONNECT_MILLIS_MIN                  100
#define CONF_ZK_RECONNECT_MILLIS_DEFAULT              1000
#define CONF_ZK_SESSION_TIMEOUT_MILLIS_MIN            1000
#define CONF_ZK_SESSION_TIMEOUT_MILLIS_DEFAULT        10000

#define init_arc_conf(c)  do {                                             \
  (c)->num_conn_per_gw = CONF_NUM_CONN_PER_GW_DEFAULT;                     \
  (c)->init_timeout_millis = CONF_INIT_TIMEOUT_MILLIS_DEFAULT;             \
  (c)->log_level = ARC_LOG_LEVEL_NOLOG;                                    \
  (c)->log_file_prefix = NULL;                                             \
  (c)->max_fd = CONF_MAX_FD_DEFAULT;                                       \
  (c)->conn_reconnect_millis = CONF_CONN_RECONNECT_MILLIS_DEFAULT;         \
  (c)->zk_reconnect_millis = CONF_ZK_RECONNECT_MILLIS_DEFAULT;             \
  (c)->zk_session_timeout_millis = CONF_ZK_SESSION_TIMEOUT_MILLIS_DEFAULT; \
} while (0)

/* ------- */
/* backend */
/* ------- */

typedef enum
{
  BE_STATE_NONE = 0,		// associated with nothing
  BE_STATE_RUNNING = 1,		// can serve jobs
  BE_STATE_SHUTDOWN = 2,	// it is in shutdown phase
} be_state_t;

typedef enum
{
  BE_HANDLE_TYPE_NONE = 0,
  BE_HANDLE_TYPE_MAIN = 1,
  BE_HANDLE_TYPE_CRON = 2,
  BE_HANDLE_TYPE_ZK_CRON = 3,
  BE_HANDLE_TYPE_GW_IN = 4,
  BE_HANDLE_TYPE_GW_OUT = 5,
  BE_HANDLE_TYPE_PP_IN = 6,
  BE_HANDLE_TYPE_PP_OUT = 7,
  BE_HANDLE_TYPE_ZK_IN = 8,
  BE_HANDLE_TYPE_ZK_OUT = 9,
} be_handle_type_t;

struct be_s
{
  /* configuration */
  arc_conf_t conf;		// configuration

  /* basic backend control */
  volatile int abrupt_exit;	// fast signal for termination set by client
  int be_exit;			// exit condition cased by backend itself
  int exit_errno;		// be_exit reason
  int be_errno;			// backend errno
  be_state_t state;		// backend state
  pipe_t pipe_fd;
  char input_buf[16];		// temporary data holder for pipe input processing
  char *ibp;			// pipe_buf position
  aeEventLoop *el;		// event loop 
  /* zookeeper integration */
  int use_zk;			// use zookeeper gateway lookup
  char *zk_host;		// zookeeper host
  char *cluster_name;		// cluster name
  zk_state_t zk_state;		// zookeeper state
  void *zh;			// zookeeper handle (opaque)
  int zk_force_reconn;		// retry flag set by zk handlers
  long long zk_retry_at;	// retry to connect to zk at this time
  long zk_gwl_version;		// gateway list version
  be_job_t zk_to_job;		// used by the zookeeper event processing.
  int zk_interest_fd;		// fd which zk has interest
  int zk_w_prepared;		// write event handler is prepared
  int zk_r_prepared;		// read event handler is prepared
  int zk_events;		// interest event of fd
  /* affinity related fields */
  int use_affinity;		// determine where to use affinity mapping structures
  pn_gw_aff_t pn_gw_aff[CLUSTER_SIZE];	// partition number to affinity info mapping
  /* gateway pool */
  char *gw_hosts;		// gateway hosts (fixed set of gateway hosts)
  int n_gw;			// number of gateway instance in gw_ary
  gw_t **gw_ary;		// array of pointer to gateway instance
  /* hashed timer wheel */
  long long htw_tick;		// number of tick from the epoc 
  dlisth htw[HASHED_TIMER_WHEEL_SIZE];	// hashed timer wheel (a wheel devides a second)
  /* job specific fields */
  int initialized;		// initialized flag
  be_job_t *init_job;		// init request. signalled when there is any GW_CONN_OK instance
  dlisth retry_arc_jobs;	// BE_JOB_ARC in ARCJOB_STATE_CONN state
  dlisth garbage_conns;		// closed connections linked via gc_head field
  /* application logging & debugging */
  int app_log_prefix_size;	// application log file prefix size
  char app_log_buf[PATH_MAX];	// application log file buffer
  int app_log_size;		// current application log size
  FILE *app_log_fp;		// log file pointer
  be_handle_type_t handle_type;	// bakend handler type
  char tmp_buf[512];		// temporary buffer
#ifdef WIN32
  HANDLE iocph;			// temporary iocp handler to put it into thread-local-storage
  void *iocp_state;		// temporary iocp_state handler to put it into thread-local-storage
  pipe_t pipe_fd_fe;		// temporary pipe_fd of front-end to delete iocp-associated data with thread-safety
#endif
};
#define init_be(c) do {                          \
  int i;                                         \
  init_arc_conf(&(c)->conf);                     \
  (c)->abrupt_exit = 0;                          \
  (c)->be_exit = 0;                              \
  (c)->exit_errno = 0;                           \
  (c)->be_errno = 0;                             \
  (c)->state = BE_STATE_NONE;                    \
  (c)->pipe_fd = INVALID_PIPE;                   \
  (c)->input_buf[0] = '\0';                      \
  (c)->ibp = &(c)->input_buf[0];                 \
  (c)->el = NULL;                                \
  (c)->use_zk = 0;                               \
  (c)->zk_host = NULL;                           \
  (c)->cluster_name = NULL;                      \
  (c)->zk_state = ZK_STATE_NONE;                 \
  (c)->zh = NULL;                                \
  (c)->zk_force_reconn = 0;                      \
  (c)->zk_retry_at = 0;                          \
  (c)->zk_gwl_version = 0;                       \
  init_be_job(&(c)->zk_to_job);                  \
  (c)->zk_interest_fd = -1;                      \
  (c)->zk_w_prepared = 0;                        \
  (c)->zk_r_prepared = 0;                        \
  (c)->zk_events = 0;                            \
  (c)->use_affinity = 0;                         \
  for(i=0;i<CLUSTER_SIZE;i++) {                  \
    init_pn_gw_aff(&(c)->pn_gw_aff[i]);          \
  }                                              \
  (c)->gw_hosts = NULL;                          \
  (c)->n_gw = 0;                                 \
  (c)->gw_ary = NULL;                            \
  (c)->htw_tick = 0LL;                           \
  for(i=0; i<HASHED_TIMER_WHEEL_SIZE;i++) {      \
    dlisth_init(&(c)->htw[i]);                   \
  }                                              \
  (c)->initialized = 0;                          \
  (c)->init_job = NULL;                          \
  dlisth_init(&(c)->retry_arc_jobs);             \
  dlisth_init(&(c)->garbage_conns);              \
  (c)->app_log_prefix_size = 0;                  \
  (c)->app_log_buf[0] = '\0';                    \
  (c)->app_log_size = 0;                         \
  (c)->app_log_fp = NULL;                        \
  (c)->handle_type = BE_HANDLE_TYPE_NONE;        \
  (c)->tmp_buf[0] = '\0';                        \
} while (0)

/* ------------- */
/* redis command */
/* ------------- */
/* Front end structure */
struct fe_s
{
#ifndef _WIN32
  int pipe_fd;			// for sending job 
#else
  HANDLE pipe_fd;
  CRITICAL_SECTION pipe_lock;
#endif
  pthread_t be_thread;		// backend thread
  dict *commands;		// command table (read only)
  be_t *be;			// backend 
};
void init_fe (fe_t * c);

/* 
 * redis parser callback structure 
 * callback function returns
 *   0: ok, continue
 *  -1: error
 *   1: ok, done
 */
#define PARSE_CB_OK_CONTINUE 0
#define PARSE_CB_OK_DONE 1
struct redis_parse_cb_s
{
  int (*on_string) (void *ctx, char *s, int len, int is_null);
  int (*on_status) (void *ctx, char *s, int len);
  int (*on_error) (void *ctx, char *s, int len);
  int (*on_integer) (void *ctx, long long val);
  int (*on_array) (void *ctx, int len, int is_null);
};

/* arcci_common.c */
extern int parse_redis_response (char *p, int len, int *size,
				 redis_parse_cb_t * cb, void *ctx);
extern void get_timespec_from_millis (struct timespec *ts, long long to);
extern long long millis_from_tv (struct timeval *tv);
extern long long currtime_millis (void);
extern be_job_t *create_be_job (be_job_type_t type,
				void (*free_func) (void *), void *data,
				int cost);
extern void addref_be_job (be_job_t * job);
extern void decref_be_job (be_job_t * job);

/* arcci_fe.c */
typedef void (*cmdcb_t) (int pos, int off, const char *val, size_t vallen,
			 void *ctx);

/* arcci_be.c */
extern void *be_thread (void *arg);
#endif

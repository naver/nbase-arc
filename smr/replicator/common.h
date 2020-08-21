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

#ifndef _COMMON_H_
#define _COMMON_H_

#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>

#include "ae.h"
#include "dlist.h"
#include "part_filter.h"
#include "smr.h"
#include "smr_log.h"
#include "stream.h"
#include "slowlog.h"
#include "ipacl.h"

/* define smr version */
#define SMR_VERSION 201

/* main event loop interval in msec. */
#define MAIN_CRON_INTERVAL 2

/* migration event loop interval in msec. */
#define MIG_CRON_INTERVAL  3

/* log rate limiting */
#define MAX_LOG_SEND_PER_EVENT (16*1024)
#define MAX_LOG_RECV_PER_EVENT (16*1024)
#define MAX_CLI_RECV_PER_EVENT (16*1024)

/* maximum write buffer size to the log file */
#define MAX_LOG_WRITE_ONCE 1024*1024

/* application log file size limit */
#define MAX_APP_LOG_SIZE 64*1024*1024

#define BE_CONF_BUF_SIZE 4096

#define DEF_SLAVE_IDLE_TO 18000

/* SMR_OP_EXT extensions */
#define SMR_OP_EXT_NOOP ('n' << 24)

/* Access control, client limit related */
#define SMR_ACCEPT_BLOCK_MSEC        1500
#define SMR_MAX_MGMT_CLIENTS_PER_IP  50

/* replicator stat */
typedef struct repStat
{
  /* aggregated client connection */
  long long client_nre;
  long long client_nwe;
  /* aggregated slave connection */
  long long slave_nre;
  long long slave_nwe;
  /* aggregated local connection */
  long long local_nre;
  long long local_nwe;
  /* event loop count */
  long long nev;
} repStat;

/* log related definitions */
typedef enum
{
  LG_ERROR = 0,			// used as a index
  LG_WARN = 1,
  LG_INFO = 2,
  LG_DEBUG = 3,
  LG_MAX_LEVEL = LG_DEBUG
} logLevel;

typedef enum
{
  CONN_TYPE_BEGIN = 0,
  NO_CONN = 0,			// used as a index
  LOCAL_CONN_IN = 1,
  LOCAL_CONN_OUT = 2,
  SLAVE_CONN_IN = 3,
  SLAVE_CONN_OUT = 4,
  CLIENT_CONN_IN = 5,
  CLIENT_CONN_OUT = 6,
  MASTER_CONN_IN = 7,
  MASTER_CONN_OUT = 8,
  MANAGEMENT_CONN_IN = 9,
  MANAGEMENT_CONN_OUT = 10,
  /* 
   * below two type is not set to s_Server.conn_type
   * These are defined for fault injection of migration thread 
   */
  MIG_CONN_IN = 11,
  MIG_CONN_OUT = 12,
  BIO_CONN = 13,
  MMAP_CONN = 14,
  CONN_TYPE_END = MMAP_CONN
} connType;

/* replicator role */
typedef enum
{
  NONE = 0, LCONN = 1, MASTER = 2, SLAVE = 3
} repRole;

/* 
 * log file mmap entry 
 * This sturcture is used ONLY within a replicator thread context.
 * Bio opens the the log file separately
 */
typedef struct logMmapEntry
{
  dlisth head;			// Must be the first member
  pthread_mutex_t mutex;	// mutex
  int ref_count;		// reference count on this entry
  int log_bytes;		// log bytes
  int age;			// age
  long long start_seq;		// sequence number
  smrLogAddr *addr;		// mapped address
} logMmapEntry;
#define init_log_mmap_entry(e)  do {     \
  dlisth_init(&(e)->head);               \
  pthread_mutex_init(&(e)->mutex, NULL); \
  (e)->ref_count = 0;                    \
  (e)->log_bytes = 0;                    \
  (e)->age = 0;                          \
  (e)->start_seq = -1LL;                 \
  (e)->addr = 0;                         \
} while(0)

typedef struct smrReplicator_ smrReplicator;

/* replication master connection structure */
typedef struct masterConn
{
  smrReplicator *rep;		// back pointer to the replicator instance
  int fd;			// connection to master replicator
  int w_prepared;		// write handler event preparation flag
  /* commit seq command seeking state  cs_xxx */
  int cs_msg_skip;		// message skip size
  int cs_need;			// like skip but input data are copied to buf
  char cs_buf[64];		// buf for skip determination (64 is sufficient)
  int cs_buf_pos;		// buf position
  long long rseq;		// recived seq at message boundary
  ioStream *ous;		// output stream
  long long read_cron_count;	// rep->cron_count seen by the read event handler
} masterConn;
#define init_master_conn(c) do { \
  (c)->rep = NULL;               \
  (c)->fd = -1;                  \
  (c)->w_prepared = 0;           \
  (c)->cs_msg_skip = 0;          \
  (c)->cs_need = 0;              \
  (c)->cs_buf_pos = 0;           \
  (c)->rseq = -1LL;              \
  (c)->ous = NULL;               \
  (c)->read_cron_count = 0LL;    \
} while(0)

/* local client connection structure */
typedef struct localConn
{
  smrReplicator *rep;		// back pointer to the replicator instance
  int init_sent;		// sent init command
  int fd;			// connection fd
  int w_prepared;		// write handler event preparation flag
  ioStream *ins;		// stream from backend
  ioStream *ous;		// stream to backend
  long long consumed_seq;	// sequence number upto which logs are consumed at the backend
  long long sent_seq;		// sequence number upto which logs are notified to the backend
} localConn;
#define init_local_conn(c) do {  \
  (c)->rep = NULL;               \
  (c)->init_sent = 0;            \
  (c)->fd = -1;                  \
  (c)->w_prepared = 0;           \
  (c)->ins = NULL;               \
  (c)->ous = NULL;               \
  (c)->consumed_seq = 0LL;       \
  (c)->sent_seq = 0LL;           \
} while(0)

/* replication client connection structure */
typedef struct clientConn
{
  dlisth head;			// Must be the first member
  smrReplicator *rep;		// back pointer to the replicator instance
  int fd;			// connection fd
  int w_prepared;		// write handler event preparation flag
  short nid;			// node id
  long long num_logged;		// total logged bytes from client conn
  ioStream *ins;		// in stream
  ioStream *ous;		// out stream
} clientConn;
#define init_client_conn(c) do { \
  (c)->rep = NULL;               \
  dlisth_init(&(c)->head);       \
  (c)->fd = -1;                  \
  (c)->w_prepared = 0;           \
  (c)->nid = -1;                 \
  (c)->num_logged = 0LL;         \
  (c)->ins = NULL;               \
  (c)->ous = NULL;               \
} while(0)

/* slave replicator connection structure */
typedef struct slaveConn
{
  dlisth head;			// linked to smrServer.slaves
  short nid;			// nid allocated
  int is_quorum_member;		// is this quourm member?
  smrReplicator *rep;		// back pointer to the replicator instance
  int fd;			// connection fd
  int w_prepared;		// write handler event preparation flag
  ioStream *ins;		// input stream
  long long slave_log_seq;	// log sequence number at slave side (recvd seq)
  long long sent_log_seq;	// log sequence number sent to the slave
  logMmapEntry *log_mmap;	// log file mapping
  long long read_cron_count;	// rep->cron_count seen by the read event handler
} slaveConn;
#define init_slave_conn(s) do {  \
  dlisth_init(&(s)->head);       \
  (s)->nid = -1;                 \
  (s)->is_quorum_member = 0;     \
  (s)->rep = NULL;               \
  (s)->fd = -1;                  \
  (s)->w_prepared = 0;           \
  (s)->ins = NULL;               \
  (s)->slave_log_seq = 0LL;      \
  (s)->sent_log_seq = 0LL;       \
  (s)->log_mmap = NULL;          \
  (s)->read_cron_count = 0LL;    \
} while (0)

/* management connection structure */
typedef struct mgmtConn
{
  dlisth head;
  smrReplicator *rep;		// back pointer to the replicator instance
  char *singleton;		// if not null, at most one conn with same singleton value
  int fd;			// connection fd
  int w_prepared;		// write handler event preparation flag
  char *ibuf;			// input buffer
  int ibuf_sz;			// input buffer size
  char *ibuf_p;			// current input buffer position for read
  ioStream *ous;		// stream to backend
  char cip[64];			// client IP
} mgmtConn;
#define init_mgmt_conn(s) do { \
  dlisth_init(&(s)->head);     \
  (s)->rep = NULL;             \
  (s)->singleton = NULL;       \
  (s)->fd = -1;                \
  (s)->w_prepared = 0;         \
  (s)->ibuf = NULL;            \
  (s)->ibuf_sz = 0;            \
  (s)->ibuf_p = NULL;          \
  (s)->ous = NULL;             \
  (s)->cip[0] = '\0';          \
} while (0)

/* atomic sequence number */
typedef struct smrAtomicSeq
{
  pthread_spinlock_t spin;
  long long seq;
} smrAtomicSeq;

/* smr replicator */
#define EBUFSZ 1024
#define PORT_OFF_LOCAL  0
#define PORT_OFF_CLIENT 1
#define PORT_OFF_SLAVE  2
#define PORT_OFF_MGMT   3

/* migration related structure */
typedef struct migStruct
{
  smrReplicator *rep;		// backpointer to replicator
  /* argument */
  char *dest_host;		// migration destination host
  int dest_base_port;		// migration destination port
  long long from_seq;		// migration start port
  smrAtomicSeq buffered;	// buffered in stream for transmit
  smrAtomicSeq sent;		// sent logs (in bytes) 
  smrAtomicSeq acked;		// acked logs (in bytes)
  partFilter *filter;		// filter to send the subset of the log 
  int num_part;			// number of total partitions
  volatile int tps_limit;	// for rate limiting
  /* processing in/out */
  aeEventLoop *el;
  int fd;			// connection to the destination master slave
  long long teid;		// timer event id
  struct timeval curr_time;	// curr time (advanced in second granule)
  char ibuf[1024];		// input buffer
  char *ibp;			// input buffer pointer
  ioStream *ous;		// output stream buffer
  int w_prepared;		// flag used to enable writer
  /* log file tracking */
  long long fseq;		// log file start seq
  logMmapEntry *log;		// log
  int n_skip;			// skip size
  int n_need;			// need size to determine message
  int is_msg;			// skipped size need to be copyied to ous
  char tbuf[64];		// tracking buffer
  int tbuf_pos;			// tracking buffer position
} migStruct;
#define init_mig_conn(m,r) do { \
  (m)->rep = r;                 \
  (m)->dest_host = NULL;        \
  (m)->dest_base_port = -1;     \
  (m)->from_seq = -1;           \
  aseq_init(&(m)->buffered);    \
  aseq_init(&(m)->sent);        \
  aseq_init(&(m)->acked);       \
  (m)->filter = NULL;           \
  (m)->num_part = 0;            \
  (m)->tps_limit = 0;           \
  (m)->el = NULL;               \
  (m)->fd = -1;                 \
  (m)->teid = -1LL;             \
  (m)->curr_time.tv_sec = 0;    \
  (m)->curr_time.tv_usec = 0;   \
  (m)->ibp = NULL;              \
  (m)->ous = NULL;              \
  (m)->w_prepared = 0;          \
  (m)->fseq = -1LL;             \
  (m)->log = NULL;		\
  (m)->n_skip = 0;		\
  (m)->n_need = 0;		\
  (m)->is_msg = 0;		\
  (m)->tbuf[0] = '\0';		\
  (m)->tbuf_pos = 0;		\
} while (0)

#define tv_diff(a,b) \
  (((a)->tv_sec - (b)->tv_sec) * 1000 + ((a)->tv_usec - (b)->tv_usec) / 1000)

/*
 * app log
 */

#define LOG_TEMPLATE(l, ...) do {        \
  if((l) <= s_Server.log_level)          \
    log_msg(&s_Server, l,__VA_ARGS__);   \
} while (0)

/* 
 * slow log related macros 
 */
#define DEF_SLOWLOG_CAP 1000
#define DEF_SLOWLOG_NUM 4

#if 1
#define SLOWLOG_DECLARE(s1,s2)    \
  char _s1 = s1;                  \
  char _s2 = s2;                  \
  struct timeval _st;             \
  struct timeval _et

#define SLOWLOG_ENTER() do {      \
  gettimeofday(&_st, NULL);       \
} while (0)

#define SLOWLOG_LEAVE(_rep) do {                                         \
  int _duration;                                                         \
  gettimeofday(&_et, NULL);                                              \
  _duration = tvdiff_usec(&_et, &_st) / 1000;                            \
  slowlog_add((_rep)->slow[0], msec_from_tv(&_st), _duration, _s1, _s2); \
} while(0)
#else
#define SLOWLOG_DECLARE(s1,s2) char _unused
#define SLOWLOG_ENTER() _unused = 1
#define SLOWLOG_LEAVE(_rep) _unused = 0
#endif

/* ---------- */
/* Replicator */
/* ---------- */
struct smrReplicator_
{
  /* general conf */
  int need_rckpt;		// is remote checkpoint needed? can be set in LCONN state
  long long role_id;		// initialized with uptime in msec and incremented at every role change
  repRole role;			// master or slave
  int base_port;		// base listen port (+0: local, +1: client, +2: slave, +3: mgmt)
  int local_port;		// listen port for local client
  int local_lfd;		// socket fd
  int client_port;		// listen port for replicator client (role MASTER)
  int client_lfd;		// socket fd
  int slave_port;		// listen port for slave replicator (role MASTER)
  int slave_lfd;		// socket fd
  int mgmt_port;		// listen port for management
  int mgmt_lfd;			// socket fd
  char *log_dir;		// log directory
  // minimum number of slave nodes for master to publish commit_seq w.r.t number of slaves
  int commit_quorum;
  int has_quorum_members;	// has quorum members
  short quorum_members[SMR_MAX_SLAVES];	// quourm members. valid when has_quorum_mebers is 1
  char *master_host;		// master host (or NULL if it is a master)
  int master_base_port;		// master base port (or -1 if it is a master)
  /* smr log related */
  smrLog *smrlog;		// smr log handle
  /* */
  logMmapEntry *curr_log;	// current log entry used by replicator for logging
  smrAtomicSeq min_seq;		// minimum sequncce number available
  long long be_ckpt_seq;	// checkpoint sequence of the backend. 0 if it is not known.
  long long commit_seq;		// committed sequence number to the log file
  long long last_commit_msg_end;	// sequnce number of last commit message end position
  int num_trailing_commit_msg;	// number of trailing commit messages
  smrAtomicSeq log_seq;		// logged seq. (bio thread can see this)
  smrAtomicSeq sync_seq;	// synched seq. (replicator can see this)
  smrAtomicSeq cron_est_min_seq;	// minimun sequence estimated by server cron (log file boundary)
  smrAtomicSeq mig_seq;		// upto which migration log caught-up
  int log_delete_gap;		// checkpointed log delete gap in seconds from now
  smrAtomicSeq log_delete_seq;	// checkpointed log delete sequence
  /* general */
  short nid;			// node id of this replicator
  struct timeval stat_time;	// stat report time
  repStat stat;			// statistics
  /* local */
  localConn *local_client;	// local client
  /* salve only */
  masterConn *master;		// (slave) master replicator
  /* client */
  long long last_timestamp;	// last timestamp that master replicator issues (non decreasing)
  /* master only */
  dlisth clients;		// list haed for clientConn
  volatile int num_slaves;	// number of current slaves
  dlisth slaves;		// list head for slaveConn
  int slave_idle_to;		// slave idle timeout in msec.
  /* management */
  dlisth managers;		// management clients
  /* event loop */
  long long teid;		// server cron job timer
  aeEventLoop *el;		// main event loop
  long long cron_count;		// event loop count
  volatile int interrupted;	// set by sig handler
  int cron_need_publish;	// cron job should publish commit seq
  int cron_need_progress;	// cron job should append dummy message if needed
  long long progress_seq;	// dummy message appended if current log_seq is l.e. this value
  /* logging & debugging */
  int app_log_prefix_size;	// application log file prefix size
  char app_log_buf[256];	// application log file buffer
  int app_log_size;		// current application log size
  FILE *app_log_fp;		// log file pointer
  int log_level;		// log level
  char ebuf[EBUFSZ];		// temporary error buffer
  /* temporary fields for each handlers */
  pthread_key_t log_key;
  /* bio thread */
  volatile int bio_interrupted;	// set when role is changed to NONE mode
  pthread_t *bio_thr;		// background io thread
  /* slow log */
  slowLog *slow[DEF_SLOWLOG_NUM];	// 1, 10, 100, 1000 ... msec
  /* log unmapping */
  pthread_mutex_t unmaph_mutex;	// unmap entry header accessor
  dlisth unmaph;		// unmap entry header
  /* migration related */
  volatile int mig_interrupted;	// set 1 to stop mig thread
  pthread_t *mig_thr;		// migration thread
  migStruct *mig;		// migration structure
  long long mig_id;		// migration ID. valid if mig is not null
  int mig_ret;			// error code. 0 if successful          
  char mig_ebuf[1024];		// error message
  /* ACL related */
  ipacl_t *acl;			// black list
  char cip[64];			// temporary buffer used in accept handlers (0..3)
  int is_bad_client;		// flag used accept ACL handling
};

#define UNMAPH_LOCK(r) pthread_mutex_lock(&(r)->unmaph_mutex)
#define UNMAPH_UNLOCK(r) pthread_mutex_unlock(&(r)->unmaph_mutex)

#define ENTRY_LOCK(e) pthread_mutex_lock(&(e)->mutex)
#define ENTRY_UNLOCK(e) pthread_mutex_unlock(&(e)->mutex)

/* 
 * segPeek is used to find segments of stream that will be used later.
 * No more than NUM_SEG_PEEK segments is permitted
 */
#define NUM_SEG_PEEK 2
struct segPeek
{
  struct
  {
    char *buf;
    int sz;
  } seg[NUM_SEG_PEEK];
};
#define init_seg_peek(s) do {         \
  int i;                              \
  for(i = 0; i < NUM_SEG_PEEK; i++) { \
    (s)->seg[i].buf = NULL;           \
    (s)->seg[i].sz = 0;               \
  }                                   \
} while (0)

/* 
 * Helper macros for buffered stream read processing in a function
 * Assumes...
 *
 *   int bsr_avail (local variable)
 *   int bsr_used  (local variable)
 *   bsr_done      (label)
 *   bsr_error     (label)
 */

#define BSR_READ(rep, s,buf,sz, sp) do {  \
  int ret;                                \
                                          \
  if(bsr_avail < sz) {                    \
     goto bsr_done;                       \
  }                                       \
  ret = stream_read(s,buf,sz,0, sp);      \
  if(ret == -1)                           \
    {                                     \
      goto bsr_error;                     \
    }                                     \
  assert(ret == sz);                      \
  bsr_used += sz;                         \
  bsr_avail -= sz;                        \
} while(0)

#define BSR_COMMIT(s) do {            \
  int ret;                            \
                                      \
  ret = io_stream_purge(s,bsr_used);  \
  if(ret == -1)                       \
    {                                 \
      goto bsr_error;                 \
    }                                 \
  bsr_used = 0;                       \
} while(0)

#define BSR_ROLLBACK(s) do {          \
  int ret;                            \
                                      \
  ret = io_stream_reset_read(s);      \
  if(ret == -1)                       \
    {                                 \
      goto bsr_error;                 \
    }                                 \
  bsr_avail += bsr_used;              \
  bsr_used = 0;                       \
} while(0)

/* check sequence w.r.t. buffer position within a mmapped log file */
#define CHECK_SMRLOG_BUF(a,buf,seq) do {                                   \
  long long sq_ = seq;                                                     \
  assert (((char *)buf - (char *)a->addr) == (sq_ - seq_round_down(sq_))); \
} while(0)


/* fault injection related structures */
typedef struct
{
  int count;
  int per_sleep_msec;
} fiDelay;

typedef struct
{
  int conn_type;
  int count;
  int per_sleep_msec;
} fiRwDelay;

typedef struct
{
  int conn_type;
  int rval;
  int erno;
} fiRwReturn;

typedef struct
{
  connType type;
  int fd;
  long long sid;
} logSession;
#define init_log_session(s) do { \
  (s)->type = NO_CONN;           \
  (s)->fd = 0;                   \
  (s)->sid = 0LL;                \
} while(0)

/* -------- */
/* EXPORTED */
/* -------- */
extern smrReplicator s_Server;

/* bio.c */
extern void *bio_thread (void *);

/* util.c */
extern void smr_panic (char *msg, char *file, int line);
extern void aseq_init (smrAtomicSeq * aseq);
extern long long aseq_get (smrAtomicSeq * aseq);
extern long long aseq_set (smrAtomicSeq * aseq, long long seq);
extern long long aseq_add (smrAtomicSeq * aseq, int delta);
extern long long usec_from_tv (struct timeval *tv);
extern long long msec_from_tv (struct timeval *tv);
extern long long currtime_ms (void);
extern long long tvdiff_usec (struct timeval *et, struct timeval *st);

/* applog.c */
extern int applog_init (smrReplicator * rep);
extern void applog_term (smrReplicator * rep);
extern void applog_enter_session (smrReplicator * rep, connType conn_type,
				  int conn_fd);
extern void applog_leave_session (smrReplicator * rep);

extern void log_msg (smrReplicator * rep, int level, char *fmt, ...);
extern char *get_command_string (char cmd);


//borrowed from redis/src/config.h
#if (__i386 || __amd64 || __powerpc__) && __GNUC__
#define GNUC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if (defined(__GLIBC__) && defined(__GLIBC_PREREQ))
#if (GNUC_VERSION >= 40100 && __GLIBC_PREREQ(2, 6))
#define HAVE_MEMFENCE
#endif
#endif
#endif

#if !defined(HAVE_MEMFENCE)
#error "Need glibc and gcc version that supports __sync_synchronize"
#endif

#endif

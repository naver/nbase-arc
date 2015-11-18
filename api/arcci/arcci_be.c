#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#pragma warning(push)
#pragma warning(disable : 4244 4267)	// for pointer offset operation, for converting datatype from size_t to int
#endif
#include "fmacros.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <stddef.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#ifdef _WIN32
#include "win32fixes.h"
#include <time.h>
#else
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#endif

#include "arcci_common.h"
#include "zk/c/include/zookeeper.h"
#include "anet.h"

/* 
  Design note of nbase ARC C API (arcci)
  - See this comment first before reading the source.
 
  +----------+
  | OVERVIEW |
  +----------+
  - Front-end (FE), back-end (BE) separation
  - FE
    + Thread safe handle. Multiple concurrent user threads can use FE 
    + User thread makes ARC job, create REDIS request in the job structure, and parse 
      REDIS response in ARC job into replies after completion
  - BE
    + Single-threaded
    + Event driven processing of ARC job and other back-end jobs
    + Send REDIS request to proper gateway and get's REDIS response then completes ARC job
  - ARC job 
    + One kind of back-end job (BE_JOB_ARC)
    + Composed of REDIS request, response, notification, and error information
    + Made by FE and its reference (pointer) is sent to BE via pipe (2)
    + Completion is notified by BE via pthread_signal method
    + Lifetime is managed by reference counting
 
  +-------------------------+
  | BACKEND JOB DESCRIPTION |
  +-------------------------+
  BE_JOB_INIT
    Used in creation of arc_t handle
 
  BE_JOB_ARC
    Used to process REDIS request. It contains request and response information
 
  BE_JOB_NOOP
    This job is used to preserve some information when an ARC job must be completed
    with error (timeout, connection, etc.)
   
    Example)
    ARC jobs in conn->sent_jobs has time-out error, this must be completed
    with error and others must go on. In this case NOOP job is created and retains 
    information about how many REDIS responses are remained for its correspondent job.
 
  BE_JOB_RECONNECT
    This job is to manage gateway connection.
    1) At creation, BE_JOB_RECONNECT job is registered in HTW to handle connection timeout.
       When the connection is established successfully (connected and ping/pong has done),
       this job is completed.
    2) When connection error occurred (connection closed etc.) and the gateway is USED state,
       RECONNECT job is registered to retry to connect. this repeats.
    Note: connection management for more information
 
  Lifetime management of the back-end job is done by reference counting
    If FE creates the job, reference count is protected by ref_mutex defined in it
    If BE creates the job, reference count is managed without mutex because back-end is single-threaded
 
  Owner (reference holder) of the back-end jobs.
   1) conn->jobs            (ARC, NOOP) (linked via job->head)
   2) conn->sent_jobs       (ARC, NOOP) (linked via job->head)
   3) conn->retry_arc_jobs  (ARC, NOOP) (linked via job->head) 
   4) be->init_job          (INIT)      (pointer)
   5) be->htw               (ARC)       (linked via job->htw_head. see HTW)
   Note: connection has weak (does not have reference count) pointer to the RECONNECT job
         
  +--------------------------+
  | HASHED TIMER WHEEL (HTW) |
  +--------------------------+
  To manage timeout in back-end thread, hashed timer wheel is used.
  Hashed timer wheel is a set of time slots each of them is actually a doublely linked list header.
  Every ARC job is linked (via htw_head field of the job) 
 
  +-----------------------+
  | CONNECTION MANAGEMENT |
  +-----------------------+
  connection (gw_conn_t) is binded to gateway (gw_t) at creation and its linkage to gateway is not changed
  until destruction.
 
  gateway state is USED when it is a member of current gateway list of the cluster, and UNUSED if not.
  connection state is NONE, CONNECTING, OK, CLOSING, CLOSED (see gw_conn_state_t definition).
 
  Follow state changes are life cycle of connection (gateway state, connection state) 
  gw_add_conn function is the start point of gateway addition
    - when gateway list is given it is constructed at the time of handle creation
    - when zookeeper is used, it is initialized when zookeeper handler gets the gateway list

  [1]
  gw_add_conn (USED, NONE) 
  |
  +-> gw_conn_try_to_connect 
      | (try to register conn_send_ping event handler)
      |
      +-> Y (USED, CONNECTING) --> register RECONNECT JOB (used as a connection timeout)
      +-> N  register RECONNECT JOB (used as a retry)

  [2]
  RECONNECT JOB (in HTW) --> gw_conn_reset_for_schedule --> gw_conn_try_to_connect

  [3]
  conn_send_ping (USED | CLOSING, CONNECTING)
  |  (send jping command to the gateway)
  |
  +-> sent, try to register conn_recv_pong
  +-> otherwise, gw_conn_try_reincarnate 

  conn_recv_pong (USED | CLOSING, CONNECTING)
  |  (receive +PONG message from the gateway)
  |
  +--> received, tgry to register gw_conn_read_handler (USED, OK) 
  +--> otherwise, gw_conn_try_reincarnate

  [4]
  gw_conn_read_hander, gw_conn_write_handler processes REDIS request (USED, OK)
    if any failure --> gw_conn_try_reincarnate

  +--------------------+
  | GATEWAY MANAGEMENT |
  +--------------------+
  When gateway list are given, no gateway addition/removal is allowed. (always USED)
  When zookeeper integration is enabled, gateway can be added/removed when notified by zookeeper.

  ==== GATEWAY ADDITION 
  same as gw_add_conn [1]


  ==== GATEWAY DELETION
  gateway state is set to UNUSED and every connection of the gateway is marked as CLOSING
  i.e. (UNUSED, CLOSING)

  UNUSED gateway is removed from be->gw_ary structure and the removal of the gateway
  is done at be cron.conne 

  (initiated by zookeeper notification)
  for each conns call gw_conn_try_to_close ()
    - remove HTW reconnect job if exists
    - links connection (via gc_head field) be->gabage_conns list head if nothing to handle
      (i.e. no ARC jobs in conn->jobs or conn->sent_jobs. NOOP jobs can be in the list)

  for every job completion of ARC
    - if connection state is CLOSING, gw_conn_try_to_close is called.
      (UNUSED, NONE | CONNECTING) --> moved to be->garbage_conn because there is no jobs in it.
      (UNUSED, OK) --> there is at least one ARC job --> complete_be_job handles this case

  be_cron periodically checks be->gabage_conns and calls gw_conn_gc ()
    - destroy_gw_conn () and if NO conn in the gateway then destroy_gw ()

  +-----------------------+
  | ZOOKEEPER INTEGRATION |
  +-----------------------+
  zookeeper is processed by be_zk_cron 

 */

#define BE_ERROR(err) errno_point(be, err,__FILE__,__LINE__)

typedef struct be_parse_ctx_s be_parse_ctx_t;
typedef struct gw_completion_s gw_completion_t;

struct be_parse_ctx_s
{
  int count;
};

struct gw_completion_s
{
  short id;
  long version;
  be_t *be;
};
#define init_gw_completion(c) do {             \
  (c)->id = 0;                                 \
  (c)->version = 0;                            \
  (c)->be = NULL;                              \
} while (0)

/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */

/* application logging */
#define LOG_ERROR(be,...) do {                                                                      \
    if (be->conf.log_level >= ARC_LOG_LEVEL_ERROR) log_msg (be, ARC_LOG_LEVEL_ERROR, __VA_ARGS__);  \
} while (0)

#define LOG_WARN(be,...) do {                                                                       \
    if (be->conf.log_level >= ARC_LOG_LEVEL_WARN) log_msg (be, ARC_LOG_LEVEL_WARN, __VA_ARGS__);    \
} while (0)

#define LOG_INFO(be,...) do {                                                                       \
    if (be->conf.log_level >= ARC_LOG_LEVEL_INFO) log_msg (be, ARC_LOG_LEVEL_INFO, __VA_ARGS__);    \
} while (0)

#define LOG_DEBUG(be,...) do {                                                                      \
    if (be->conf.log_level >= ARC_LOG_LEVEL_DEBUG) log_msg (be, ARC_LOG_LEVEL_DEBUG, __VA_ARGS__);  \
} while (0)
static void log_msg (be_t * be, arc_log_level_t level, char *fmt, ...);
static char *arc_job_state2str (arc_job_state_t state);
static char *be_job_type2str (be_job_type_t type);
static int format_binary (char *buf, int bufsz, char *data, int data_size);
static void debug_log_be_job (be_t * be, be_job_t * job, int err);

/* utilities */
static char *format_conn (be_t * be, gw_conn_t * conn);
static char *format_gw (be_t * be, gw_t * gw);
static void errno_point (be_t * be, int err, char *file, int line);
static char *expect_token (char *bp, int c);
static char *expect_number (char *bp);
static int json_peek_string (be_t * be, const char *json, int len,
			     char *field, char **ret_ptr, int *ret_len);
static int json_peek_integer (be_t * be, const char *json, int len,
			      char *field, char **ret_ptr, int *ret_len);
static int gw_apply_completion (be_t * be, gw_t * gw, gw_completion_t * gwc,
				const char *value, int value_len);

/* 
 * NOTE:
 * Every backend functions has the reference to the backend for error reporting.
 * Exceptions are callback.
 */
/* gateway management */
static void gw_conn_change_state (be_t * be, gw_conn_t * conn, int to_state);
static gw_t *create_gw_raw (be_t * be, short id, long target_version);
static void destroy_gw (be_t * be, gw_t * gw);
static int gw_conn_try_to_connect (be_t * be, gw_conn_t * conn);
static int gw_add_conn (be_t * be, gw_t * gw, int dedicated);
static int make_connection_pool (be_t * be);
static gw_conn_t *create_gw_conn (be_t * be, gw_t * gw, int dedicated);
static void destroy_gw_conn (be_t * be, gw_conn_t * gw_conn);
static void conn_send_ping_wrap (aeEventLoop * el, socket_t fd,
				 void *clientData, int mask);
static void conn_send_ping (aeEventLoop * el, socket_t fd,
			    void *clientData, int mask);
static void conn_recv_pong_wrap (aeEventLoop * el, socket_t fd,
				 void *clientData, int mask);
static void conn_recv_pong (aeEventLoop * el, socket_t fd, void *clientData,
			    int mask);
static long long gw_conn_get_busy_cost (gw_conn_t * conn, long long curr);
static long long gw_conn_get_affinity_cost (gw_conn_t * conn, arc_job_t * aj);
static int gw_conn_cost (gw_conn_t * conn, long long curr, arc_job_t * aj,
			 int *rqst_cost);
static void gw_conn_read_handler_wrap (aeEventLoop * el, socket_t fd,
				       void *clientData, int mask);
static void gw_conn_read_handler (aeEventLoop * el, socket_t fd,
				  void *clientData, int mask);
static int gw_conn_process_ibuf (be_t * be, gw_conn_t * conn);
static int gw_conn_prepare_write (be_t * be, gw_conn_t * conn);
static void gw_conn_write_handler_wrap (aeEventLoop * el, socket_t fd,
					void *clientData, int mask);
#ifdef _WIN32
static void gw_conn_write_completion_handler (aeEventLoop * el, socket_t fd,
					      void *req, int mask);
#endif
static void gw_conn_write_handler (aeEventLoop * el, socket_t fd,
				   void *clientData, int mask);
static void gw_conn_reset_for_schedule (be_t * be, gw_conn_t * conn, int err);
static void gw_conn_try_reincarnate (be_t * be, gw_conn_t * conn, int err);
static void gw_conn_try_to_close (be_t * be, gw_conn_t * conn);
static void gw_conn_gc (be_t * be, gw_conn_t * conn);
static int gw_conn_safe_to_abandon (be_t * be, gw_conn_t * conn);

/* redis parser callback implementation */
static int be_on_string (void *ctx, char *s, int len, int is_null);
static int be_on_status (void *ctx, char *s, int len);
static int be_on_error (void *ctx, char *s, int len);
static int be_on_integer (void *ctx, long long val);
static int be_on_array (void *ctx, int len, int is_null);

/* conditional job */
#ifndef _WIN32
static void complete_cond (be_t * be, cond_t * cj, int err);
#else
static void complete_cond (be_t * be, HANDLE * cj, int err);
#endif
static void complete_arc_job (be_t * be, arc_job_t * aj, int err);
static void reset_arc_job_for_retry (be_t * be, arc_job_t * aj);
static int
gw_find_best_conn (gw_t ** cand, int ncand, long long curr, arc_job_t * aj,
		   int *best_cost, int *best_rqst_cost,
		   gw_conn_t ** best_conn);
static gw_t *find_gw_if_active (be_t * be, short id);
static gw_t *peek_wgw (be_t * be, pn_gw_aff_t * aff);
static gw_t *peek_rgw (be_t * be, pn_gw_aff_t * aff, gw_t * wgw);
static int arc_job_try_alloc_conn (be_t * be, be_job_t * job,
				   dlisth * retry_head);

/* backend job */
static void complete_be_job (be_t * be, be_job_t * job, int err);

/* hashed timer wheel */
static long long htw_millis_to_tick (be_t * be, long long millis);
static void htw_add (be_t * be, be_job_t * job);
static void htw_del (be_t * be, be_job_t * job);
static int htw_make_reconnect_job (be_t * be, gw_conn_t * conn,
				   int to_millis);
static int htw_process_job (be_t * be, be_job_t * job);
static int htw_process_line (be_t * be, dlisth * head, long long curr);
static void htw_delete_jobs_in_line (be_t * be, dlisth * head);
static void htw_abort_jobs_in_line (be_t * be, dlisth * head);

/* backend */
static void destroy_be (be_t * be);
static void abort_be (be_t * be);
static void be_signal_exit (be_t * be, int err);

/* backend thread */
static void be_job_handler_wrap (aeEventLoop * el, pipe_t fd,
				 void *clientData, int mask);
static void be_job_handler (aeEventLoop * el, pipe_t fd,
			    void *clientData, int mask);
static void process_retry_arc_jobs (be_t * be);
static int be_cron_wrap (aeEventLoop * el, long long id, void *clientData);
static int be_cron (aeEventLoop * el, long long id, void *clientData);

/* zookeeper related */
static int be_zk_cron_wrap (aeEventLoop * el, long long id, void *clientData);
static int be_zk_cron (aeEventLoop * el, long long id, void *clientData);
static void zk_cleanup (be_t * be);
static void zk_force_reconn (be_t * be);
static void zk_try_to_connect (be_t * be);
static void zk_watcher (zhandle_t * zzh, int type, int state,
			const char *path, void *context);
static int zk_evl_top_half (be_t * be);
static void zk_read_handler_wrap (aeEventLoop * el, socket_t fd,
				  void *clientData, int mask);
static void zk_read_handler (aeEventLoop * el, socket_t fd, void *clientData,
			     int mask);
static void zk_write_handler_wrap (aeEventLoop * el, socket_t fd,
				   void *clientData, int mask);
static void zk_write_handler (aeEventLoop * el, socket_t fd, void *clientData,
			      int mask);
static int zk_evl_bottom_half (be_t * be);
static short get_short_val (char *idp);
static void zk_getchildren_completion (int rc,
				       const struct String_vector *strings,
				       const void *data);
static void zk_getgw_completion (int rc, const char *value, int value_len,
				 const struct Stat *stat, const void *data);
static gw_completion_t *create_gw_completion (be_t * be, short id,
					      long version);
static void destroy_gw_completion (gw_completion_t * gwc);

/* affinity */
static int aff_check_rle (char *rle);
static int
aff_data_parse_check_and_peek (be_t * be, char *buf, short gwid_ary[],
			       char *aff_ary[], int *n_item);
static void zk_getaffinity_completion (int rc, const char *value,
				       int value_len, const struct Stat *stat,
				       const void *data);

/* --------------- */
/* STATIC VARIABLE */
/* --------------- */
static redis_parse_cb_t beParseCb = {
  be_on_string,
  be_on_status,
  be_on_error,
  be_on_integer,
  be_on_array
};

static const char *const s_Log_level_str[] =
  { "NON", "ERR", "WRN", "INF", "DBG" };

static const char *const s_Handle_type_str[] =
  { "   ", "MAN", "CRN", "ZOO", "GW<", "GW>", "PP<", "PP>", "ZK<", "ZK>" };

#ifdef FI_ENABLED
int fi_Enabled = 0;
fi_t *be_Fi = NULL;
#endif

/* ------------------------- */
/* LOCAL FUNCTION DEFINITION */
/* ------------------------- */
static void
log_msg (be_t * be, arc_log_level_t level, char *fmt, ...)
{
  va_list ap;
  char msg_buf[8196];
  char time_buf[APP_LOG_MAX_SUFFIX_LEN];
  struct timeval tv;
  int off;
  int wsz;

  if (be == NULL)
    {
      return;
    }
  else if (level > be->conf.log_level)
    {
      return;
    }

  va_start (ap, fmt);
  vsnprintf (msg_buf, sizeof (msg_buf), fmt, ap);
  va_end (ap);

  gettimeofday (&tv, NULL);
#ifndef _WIN32
  off =
    strftime (time_buf, sizeof (time_buf), "%d %b %H:%M:%S.",
	      localtime (&tv.tv_sec));
#else
  __int64 tv_sec = tv.tv_sec;
  off =
    strftime (time_buf, sizeof (time_buf), "%d %b %H:%M:%S.",
	      localtime (&tv_sec));
#endif
  snprintf (time_buf + off, sizeof (time_buf) - off, "%03d",
	    (int) tv.tv_usec / 1000);


  /* lazy create log file */
  if (be->app_log_fp == NULL && be->app_log_prefix_size > 0)
    {
      char *cp = &be->app_log_buf[0] + be->app_log_prefix_size;

#ifndef _WIN32
      strftime (cp, APP_LOG_MAX_SUFFIX_LEN, "-%d-%b-%H-%M-%S",
		localtime (&tv.tv_sec));
#else
      strftime (cp, APP_LOG_MAX_SUFFIX_LEN, "-%d-%b-%H-%M-%S",
		localtime (&tv_sec));
#endif
      be->app_log_fp = fopen (be->app_log_buf, "a");
      if (be->app_log_fp == NULL)
	{
	  return;
	}
    }

  wsz =
    fprintf (be->app_log_fp ? be->app_log_fp : stdout, "%s [%s][%s] %s\n",
	     time_buf, s_Handle_type_str[be->handle_type],
	     s_Log_level_str[level], msg_buf);

  if (be->app_log_prefix_size > 0)
    {
      be->app_log_size += wsz;
      if (be->app_log_size > APP_LOG_ROTATE_LIMIT)
	{
	  fclose (be->app_log_fp);
	  be->app_log_fp = NULL;
	  be->app_log_size = 0;
	}
      else
	{
	  fflush (be->app_log_fp);
	}
    }
}

static char *
arc_job_state2str (arc_job_state_t state)
{
  switch (state)
    {
    case ARCJOB_STATE_NONE:
      return "none";
    case ARCJOB_STATE_SUBMITTED:
      return "submitted";
    case ARCJOB_STATE_CONN:
      return "conn";
    case ARCJOB_STATE_WAITING:
      return "wait";
    case ARCJOB_STATE_SENDING:
      return "sending";
    case ARCJOB_STATE_SENT:
      return "sent";
    case ARCJOB_STATE_COMPLETED:
      return "completed";
    default:
      return "unknown";
    }
}

static char *
be_job_type2str (be_job_type_t type)
{
  switch (type)
    {
    case BE_JOB_INIT:
      return "init";
    case BE_JOB_ARC:
      return "arc";
    case BE_JOB_NOOP:
      return "noop";
    case BE_JOB_RECONNECT:
      return "reconnect";
    case BE_JOB_ZK_TO:
      return "zk_to";
    default:
      return "unknown";
    }
}

static int
format_binary (char *buf, int bufsz, char *data, int data_size)
{
  int count = bufsz > data_size ? data_size : bufsz;
  int i;
  char *bp = buf;
  char *dp = data;

  for (i = 0; i < count; i++, bp++, dp++)
    {
      if (isprint (*dp))
	{
	  *bp = *dp;
	}
      else
	{
	  *bp = '.';
	}
    }
  return count;
}

#define FMT_SEG_PRINTF(...) do {                          \
  int ret = snprintf(bp, ep-bp, __VA_ARGS__);             \
  if (ret == -1 || ret > ep - bp)                         \
    {                                                     \
      goto done;                                          \
    }                                                     \
  bp += ret;                                              \
} while (0)

static void
debug_log_be_job (be_t * be, be_job_t * job, int err)
{
  char buf[2048];
  char *bp = &buf[0];
  char *ep = bp + sizeof (buf) - 8;	// 8: (skip)\n<null>
  int incomplete = 1;

  /* debug log BE job */
  FMT_SEG_PRINTF
    ("JOB completed. err:%d type:%s timeout:%lld refcnt:%d cost:%d ", err,
     be_job_type2str (job->type), job->to, job->ref_count, job->cost);
  if (job->conn != NULL)
    {
      FMT_SEG_PRINTF ("conn:%s", format_conn (be, job->conn));
    }
  FMT_SEG_PRINTF ("\n");

  /* job specific log */
  if (job->type == BE_JOB_ARC)
    {
      int ret;
      arc_job_t *aj = (arc_job_t *) job->data;

      FMT_SEG_PRINTF ("\tARC JOB state:%s pred:%d err:%d\n",
		      arc_job_state2str (aj->state), aj->pred, aj->err);
      FMT_SEG_PRINTF ("\tRQST ncmd:%d is_write:%d crc:%d sent_cnt:%d\n",
		      aj->rqst.ncmd, aj->rqst.is_write, aj->rqst.crc,
		      aj->rqst.sent_cnt);
      if (aj->rqst.obuf)
	{
	  int bsz;

	  FMT_SEG_PRINTF ("\tRQST size:%lld data:",
			  (long long) sdslen (aj->rqst.obuf));
	  bsz = ep - bp > 64 ? 64 : ep - bp;
	  ret =
	    format_binary (bp, bsz, aj->rqst.obuf, sdslen (aj->rqst.obuf));
	  bp += ret;
	  FMT_SEG_PRINTF ("\n");
	}
      if (aj->resp.ibuf)
	{
	  int bsz;

	  FMT_SEG_PRINTF ("\tRESP size:%lld data:",
			  (long long) sdslen (aj->resp.ibuf));
	  bsz = ep - bp > 64 ? 64 : ep - bp;
	  ret =
	    format_binary (bp, bsz, aj->resp.ibuf, sdslen (aj->resp.ibuf));
	  bp += ret;
	  FMT_SEG_PRINTF ("\n");
	}
    }
  incomplete = 0;
done:
  if (incomplete)
    {
      strcpy (bp, "(skip)\n");
    }
  LOG_DEBUG (be, "%s", buf);
}


/* 
 * NOTE: format_conn uses buffer area of the be_t,
 * so it is not safe both of them in single printf call 
 */
static char *
format_conn (be_t * be, gw_conn_t * conn)
{
  if (conn == NULL || conn->gw == NULL)
    {
      return "[null]";
    }
  else
    {
      gw_t *gw = conn->gw;

      be->tmp_buf[0] = '\0';
      snprintf (be->tmp_buf, sizeof (be->tmp_buf),
		"[%s:%d(fd:%d,id:%d,vn:%ld)]",
		gw->host ? gw->host : "<null>", gw->port, conn->fd,
		gw->id, gw->gwl_version);
      return be->tmp_buf;
    }
}

/* 
 * NOTE: format_gw uses buffer area of the be_t,
 * so it is not safe both of them in single printf call 
 */
static char *
format_gw (be_t * be, gw_t * gw)
{
  if (gw == NULL)
    {
      return "[null]";
    }
  else
    {
      be->tmp_buf[0] = '\0';
      snprintf (be->tmp_buf, sizeof (be->tmp_buf),
		"[%s:%d(id:%d,vn:%ld)]", gw->host ? gw->host : "<null>",
		gw->port, gw->id, gw->gwl_version);
      return be->tmp_buf;
    }
}

static void
errno_point (be_t * be, int err, char *file, int line)
{
  if (be->be_errno == 0)
    {
      be->be_errno = err;
    }
}

static void
gw_conn_change_state (be_t * be, gw_conn_t * conn, int to_state)
{
  LOG_DEBUG (be, "Connection %s: state changed %d->%d",
	     format_conn (be, conn), conn->state, to_state);
  conn->state = to_state;
}

static gw_t *
create_gw_raw (be_t * be, short id, long target_version)
{
  gw_t *gw;

  BE_FI (gw, NULL, 0);
  gw = malloc (sizeof (gw_t));
  FI_END ();
  if (gw == NULL)
    {
      BE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_gw (gw);
  gw->id = id;
  gw->state = GW_STATE_UNUSED;
  gw->gwl_version = target_version;

  return gw;
}

static void
destroy_gw (be_t * be, gw_t * gw)
{

  if (gw == NULL)
    {
      return;
    }

  //LOG_DEBUG (be, "Destroy gateway structure:%s", format_gw (be, gw));
  while (!dlisth_is_empty (&gw->conns))
    {
      dlisth *h;

      h = gw->conns.next;
      dlisth_delete (h);
      destroy_gw_conn (be, (gw_conn_t *) h);
    }

  if (gw->host != NULL)
    {
      free (gw->host);
    }

  free (gw);
}

static int
gw_conn_try_to_connect (be_t * be, gw_conn_t * conn)
{
  char err[ANET_ERR_LEN];
  socket_t fd;
  int ret;

  assert (conn->conn_to_job == NULL);
  fd = anetTcpNonBlockConnect (err, conn->gw->host, conn->gw->port);
  if (fd != -1)
    {
      ret =
	aeCreateFileEvent (be->el, fd, AE_WRITABLE, conn_send_ping_wrap,
			   conn);
      if (ret != AE_ERR)
	{
	  assert (conn->fd == -1);
	  conn->fd = fd;
	  gw_conn_change_state (be, conn, GW_CONN_CONNECTING);
	  goto done;
	}
#ifndef _WIN32
      LOG_WARN (be, "Failed to connect:%s ret=%d errno=%d",
		format_conn (be, conn), ret, errno);
#else
      LOG_WARN (be, "Failed to connect:%s ret=%d errno=%d winerrno=%d",
		format_conn (be, conn), ret, errno, WSAGetLastError ());
#endif
      close_socket (fd);
    }
  else
    {
      LOG_WARN (be, "Failed to connect %s", format_conn (be, conn));
    }

  LOG_DEBUG (be, "Register reconnect job triggered after %d msec.",
	     be->conf.conn_reconnect_millis);

done:
  /* NOTE: no automatic fault injection (this failure ends the back-end) */
  ret = htw_make_reconnect_job (be, conn, be->conf.conn_reconnect_millis);
  if (ret == -1)
    {
      BE_ERROR (ARC_ERR_GENERIC);
      return -1;
    }
  return 0;
}

static int
gw_add_conn (be_t * be, gw_t * gw, int dedicated)
{
  gw_conn_t *conn;

  conn = create_gw_conn (be, gw, dedicated);
  if (conn == NULL)
    {
      BE_ERROR (ARC_ERR_GENERIC);
      return -1;
    }
  dlisth_insert_before (&conn->head, &gw->conns);

  return gw_conn_try_to_connect (be, conn);
}

static int
make_connection_pool (be_t * be)
{
  int i, j;
  int ret;

  for (i = 0; i < be->n_gw; i++)
    {
      gw_t *gw = be->gw_ary[i];

      for (j = 0; j < be->conf.num_conn_per_gw; j++)
	{
	  ret = gw_add_conn (be, gw, (j == 0));
	  if (ret == -1)
	    {
	      BE_ERROR (ARC_ERR_GENERIC);
	      return -1;
	    }
	}
    }
  return 0;
}

static gw_conn_t *
create_gw_conn (be_t * be, gw_t * gw, int dedicated)
{
  gw_conn_t *conn;

  conn = malloc (sizeof (gw_conn_t));
  if (conn == NULL)
    {
      BE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }
  init_gw_conn (conn);
  conn->be = be;
  conn->gw = gw;
  conn->dedicated = dedicated;
  return conn;
}

static void
destroy_gw_conn (be_t * be, gw_conn_t * conn)
{
  if (conn == NULL)
    {
      return;
    }

  dlisth_delete (&conn->head);
  dlisth_delete (&conn->gc_head);

  if (conn->fd != -1)
    {
      aeDeleteFileEvent (be->el, conn->fd, AE_READABLE | AE_WRITABLE);
      close_socket (conn->fd);
    }

  if (conn->conn_to_job != NULL)
    {
      complete_be_job (be, conn->conn_to_job, 0);
    }

  while (!dlisth_is_empty (&conn->jobs))
    {
      dlisth *h;
      h = conn->jobs.next;
      dlisth_delete (h);
      complete_be_job (be, (be_job_t *) h, be->exit_errno);
    }

  while (!dlisth_is_empty (&conn->sent_jobs))
    {
      dlisth *h;
      h = conn->sent_jobs.next;
      dlisth_delete (h);
      complete_be_job (be, (be_job_t *) h, be->exit_errno);
    }

  if (conn->ibuf != NULL)
    {
      sdsfree (conn->ibuf);
    }

  assert (conn->total_cost == 0);
  free (conn);
}

static void
conn_send_ping_wrap (aeEventLoop * el, socket_t fd, void *clientData,
		     int mask)
{
  gw_conn_t *conn = (gw_conn_t *) clientData;
  be_handle_type_t saved = conn->be->handle_type;

  conn->be->handle_type = BE_HANDLE_TYPE_GW_OUT;
  conn_send_ping (el, fd, clientData, mask);
  conn->be->handle_type = saved;
}

static void
conn_send_ping (aeEventLoop * el, socket_t fd, void *clientData, int mask)
{
  gw_conn_t *conn;
  int reconnect;
  int ret;
  be_t *be;

  conn = (gw_conn_t *) clientData;

  be = conn->be;
  be->be_errno = 0;
  if (be->be_exit)
    {
      return;
    }

  if (conn->bp == &conn->buf[0])
    {
      strcpy (conn->buf, "PING\r\n");
      conn->ep = conn->bp + 6;
    }

  reconnect = 0;
  while (1)
    {
      int avail, nw;

      avail = conn->ep - conn->bp;
      BE_FI (nw, -1, EPIPE, 0);
#ifndef _WIN32
      nw = write (fd, conn->bp, avail);
#else
      nw = send (fd, conn->bp, avail, 0);
      if (nw == SOCKET_ERROR)
	nw = -1;
#endif
      FI_END ();
      if (nw == -1)
	{
	  if (errno == EAGAIN)
	    {
	      break;
	    }
	  else
	    {
	      BE_ERROR (ARC_ERR_SYSCALL);
	      reconnect = 1;
	      break;
	    }
	}
      else if (nw == 0)
	{
	  BE_ERROR (ARC_ERR_CONN_CLOSED);
	  LOG_WARN (be, "Connection closed:%s", format_conn (be, conn));
	  reconnect = 1;
	  break;
	}

      conn->bp += nw;
      if (conn->bp == conn->ep)
	{
	  /* ping request is sent */
	  LOG_DEBUG (be, "Ping sent:%s", format_conn (be, conn));
	  conn->bp = conn->ep = &conn->buf[0];
	  aeDeleteFileEvent (be->el, fd, AE_WRITABLE);

	  BE_FI (ret, AE_OK + 1, 0);
	  ret =
	    aeCreateFileEvent (be->el, fd, AE_READABLE, conn_recv_pong_wrap,
			       conn);
	  FI_END ();
	  if (ret != AE_OK)
	    {
	      BE_ERROR (ARC_ERR_NOMEM);
	      reconnect = 1;
	    }
	  break;
	}
    }

  if (reconnect)
    {
      gw_conn_try_reincarnate (be, conn, be->be_errno);
    }
  return;
}

static void
gw_conn_try_to_close (be_t * be, gw_conn_t * conn)
{
  assert (conn->state == GW_CONN_CLOSING);

  if (conn->conn_to_job)
    {
      complete_be_job (be, conn->conn_to_job, 0);
    }

  if (gw_conn_safe_to_abandon (be, conn))
    {
      /* we can not call gw_conn_try_reincarnate because complete_be_job (within conn) can call me */
      gw_conn_change_state (be, conn, GW_CONN_CLOSED);
      dlisth_insert_before (&conn->gc_head, &be->garbage_conns);
      return;
    }
}

static void
gw_conn_gc (be_t * be, gw_conn_t * conn)
{
  gw_t *gw;

  gw = conn->gw;
  destroy_gw_conn (be, conn);
  if (dlisth_is_empty (&gw->conns))
    {
      destroy_gw (be, gw);
    }
}

static int
gw_conn_safe_to_abandon (be_t * be, gw_conn_t * conn)
{
  dlisth *h;
  be_job_t *job;

  for (h = conn->jobs.next; h != &conn->jobs; h = h->next)
    {
      job = (be_job_t *) h;
      if (job->type != BE_JOB_NOOP)
	{
	  return 0;
	}
    }
  for (h = conn->sent_jobs.next; h != &conn->sent_jobs; h = h->next)
    {
      job = (be_job_t *) h;
      if (job->type != BE_JOB_NOOP)
	{
	  return 0;
	}
    }
  return 1;
}

static char *
expect_token (char *bp, int c)
{
  if (bp == NULL)
    {
      return NULL;
    }

  while (*bp && isspace (*bp))
    {
      bp++;
    }

  if (*bp == c)
    {
      return bp;
    }
  else
    {
      return NULL;
    }
}

static char *
expect_number (char *bp)
{
  if (bp == NULL)
    {
      return NULL;
    }

  while (*bp && isspace (*bp))
    {
      bp++;
    }

  if (isdigit (*bp))
    {
      return bp;
    }
  else
    {
      return NULL;
    }
}

#define MAX_FIELD_NAME_LEN 32
#define ERROR_IF_NULL(expr) if ((expr) == NULL) { BE_ERROR(ARC_ERR_BAD_ZKDATA); return -1; }
/*
 * NOTE: 
 * this function is somewhat weak in the sense that It does not 
 * peek all possible json format (object in object case), and does not handle
 * escape \"
 */
static int
json_peek_string (be_t * be, const char *json, int len, char *field,
		  char **ret_ptr, int *ret_len)
{
  int field_len;
  char buf[MAX_FIELD_NAME_LEN + 3];
  char *begin = NULL, *end = NULL;

  BE_FI (field_len, MAX_FIELD_NAME_LEN + 1, 0);
  field_len = strlen (field);
  FI_END ();
  if (field_len > MAX_FIELD_NAME_LEN)
    {
      BE_ERROR (ARC_ERR_BAD_ZKDATA);
      return -1;
    }
  sprintf (buf, "\"%s\"", field);

  ERROR_IF_NULL (begin = strstr (json, buf));
  begin += field_len + 2;

  ERROR_IF_NULL (begin = expect_token (begin, ':'));
  begin++;

  ERROR_IF_NULL (begin = expect_token (begin, '"'));
  begin++;

  ERROR_IF_NULL (end = strchr (begin, '"'));

  *ret_ptr = begin;
  *ret_len = (end - begin);
  return 0;
}

static int
json_peek_integer (be_t * be, const char *json, int len, char *field,
		   char **ret_ptr, int *ret_len)
{
  int field_len;
  char buf[MAX_FIELD_NAME_LEN + 3];
  char *begin = NULL, *end = NULL;

  BE_FI (field_len, MAX_FIELD_NAME_LEN + 1, 0);
  field_len = strlen (field);
  FI_END ();
  if (field_len > MAX_FIELD_NAME_LEN)
    {
      BE_ERROR (ARC_ERR_BAD_ZKDATA);
      return -1;
    }
  sprintf (buf, "\"%s\"", field);

  ERROR_IF_NULL (begin = strstr (json, buf));
  begin += field_len + 2;

  ERROR_IF_NULL (begin = expect_token (begin, ':'));
  begin++;

  ERROR_IF_NULL (begin = expect_number (begin));
  end = begin;
  while (isdigit (*end))
    {
      end++;
    }

  *ret_ptr = begin;
  *ret_len = (end - begin);
  return 0;
}

/* 
 * value format example:
 * {"ip":"127.0.0.1","port":6379}
 */
static int
gw_apply_completion (be_t * be, gw_t * gw, gw_completion_t * gwc,
		     const char *value, int value_len)
{
  char *host_part, *port_part;
  int host_part_len, port_part_len;
  char *host = NULL;
  int ret, port;
  char saved_char;
  int i;

  BE_FI (ret, -1, 0);
  ret =
    json_peek_string (be, value, value_len, "ip", &host_part, &host_part_len);
  FI_END ();
  if (ret == -1 || host_part_len < 1 || host_part_len > 255)
    {
      /* 255 is DNS name limit */
      BE_ERROR (ARC_ERR_BAD_ZKDATA);
      return -1;
    }

  BE_FI (ret, -1, 0);
  ret =
    json_peek_integer (be, value, value_len, "port", &port_part,
		       &port_part_len);
  FI_END ();
  if (ret == -1 || port_part_len < 1)
    {
      BE_ERROR (ARC_ERR_BAD_ZKDATA);
      return -1;
    }

  /* parse port */
  saved_char = port_part[port_part_len];
  port_part[port_part_len] = '\0';
  port = atoi (port_part);
  port_part[port_part_len] = saved_char;
  BE_FI (port, 0, 0);
  FI_END ();
  if (port < 0)
    {
      BE_ERROR (ARC_ERR_BAD_ZKDATA);
      return -1;
    }

  BE_FI (host, NULL, 0);
  host = malloc (host_part_len + 1);
  FI_END ();
  if (host == NULL)
    {
      BE_ERROR (ARC_ERR_NOMEM);
      return -1;
    }
  memcpy (host, host_part, host_part_len);
  host[host_part_len] = '\0';

  /* host value is transferred to thw 'gw' instance that is in be->gw_ary */
  gw->host = host;
  gw->port = port;
  gw->state = GW_STATE_USED;

  for (i = 0; i < be->conf.num_conn_per_gw; i++)
    {
      ret = gw_add_conn (be, gw, (i == 0));
      if (ret == -1)
	{
	  BE_ERROR (ARC_ERR_GENERIC);
	  return -1;
	}
    }

  return 0;
}

static void
conn_recv_pong_wrap (aeEventLoop * el, socket_t fd, void *clientData,
		     int mask)
{
  gw_conn_t *conn = (gw_conn_t *) clientData;
  be_handle_type_t saved = conn->be->handle_type;

  conn->be->handle_type = BE_HANDLE_TYPE_GW_IN;
  conn_recv_pong (el, fd, clientData, mask);
  conn->be->handle_type = saved;
}

static void
conn_recv_pong (aeEventLoop * el, socket_t fd, void *clientData, int mask)
{
  gw_conn_t *conn;
  int reconnect;
  int ret;
  be_t *be;
  int has_data;

  conn = (gw_conn_t *) clientData;

  be = conn->be;
  be->be_errno = 0;
  if (be->be_exit)
    {
      return;
    }

  reconnect = 0;
  while (1)
    {
      int nr;
      char buf[64];

      BE_FI (nr, -1, EINVAL, 0);
#ifndef _WIN32
      nr = read (fd, buf, sizeof (buf));
#else
      nr = recv (fd, buf, sizeof (buf), 0);
      if (nr == SOCKET_ERROR)
	nr = -1;
#endif
      FI_END ();
      if (nr == -1)
	{
	  if (errno == EAGAIN)
	    {
	      break;
	    }
	  else
	    {
	      BE_ERROR (ARC_ERR_SYSCALL);
	      reconnect = 1;
	      break;
	    }
	}
      else if (nr == 0)
	{
	  BE_ERROR (ARC_ERR_CONN_CLOSED);
	  reconnect = 1;
	  break;
	}

      BE_FI (has_data, 0, 0);
      has_data = ((conn->bp - &conn->buf[0]) + nr > 7);
      FI_END ();
      if (has_data)
	{
	  /* expects: +PONG\r\n */
	  BE_ERROR (ARC_ERR_GW_PROTOCOL);
	  reconnect = 1;
	  break;
	}
      memcpy (conn->bp, buf, nr);
      conn->bp += nr;
      *conn->bp = '\0';

      if (strcasecmp (conn->buf, "+pong\r\n") == 0)
	{
	  conn->bp = conn->ep = &conn->buf[0];
	  aeDeleteFileEvent (be->el, fd, AE_READABLE);
	  BE_FI (ret, AE_OK + 1, 0);
	  ret =
	    aeCreateFileEvent (be->el, conn->fd, AE_READABLE,
			       gw_conn_read_handler_wrap, conn);
	  FI_END ();
	  if (ret != AE_OK)
	    {
	      BE_ERROR (ARC_ERR_AE_ADDFD);
	      reconnect = 1;
	      break;
	    }
	  LOG_INFO (be, "Connected:%s", format_conn (be, conn));
	  gw_conn_change_state (be, conn, GW_CONN_OK);
	  conn->gw->n_active++;

	  /* complete reconnect job (not timeout) */
	  assert (conn->conn_to_job != NULL);
	  complete_be_job (be, conn->conn_to_job, 0);

	  /* notify init job if exists */
	  be->initialized = 1;
	  be->state = BE_STATE_RUNNING;
	  if (be->init_job != NULL)
	    {
	      complete_be_job (be, be->init_job, 0);
	      be->init_job = NULL;
	    }

	  /* assign arc jobs waiting for connection */
	  process_retry_arc_jobs (be);
	  break;
	}
    }

  if (reconnect)
    {
      gw_conn_try_reincarnate (be, conn, be->be_errno);
    }
  return;
}

static long long
gw_conn_get_busy_cost (gw_conn_t * conn, long long curr)
{
  be_job_t *job = NULL;
  long long busy_cost;

  if (!dlisth_is_empty (&conn->sent_jobs))
    {
      job = (be_job_t *) conn->sent_jobs.next;
    }
  else if (!dlisth_is_empty (&conn->jobs))
    {
      job = (be_job_t *) conn->jobs.next;
    }

  if (job == NULL)
    {
      return 0LL;
    }

  busy_cost = curr - job->conn_ts;
  if (busy_cost < 0LL)
    {
      busy_cost = 0LL;
    }
  if (busy_cost > INT_MAX)
    {
      busy_cost = INT_MAX;
    }
  return busy_cost;
}

static long long
gw_conn_get_affinity_cost (gw_conn_t * conn, arc_job_t * aj)
{

  if (!conn->be->use_affinity || !conn->dedicated)
    {
      return 0;
    }

  if (!aj->rqst.diff_pn && aj->rqst.crc != -1)
    {
      int i;
      int pn = aj->rqst.crc % CLUSTER_SIZE;
      pn_gw_aff_t *aff = &conn->be->pn_gw_aff[pn];

      for (i = 0; i < MAX_WRITE_GW_AFF_IDX; i++)
	{
	  if (aff->w_ids[i] == conn->gw->id)
	    {
	      return 0;
	    }
	}
      for (i = 0; i < MAX_READ_GW_AFF_IDX; i++)
	{
	  if (aff->r_ids[i] == conn->gw->id)
	    {
	      return 0;
	    }
	}
    }

  return INT_MAX;
}

static int
gw_conn_cost (gw_conn_t * conn, long long curr, arc_job_t * aj,
	      int *rqst_cost)
{
  long long job_cost;
  long long busy_cost;
  long long affinity_cost;
  long long conn_cost;

  if (conn->state != GW_CONN_OK)
    {
      return INT_MAX;
    }

  job_cost = 2;
  busy_cost = gw_conn_get_busy_cost (conn, curr);
  affinity_cost = gw_conn_get_affinity_cost (conn, aj);

  *rqst_cost = job_cost;
  conn_cost = job_cost + busy_cost + affinity_cost + conn->total_cost;

  if (conn_cost > INT_MAX)
    {
      return INT_MAX;
    }

  return (int) conn_cost;
}

static void
gw_conn_read_handler_wrap (aeEventLoop * el, socket_t fd, void *clientData,
			   int mask)
{
  gw_conn_t *conn = (gw_conn_t *) clientData;
  be_handle_type_t saved = conn->be->handle_type;

  conn->be->handle_type = BE_HANDLE_TYPE_GW_IN;
  gw_conn_read_handler (el, fd, clientData, mask);
  conn->be->handle_type = saved;
}

static void
gw_conn_read_handler (aeEventLoop * el, socket_t fd, void *clientData,
		      int mask)
{
  gw_conn_t *conn;
  int readlen, nr;
  int ibuf_len;
  int ret;
  be_t *be;
  int is_too_big;

  conn = (gw_conn_t *) clientData;

  be = conn->be;
  be->be_errno = 0;
  if (be->be_exit)
    {
      return;
    }

  /* initial condition */
  if (conn->ibuf == NULL)
    {
      BE_FI (conn->ibuf, NULL, 0);
      conn->ibuf = sdsempty ();
      FI_END ();
      if (conn->ibuf == NULL)
	{
	  BE_ERROR (ARC_ERR_NOMEM);
	  gw_conn_try_reincarnate (be, conn, be->be_errno);
	  return;
	}
    }

  /* read data into input buffer */
  readlen = IBUF_LEN;
  conn->ibuf = sdsMakeRoomFor (conn->ibuf, readlen);
  if (conn->ibuf == NULL)
    {
      BE_ERROR (ARC_ERR_NOMEM);
      gw_conn_try_reincarnate (be, conn, be->be_errno);
      return;
    }
  ibuf_len = sdslen (conn->ibuf);

  BE_FI (nr, -1, EINVAL, 0);
#ifndef WIN32
  nr = read (fd, conn->ibuf + ibuf_len, readlen);
#else
  nr = recv (fd, conn->ibuf + ibuf_len, readlen, 0);
#endif
  FI_END ();
  if (nr == -1)
    {
      if (errno == EAGAIN)
	{
	  return;
	}
      else
	{
	  BE_ERROR (ARC_ERR_SYSCALL);
	  gw_conn_try_reincarnate (be, conn, be->be_errno);
	  return;
	}
    }
  else if (nr == 0)
    {
      BE_ERROR (ARC_ERR_CONN_CLOSED);
      gw_conn_try_reincarnate (be, conn, be->be_errno);
      return;
    }

  sdsIncrLen (conn->ibuf, nr);
  BE_FI (is_too_big, 1, 0);
  is_too_big = (sdslen (conn->ibuf) > MAX_BUF_LEN);
  FI_END ();
  if (is_too_big)
    {
      BE_ERROR (ARC_ERR_TOO_BIG_DATA);
      gw_conn_try_reincarnate (be, conn, be->be_errno);
      return;
    }

  /* process buffer */
  ret = gw_conn_process_ibuf (be, conn);
  if (ret == -1)
    {
      BE_ERROR (ARC_ERR_GENERIC);
      gw_conn_try_reincarnate (be, conn, be->be_errno);
      return;
    }

#ifdef WIN32
  ret = aeWinReceiveDone (fd);
  if (ret != AE_OK)
    {
      BE_ERROR (ARC_ERR_AE_ADDFD);
      return;
    }
#endif
}

static int
gw_conn_process_ibuf (be_t * be, gw_conn_t * conn)
{
  be_job_t *be_job;
  int ret;
  int n_remain_cmd;
  int n_avail;
  int off;
  int ncmd;
  arc_job_t *job;
  noop_job_t *noop;

next_iter:
  /* find job to get response */
  if (!dlisth_is_empty (&conn->sent_jobs))
    {
      be_job = (be_job_t *) conn->sent_jobs.next;
    }
  else if (!dlisth_is_empty (&conn->jobs))
    {
      be_job = (be_job_t *) conn->jobs.next;
    }
  else
    {
      LOG_ERROR (be,
		 "Something received from the connection, but has no job to get the response");
      BE_ERROR (ARC_ERR_BE_INTERNAL);
      return -1;
    }

  n_avail = sdslen (conn->ibuf);
  off = 0;
  ncmd = 0;
  job = NULL;
  noop = NULL;

  if (be_job->type == BE_JOB_NOOP)
    {
      noop = (noop_job_t *) be_job->data;
      n_remain_cmd = noop->rqst_ncmd - noop->resp_ncmd;
    }
  else
    {
      assert (be_job->type == BE_JOB_ARC);
      job = (arc_job_t *) be_job->data;
      n_remain_cmd = job->rqst.ncmd - job->resp.ncmd;
    }

  while (n_remain_cmd > 0 && n_avail > 0)
    {
      int size = 0;
      be_parse_ctx_t ctx;

      ctx.count = 1;
      ret =
	parse_redis_response (conn->ibuf + off, n_avail, &size, &beParseCb,
			      &ctx);
      if (ret == -1)
	{
	  BE_ERROR (ARC_ERR_GENERIC);
	  return -1;
	}

      if (ctx.count == 0)
	{
	  ncmd++;
	  n_remain_cmd--;
	  off += size;
	  n_avail -= size;
	  continue;
	}
      else
	{
	  /* insufficient data for single reponse */
	  return 0;
	}
    }

  /* copy incremental result */
  if (off > 0)
    {
      if (noop)
	{
	  noop->recv_off += off;
	  noop->resp_ncmd += ncmd;
	}
      else
	{
	  if (job->resp.ibuf == NULL)
	    {
	      BE_FI (job->resp.ibuf, NULL, 0);
	      job->resp.ibuf = sdsnewlen (conn->ibuf, off);
	      FI_END ();
	    }
	  else
	    {
	      /* NOTE: no fault injection, memory leaks */
	      job->resp.ibuf = sdsMakeRoomFor (job->resp.ibuf, off);
	      if (job->resp.ibuf)
		{
		  memcpy (job->resp.ibuf + job->resp.off, conn->ibuf, off);
		  *(job->resp.ibuf + job->resp.off + off) = '\0';
		}
	      sdsIncrLen (job->resp.ibuf, off);
	    }
	  if (job->resp.ibuf == NULL)
	    {
	      /* reset resp fields */
	      job->resp.ncmd = 0;
	      job->resp.off = 0;
	      BE_ERROR (ARC_ERR_NOMEM);
	      return -1;
	    }
	  job->resp.ncmd += ncmd;
	  job->resp.off += off;
	}
      sdsrange (conn->ibuf, off, -1);
    }

  if (n_remain_cmd == 0)
    {
      dlisth_delete (&be_job->head);	// no NOOP
      complete_be_job (be, be_job, 0);
      if (!dlisth_is_empty (&conn->sent_jobs) && n_avail > 0)
	{
	  goto next_iter;
	}
    }
  return 0;
}

static int
gw_conn_prepare_write (be_t * be, gw_conn_t * conn)
{
  if (conn->w_prepared == 0)
    {
      int ret;

      assert (conn->fd > 0);
      BE_FI (ret, -1, 0);
      ret =
	aeCreateFileEvent (be->el, conn->fd, AE_WRITABLE,
			   gw_conn_write_handler_wrap, conn);
      FI_END ();
      if (ret != AE_OK)
	{
	  BE_ERROR (ARC_ERR_AE_ADDFD);
	  return -1;
	}
      conn->w_prepared = 1;
    }
  return 0;
}

static void
gw_conn_write_handler_wrap (aeEventLoop * el, socket_t fd, void *clientData,
			    int mask)
{
  gw_conn_t *conn = (gw_conn_t *) clientData;
  be_handle_type_t saved = conn->be->handle_type;

  conn->be->handle_type = BE_HANDLE_TYPE_GW_OUT;
  gw_conn_write_handler (el, fd, clientData, mask);
  conn->be->handle_type = saved;
}

#ifndef _WIN32
static void
gw_conn_write_handler (aeEventLoop * el, socket_t fd, void *clientData,
		       int mask)
{
  gw_conn_t *conn;
  be_job_t *job;
  arc_job_t *aj;
  be_t *be;

  conn = (gw_conn_t *) clientData;

  be = conn->be;
  be->be_errno = 0;
  if (be->be_exit)
    {
      return;
    }

next_iter:
  if (dlisth_is_empty (&conn->jobs))
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      conn->w_prepared = 0;
      return;
    }

  job = (be_job_t *) conn->jobs.next;
  if (job->type == BE_JOB_NOOP)
    {
      noop_job_t *noop = (noop_job_t *) job->data;

      complete_be_job (be, job, 0);
      if (noop->job_state != ARCJOB_STATE_WAITING)
	{
	  /* some of bytes are sent, and it it not recoverable */
	  gw_conn_try_reincarnate (be, conn, ARC_ERR_CONN_CLOSED);
	  return;
	}
      else
	{
	  goto next_iter;
	}
    }

  assert (job->type == BE_JOB_ARC);
  aj = (arc_job_t *) job->data;
  while (1)
    {
      int avail, nw;
      char *cp;

      avail = sdslen (aj->rqst.obuf) - aj->rqst.sent_cnt;
      cp = aj->rqst.obuf + aj->rqst.sent_cnt;
      BE_FI (nw, -1, EINVAL, 0);
      nw = write (fd, cp, avail);
      FI_END ();
      if (nw == -1)
	{
	  if (errno == EAGAIN)
	    {
	      break;
	    }
	  BE_ERROR (ARC_ERR_SYSCALL);
	  gw_conn_try_reincarnate (be, conn, be->be_errno);
	  return;
	}
      else if (nw == 0)
	{
	  BE_ERROR (ARC_ERR_CONN_CLOSED);
	  gw_conn_try_reincarnate (be, conn, be->be_errno);
	  return;
	}

      if (aj->rqst.sent_cnt == 0)
	{
	  aj->state = ARCJOB_STATE_SENDING;
	}

      aj->rqst.sent_cnt += nw;
      if (aj->rqst.sent_cnt == sdslen (aj->rqst.obuf))
	{
	  aj->state = ARCJOB_STATE_SENT;
	  dlisth_delete ((dlisth *) job);
	  dlisth_insert_before (&job->head, &conn->sent_jobs);
	  goto next_iter;
	}
    }

  return;
}
#else
static void
gw_conn_write_handler (struct aeEventLoop *el, socket_t fd, void *clientData,
		       int mask)
{
  gw_conn_t *conn = (gw_conn_t *) clientData;
  arc_job_t *aj;
  be_job_t *job;
  be_t *be;
  int ret, win_errno;
  int avail, nw;
  char *cp;

  be = conn->be;
  be->be_errno = 0;
  if (be->be_exit)
    {
      return;
    }

next_iter:
  if (dlisth_is_empty (&conn->jobs))
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      conn->w_prepared = 0;
      return;
    }

  job = (be_job_t *) conn->jobs.next;
  if (job->type == BE_JOB_NOOP)
    {
      noop_job_t *noop = (noop_job_t *) job->data;

      complete_be_job (be, job, 0);
      if (noop->job_state != ARCJOB_STATE_WAITING)
	{
	  /* some of bytes are sent, and it it not recoverable */
	  gw_conn_try_reincarnate (be, conn, ARC_ERR_CONN_CLOSED);
	  return;
	}
      else
	{
	  goto next_iter;
	}
    }

  assert (job->type == BE_JOB_ARC);
  aj = (arc_job_t *) job->data;

  avail = sdslen (aj->rqst.obuf) - aj->rqst.sent_cnt;
  cp = aj->rqst.obuf + aj->rqst.sent_cnt;
  BE_FI (nw, -1, EINVAL, 0);
  ret =
    aeWinSocketSend (fd, cp, avail, 0, el, conn,
		     gw_conn_write_completion_handler);
  win_errno = WSAGetLastError ();
  if (ret == SOCKET_ERROR)
    {
      if (win_errno != 0 && win_errno != WSA_IO_PENDING)
	{
	  BE_ERROR (ARC_ERR_CONN_CLOSED);
	  gw_conn_try_reincarnate (be, conn, ARC_ERR_CONN_CLOSED);
	  return;
	}
    }
  FI_END ();

  if (aj->rqst.sent_cnt == 0)
    {
      aj->state = ARCJOB_STATE_SENDING;
    }

  aj->rqst.sent_cnt += avail;
}

static void
gw_conn_write_completion_handler (struct aeEventLoop *el, socket_t fd,
				  void *clientData, int written)
{
  aeWinSendReq *req = (aeWinSendReq *) clientData;
  gw_conn_t *conn = (gw_conn_t *) req->data;
  be_job_t *job = conn->jobs.next;
  arc_job_t *aj;

  aj = job->data;

  if (req->len == written)
    {
      aj->state = ARCJOB_STATE_SENT;
      dlisth_delete ((dlisth *) job);
      dlisth_insert_before (&job->head, &conn->sent_jobs);
    }
}
#endif

static void
gw_conn_reset_for_schedule (be_t * be, gw_conn_t * conn, int err)
{
  int ret;
  dlisth *h;
  be_job_t *job;
  arc_job_t *aj;

  if (conn->fd != -1)
    {
      aeDeleteFileEvent (be->el, conn->fd, AE_READABLE);
      aeDeleteFileEvent (be->el, conn->fd, AE_WRITABLE);
      close_socket (conn->fd);
      conn->fd = -1;
      conn->w_prepared = 0;
    }

  if (conn->conn_to_job != NULL)
    {
      complete_be_job (be, conn->conn_to_job, 0);
    }

  if (conn->ibuf != NULL)
    {
      sdsfree (conn->ibuf);
      conn->ibuf = NULL;
    }
  conn->bp = conn->ep = &conn->buf[0];

  if (conn->state == GW_CONN_OK)
    {
      conn->gw->n_active--;
      assert (conn->gw->n_active >= 0);
    }

  if (conn->state != GW_CONN_CLOSING)
    {
      gw_conn_change_state (be, conn, GW_CONN_NONE);
    }

  /* abort jobs in request phase */
  while (!dlisth_is_empty (&conn->jobs))
    {
      h = conn->jobs.next;
      job = (be_job_t *) h;

      /* aborted arc job */
      if (job->type == BE_JOB_NOOP)
	{
	  complete_be_job (be, job, err);
	  continue;
	}
      assert (job->type == BE_JOB_ARC);
      aj = (arc_job_t *) job->data;
      assert (aj->state > ARCJOB_STATE_CONN);

      dlisth_delete (h);	//no NOOP
      if (aj->state < ARCJOB_STATE_SENT)
	{
	  /* ajust cost: it is not completed at this connection */
	  if (job->conn != NULL)
	    {
	      job->conn->total_cost -= job->cost;
	      job->conn = NULL;
	    }
	  job->cost = 0;
	  reset_arc_job_for_retry (be, aj);
	  ret = arc_job_try_alloc_conn (be, job, &be->retry_arc_jobs);
	  if (ret == -1)
	    {
	      BE_ERROR (ARC_ERR_GENERIC);
	      complete_be_job (be, job, be->be_errno);
	    }
	  continue;
	}
      else
	{
	  complete_be_job (be, job, err);
	}
    }

  /* abort jobs waiting for response */
  while (!dlisth_is_empty (&conn->sent_jobs))
    {
      h = conn->sent_jobs.next;
      job = (be_job_t *) h;
      dlisth_delete (h);	//no NOOP
      complete_be_job (be, job, err);
    }
}

static void
gw_conn_try_reincarnate (be_t * be, gw_conn_t * conn, int err)
{
  int ret;

  gw_conn_reset_for_schedule (be, conn, err);
  if (conn->state == GW_CONN_CLOSING)
    {
      gw_conn_try_to_close (be, conn);
      return;
    }
  else
    {
      ret = htw_make_reconnect_job (be, conn, be->conf.conn_reconnect_millis);
      if (ret == -1)
	{
	  BE_ERROR (ARC_ERR_GENERIC);
	}
      return;
    }
}

static int
be_on_string (void *ctx, char *s, int len, int is_null)
{
  be_parse_ctx_t *c = (be_parse_ctx_t *) ctx;

  c->count--;
  return c->count == 0 ? PARSE_CB_OK_DONE : PARSE_CB_OK_CONTINUE;
}

static int
be_on_status (void *ctx, char *s, int len)
{
  be_parse_ctx_t *c = (be_parse_ctx_t *) ctx;

  c->count--;
  return c->count == 0 ? PARSE_CB_OK_DONE : PARSE_CB_OK_CONTINUE;
}

static int
be_on_error (void *ctx, char *s, int len)
{
  be_parse_ctx_t *c = (be_parse_ctx_t *) ctx;

  c->count--;
  return c->count == 0 ? PARSE_CB_OK_DONE : PARSE_CB_OK_CONTINUE;
}

static int
be_on_integer (void *ctx, long long val)
{
  be_parse_ctx_t *c = (be_parse_ctx_t *) ctx;

  c->count--;
  return c->count == 0 ? PARSE_CB_OK_DONE : PARSE_CB_OK_CONTINUE;
}

static int
be_on_array (void *ctx, int len, int is_null)
{
  be_parse_ctx_t *c = (be_parse_ctx_t *) ctx;

  c->count--;
  c->count += len;
  return c->count == 0 ? PARSE_CB_OK_DONE : PARSE_CB_OK_CONTINUE;
}

#ifndef _WIN32
static void
complete_cond (be_t * be, cond_t * cj, int err)
{
  pthread_mutex_lock (&cj->mutex);
  cj->pred = 1;
  cj->err = err;
  pthread_cond_signal (&cj->cond);
  pthread_mutex_unlock (&cj->mutex);
}
#else
static void
complete_cond (be_t * be, HANDLE * cj, int err)
{
  SetEvent (*cj);
}
#endif

static void
complete_arc_job (be_t * be, arc_job_t * aj, int err)
{
  /* notification */
  pthread_mutex_lock (&aj->mutex);
  aj->pred = 1;
  aj->err = err;
  aj->state = ARCJOB_STATE_COMPLETED;
  pthread_cond_signal (&aj->cond);
  pthread_mutex_unlock (&aj->mutex);
}

static void
reset_arc_job_for_retry (be_t * be, arc_job_t * aj)
{
  assert (aj->state != ARCJOB_STATE_COMPLETED);

  aj->rqst.sent_cnt = 0;
  if (aj->resp.ibuf != NULL)
    {
      sdsfree (aj->resp.ibuf);
      aj->resp.ibuf = NULL;
    }
  aj->resp.off = 0;
  aj->resp.ncmd = 0;
  aj->state = ARCJOB_STATE_CONN;
}

static gw_t *
find_gw_if_active (be_t * be, short id)
{
  int i;

  if (id < 0)
    {
      return NULL;
    }

  for (i = 0; i < be->n_gw; i++)
    {
      gw_t *gw = be->gw_ary[i];
      if (gw->id == id && gw->state == GW_STATE_USED && gw->n_active > 0)
	{
	  return gw;
	}
    }
  return NULL;
}

static int
gw_find_best_conn (gw_t ** cand, int ncand, long long curr, arc_job_t * aj,
		   int *best_cost, int *best_rqst_cost,
		   gw_conn_t ** best_conn)
{
  int i;
  gw_conn_t *b_conn = NULL;
  int b_cost = INT_MAX;
  int b_rqst_cost = 0;
  long long b_conn_peek_cnt = LLONG_MAX;
  int found = 0;

  for (i = 0; i < ncand; i++)
    {
      dlisth *h;
      int cost, rqst_cost;
      gw_t *gw;

      gw = cand[i];
      for (h = gw->conns.next; h != &gw->conns; h = h->next)
	{
	  gw_conn_t *conn = (gw_conn_t *) h;

	  cost = gw_conn_cost (conn, curr, aj, &rqst_cost);
	  if (cost == INT_MAX)
	    {
	      continue;
	    }
	  if (b_conn == NULL || cost <= b_cost)
	    {
	      if (b_conn != NULL && cost == b_cost
		  && b_conn_peek_cnt < conn->recent_peek_cnt)
		{
		  continue;
		}
	      b_conn = conn;
	      b_cost = cost;
	      b_rqst_cost = rqst_cost;
	      b_conn_peek_cnt = conn->recent_peek_cnt;
	      found = 1;
	    }
	}
    }

  *best_cost = b_cost;
  *best_rqst_cost = b_rqst_cost;
  *best_conn = b_conn;
  return found;
}

static gw_t *
peek_wgw (be_t * be, pn_gw_aff_t * aff)
{
  int nr;
  gw_t *gw;

  nr = 0;
  while (nr < MAX_WRITE_GW_AFF_IDX && aff->w_ids[nr] != -1)
    {
      nr++;
    }

  if (nr > 0)
    {
      int rand_idx = rand () % nr;
      int i;

      for (i = 0; i < nr; i++)
	{
	  gw = find_gw_if_active (be, aff->w_ids[(rand_idx + i) % nr]);
	  if (gw != NULL)
	    {
	      return gw;
	    }
	}
    }
  return NULL;
}

static gw_t *
peek_rgw (be_t * be, pn_gw_aff_t * aff, gw_t * wgw)
{
  int nr;
  gw_t *gw;

  nr = 0;
  while (nr < MAX_READ_GW_AFF_IDX && aff->r_ids[nr] != -1)
    {
      nr++;
    }

  if (nr > 0)
    {
      int rand_idx = rand () % nr;
      int i;

      for (i = 0; i < nr; i++)
	{
	  gw = find_gw_if_active (be, aff->r_ids[(rand_idx + i) % nr]);
	  if (gw != NULL && gw != wgw)
	    {
	      return gw;
	    }
	}
    }
  return NULL;
}


#define NUM_GW_PEEK 3
static int
arc_job_try_alloc_conn (be_t * be, be_job_t * job, dlisth * retry_head)
{
  long long curr;
  gw_t *cand[NUM_GW_PEEK] = { NULL, };
  int cand_affinity[NUM_GW_PEEK];
  arc_job_t *aj = NULL;
  int num_cand = 0;
  int rand_idx;
  int i, ret;
  int best_cost;
  int best_rqst_cost;
  int found;
  int has_affinity;
  gw_conn_t *best_conn;
  gw_t *wgw = NULL, *rgw = NULL;


  assert (job->type == BE_JOB_ARC);
  aj = (arc_job_t *) job->data;
  assert (aj->state == ARCJOB_STATE_SUBMITTED
	  || aj->state == ARCJOB_STATE_CONN);

  if (be->n_gw == 0)
    {
      goto gw_not_found;
    }

  curr = currtime_millis ();

  has_affinity = (be->use_affinity && !aj->rqst.diff_pn
		  && aj->rqst.crc != -1);
  // add gateway that has affinity with the job 
  if (has_affinity)
    {
      int pn = aj->rqst.crc % CLUSTER_SIZE;
      pn_gw_aff_t *aff = &be->pn_gw_aff[pn];

      wgw = peek_wgw (be, aff);
      if (wgw != NULL)
	{
	  cand[num_cand] = wgw;
	  cand_affinity[num_cand] = GW_AFF_WRITE;
	  num_cand++;
	}

      rgw = peek_rgw (be, aff, wgw);
      if (rgw != NULL)
	{
	  cand[num_cand] = rgw;
	  cand_affinity[num_cand] = GW_AFF_READ;
	  num_cand++;
	}
    }

  // select gateways from random position 
  rand_idx = rand () % be->n_gw;
  for (i = 0; i < be->n_gw && num_cand < NUM_GW_PEEK; i++)
    {
      gw_t *gw;

      gw = be->gw_ary[(rand_idx + i) % be->n_gw];
      if (gw != wgw && gw != rgw && gw->state == GW_STATE_USED
	  && gw->n_active > 0)
	{
	  cand[num_cand] = gw;
	  cand_affinity[num_cand] = GW_AFF_NONE;
	  num_cand++;
	}
    }

  //
  // Select best connection from candidates. Candidates are ordered of w.r.t. affinity.
  // i.e.) write(can be null) -> read(can be null) -> none (can be null) 
  //
  // +-------+    +-------+   +-------+
  // | gw1   |    | gw2   |   | gw3   |
  // |       |    |       |   |       | 
  // | r1    |    | r2    |   | r3    |
  // | W aff.|    | R aff.|   | N aff.|
  // +-------+    +-------+   +-------+
  //   PM1          PM2         PM3
  //
  // Connection selection rule is as follows.
  // (1). for write operation
  //   - check write affinity gateway and return best conn if found
  //     (hop: gw1 -> r1, replication: optimal)
  //   - check read affinity gateway and return best conn if found
  //     (hop: gw2 -> r2, replication data: +1)
  //   - check none affinity gateway and return best conn
  //     (hop: gw3 -> (r1 or r2), replication data: +1 + alpha)
  // (2). for read operation 
  //   - check write, read affinity gateway and return best conn if found
  //     same as (1), but read operation is more distributed
  //   - check none affinity gateway and return best conn
  // (3) otherwise
  //   - check write, read, none affinity gateway and return best conn 
  //
  best_cost = INT_MAX;
  best_conn = NULL;
  best_rqst_cost = 0;

  found = 0;
  if (has_affinity && aj->rqst.is_write)
    {
      int counted = 0;

      if (wgw != NULL)
	{
	  found =
	    gw_find_best_conn (cand, 1, curr, aj, &best_cost, &best_rqst_cost,
			       &best_conn);
	  counted++;
	}
      if (!found && rgw != NULL)
	{
	  found =
	    gw_find_best_conn (cand + counted, 1, curr, aj, &best_cost,
			       &best_rqst_cost, &best_conn);
	  counted++;
	}
      if (!found && num_cand - counted > 0)
	{
	  found =
	    gw_find_best_conn (cand + counted, num_cand - counted, curr, aj,
			       &best_cost, &best_rqst_cost, &best_conn);
	}
    }
  else if (has_affinity && aj->rqst.is_write == 0)
    {
      int counted = 0;
      if (wgw != NULL)
	{
	  counted++;
	}
      if (rgw != NULL)
	{
	  counted++;
	}
      if (counted > 0)
	{
	  found =
	    gw_find_best_conn (cand, counted, curr, aj, &best_cost,
			       &best_rqst_cost, &best_conn);
	}
      if (!found && num_cand - counted > 0)
	{
	  found =
	    gw_find_best_conn (cand + counted, num_cand - counted, curr, aj,
			       &best_cost, &best_rqst_cost, &best_conn);
	}
    }
  else
    {
      found =
	gw_find_best_conn (cand, num_cand, curr, aj, &best_cost,
			   &best_rqst_cost, &best_conn);
    }

  if (found)
    {
      assert (best_conn != NULL);
      ret = gw_conn_prepare_write (be, best_conn);
      if (ret == -1)
	{
	  BE_ERROR (ARC_ERR_GENERIC);
	  return -1;
	}
      aj->state = ARCJOB_STATE_WAITING;
      job->cost = best_rqst_cost;
      job->conn_ts = curr;
      job->conn = best_conn;
      best_conn->total_cost += job->cost;
      best_conn->recent_peek_cnt++;
      dlisth_insert_before (&job->head, &best_conn->jobs);
      return 0;
    }

gw_not_found:
  aj->state = ARCJOB_STATE_CONN;
  dlisth_insert_before (&job->head, retry_head);
  return 0;
}

static void
complete_be_job (be_t * be, be_job_t * job, int err)
{
  gw_conn_t *conn = NULL;

  switch (job->type)
    {
    case BE_JOB_NOOP:
      dlisth_delete (&job->head);
      decref_be_job (job);
      break;
    case BE_JOB_INIT:
#ifndef _WIN32
      complete_cond (be, (cond_t *) job->data, err);
#else
      complete_cond (be, (HANDLE *) job->data, err);
#endif
      decref_be_job (job);
      break;
    case BE_JOB_ARC:
      if (be->conf.log_level >= ARC_LOG_LEVEL_DEBUG)
	{
	  debug_log_be_job (be, job, err);
	}
      /* replace job with NOOP if job is linked */
      if (!dlisth_is_empty (&job->head))
	{
	  be_job_t *rep;
	  arc_job_t *aj;
	  noop_job_t *noop;

	  aj = job->data;
	  /* use NOOP in ARC job */
	  rep = aj->noop;
	  aj->noop = NULL;
	  rep->conn = job->conn;
	  noop = rep->data;
	  noop->job_state = aj->state;
	  noop->rqst_ncmd = aj->rqst.ncmd;
	  noop->recv_off = aj->resp.off;
	  noop->resp_ncmd = aj->resp.ncmd;
	  dlisth_insert_after (&rep->head, &job->head);
	}
      dlisth_delete (&job->head);
      /* connection state management */
      if (job->conn != NULL)
	{
	  conn = job->conn;
	  conn->total_cost -= job->cost;
	  assert (conn->total_cost >= 0);
	  if (conn->state == GW_CONN_CLOSING)
	    {
	      gw_conn_try_to_close (be, conn);
	    }
	  else if (conn->state == GW_CONN_OK)
	    {
	      if (err == ARC_ERR_TIMEOUT)
		{
		  /* 
		   * This job completion is from htw_process_job 
		   * Check if it is safe to move to NONE
		   */
		  if (gw_conn_safe_to_abandon (be, conn))
		    {
		      LOG_INFO
			(be,
			 "Timeout causes the connection to be retried:%s",
			 format_conn (be, conn));
		      gw_conn_try_reincarnate (be, conn, 0);
		    }
		}
	    }
	  job->conn = NULL;
	}
      job->cost = 0;
      complete_arc_job (be, (arc_job_t *) job->data, err);
      htw_del (be, job);
      break;
    case BE_JOB_RECONNECT:
      conn = (gw_conn_t *) job->data;
      if (conn->conn_to_job)
	{
	  /* conn is not owner */
	  conn->conn_to_job = NULL;
	}
      dlisth_delete (&job->head);
      htw_del (be, job);
      break;
    case BE_JOB_ZK_TO:
      dlisth_delete (&job->head);
      htw_del (be, job);
      break;
    default:
      assert (0);
      break;
    }
}

static long long
htw_millis_to_tick (be_t * be, long long millis)
{
  return millis / (1000 / HASHED_TIMER_WHEEL_SIZE);
}

static void
htw_add (be_t * be, be_job_t * job)
{
  long long tick;
  int idx;

  assert (dlisth_is_empty (&job->htwhead));
  tick = htw_millis_to_tick (be, job->to);
  idx = tick % HASHED_TIMER_WHEEL_SIZE;

  dlisth_insert_before (&job->htwhead, &be->htw[idx]);
  addref_be_job (job);		// +1 for hashed timer wheel
}

static void
htw_del (be_t * be, be_job_t * job)
{
  if (!dlisth_is_empty (&job->htwhead))
    {
      dlisth_delete (&job->htwhead);
      decref_be_job (job);
    }
}

static int
htw_make_reconnect_job (be_t * be, gw_conn_t * conn, int to_millis)
{
  be_job_t *job;

  job = create_be_job (BE_JOB_RECONNECT, NULL, conn, 0);
  if (job == NULL)
    {
      BE_ERROR (ARC_ERR_GENERIC);
      LOG_ERROR (be,
		 "Fatal: failed to create new backend job to reconect gateway. errno=%d",
		 be->be_errno);
      be_signal_exit (be, be->be_errno);
      return -1;
    }

  job->to = currtime_millis () + to_millis;
  htw_add (be, job);
  decref_be_job (job);		// ownership is transferred to htw
  conn->conn_to_job = job;
  return 0;
}

static int
htw_process_job (be_t * be, be_job_t * job)
{
  int ret = 0;
  gw_conn_t *conn;

  switch (job->type)
    {
    case BE_JOB_RECONNECT:
      conn = (gw_conn_t *) job->data;

      assert (conn != NULL);
      assert (conn->state != GW_CONN_CLOSING);
      assert (conn->conn_to_job == job);
      LOG_WARN (be, "BE_JOB_RECONNECT (timeout) retry to connect %s",
		format_conn (be, conn));
      gw_conn_reset_for_schedule (be, conn, 0);
      ret = gw_conn_try_to_connect (be, conn);
      if (ret == -1)
	{
	  BE_ERROR (ARC_ERR_GENERIC);
	  return -1;
	}
      break;
    case BE_JOB_ARC:
      complete_be_job (be, job, ARC_ERR_TIMEOUT);
      break;
    case BE_JOB_ZK_TO:
      ret = zk_evl_bottom_half (be);
      if (ret == -1)
	{
	  zk_cleanup (be);
	  zk_force_reconn (be);
	}
      break;
    default:
      assert (0);
      BE_ERROR (ARC_ERR_BE_INTERNAL);
      return -1;
    }
  return ret;
}


static int
htw_process_line (be_t * be, dlisth * head, long long curr)
{
  int ret;
  dlisth *h, *next;

  for (h = head->next; h != head; h = next)
    {
      be_job_t *job = htwhead_to_be_job (h);

      next = h->next;
      if (job->to <= curr)
	{
	  /* NOTE: no fault injection: caller may be lost timeout */
	  ret = htw_process_job (be, job);
	  if (ret == -1)
	    {
	      BE_ERROR (ARC_ERR_GENERIC);
	      return -1;
	    }
	}
    }
  return 0;
}

static void
htw_delete_jobs_in_line (be_t * be, dlisth * head)
{
  dlisth *h;

  while (!dlisth_is_empty (head))
    {
      h = head->next;
      htw_del (be, htwhead_to_be_job (h));
    }
}

static void
htw_abort_jobs_in_line (be_t * be, dlisth * head)
{
  dlisth *h;

  while (!dlisth_is_empty (head))
    {
      be_job_t *job;

      h = head->next;
      job = htwhead_to_be_job (h);
      complete_be_job (be, job, be->exit_errno);
    }
}

static void
destroy_be (be_t * be)
{
  int i;
  dlisth *h, *next;

  if (be == NULL)
    {
      return;
    }

  if (be->pipe_fd != INVALID_PIPE)
    {
      close_pipe (be->pipe_fd);
    }

#ifdef _WIN32
  if (be->pipe_fd_fe != INVALID_PIPE)
    {
      close_pipe (be->pipe_fd_fe);
    }
#endif

  if (be->zh != NULL)
    {
      zk_cleanup (be);
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
	      destroy_gw (be, be->gw_ary[i]);
	    }
	}
      free (be->gw_ary);
    }

  if (be->gw_hosts != NULL)
    {
      free (be->gw_hosts);
    }

  for (i = 0; i < HASHED_TIMER_WHEEL_SIZE; i++)
    {
      htw_delete_jobs_in_line (be, &be->htw[i]);
    }

  if (be->init_job != NULL)
    {
      complete_be_job (be, be->init_job, be->exit_errno);
    }

  assert (dlisth_is_empty (&be->retry_arc_jobs));
  for (h = be->garbage_conns.next; h != &be->garbage_conns; h = next)
    {
      gw_conn_t *conn;

      next = h->next;
      conn = gc_head_to_gw_conn (h);
      gw_conn_gc (be, conn);
    }

  if (be->el != NULL)
    {
      aeDeleteEventLoop (be->el);
    }

  free (be);
}

static void
abort_be (be_t * be)
{
  int i;

  /* no more request from the pipe */
  if (be->pipe_fd != INVALID_PIPE)
    {
      close_pipe (be->pipe_fd);
      be->pipe_fd = INVALID_PIPE;
    }

  for (i = 0; i < HASHED_TIMER_WHEEL_SIZE; i++)
    {
      htw_abort_jobs_in_line (be, &be->htw[i]);
      assert (dlisth_is_empty (&be->htw[i]));
    }

  while (!dlisth_is_empty (&be->retry_arc_jobs))
    {
      dlisth *h = be->retry_arc_jobs.next;
      complete_be_job (be, (be_job_t *) h, be->exit_errno);
    }


  if (be->init_job != NULL)
    {
      complete_be_job (be, be->init_job, be->exit_errno);
      be->init_job = NULL;
    }

  if (be->app_log_fp != NULL)
    {
      fclose (be->app_log_fp);
    }
}

static void
be_signal_exit (be_t * be, int err)
{
  be->be_exit = 1;
  be->exit_errno = err;
  aeStop (be->el);
  be->state = BE_STATE_SHUTDOWN;
}

static void
be_job_handler_wrap (aeEventLoop * el, pipe_t fd, void *clientData, int mask)
{
  be_t *be = (be_t *) clientData;
  be_handle_type_t saved = be->handle_type;

  be->handle_type = BE_HANDLE_TYPE_PP_IN;
  be_job_handler (el, fd, clientData, mask);
  be->handle_type = saved;
}

static void
be_job_handler (aeEventLoop * el, pipe_t fd, void *clientData, int mask)
{
  be_t *be = (be_t *) clientData;
  be_job_t *jobp_buf[512];
  char *bp, *ep;
  int n_remain, nr, njobs, idx, ret;

  be->be_errno = 0;
  if (be->be_exit)
    {
      return;
    }

  /* setup jobp_buf pointers */
  bp = ep = (char *) &jobp_buf[0];

  /* copy unprocessed data */
  n_remain = be->ibp - be->input_buf;
  if (n_remain > 0)
    {
      memcpy (ep, be->input_buf, n_remain);
      ep += n_remain;
      be->ibp = &be->input_buf[0];
    }

  /* read from pipe */
#ifndef WIN32
  nr = read (fd, ep, sizeof (jobp_buf) - (ep - bp));
#else
  DWORD read_bytes = 0;
  ret = ReadFile (fd, ep, sizeof (jobp_buf) - (ep - bp), &read_bytes, NULL);
  if (ret == FALSE)
    {
      BE_ERROR (ARC_ERR_AE_ADDFD);
      LOG_ERROR (be, "Fatal: failed to read from pipe. errno=%d, winerrno=%d",
		 be->be_errno, WSAGetLastError ());
      be_signal_exit (be, be->be_errno);
      return;
    }
  nr = read_bytes;

  ret = aeWinReceiveDone_Pipe (be->pipe_fd);
  if (ret != AE_OK)
    {
      BE_ERROR (ARC_ERR_AE_ADDFD);
      LOG_ERROR (be,
		 "Fatal: failed to register pipe read operation. errno=%d, winerrno=%d",
		 be->be_errno, WSAGetLastError ());
      be_signal_exit (be, be->be_errno);
      return;
    }
#endif
  if (nr == -1 || nr == 0)
    {
      /* pipe can't be closed. it is unrecoverable situation */
      BE_ERROR (ARC_ERR_SYSCALL);
      LOG_ERROR (be, "Fatal: failed to read from pipe. errno=%d",
		 be->be_errno);
      be_signal_exit (be, be->be_errno);
      return;
    }
  ep += nr;

  /* process jobs */
  njobs = (ep - bp) / sizeof (be_job_t *);
  idx = 0;
  while (be->be_exit == 0 && idx < njobs)
    {
      be_job_t *job = jobp_buf[idx++];

      /* job is already addref'ed (at the time of submission) */
      switch (job->type)
	{
	case BE_JOB_INIT:
	  be->init_job = job;	//+1 (from pipe) is transferred
	  break;
	case BE_JOB_ARC:
	  ret = arc_job_try_alloc_conn (be, job, &be->retry_arc_jobs);
	  if (ret == -1)
	    {
	      BE_ERROR (ARC_ERR_GENERIC);
	      complete_be_job (be, job, be->be_errno);
	      break;
	    }
	  htw_add (be, job);	//+1
	  decref_be_job (job);	//-1 (from pipe)
	  break;
	default:
	  assert (0);
	  /* protocol is broken */
	  LOG_ERROR (be,
		     "Fatal: something wrong has been read from command pipe, bp%d  ep%d njobs%d idx%d",
		     bp, ep, njobs, idx);
	  be_signal_exit (be, ARC_ERR_BAD_BE_JOB);
	  break;
	}
    }

  /* backup unprocessed data */
  if (be->be_exit == 0)
    {
      bp += njobs * sizeof (be_job_t *);
      assert (ep - bp < sizeof (be_job_t *));
      if (ep - bp > 0)
	{
	  memcpy (be->input_buf, bp, ep - bp);
	  be->ibp = be->input_buf + (ep - bp);
	}
    }
  return;
}

static void
process_retry_arc_jobs (be_t * be)
{
  dlisth tmp_head;
  dlisth *h;

  dlisth_init (&tmp_head);
  while (!dlisth_is_empty (&be->retry_arc_jobs))
    {
      be_job_t *be_job;
      int ret;

      h = be->retry_arc_jobs.next;
      be_job = (be_job_t *) h;
      dlisth_delete (h);

      if (be_job->type == BE_JOB_NOOP)
	{
	  complete_be_job (be, be_job, 0);
	  continue;
	}

      assert (be_job->type == BE_JOB_ARC);
      ret = arc_job_try_alloc_conn (be, be_job, &tmp_head);
      if (ret == -1)
	{
	  BE_ERROR (ARC_ERR_GENERIC);
	  complete_be_job (be, be_job, be->be_errno);
	  continue;
	}
    }

  while (!dlisth_is_empty (&tmp_head))
    {
      h = tmp_head.next;

      dlisth_delete (h);
      dlisth_insert_before (h, &be->retry_arc_jobs);
    }

  return;
}

static int
be_cron_wrap (aeEventLoop * el, long long id, void *clientData)
{
  be_t *be = (be_t *) clientData;
  be_handle_type_t saved = be->handle_type;
  int ret;

  be->handle_type = BE_HANDLE_TYPE_CRON;
  ret = be_cron (el, id, clientData);
  be->handle_type = saved;
  return ret;
}

static int
be_cron (aeEventLoop * el, long long id, void *clientData)
{
  be_t *be = (be_t *) clientData;
  dlisth *h, *next;
  int interval = BACKEND_DEFAULT_CRON_INTERVAL;
  long long st, et;
  long long curr_tick, target_tick, i, num_ticks;
  int ret;
  static long long last_conn_recent_peek_flush = 0LL;

  be->be_errno = 0;
  if (be->be_exit)
    {
      return AE_NOMORE;
    }

  /* handle exit condition initiated by client */
  if (be->abrupt_exit)
    {
      LOG_WARN (be, "Graceful shutdown caused by API");
      be_signal_exit (be, ARC_ERR_INTERRUPTED);
      return AE_NOMORE;
    }

  st = currtime_millis ();
  /* -------------------------- */
  /* process hashed timer wheel */
  /* -------------------------- */

  /* calculate number of ticks to process */
  curr_tick = be->htw_tick;
  target_tick = htw_millis_to_tick (be, st);
  num_ticks = target_tick - curr_tick;
  if (num_ticks > HASHED_TIMER_WHEEL_SIZE)
    {
      num_ticks = HASHED_TIMER_WHEEL_SIZE;
    }

  /* process lines */
  for (i = 0; i < num_ticks; i++, curr_tick++)
    {
      int idx = curr_tick % HASHED_TIMER_WHEEL_SIZE;

      ret = htw_process_line (be, &be->htw[idx], st);
      if (ret == -1)
	{
	  BE_ERROR (ARC_ERR_GENERIC);
	  goto done;
	}
    }
  be->htw_tick = target_tick;

  /* flush peek counter for every gateways */
  if (st > last_conn_recent_peek_flush + 1000)
    {
      int i;
      gw_t *gw;
      dlisth *h;

      for (i = 0; i < be->n_gw; i++)
	{
	  gw = be->gw_ary[i];
	  for (h = gw->conns.next; h != &gw->conns; h = h->next)
	    {
	      gw_conn_t *conn = (gw_conn_t *) h;
	      conn->recent_peek_cnt = 0LL;
	    }
	}
      last_conn_recent_peek_flush = st;
    }

  /* try to allocate arc jobs waiting for connection */
  process_retry_arc_jobs (be);

  if (be->initialized && be->init_job != NULL)
    {
      complete_be_job (be, be->init_job, 0);
      be->init_job = NULL;
    }

  /* garbage collect connections */
  for (h = be->garbage_conns.next; h != &be->garbage_conns; h = next)
    {
      gw_conn_t *conn;

      next = h->next;
      conn = gc_head_to_gw_conn (h);

      gw_conn_gc (be, conn);
    }

done:
  /* adjust interval and htw_ticks */
  et = currtime_millis ();
  interval = interval - (et - st);
  if (interval < 0)
    {
      interval = 0;
    }

  return interval;
}


/* 
 * This cron job serves one iteration of the zookeeper integration
 */
static int
be_zk_cron_wrap (aeEventLoop * el, long long id, void *clientData)
{
  be_t *be = (be_t *) clientData;
  be_handle_type_t saved = be->handle_type;
  int ret;

  be->handle_type = BE_HANDLE_TYPE_ZK_CRON;
  ret = be_zk_cron (el, id, clientData);
  be->handle_type = saved;
  return ret;
}

static int
be_zk_cron (aeEventLoop * el, long long id, void *clientData)
{
  be_t *be;
  long long curr;

  be = (be_t *) clientData;
  be->be_errno = 0;
  if (be->be_exit)
    {
      return AE_NOMORE;
    }

  curr = currtime_millis ();
  if (be->zk_force_reconn)
    {
      be->zk_retry_at = curr + be->conf.zk_reconnect_millis;
      be->zk_force_reconn = 0;
    }
  else if (be->zk_retry_at > 0 && be->zk_retry_at <= curr)
    {
      be->zk_retry_at = 0;
      zk_try_to_connect (be);
    }

  return BACKEND_DEFAULT_ZK_CRON_INTERVAL;
}

static void
zk_cleanup (be_t * be)
{
  be->zk_state = ZK_STATE_NONE;
  if (be->zh != NULL)
    {
      zookeeper_close ((zhandle_t *) be->zh);
      be->zh = NULL;
    }

  /* also ignore error during close */
  be->zk_force_reconn = 0;
  be->zk_retry_at = 0;

  htw_del (be, &be->zk_to_job);
  if (be->zk_interest_fd != -1)
    {
      if (be->zk_r_prepared)
	{
	  aeDeleteFileEvent (be->el, be->zk_interest_fd, AE_READABLE);
	  be->zk_r_prepared = 0;
	}
      if (be->zk_w_prepared)
	{
	  aeDeleteFileEvent (be->el, be->zk_interest_fd, AE_WRITABLE);
	  be->zk_w_prepared = 0;
	}
      /* zookeepr had the ownership of the descriptor */
      be->zk_interest_fd = -1;
    }
  be->zk_events = 0;
}

static void
zk_force_reconn (be_t * be)
{
  LOG_DEBUG (be, "Register flag to forced reconnection to the zookeeper");
  be->zk_force_reconn = 1;
}

static void
zk_try_to_connect (be_t * be)
{
  int ret;

  zk_cleanup (be);

  LOG_INFO (be, "Try to connect zookeeper %s", be->zk_host);
#ifndef _WIN32
  be->zh =
    zookeeper_init (be->zk_host, zk_watcher,
		    be->conf.zk_session_timeout_millis, NULL, be, 0);
#else
  be->zh =
    zookeeper_init (be->zk_host, zk_watcher,
		    be->conf.zk_session_timeout_millis, NULL, be, 0,
		    aeWinCloseComm);
#endif
  if (be->zh != NULL)
    {
      be->zk_retry_at = 0;
      ret = zk_evl_top_half (be);
      if (ret == 0)
	{
	  return;
	}
    }

  LOG_ERROR (be, "Failed to connect to zookeeper:%s errno=%d",
	     be->zk_host, be->be_errno);
  zk_force_reconn (be);
  return;
}

static void
zk_watcher (zhandle_t * zzh, int type, int state, const char *path,
	    void *context)
{
  be_t *be = (be_t *) context;
  int add_watch = 0;
  int add_get = 0;
  int ret;

  LOG_INFO (be, "Callback zk_watcher type:%d state:%d path:%s", type, state,
	    path ? path : "");
  if (type == ZOO_SESSION_EVENT && state == ZOO_CONNECTED_STATE)
    {
      LOG_INFO (be, "Connected to the zookeeper");
      add_watch = 1;
      add_get = 1;
    }
  else if (type == ZOO_CHILD_EVENT)
    {
      LOG_INFO (be, "Got ZOO_CHILD_EVENT");
      add_watch = 1;
    }
  else if (type == ZOO_CHANGED_EVENT)
    {
      LOG_INFO (be, "Got ZOO_CHANGED_EVENT");
      add_get = 1;
    }
  else
    {
      goto zk_error;
    }

  if (add_watch)
    {
      static const char *gw_list_fmt = "/RC/NOTIFICATION/CLUSTER/%s/GW";
      char buf[8192];
      char *path = &buf[0];

      /* register gateway list watcher */
      ret = snprintf (path, sizeof (buf), gw_list_fmt, be->cluster_name);
      if (ret < 0 || ret >= sizeof (buf))
	{
	  goto zk_error;
	}

      ret =
	zoo_aget_children ((zhandle_t *) be->zh, path, 1,
			   zk_getchildren_completion, be);
      if (ret != ZOK)
	{
	  LOG_ERROR (be, "Failed zoo_aget_children %s", buf);
	  goto zk_error;
	}
      LOG_INFO (be, "Register asynchronous get children watcher event on %s",
		buf);
    }

  if (add_get)
    {
      static const char *gw_aff_fmt = "/RC/NOTIFICATION/CLUSTER/%s/AFFINITY";
      char buf[8192];
      char *path = &buf[0];

      /* register affinity node change event handler */
      ret = snprintf (path, sizeof (buf), gw_aff_fmt, be->cluster_name);
      if (ret < 0 || ret >= sizeof (buf))
	{
	  goto zk_error;
	}

      ret =
	zoo_aget ((zhandle_t *) be->zh, path, 1, zk_getaffinity_completion,
		  be);
      if (ret != ZOK)
	{
	  LOG_ERROR (be, "Failed zoo_aget %s", buf);
	  goto zk_error;
	}
      LOG_INFO (be, "Register asynchronous get watcher event on %s", buf);
    }
  return;

zk_error:
  zk_force_reconn (be);
  return;
}

static int
zk_evl_top_half (be_t * be)
{
  socket_t fd = -1;
  int interest = 0;
  struct timeval tv;
  int rc, ret;
  long long curr;
#ifdef _WIN32
  rc =
    zookeeper_interest ((zhandle_t *) be->zh, &fd, &interest, &tv,
			anetTcpNonBlockConnect);
#else
  rc = zookeeper_interest ((zhandle_t *) be->zh, &fd, &interest, &tv);
#endif
  if (rc == ZOK && fd != -1 && interest)
    {
      int to_read = 0;
      int to_write = 0;

      assert (be->zk_interest_fd == -1);
      assert (be->zk_r_prepared == 0);
      assert (be->zk_w_prepared == 0);

      be->zk_interest_fd = fd;
      to_read = (interest & ZOOKEEPER_READ) != 0;
      to_write = (interest & ZOOKEEPER_WRITE) != 0;

      if (to_read && be->zk_r_prepared == 0)
	{
	  BE_FI (ret, AE_ERR, 0);
	  ret =
	    aeCreateFileEvent (be->el, fd, AE_READABLE, zk_read_handler_wrap,
			       be);
	  FI_END ();
	  if (ret == AE_ERR)
	    {
	      BE_ERROR (ARC_ERR_AE_ADDFD);
	      zk_force_reconn (be);
	      return -1;
	    }
	  be->zk_r_prepared = 1;
	}

      if (to_write && be->zk_w_prepared == 0)
	{
	  BE_FI (ret, AE_ERR, 0);
	  ret =
	    aeCreateFileEvent (be->el, fd, AE_WRITABLE, zk_write_handler_wrap,
			       be);
	  FI_END ();
	  if (ret == AE_ERR)
	    {
	      BE_ERROR (ARC_ERR_AE_ADDFD);
	      zk_force_reconn (be);
	      return -1;
	    }
	  be->zk_w_prepared = 1;
	}
    }

  /* register timer event */
  curr = currtime_millis ();
  be->zk_to_job.to = curr + tv.tv_sec * 1000 + tv.tv_usec / 1000;
  htw_add (be, &be->zk_to_job);
  return 0;
}

static void
zk_read_handler_wrap (aeEventLoop * el, socket_t fd, void *clientData,
		      int mask)
{
  be_t *be = (be_t *) clientData;
  be_handle_type_t saved = be->handle_type;

  be->handle_type = BE_HANDLE_TYPE_ZK_IN;
  zk_read_handler (el, fd, clientData, mask);
  be->handle_type = saved;

}

static void
zk_read_handler (aeEventLoop * el, socket_t fd, void *clientData, int mask)
{
  be_t *be;
  int ret;

  be = (be_t *) clientData;
  be->zk_events |= ZOOKEEPER_READ;
  /* 
   * NOTE: if both AE_READABLE and AE_WRITABLE event is set in mask, 
   * ae calls read handler and then write handler
   */
  if (mask & AE_WRITABLE)
    {
      return;
    }

  ret = zk_evl_bottom_half (be);
  if (ret == -1)
    {
      BE_ERROR (ARC_ERR_GENERIC);
      zk_cleanup (be);
      zk_force_reconn (be);
    }
}

static void
zk_write_handler_wrap (aeEventLoop * el, socket_t fd, void *clientData,
		       int mask)
{
  be_t *be = (be_t *) clientData;
  be_handle_type_t saved = be->handle_type;

  be->handle_type = BE_HANDLE_TYPE_ZK_OUT;
  zk_write_handler (el, fd, clientData, mask);
  be->handle_type = saved;
}

static void
zk_write_handler (aeEventLoop * el, socket_t fd, void *clientData, int mask)
{
  be_t *be;
  int ret;

  be = (be_t *) clientData;
  be->zk_events |= ZOOKEEPER_WRITE;

  ret = zk_evl_bottom_half (be);
  if (ret == -1)
    {
      BE_ERROR (ARC_ERR_GENERIC);
      zk_cleanup (be);
      zk_force_reconn (be);
    }
}

static int
zk_evl_bottom_half (be_t * be)
{
  int rc;

  BE_FI (rc, ZOK + 1, 0);
  rc = zookeeper_process ((zhandle_t *) be->zh, be->zk_events);
  FI_END ();
  if (rc != ZOK)
    {
      BE_ERROR (ARC_ERR_ZOOKEEPER);
      zk_force_reconn (be);
      return -1;
    }

  if (be->zk_interest_fd != -1)
    {
      if (be->zk_r_prepared)
	{
	  aeDeleteFileEvent (be->el, be->zk_interest_fd, AE_READABLE);
	  be->zk_r_prepared = 0;
	}
      if (be->zk_w_prepared)
	{
	  aeDeleteFileEvent (be->el, be->zk_interest_fd, AE_WRITABLE);
	  be->zk_w_prepared = 0;
	}
      be->zk_interest_fd = -1;
    }
  htw_del (be, &be->zk_to_job);
  be->zk_events = 0;

  return zk_evl_top_half (be);
}

static short
get_short_val (char *idp)
{
  int gw_id = -1;

  if (isdigit (*idp))
    {
      gw_id = atoi (idp);
    }
  if (gw_id < 0 || gw_id > SHRT_MAX)
    {
      return -1;
    }
  return (short) gw_id;
}

static void
zk_getchildren_completion (int rc,
			   const struct String_vector *strings,
			   const void *data)
{
  be_t *be;
  int i, ret;
  char buf[8192];
  static const char *gw_fmt = "/RC/NOTIFICATION/CLUSTER/%s/GW/%d";
  gw_t **new_gw_ary = NULL;
  int new_n_gw = 0, new_gw_idx = 0;
  int is_also_error = 0;

  be = (be_t *) data;

  LOG_INFO (be, "Got zookeeper children event count:%d rc:%d",
	    strings ? strings->count : 0, rc);

  if (rc != ZOK)
    {
      /* 
       * This actually happens.
       *
       * GW
       *  |
       *  +-- A
       *  |
       *  *-- B
       *
       * When a test remove all entries including GW znode itself, event notification
       * sequence from zookeeper will be one of the below two cases.
       * (1) GW (A) -> GW () -> ZNONODE 
       * (2) GW (B) -> GW () -> ZNONODE
       * Take case (1) because they are isomorphic.
       *
       * Scenario.
       * 1. BE got GW(A)
       * 2. BE send watch(GW) to the zookeeper server
       * 3. the zookeeper server alreay applied the sequence upto ZNONODE state
       *
       * So it is possible for the BE to have active gateway if this kind of error is not handled separately.
       */
      be->zk_gwl_version++;
      is_also_error = 1;
      goto apply_new_gw_ary;
    }
  else
    {
      be->zk_gwl_version++;
    }

  if (!strings || strings->count < 1)
    {
      goto apply_new_gw_ary;
    }

  new_n_gw = strings->count;
  new_gw_ary = calloc (new_n_gw, sizeof (gw_t *));
  if (new_gw_ary == NULL)
    {
      goto zk_error;
    }

  /* make new gw_ary */
  for (i = 0; i < new_n_gw; i++)
    {
      int j;
      gw_t *gw = NULL;
      short gw_id = -1;
      char *idp = strings->data[i];

      gw_id = get_short_val (idp);
      if (gw_id < 0)
	{
	  LOG_ERROR (be, "Gateway ID:%s is not a short range number", idp);
	  goto zk_error;
	}

      for (j = 0; j < be->n_gw; j++)
	{
	  gw_t *gw_old = be->gw_ary[j];

	  if (gw_old == NULL)
	    {
	      continue;
	    }

	  if (gw_old->id == gw_id)
	    {
	      gw = gw_old;
	      break;
	    }
	}

      if (gw == NULL)
	{
	  LOG_INFO (be, "Gateway:%d is new", gw_id);
	  gw = create_gw_raw (be, gw_id, be->zk_gwl_version);
	  if (gw == NULL)
	    {
	      goto zk_error;
	    }
	}
      else
	{
	  LOG_INFO (be, "Gateway:%s is already in my gateway list",
		    format_gw (be, gw));
	}
      new_gw_ary[new_gw_idx++] = gw;
    }

  /* create zookeeper asynchonous get jobs */
  for (i = 0; i < new_n_gw; i++)
    {
      gw_t *gw = new_gw_ary[i];

      if (gw->gwl_version == be->zk_gwl_version)
	{
	  gw_completion_t *gwc = NULL;

	  gwc = create_gw_completion (be, gw->id, be->zk_gwl_version);
	  if (gwc == NULL)
	    {
	      BE_ERROR (ARC_ERR_GENERIC);
	      goto zk_error;
	    }

	  snprintf (buf, sizeof (buf), gw_fmt, be->cluster_name, gw->id);
	  BE_FI (ret, -1, 0);
	  ret =
	    zoo_aget ((zhandle_t *) be->zh, buf, 0, zk_getgw_completion, gwc);
	  FI_END ();
	  if (ret != 0)
	    {
	      LOG_ERROR (be,
			 "Failed to asynchronous get node data of %s version=%ld",
			 buf, gwc->version);
	      destroy_gw_completion (gwc);
	      BE_ERROR (ARC_ERR_ZOOKEEPER);
	      goto zk_error;
	    }
	  LOG_INFO (be,
		    "Register asynchronous get node data of %s version=%ld",
		    buf, gwc->version);
	}
    }

apply_new_gw_ary:
  /* purge old gateways.  After this point, no redo can be done! */
  for (i = 0; i < new_n_gw; i++)
    {
      new_gw_ary[i]->gwl_version = be->zk_gwl_version;
    }

  for (i = 0; i < be->n_gw; i++)
    {
      gw_t *gw = be->gw_ary[i];
      if (gw->gwl_version != be->zk_gwl_version)
	{
	  dlisth *h, *next;

	  LOG_INFO (be, "Gateway:%s is set to unused", format_gw (be, gw));
	  gw->state = GW_STATE_UNUSED;
	  gw->n_active = 0;
	  for (h = gw->conns.next; h != &gw->conns; h = next)
	    {
	      gw_conn_t *conn = (gw_conn_t *) h;
	      next = h->next;

	      gw_conn_change_state (be, conn, GW_CONN_CLOSING);
	      gw_conn_try_to_close (be, conn);
	    }
	}
    }

  if (be->gw_ary != NULL)
    {
      free (be->gw_ary);
      be->gw_ary = NULL;
      be->n_gw = 0;
    }

  be->n_gw = new_n_gw;
  be->gw_ary = new_gw_ary;
  new_gw_ary = NULL;
  if (is_also_error == 0)
    {
      return;
    }

zk_error:
  if (new_gw_ary != NULL)
    {
      for (i = 0; i < new_n_gw; i++)
	{
	  /* remove my version only */
	  if (new_gw_ary[i] != NULL
	      && new_gw_ary[i]->gwl_version == be->zk_gwl_version)
	    {
	      destroy_gw (be, new_gw_ary[i]);
	    }
	}
      free (new_gw_ary);
    }
  zk_force_reconn (be);
}

static void
zk_getgw_completion (int rc, const char *value, int value_len,
		     const struct Stat *stat, const void *data)
{
  int ret;
  gw_completion_t *gwc = NULL;
  gw_t *gw = NULL;
  int i;
  be_t *be;
  char buf[4096];
  char *bp = &buf[0];

  gwc = (gw_completion_t *) data;
  assert (gwc != NULL);
  be = gwc->be;

  if (rc != ZOK || value == NULL || value_len < 0)
    {
      LOG_ERROR (be, "Zookeeper error:%d", rc);
      if (gwc != NULL)
	{
	  destroy_gw_completion (gwc);
	}
      return;
    }

  if (value_len + 1 > sizeof (buf))
    {
      bp = malloc (value_len + 1);
      if (bp == NULL)
	{
	  LOG_ERROR (be, "Failed to malloc:%d", value_len + 1);
	  if (gwc != NULL)
	    {
	      destroy_gw_completion (gwc);
	    }
	  return;
	}
    }
  memcpy (bp, value, value_len);
  bp[value_len] = '\0';

  LOG_INFO (be, "Got zookeeper node id:%d, version:%lu data:%s", gwc->id,
	    gwc->version, bp);

  /* find raw gateway whose id and version matches */
  for (i = 0; i < be->n_gw; i++)
    {
      gw = be->gw_ary[i];
      if (gw->state == GW_STATE_UNUSED && gw->gwl_version == gwc->version
	  && gw->id == gwc->id)
	{
	  break;
	}
      gw = NULL;
    }

  if (gw == NULL)
    {
      goto done;
    }

  assert (gw->host == NULL);
  ret = gw_apply_completion (be, gw, gwc, value, value_len);
  if (ret == -1)
    {
      BE_ERROR (ARC_ERR_GENERIC);
      zk_force_reconn (gwc->be);
    }

done:
  if (gwc)
    {
      destroy_gw_completion (gwc);
    }
  if (bp != &buf[0])
    {
      free (bp);
    }
  return;
}

static gw_completion_t *
create_gw_completion (be_t * be, short id, long version)
{
  gw_completion_t *gwc;

  BE_FI (gwc, NULL, 0);
  gwc = malloc (sizeof (gw_completion_t));
  FI_END ();
  if (gwc == NULL)
    {
      BE_ERROR (ARC_ERR_NOMEM);
      return NULL;
    }

  gwc->id = id;
  gwc->version = version;
  gwc->be = be;

  return gwc;
}

static void
destroy_gw_completion (gw_completion_t * gwc)
{
  if (gwc == NULL)
    {
      return;
    }

  free (gwc);
  return;
}

static int
aff_check_rle (char *rle)
{
  int count = 0;
  char *p = rle;

  // skip leading spaces
  while (isspace (*p))
    {
      p++;
    }

  while (*p)
    {
      int c;
      if (*p != GW_AFF_NONE && *p != GW_AFF_READ && *p != GW_AFF_WRITE
	  && *p != GW_AFF_ALL)
	{
	  return -1;
	}
      p++;
      c = get_short_val (p);
      if (c < 0)
	{
	  return -1;
	}
      count += c;
      if (count > CLUSTER_SIZE)
	{
	  return -1;
	}
      while (*p && isdigit (*p))
	{
	  p++;
	}
    }

  // skip trailing spaces
  while (isspace (*p))
    {
      p++;
    }
  if (*p)
    {
      return -1;
    }

  if (count != CLUSTER_SIZE)
    {
      return -1;
    }
  return 0;
}

/* 
 * Note: 
 * 1. This function does not check the well-formedness of JSON data and asuumes..
 *    - gw_id property value type is a number
 *    - affinity property value type is a string
 *    - total length of RLE is CLUSTER_SIZE (8192)
 * 2. The content of the buf is touched
 *
 * Example
 * [{"gw_id":1,"affinity":"A8192"},{"gw_id":2,"affinity":"A8192"}]
 *
 */
static int
aff_data_parse_check_and_peek (be_t * be, char *buf, short gwid_ary[],
			       char *aff_ary[], int *n_item)
{
  int ni = 0;
  int ret;
  char *p = buf;
  char *ep = buf + strlen (buf);
  char *gw_id = NULL, *affinity = NULL;
  int gw_id_len, affinity_len;

  ERROR_IF_NULL ((p = expect_token (p, '[')));
  p++;

  while (strchr (p, '{') != NULL && ni < CLUSTER_SIZE)
    {
      p++;

      // gw_id
      ret = json_peek_integer (be, p, ep - p, "gw_id", &gw_id, &gw_id_len);
      if (ret < 0)
	{
	  BE_ERROR (ARC_ERR_BAD_ZKDATA);
	  return -1;
	}
      gwid_ary[ni] = get_short_val (gw_id);
      if (gwid_ary[ni] < 0)
	{
	  BE_ERROR (ARC_ERR_BAD_ZKDATA);
	  return -1;
	}

      // affinity
      ret =
	json_peek_string (be, p, ep - p, "affinity", &affinity,
			  &affinity_len);
      if (ret < 0)
	{
	  BE_ERROR (ARC_ERR_BAD_ZKDATA);
	  return -1;
	}
      *(affinity + affinity_len) = '\0';
      ret = aff_check_rle (affinity);
      if (ret < 0)
	{
	  BE_ERROR (ARC_ERR_BAD_ZKDATA);
	  return -1;
	}
      aff_ary[ni] = affinity;

      // ajust p
      if (affinity > gw_id)
	{
	  p = affinity + affinity_len;
	}
      else
	{
	  p = gw_id + gw_id_len;
	}
      ERROR_IF_NULL (p = strchr (p, '}'));
      p++;
      ni++;
    }
  ERROR_IF_NULL (p = expect_token (p, ']'));

  *n_item = ni;
  return 0;
}


static void
zk_getaffinity_completion (int rc, const char *value,
			   int value_len, const struct Stat *stat,
			   const void *data)
{
  be_t *be = (be_t *) data;
  char *buf = NULL;
  short gwid_ary[CLUSTER_SIZE];
  char *aff_ary[CLUSTER_SIZE];	// pointer to affinity property value over buf (peek)
  int i, j, n_item;

  LOG_INFO (be, "Callback zk_getaffinity_completion rc:%d value:%s", rc,
	    value ? value : "");
  if (rc != ZOK || value == NULL || value_len < 0)
    {
      LOG_ERROR (be,
		 "Zookeeper error:%d, No more affinity watching until reconnect",
		 rc);
      return;
    }

  buf = malloc (value_len + 1);
  if (!buf)
    {
      LOG_ERROR (be, "Failed to allocate temporay memory. size:%d",
		 strlen (value));
      return;
    }
  memcpy (buf, value, value_len);
  buf[value_len] = '\0';

  // Parse affinity data
  if (aff_data_parse_check_and_peek (be, buf, gwid_ary, aff_ary, &n_item) < 0)
    {
      LOG_ERROR (be, "Failed to parse affinity data, ignore this change");
      goto done;
    }

  // reset current gateway
  for (i = 0; i < CLUSTER_SIZE; i++)
    {
      init_pn_gw_aff (&be->pn_gw_aff[i]);
    }

  // Apply new affinity specification 
  for (i = 0; i < n_item; i++)
    {
      short gwid = gwid_ary[i];
      char *p = aff_ary[i];
      int curr = 0;

      while (isspace (*p))
	{
	  p++;
	}
      while (*p)
	{
	  int aff;
	  int len;

	  aff = *p++;
	  len = get_short_val (p);

	  for (j = curr; j < curr + len; j++)
	    {
	      pn_gw_aff_t *pn_gw_aff = &be->pn_gw_aff[j];

	      if (aff == GW_AFF_WRITE || aff == GW_AFF_ALL)
		{
		  int k;
		  for (k = 0; k < MAX_WRITE_GW_AFF_IDX; k++)
		    {
		      if (pn_gw_aff->w_ids[k] == gwid)
			{
			  // already registered
			  break;
			}
		      else if (pn_gw_aff->r_ids[k] == -1)
			{
			  pn_gw_aff->w_ids[k] = gwid;
			  break;
			}
		    }
		}

	      if (aff == GW_AFF_ALL || aff == GW_AFF_READ)
		{
		  int k;
		  for (k = 0; k < MAX_READ_GW_AFF_IDX; k++)
		    {
		      if (pn_gw_aff->r_ids[k] == gwid)
			{
			  // already registered
			  break;
			}
		      else if (pn_gw_aff->r_ids[k] == -1)
			{
			  pn_gw_aff->r_ids[k] = gwid;
			  break;
			}
		    }
		}
	    }
	  curr += len;

	  while (isdigit (*p))
	    {
	      p++;
	    }
	  while (isspace (*p))
	    {
	      p++;
	    }
	}
    }
  be->use_affinity = 1;

done:
  if (buf != NULL)
    {
      free (buf);
    }

  return;
}

/* -------- */
/* EXPORTED */
/* -------- */
void *
be_thread (void *arg)
{
  be_t *be = (be_t *) arg;
  sigset_t sigset;
  long long cron_id = -1LL;
  long long zk_cron_id = -1LL;
  int ret;

  /* no bla bla bla */
  zoo_set_debug_level (0);
  be->handle_type = BE_HANDLE_TYPE_MAIN;

  /* LOG configuration */
  LOG_INFO (be, "Backend started");
  LOG_INFO (be, "\tnum_conn_per_gw:%d", be->conf.num_conn_per_gw);
  LOG_INFO (be, "\tinit_timeout_millis:%d", be->conf.init_timeout_millis);
  LOG_INFO (be, "\tlog_level:%d", be->conf.log_level);
  if (be->app_log_prefix_size > 0)
    {
      char buf[PATH_MAX];
      memcpy (buf, be->app_log_buf, be->app_log_prefix_size);
      buf[be->app_log_prefix_size] = '\0';
      LOG_INFO (be, "\tlog_file_prefix:%s", buf);
    }
  LOG_INFO (be, "\tmax_fd:%d", be->conf.max_fd);
  LOG_INFO (be, "\tconn_reconnect_millis:%d", be->conf.conn_reconnect_millis);
  LOG_INFO (be, "\tzk_reconnect_millis:%d", be->conf.zk_reconnect_millis);
  LOG_INFO (be, "\tzk_session_timeout_millis:%d",
	    be->conf.zk_session_timeout_millis);

#ifdef _WIN32
  /* set iocp handles */
  aeWinSetIOCP (be->iocph);
  aeWinSetIOCPState (be->iocp_state);
#endif

  if (be->use_zk)
    {
      LOG_INFO (be, "\tzookeeper host:%s, cluster name:%s", be->zk_host,
		be->cluster_name);
    }
  else
    {
      LOG_INFO (be, "\tgateway hosts:%s", be->gw_hosts);
    }

  /* signal handling */
  sigemptyset (&sigset);
  sigaddset (&sigset, SIGALRM);
  if ((ret = pthread_sigmask (SIG_BLOCK, &sigset, NULL)) != 0)
    {
      goto wait_for_fe_term;
    }

  /* setup cron job */
  cron_id = aeCreateTimeEvent (be->el, 1, be_cron_wrap, be, NULL);
  if (cron_id == -1LL)
    {
      goto wait_for_fe_term;
    }

  /* setup zookeeper cron job */
  if (be->use_zk)
    {
      zk_cron_id = aeCreateTimeEvent (be->el, 1, be_zk_cron_wrap, be, NULL);
      if (zk_cron_id == -1LL)
	{
	  goto wait_for_fe_term;
	}
    }

  /* setup pipe request handler */
  ret =
    aeCreateFileEvent (be->el, (socket_t) be->pipe_fd, AE_READABLE,
		       be_job_handler_wrap, be);
  if (ret != AE_OK)
    {
      BE_ERROR (ARC_ERR_AE_ADDFD);
      goto wait_for_fe_term;
    }

  /* set backend htw_tick */
  be->htw_tick = htw_millis_to_tick (be, currtime_millis ());

  /* prepare connections */
  if (be->use_zk)
    {
      zk_try_to_connect (be);
    }
  else
    {
      ret = make_connection_pool (be);
      if (ret == -1)
	{
	  goto wait_for_fe_term;
	}
    }

  /* run main thread */
  aeMain (be->el);

  if (cron_id != -1LL)
    {
      aeDeleteTimeEvent (be->el, cron_id);
    }

  if (zk_cron_id != -1LL)
    {
      aeDeleteTimeEvent (be->el, zk_cron_id);
    }

  /* graceful exit */
  abort_be (be);

wait_for_fe_term:
  while (be->abrupt_exit == 0)
    {
      usleep (1000);		//1 msec
    }
  destroy_be (be);

  return NULL;
}

#ifdef _WIN32
#pragma warning(pop)
#endif

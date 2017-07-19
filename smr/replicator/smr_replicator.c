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

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <stdarg.h>
#include <limits.h>
#include <ctype.h>
#include <fcntl.h>

#include "common.h"
#include "log_mmap.h"
#include "tcp.h"
#include "smr.h"
#include "smrmp.h"
#include "crc16.h"
#include "sfi.h"
#include "gpbuf.h"

static const char *_usage =
  "smrReplicator                                                                   \n"
  "Usage: %s <options>                                                             \n"
  "option:                                                                         \n"
  "    -D daemonize  (default false)                                               \n"
  "    -l <app log file prefix> (default stdout)                                   \n"
  "    -d <smr log file directory path>                                            \n"
  "    -b <base listen port> (default: 1900)                                       \n"
  "    -x <checkpointed log delete delay in seconds> (default: 86400)              \n"
  "    -k slave connection idle timeout in msec. (0: no timeout) (default0)        \n"
  "    -v <verbose level> (0: error, 1: warn, 2: info, 3:debug)  (default 2)       \n";

static const char *_logo =
  "     ___           ___           ___                     \n"
  "    /  /\\         /__/\\         /  /\\                 \n"
  "   /  /:/_       |  |::\\       /  /::\\                 \n"
  "  /  /:/ /\\      |  |:|:\\     /  /:/\\:\\              \n"
  " /  /:/ /::\\   __|__|:|\\:\\   /  /:/~/:/               \n"
  "/__/:/ /:/\\:\\ /__/::::| \\:\\ /__/:/ /:/___            \n"
  "\\  \\:\\/:/~/:/ \\  \\:\\~~\\__\\/ \\  \\:\\/:::::/     \n"
  " \\  \\::/ /:/   \\  \\:\\        \\  \\::/~~~~          \n"
  "  \\__\\/ /:/     \\  \\:\\        \\  \\:\\             \n"
  "    /__/:/       \\  \\:\\        \\  \\:\\              \n"
  "    \\__\\/         \\__\\/         \\__\\/              \n"
  "                                                         \n"
  " General purpose state machine replicator.               \n"
  "                                                         \n";

/* ---------------------- */
/* Local type definitions */
/* ---------------------- */
#define CHECK(_r) do {    \
    if (_r < 0)           \
      goto error;         \
} while(0)


#define MAX_SLOW_SUB 16
typedef struct slowBrief_ slowBrief;
struct slowBrief_
{
  int num_subs;
  struct
  {
    char sub1;
    char sub2;
    int count;
  } subs[MAX_SLOW_SUB];
};
#define init_slow_brief(b) do {        \
  int _i;                              \
  (b)->num_subs = 0;                   \
  for(_i=0;_i<MAX_SLOW_SUB;_i++) {     \
    (b)->subs[_i].sub1 = '\0';         \
    (b)->subs[_i].sub2 = '\0';         \
    (b)->subs[_i].count = 0;           \
  }                                    \
} while(0)

#define SLOWLOG_QUERY_INDEX_UB  100000
typedef struct slowLogQuery_ slowLogQuery;
struct slowLogQuery_
{
  int asc;
  int is_index;
  union
  {
    int index;
    long long ts;
  } index;
  int count;
  int curr_index;
  int curr_count;
  gpbuf_t *gp;
};
#define init_slowlog_query(q) do {    \
  (q)->asc = 0;                       \
  (q)->is_index = 0;                  \
  (q)->index.index = 0;               \
  (q)->index.ts = 0LL;                \
  (q)->count = 0;                     \
  (q)->curr_index = 0;                \
  (q)->curr_count = 0;                \
  (q)->gp = NULL;                     \
} while(0)

#define LOG LOG_TEMPLATE

#define MIG_ERR(r,...) do {                                           \
  (r)->mig_ret = -1;                                                  \
  snprintf((r)->mig_ebuf, sizeof((r)->mig_ebuf), __VA_ARGS__);        \
  if(LG_ERROR <= (r)->log_level) {                                    \
    log_msg((r), LG_ERROR,__VA_ARGS__);                               \
  }                                                                   \
} while (0)

#define DEFAULT_HANDSHAKE_TIMEOUT_MSEC 1000

/* --------------------------- */
/* Local function declarations */
/* --------------------------- */
/* init/term */
static void init_replicator (smrReplicator * rep,
			     repRole role, smrLog * smrlog, int base_port,
			     int log_level, int slave_idle_to);
static void term_replicator (smrReplicator * rep);
/* -------------------- */
/* basic event handling */
/* -------------------- */
/* master role */
static int check_client_nid (smrReplicator * rep, short nid);
static int check_slave_nid (smrReplicator * rep, short nid, int *duplicated);
static int prepare_slaves_for_write (smrReplicator * rep);
static int seq_compar (const void *v1, const void *v2);
static int check_publish_seq (smrReplicator * rep, long long rcvd_seq,
			      int *ok_commit, long long *log_commit_seq);
static int publish_commit_or_safe_seq (smrReplicator * rep,
				       long long rcvd_seq);

/* log file management */
static int log_rewind_to_cseq (smrReplicator * rep, long long seq);
static int log_write_wait_room (smrReplicator * rep);
static int log_write (smrReplicator * rep, char *buf, int sz);
static int log_write_client_conn (clientConn * cc, int n_safe);
static int adjust_log_before_lconn (smrReplicator * rep, long long be_seq);

/* common stream functions */
static int stream_read (ioStream * ios, void *buf_, int size, int purge,
			struct segPeek *sp);
static int stream_write (ioStream * ios, void *buf_, int size);
static int stream_write_fd (connType conn_type, ioStream * ios, int fd);
static int fd_write_stream (connType conn_type, int fd, ioStream * ios,
			    int *has_more, int *n_sent);

/* be configuration */
static int be_configure_msg (char *buf, int bufsz, char *fmt, ...);

/* ---------------- */
/* local connection */
/* ---------------- */
static int local_publish_safe_seq (localConn * lc, long long safe_seq);
static void free_local_conn_with_role_change (localConn * lc, int change);
static void free_local_conn (localConn * lc);
static int local_prepare_write (localConn * lc);
static void local_accept (smrReplicator * rep, aeEventLoop * el, int fd);
static void local_read (localConn * lc, aeEventLoop * el, int fd);
static void local_write (localConn * lc, aeEventLoop * el, int fd);

static void local_accept_handler (aeEventLoop * el, int fd, void *data,
				  int mask);
static void local_read_handler (aeEventLoop * el, int fd, void *data,
				int mask);
static void local_write_handler (aeEventLoop * el, int fd, void *data,
				 int mask);
/* ---------------- */
/* slave connection */
/* ---------------- */
static void free_slave_conn (slaveConn * sc);
static void free_slave_conn_by_nid (smrReplicator * rep, int nid);
static int slave_prepare_write (slaveConn * sc);
static void slave_accept (smrReplicator * rep, aeEventLoop * el, int fd);
static void slave_read (slaveConn * sc, aeEventLoop * el, int fd);
static void slave_write (slaveConn * sc, aeEventLoop * el, int fd);
static void slave_accept_handler (aeEventLoop * el, int fd, void *data,
				  int mask);
static void slave_read_handler (aeEventLoop * el, int fd, void *data,
				int mask);
static void slave_write_handler (aeEventLoop * el, int fd, void *data,
				 int mask);
static int slave_log_get_buf (slaveConn * sc, long long from, long long to,
			      char **buf, int *avail);
static int slave_send_log (slaveConn * sc);
/* ----------------- */
/* client connection */
/* ----------------- */
static void free_client_conn (clientConn * cc);
static void free_client_conn_by_nid (smrReplicator * rep, int nid);
static int client_prepare_write (clientConn * cc);
static void client_accept (smrReplicator * rep, aeEventLoop * el, int fd);
static void timestamp_peek (struct segPeek *sp);
static int client_read (clientConn * cc, aeEventLoop * el, int fd, int force);
static void client_write (clientConn * cc, aeEventLoop * el, int fd);

static void client_accept_handler (aeEventLoop * el, int fd, void *data,
				   int mask);
static void client_read_handler (aeEventLoop * el, int fd, void *data,
				 int mask);
static void client_write_handler (aeEventLoop * el, int fd, void *data,
				  int mask);
/* ----------------- */
/* master connection */
/* ----------------- */
static void free_master_conn (masterConn * mc);
static int connect_to_master (smrReplicator * rep, char *host, int port,
			      long long seq,
			      long long *master_max_msgend_seq);
static int master_prepare_write (masterConn * mc);
static int process_master_input (masterConn * mc, char *buf, int sz);
static void master_read (masterConn * mc, aeEventLoop * el, int fd);
static void master_write (masterConn * mc, aeEventLoop * el, int fd);

static void master_read_handler (aeEventLoop * el, int fd, void *data,
				 int mask);
static void master_write_handler (aeEventLoop * el, int fd, void *data,
				  int mask);
/* --------------------- */
/* management connection */
/* --------------------- */
static int mgmt_reply_cstring (mgmtConn * conn, char *fmt, ...);
static void free_mgmt_conn (mgmtConn * conn);
static int mgmt_prepare_write (mgmtConn * conn);
static void mgmt_accept (smrReplicator * rep, aeEventLoop * el, int fd);
static void mgmt_accept_handler (aeEventLoop * el, int fd, void *data,
				 int mask);
static void mgmt_read (mgmtConn * conn, aeEventLoop * el, int fd);
static void mgmt_read_handler (aeEventLoop * el, int fd, void *data,
			       int mask);
static void mgmt_write (mgmtConn * conn, aeEventLoop * el, int fd);
static void mgmt_write_handler (aeEventLoop * el, int fd, void *data,
				int mask);
/* -------------------- */
/* management functions */
/* -------------------- */
/* util */
static long long cron_count_diff_msec (long long was, long long is);
static int parse_ll (char *tok, long long *seq);
static int parse_seq (char *tok, long long *seq);
static int parse_int (char *tok, int *rval);
static int parse_nid (char *tok, short *nid);
static int parse_quorum_policy (char *pol, int *dest);
static int parse_quorum_members (short master_nid, char *members[],
				 short dest[SMR_MAX_SLAVES],
				 int *num_members);
static int notify_be_new_master (smrReplicator * rep, char *master_host,
				 int client_port, long long last_cseq,
				 short nid);
static int notify_be_rckpt (smrReplicator * rep, char *behost, int beport);
static int notify_be_init (smrReplicator * rep, char *log_dir,
			   char *master_host, int master_client_port,
			   long long commit_seq, short nid);
static int notify_be_lconn (smrReplicator * rep);
static int do_virtual_progress (smrReplicator * rep);
static int nid_is_quorum_member (smrReplicator * rep, short nid);
/* role master */
static int do_role_master (mgmtConn * conn, long long rewind_cseq);
static int role_master_request (mgmtConn * conn, char **tokens, int num_tok);
/* role slave */
static int do_role_slave (mgmtConn * conn, const char *host,
			  int base_port, long long rewind_cseq);
static int role_slave_request (mgmtConn * conn, char **tokens, int num_tok);
/* role none */
static int do_role_lconn (smrReplicator * rep);
static int role_lconn_request (mgmtConn * conn, char **tokens, int num_tok);
/* role none */
static int do_role_none (smrReplicator * rep);
static int role_none_request (mgmtConn * conn, char **tokens, int num_tok);
/* rckpt */
static int do_rckpt (smrReplicator * rep, char *behost, int beport);
static int rckpt_request (mgmtConn * conn, char **tokens, int num_tok);
/* migrate */
static int migrate_start_request (smrReplicator * rep, char **tokens,
				  int num_tok);
#ifdef SFI_ENABLED
/* fault injection */
static SFI_CB_RET fi_delay_callback (const char *name, sfi_probe_arg * pargs,
				     void *arg);
static SFI_CB_RET fi_rw_delay_callback (const char *name,
					sfi_probe_arg * pargs, void *arg);
static SFI_CB_RET fi_rw_return_callback (const char *name,
					 sfi_probe_arg * pargs, void *arg);
#endif

static int fi_delay_request (mgmtConn * conn, char **tokens, int num_tok);
static int fi_rw_request (char *rw, mgmtConn * conn, char **tokens,
			  int num_tok);

/* general */
static int role_request (mgmtConn * conn, char **tokens, int num_tok);
static int migrate_request (mgmtConn * conn, char **tokens, int num_tok);
static int getseq_request (mgmtConn * conn, char **tokens, int num_tok);
static int setquorum_request (mgmtConn * conn, char **tokens, int num_tok);
static int getquorum_request (mgmtConn * conn, char **tokens, int num_tok);
static int getquorumv_request (mgmtConn * conn, char **tokens, int num_tok);
static int singleton_request (mgmtConn * conn, char **tokens, int num_tok);
static int confget_request (mgmtConn * conn, char **tokens, int num_tok);
static int confset_request (mgmtConn * conn, char **tokens, int num_tok);
static char *get_role_string (repRole role);
static int info_replicator (smrReplicator * rep, gpbuf_t * gp);
static int info_slowlog_brief (slowLogEntry * e, void *arg);
static int info_slowlog (smrReplicator * rep, gpbuf_t * gp);
static int info_request (mgmtConn * conn, char **tokens, int num_tok);
static int slowlog_mapf (slowLogEntry * e, void *arg);
static int slowlog_request (mgmtConn * conn, char **tokens, int num_tok);
static int smrversion_request (mgmtConn * conn, char **tokens, int num_tok);
/* top handler */
static int mgmt_request (mgmtConn * conn, char *bp, char *ep);

/* ---- */
/* misc */
/* ---- */
/* signal handlers */
static void sig_handler (int sig);
static void setup_sigterm_handler (void);
static void setup_sigint_handler (void);
/* migration thread */
static void free_mig_conn (migStruct * m, int interrupt);
static void free_mig_struct (migStruct * m);
static int launch_mig_thread (smrReplicator * rep, char *dest_host,
			      int dest_base_port, long long from_seq,
			      int num_part, char **tokens, int num_tok,
			      int tps_limit);
static void mig_read_handler (aeEventLoop * el, int fd, void *data, int mask);
static void mig_read (aeEventLoop * el, int fd, void *data, int mask);
static int mig_prepare_write (migStruct * mig);
static int mig_log_get_buf (migStruct * mig, long long seq, long long limit,
			    char **buf, int *buf_sz);
static int mig_process_log (migStruct * mig, int max_send);
static int
mig_process_buf (migStruct * mig, char *buf, int buf_sz, int max_msg,
		 int *bytes_used, int *msg_sent, int *n_buffered);
static void mig_write_handler (aeEventLoop * el, int fd, void *data,
			       int mask);
static void mig_write (aeEventLoop * el, int fd, void *data, int mask);
static int mig_cron (struct aeEventLoop *eventLoop, long long id,
		     void *clientData);
static int mig_dest_connect (smrReplicator * rep);
static void *mig_thread (void *arg);
/* background io thread */
static int launch_bio_thread (smrReplicator * rep);
/* ae event timer etc. */
static void cron_update_est_min_seq (smrReplicator * rep);
static void cron_update_stat (smrReplicator * rep, repStat * prev_stat,
			      time_t * prev_time);
static int server_cron (struct aeEventLoop *eventLoop, long long id,
			void *clientData);
static void before_sleep_handler (struct aeEventLoop *eventLoop);
/* misc for main */
static void print_usage (char *prog);
/* servers */
static int initialize_services (smrReplicator * rep);
/* slow log */
static int initialize_slowlog (smrReplicator * rep);

/* ------------------ */
/* Exported Variables */
/* ------------------ */
smrReplicator s_Server;

/* -------------------------- */
/* Local function definitions */
/* -------------------------- */
static void
init_replicator (smrReplicator * rep, repRole role, smrLog * smrlog,
		 int base_port, int log_level, int slave_idle_to)
{
  struct timeval tv;
  long long ts;
  int i;
  assert (rep != NULL);

  gettimeofday (&tv, NULL);
  ts = ((long long) tv.tv_sec) * 1000;
  ts += tv.tv_usec / 1000;

  /* general conf */
  rep->need_rckpt = 0;
  rep->role = role;
  rep->role_id = ts;
  rep->base_port = base_port;
  rep->local_port = base_port + PORT_OFF_LOCAL;
  rep->local_lfd = -1;
  rep->client_port = base_port + PORT_OFF_CLIENT;
  rep->client_lfd = -1;
  rep->slave_port = base_port + PORT_OFF_SLAVE;
  rep->slave_lfd = -1;
  rep->mgmt_port = base_port + PORT_OFF_MGMT;
  rep->mgmt_lfd = -1;
  rep->log_dir = NULL;
  rep->commit_quorum = 0;
  rep->has_quorum_members = 0;
  for (i = 0; i < SMR_MAX_SLAVES; i++)
    {
      rep->quorum_members[i] = -1;
    }
  rep->master_host = NULL;
  rep->master_base_port = -1;
  /* smr log related */
  rep->smrlog = smrlog;
  rep->curr_log = NULL;
  aseq_init (&rep->min_seq);
  rep->be_ckpt_seq = 0LL;
  rep->commit_seq = 0LL;
  rep->last_commit_msg_end = 0LL;
  rep->num_trailing_commit_msg = 0;
  aseq_init (&rep->log_seq);
  aseq_init (&rep->sync_seq);
  aseq_init (&rep->cron_est_min_seq);
  aseq_init (&rep->mig_seq);
  rep->log_delete_gap = 86400;
  /* general */
  rep->nid = -1;
  rep->stat_time = tv;
  memset (&rep->stat, 0, sizeof (repStat));
  /* local */
  rep->local_client = NULL;
  /* salve only */
  rep->master = NULL;
  /* master only */
  dlisth_init (&rep->clients);
  rep->num_slaves = 0;
  dlisth_init (&rep->slaves);
  rep->slave_idle_to = slave_idle_to;
  /* management */
  dlisth_init (&rep->managers);
  /* event loop */
  rep->teid = -1LL;
  rep->el = NULL;
  rep->cron_count = 0LL;
  rep->interrupted = 0;
  rep->cron_need_publish = 0;
  rep->cron_need_progress = 0;
  rep->progress_seq = 0LL;
  /* logging & debugging */
  rep->app_log_prefix_size = 0;
  rep->app_log_buf[0] = '\0';
  rep->app_log_size = 0;
  rep->app_log_fp = NULL;
  rep->log_level = log_level;
  /* temporary fields for each handlers */
  memset (&rep->log_key, 0, sizeof (pthread_key_t));
  /* background log flush thread */
  rep->bio_interrupted = 0;
  rep->bio_thr = NULL;
  /* slow log */
  for (i = 0; i < DEF_SLOWLOG_NUM; i++)
    {
      rep->slow[i] = NULL;
    }
  /* log unmapping */
  pthread_mutex_init (&rep->unmaph_mutex, NULL);
  dlisth_init (&rep->unmaph);
  /* migration thread */
  rep->mig_interrupted = 0;
  rep->mig_thr = NULL;
  rep->mig = NULL;
  rep->mig_id = 0LL;
  rep->mig_ret = 0;
  rep->mig_ebuf[0] = '\0';
}

static void
term_replicator (smrReplicator * rep)
{
  int i;
  assert (rep != NULL);

  if (rep->log_dir != NULL)
    {
      free (rep->log_dir);
    }

  if (rep->master_host != NULL)
    {
      free (rep->master_host);
    }

  if (rep->local_lfd > 0)
    {
      close (rep->local_lfd);
    }

  if (rep->local_client != NULL)
    {
      free_local_conn_with_role_change (rep->local_client, 0);
    }

  if (rep->master != NULL)
    {
      free_master_conn (rep->master);
    }

  if (rep->client_lfd > 0)
    {
      close (rep->client_lfd);
    }

  while (!dlisth_is_empty (&rep->clients))
    {
      clientConn *cc = (clientConn *) rep->clients.next;
      free_client_conn (cc);
    }

  if (rep->slave_lfd > 0)
    {
      close (rep->slave_lfd);
    }

  while (!dlisth_is_empty (&rep->slaves))
    {
      slaveConn *sc = (slaveConn *) rep->slaves.next;
      free_slave_conn (sc);
    }

  if (rep->mgmt_lfd > 0)
    {
      close (rep->mgmt_lfd);
    }

  while (!dlisth_is_empty (&rep->managers))
    {
      mgmtConn *mgmc = (mgmtConn *) rep->managers.next;
      free_mgmt_conn (mgmc);
    }

  if (rep->teid != -1 && rep->el != NULL)
    {
      aeDeleteTimeEvent (rep->el, rep->teid);
    }

  if (rep->el != NULL)
    {
      aeDeleteEventLoop (rep->el);
    }

  if (rep->curr_log != NULL)
    {
      logmmap_entry_release (rep, rep->curr_log);
      rep->curr_log = NULL;
    }

  pthread_mutex_destroy (&rep->unmaph_mutex);
  while (!dlisth_is_empty (&rep->unmaph))
    {
      dlisth *h = rep->unmaph.next;
      logMmapEntry *entry;
      dlisth_delete (h);
      entry = (logMmapEntry *) h;
      logmmap_entry_release (rep, entry);
    }

  if (rep->smrlog != NULL)
    {
      smrlog_destroy (rep->smrlog);
    }

  if (rep->app_log_prefix_size > 0 && rep->app_log_fp != NULL)
    {
      fclose (rep->app_log_fp);
      rep->app_log_fp = NULL;
    }

  for (i = 0; i < DEF_SLOWLOG_NUM; i++)
    {
      if (rep->slow[i] != NULL)
	{
	  delete_slowlog (rep->slow[i]);
	}
    }

  applog_term (rep);
}

static int
check_client_nid (smrReplicator * rep, short nid)
{
  dlisth *h;

  assert (rep != NULL);

  if (nid < 0)
    {
      return -1;
    }

  for (h = rep->clients.next; h != &rep->clients; h = h->next)
    {
      clientConn *cc = (clientConn *) h;
      if (cc->nid == nid)
	{
	  return -1;
	}
    }
  return 0;
}

static int
check_slave_nid (smrReplicator * rep, short nid, int *duplicated)
{
  dlisth *h;

  assert (rep != NULL);

  if (nid < 0)
    {
      return -1;
    }
  else if (nid == rep->nid)
    {
      LOG (LG_ERROR, "slave nid is equal to mine:%d", nid);
      return -1;
    }

  for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
    {
      slaveConn *sc = (slaveConn *) h;
      if (sc->nid == nid)
	{
	  *duplicated = 1;
	  return -1;
	}
    }
  return 0;
}

static int
prepare_slaves_for_write (smrReplicator * rep)
{
  dlisth *h;
  int np = 0;

  for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
    {
      slaveConn *sc = (slaveConn *) h;
      if (slave_prepare_write (sc) < 0)
	{
	  return -1;
	}
      np++;
    }

  LOG (LG_DEBUG, "%d slaves prepared for write", np);
  return 0;
}

static int
seq_compar (const void *v1, const void *v2)
{
  long long ll1 = *(long long *) v1;
  long long ll2 = *(long long *) v2;

  return (ll1 < ll2) ? -1 : ((ll1 == ll2) ? 0 : 1);
}

static int
check_publish_seq (smrReplicator * rep, long long rcvd_seq,
		   int *ok_commit, long long *commit_seq)
{
  long long seq_ary[SMR_MAX_SLAVES];
  int member = 0;		// composed of master and slaves
  long long cand_commit;
  dlisth *h;

  *ok_commit = 0;

  if (rep->role != MASTER)
    {
      LOG (LG_ERROR,
	   "publishing commit sequence initiated but my role is not master");
      return -1;
    }

  if (rcvd_seq <= rep->commit_seq)
    {
      return 0;
    }

  /* gather logged sequence number */
  seq_ary[member++] = aseq_get (&rep->log_seq);
  for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
    {
      slaveConn *sc = (slaveConn *) h;
      if (sc->is_quorum_member)
	{
	  seq_ary[member++] = sc->slave_log_seq;
	}
    }

  /* member - 1 means current quorum */
  if (member - 1 < rep->commit_quorum)
    {
      /* need more member */
      return 0;
    }

  /* select commit sequence number */
  qsort (seq_ary, member, sizeof (long long), seq_compar);
  /* 
   * explain by example. 
   * member = 4, quorum =  1, seq_ary = [100, 200, 300, 400]
   * we must peek 300 for commit seq
   */
  cand_commit = seq_ary[member - 1 - rep->commit_quorum];

  if (cand_commit <= rep->commit_seq)
    {
      return 0;
    }
  *ok_commit = 1;
  *commit_seq = cand_commit;

  return 0;
}

/*
 *
 * This function has two role (master only)
 *
 * 1) publish commit messgae to local log
 * 2) send SEQ_SAFE message to local connection
 *
 */
static int
publish_commit_or_safe_seq (smrReplicator * rep, long long rcvd_seq)
{
  long long commit_seq = 0LL;
  int ok_commit = 0;

  if (check_publish_seq (rep, rcvd_seq, &ok_commit, &commit_seq) < 0)
    {
      return -1;
    }

  if (ok_commit)
    {
      char cmd[SMR_OP_SEQ_COMMITTED_SZ];
      long long net_seq;
      char *bp;
      int net_magic;
      unsigned short cksum, net_cksum;
      long long saved_last_commit_msg_end;

      /* 
       * Check for needless commit and skip log write.
       *
       * [CASE 1] consecutive commit message is normal case
       *      a     b
       * ----------------C(a)
       * if new commit number 'b' is published, it must be committed
       *
       *  a   b   c  d   e   f   g
       *  ---------------C(a)C(b)C(c)------------> sequence
       *  if new commit number is 'f', we must commit this number
       *
       *
       * [CASE 2] needless case 
       *            a   b   c   d   e   f
       * ---------------C(a)C(b)C(c)C(d)C(e)--->
       * C(c)C(d)C(e) is needless
       *
       * (below is real example) (logical dump of smr log file)
       * ......
       * 4227831890          commit delta:30 cseq:4227831875
       * 4227831905          commit delta:30 cseq:4227831890
       * 4227831920          commit delta:30 cseq:4227831905
       * 4227831935          commit delta:30 cseq:4227831920
       * 4227831950          commit delta:30 cseq:4227831935
       * .....
       */

      /* condition 1: consecutive commit message */
      if (rep->last_commit_msg_end == aseq_get (&rep->log_seq))
	{
	  /* condition 2: current commit sequence points inside of the trailing commit messages */
	  if (rep->commit_seq >=
	      (rep->last_commit_msg_end -
	       (SMR_OP_SEQ_COMMITTED_SZ * rep->num_trailing_commit_msg)))
	    {
	      return 0;
	    }
	}

      /* update commit seq */
      rep->commit_seq = commit_seq;

      /* prepare command */
      bp = cmd;
      *bp++ = SMR_OP_SEQ_COMMITTED;
      net_magic = htonl (SEQ_COMMIT_MAGIC);
      memcpy (bp, &net_magic, sizeof (int));
      bp += sizeof (int);
      net_seq = htonll (commit_seq);
      memcpy (bp, &net_seq, sizeof (long long));
      bp += sizeof (long long);
      cksum = crc16 ((char *) &net_seq, sizeof (long long), 0);
      net_cksum = htons (cksum);
      memcpy (bp, &net_cksum, sizeof (short));

      /* log seq committed cmd */
      saved_last_commit_msg_end = rep->last_commit_msg_end;

      if (log_write (rep, cmd, sizeof (cmd)) < 0)
	{
	  return -1;
	}
      else
	{
	  LOG (LG_DEBUG, "log write(%d): %s commit seq:%lld",
	       sizeof (cmd), get_command_string (cmd[0]), commit_seq);
	}
      rep->last_commit_msg_end = aseq_get (&rep->log_seq);

      /* get the number of trailing consecutive commit messages */
      if (saved_last_commit_msg_end + SMR_OP_SEQ_COMMITTED_SZ ==
	  rep->last_commit_msg_end)
	{
	  rep->num_trailing_commit_msg++;
	}
      else
	{
	  rep->num_trailing_commit_msg = 1;
	}

      /* let this message notified to slaves */
      if (prepare_slaves_for_write (rep) < 0)
	{
	  return -1;
	}

      if (rep->local_client != NULL)
	{
	  localConn *lc = rep->local_client;
	  if (local_publish_safe_seq (lc, commit_seq) < 0)
	    {
	      return -1;
	    }
	}
    }
  return 0;
}

static int
log_rewind_to_cseq (smrReplicator * rep, long long seq)
{
  int ret;
  ret = smrlog_purge_after (rep->smrlog, seq);
  if (ret < 0)
    {
      LOG (LG_ERROR, "Failed to purge log files after %lld",
	   seq_round_down (seq + SMR_LOG_FILE_DATA_SIZE));
      return -1;
    }
  aseq_set (&rep->sync_seq, seq);
  aseq_set (&rep->log_seq, seq);
  aseq_set (&rep->cron_est_min_seq, seq_round_down (seq));
  return 0;
}

static int
log_write_wait_room (smrReplicator * rep)
{
  int n_sleep_wait = 0;
  int backoff = 1;
  /* 
   * wait for at least MAX_LOG_WRITE_ONCE room is available without 
   * violating logging constraint 
   */
  while ((aseq_get (&rep->log_seq) - aseq_get (&rep->sync_seq)) >=
	 (2 * SMR_LOG_FILE_DATA_SIZE - MAX_LOG_WRITE_ONCE))
    {
      usleep (1000 * backoff);
      n_sleep_wait++;
      if (n_sleep_wait == 30)
	{
	  LOG (LG_ERROR,
	       "too many sleep wait for log room (n_sleep_wait = 30)");
	  return -1;
	}

      backoff = backoff << 1;
      if (backoff == 2)
	{
	  LOG (LG_WARN, "wait for log room (first time)");
	}
      else if (backoff > 1000)
	{
	  backoff = 1000;
	}

      if (backoff == 1000)
	{
	  LOG (LG_WARN, "wait for log room (backoff = 1000)");
	}
    }
  return 0;
}

static int
log_write (smrReplicator * rep, char *buf, int sz)
{
  int ret;

  while (sz > 0)
    {
      int avail;
      int nw;

      ret = log_write_wait_room (rep);
      if (ret < 0)
	{
	  return ret;
	}

      /* write at most MAX_LOG_WRITE_ONCE */
      avail = smrlog_get_remain (rep->smrlog, rep->curr_log->addr);
      assert (avail > 0);

      nw = sz > avail ? avail : sz;
      nw = nw > MAX_LOG_WRITE_ONCE ? MAX_LOG_WRITE_ONCE : nw;
      if (smrlog_append (rep->smrlog, rep->curr_log->addr, buf, nw) != nw)
	{
	  smr_panic ("Log append error", __FILE__, __LINE__);
	}
      SFI_PROBE1 ("log", nw);
      sz -= nw;
      buf += nw;

      /* update some stat */
      rep->curr_log->log_bytes += nw;

      /* advance sooner for the writer peek available log memory area (zero copy) */
      if (avail - nw == 0)
	{
	  logmmap_entry_release (rep, rep->curr_log);
	  rep->curr_log =
	    logmmap_entry_get (rep, aseq_get (&rep->log_seq) + nw,
			       GET_CREATE, 1);
	  if (rep->curr_log == NULL)
	    {
	      LOG (LG_ERROR, "failed to get log file for seq %lld",
		   aseq_get (&rep->log_seq) + nw);
	      return -1;
	    }
	}

      /* (check proper comment) this part should be after early log making */
      aseq_add (&rep->log_seq, nw);
    }

  return 0;
}

static int
log_write_io_stream (smrReplicator * rep, ioStream * ios, int n_safe)
{
  int remain;

  remain = n_safe;
  while (remain > 0)
    {
      char *buf;
      int avail = 0;
      int nw;

      if (io_stream_peek_read_buffer (ios, &buf, &avail) < 0)
	{
	  LOG (LG_ERROR,
	       "Failed io_stream_peek_read_buffer %s:%d", __FILE__, __LINE__);
	  return -1;
	}

      nw = avail > remain ? remain : avail;
      if (log_write (rep, buf, nw) < 0)
	{
	  return -1;
	}

      if (io_stream_purge (ios, nw) < 0)
	{
	  LOG (LG_ERROR, "Failed io_stream_purge %s:%d", __FILE__, __LINE__);
	  return -1;
	}

      if ((avail - nw) > 0 && io_stream_rewind_readpos (ios, avail - nw) < 0)
	{
	  LOG (LG_ERROR, "Failed io_stream_rewind_readpos %s:%d", __FILE__,
	       __LINE__);
	  return -1;
	}

      remain = remain - nw;
    }

  if (io_stream_reset_read (ios) < 0)
    {
      LOG (LG_ERROR, "Failed io_stream_read_reset  %s:%d",
	   __FILE__, __LINE__);
      return -1;
    }
  return 0;
}

static int
log_write_client_conn (clientConn * cc, int n_safe)
{
  char cmd;
  short net_nid;
  int ret;

  cmd = SMR_OP_NODE_CHANGE;
  net_nid = htons (cc->nid);

  if (log_write (cc->rep, &cmd, 1) < 0
      || log_write (cc->rep, (char *) &net_nid, sizeof (short)) < 0)
    {
      return -1;
    }
  LOG (LG_DEBUG, "log write(%d): %s %d",
       1 + sizeof (short), get_command_string (cmd), cc->nid);

  ret = log_write_io_stream (cc->rep, cc->ins, n_safe);
  if (ret != -1)
    {
      LOG (LG_DEBUG, "log write(%d): nid:%d log seq:%lld",
	   n_safe, cc->nid, aseq_get (&cc->rep->log_seq));
    }
  return ret;
}

static int
adjust_log_before_lconn (smrReplicator * rep, long long be_seq)
{
  int log_purged = 0;

  /* 
   * case 1) BE_SEQ < LOG_MIN 
   * --> Remote checkpoint will be triggered.
   *
   * case 2) MIN_LOG <= BE_SEQ <= MAX_LOG
   * --> Catch-up or remote checkpoint is possible.
   *
   * case 3) MAX_LOG < BE_SEQ  
   * --> Purge log and make local log as a special one of case 2.
   */
  if (be_seq < aseq_get (&rep->min_seq))
    {
      rep->need_rckpt = 1;
    }
  else if (be_seq > aseq_get (&rep->log_seq))
    {
      if (smrlog_purge_all (rep->smrlog) != 0)
	{
	  LOG (LG_ERROR, "Failed to purge log");
	  return -1;
	}
      log_purged = 1;

      /* set other fields */
      rep->commit_seq = be_seq;
      rep->be_ckpt_seq = be_seq;
      aseq_set (&rep->min_seq, be_seq);
      aseq_set (&rep->log_seq, be_seq);
      aseq_set (&rep->sync_seq, be_seq);
      aseq_set (&rep->cron_est_min_seq, seq_round_down (be_seq));
    }

  if (log_purged && be_seq > 0)
    {
      logMmapEntry *log = logmmap_entry_get (rep, be_seq, GET_CREATE, 1);
      int ckpt_offset;
      int log_offset;

      if (log == NULL)
	{
	  LOG (LG_ERROR, "failed to get log file for seq %lld", be_seq);
	  return -1;
	}

      ckpt_offset = (int) (be_seq - log->start_seq);
      log_offset = smrlog_get_offset (rep->smrlog, log->addr);
      if (ckpt_offset > log_offset)
	{
	  char *buf = NULL;
	  int avail;
	  int nw = ckpt_offset - log_offset;

	  if (smrlog_get_buf (rep->smrlog, log->addr, &buf, &avail) != 0)
	    {
	      smr_panic ("Log mapping failure", __FILE__, __LINE__);
	    }
	  else
	    {
	      assert (avail >= nw);
	      memset (buf, 0, nw);
	    }

	  if (smrlog_append (rep->smrlog, log->addr, buf, nw) != nw)
	    {
	      smr_panic ("Log append error", __FILE__, __LINE__);
	    }

	  if (smrlog_sync (rep->smrlog, log->addr) != 0)
	    {
	      smr_panic ("Log sync error", __FILE__, __LINE__);
	    }
	}
      logmmap_entry_release (rep, log);
    }

  return 0;
}

static int
stream_read (ioStream * ios, void *buf_, int size, int purge,
	     struct segPeek *sp)
{
  int remain = size;
  char *bp = buf_;
  int sp_idx = 0;

  while (remain > 0)
    {
      char *buf;
      int avail = 0;
      int nw;

      if (io_stream_peek_read_buffer (ios, &buf, &avail) < 0)
	{
	  return -1;
	}
      else if (avail == 0)
	{
	  break;
	}

      nw = avail > remain ? remain : avail;
      if (bp != NULL)
	{
	  memcpy (bp, buf, nw);
	  bp += nw;
	}
      remain -= nw;

      if (sp != NULL)
	{
	  if (sp_idx >= NUM_SEG_PEEK)
	    {
	      return -1;
	    }
	  sp->seg[sp_idx].buf = buf;
	  sp->seg[sp_idx].sz = nw;
	  sp_idx++;
	}

      if (purge && io_stream_purge (ios, nw) < 0)
	{
	  return -1;
	}

      if ((avail - nw) > 0 && io_stream_rewind_readpos (ios, avail - nw) < 0)
	{
	  return -1;
	}
    }

  assert (remain == 0);
  return size - remain;
}

static int
stream_write (ioStream * ios, void *buf_, int size)
{
  int remain = size;
  char *bp = buf_;

  while (remain > 0)
    {
      char *buf = NULL;
      int avail = 0;
      int nw;

      if (io_stream_peek_write_buffer (ios, &buf, &avail) < 0)
	{
	  return -1;
	}

      nw = avail > remain ? remain : avail;
      memcpy (buf, bp, nw);
      remain -= nw;
      bp += nw;

      if (io_stream_commit_write (ios, nw) < 0)
	{
	  return -1;
	}
    }
  return 0;
}

static int
stream_write_fd (connType conn_type, ioStream * ios, int fd)
{
  int tr = 0;

  while (tr < MAX_CLI_RECV_PER_EVENT)
    {
      char *buf = NULL;
      int avail = 0;
      int nr;

      if (io_stream_peek_write_buffer (ios, &buf, &avail) < 0)
	{
	  return -1;
	}

      nr = read (fd, buf, avail);
      SFI_PROBE2 ("read", conn_type, (long) &nr);
      if (nr < 0)
	{
	  if (nr == -1)
	    {
	      if (errno == EAGAIN)
		{
		  if (io_stream_commit_write (ios, 0) < 0)
		    {
		      return -1;
		    }
		  break;
		}
	      return -1;
	    }
	}
      else if (nr == 0)
	{
	  return -1;
	}

      if (io_stream_commit_write (ios, nr) < 0)
	{
	  return -1;
	}
      tr += nr;
    }

  return tr;
}

static int
fd_write_stream (connType conn_type, int fd, ioStream * ios, int *has_more,
		 int *n_sent)
{
  int tot_sent = 0;

  assert (fd > 0);
  assert (ios != NULL);
  assert (has_more != NULL);
  assert (n_sent != NULL);

  *has_more = 1;
  while (1)
    {
      int ret;
      char *buf = NULL;
      int bufsz = 0;
      int nw;
      int used;

      ret = io_stream_peek_read_buffer (ios, &buf, &bufsz);
      if (ret < 0)
	{
	  return -1;
	}
      else if (bufsz == 0)
	{
	  break;
	}

      nw = write (fd, buf, bufsz);
      SFI_PROBE2 ("write", conn_type, (long) &nw);
      used = nw < 0 ? 0 : nw;
      if ((bufsz - used) > 0
	  && io_stream_rewind_readpos (ios, bufsz - used) < 0)
	{
	  return -1;
	}
      tot_sent += used;

      if (nw == -1)
	{
	  if (errno == EAGAIN)
	    {
	      *n_sent = tot_sent;
	      return 0;
	    }
	  return -1;
	}
      else if (nw == 0)
	{
	  return -1;
	}
      else
	{
	  if (io_stream_purge (ios, nw) < 0)
	    {
	      return -1;
	    }
	  if (nw < bufsz)
	    {
	      *n_sent = tot_sent;
	      return 0;
	    }
	}
    }
  *has_more = 0;
  *n_sent = tot_sent;
  return 0;
}

// \r\n(2), '\0' (1: for safety)
#define BE_CONF_MARGIN 3
static int
be_configure_msg (char *buf, int bufsz, char *fmt, ...)
{
  char *bp;
  int net_len;
  int ret;
  va_list ap;

  bp = buf;
  *bp++ = SMR_OP_CONFIGURE;
  bp += sizeof (int);

  va_start (ap, fmt);
  ret = vsnprintf (bp, bufsz - (1 + sizeof (int) + BE_CONF_MARGIN), fmt, ap);
  va_end (ap);
  if (ret >= bufsz - (1 + sizeof (int) + BE_CONF_MARGIN) || ret < 0)
    {
      return -1;
    }
  bp = bp + ret;
  *bp++ = '\r';
  *bp++ = '\n';
  *bp = '\0';
  LOG (LG_DEBUG, "be_configure_msg:%s", buf + 1 + sizeof (int));

  /* set size field */
  bp = buf + 1;
  net_len = htonl (ret + 2);
  memcpy (bp, &net_len, sizeof (int));

  return 1 + sizeof (int) + ret + 2;
}

static int
local_publish_safe_seq (localConn * lc, long long safe_seq)
{
  smrReplicator *rep;
  long long net_seq;
  char cmd[1 + sizeof (long long)];

  rep = lc->rep;
  assert (lc == rep->local_client);

  LOG (LG_DEBUG, "local_publish_safe_seq: safe_seq:%lld", safe_seq);
  if (lc->sent_seq >= safe_seq)
    {
      // it is called many times. is it normal? (yes)
      return 0;
    }

  cmd[0] = SMR_OP_SEQ_SAFE;
  net_seq = htonll (safe_seq);
  memcpy (&cmd[1], &net_seq, sizeof (long long));

  if (local_prepare_write (lc) < 0
      || stream_write (lc->ous, cmd, sizeof (cmd)) < 0)
    {
      return -1;
    }
  LOG (LG_DEBUG, "%s sent to local: %lld",
       get_command_string (SMR_OP_SEQ_SAFE), safe_seq);
  lc->sent_seq = safe_seq;

  return 0;
}

static void
free_local_conn_with_role_change (localConn * lc, int change)
{
  smrReplicator *rep = lc->rep;

  LOG (LG_WARN, "close local connection");

  rep->local_client = NULL;
  if (lc->fd > 0)
    {
      aeDeleteFileEvent (rep->el, lc->fd, AE_READABLE);
      aeDeleteFileEvent (rep->el, lc->fd, AE_WRITABLE);
      close (lc->fd);
      LOG (LG_INFO, "connection closed %d", lc->fd);
    }

  if (lc->ins != NULL)
    {
      io_stream_close (lc->ins);
    }

  if (lc->ous != NULL)
    {
      io_stream_close (lc->ous);
    }

  free (lc);

  LOG (LG_ERROR,
       "connection to backend closed. change state to NONE:: cseq:%lld lseq:%lld",
       rep->commit_seq, aseq_get (&rep->log_seq));

  if (change && do_role_none (rep) < 0)
    {
      LOG (LG_ERROR, "do_role_none failed. exiting..");
      exit (1);
    }
}

static void
free_local_conn (localConn * lc)
{
  free_local_conn_with_role_change (lc, 1);
}

static int
local_prepare_write (localConn * lc)
{
  assert (lc != NULL);
  assert (lc->fd > 0);

  if (lc->w_prepared == 0)
    {
      if (aeCreateFileEvent
	  (lc->rep->el, lc->fd, AE_WRITABLE, local_write_handler,
	   lc) == AE_OK)
	{
	  lc->w_prepared = 1;
	  return 0;
	}
      else
	{
	  LOG (LG_ERROR, "create file event (local_write_handler) failed");
	  return -1;
	}
    }
  return 0;
}

static void
local_accept (smrReplicator * rep, aeEventLoop * el, int fd)
{
  localConn *lc;
  int cfd = 0;
  char cip[64];
  int cport;
  long long net_seq;
  int ret;
  long long deadline;

  assert (fd == rep->local_lfd);

  cfd = tcp_accept (fd, cip, &cport, rep->ebuf, EBUFSZ);
  if (cfd < 0)
    {
      LOG (LG_WARN, "accept failed: %s", rep->ebuf);
      return;
    }
  LOG (LG_INFO, "connection from %s:%d fd:%d", cip, cport, cfd);

  if (rep->role != NONE)
    {
      LOG (LG_WARN, "accept request during non NONE state:%d", rep->role);
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }
  else if (rep->local_client != NULL)
    {
      LOG (LG_WARN, "no more local replicator");
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }

  lc = malloc (sizeof (localConn));
  if (lc == NULL)
    {
      LOG (LG_ERROR, "Memory allocation failed");
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }

  // after this point goto error
  init_local_conn (lc);
  lc->rep = rep;
  lc->fd = cfd;
  lc->ins = io_stream_create (1024);
  if (lc->ins == NULL)
    {
      LOG (LG_ERROR, "failed to create in stream");
      goto error;
    }
  lc->ous = io_stream_create (1024);
  if (lc->ous == NULL)
    {
      LOG (LG_ERROR, "failed to create out stream");
      goto error;
    }

  ret = tcp_set_option (cfd, TCP_OPT_NONBLOCK | TCP_OPT_NODELAY);
  SFI_PROBE2 ("accept", LOCAL_CONN_IN, (long) &ret);
  if (ret < 0)
    {
      goto error;
    }

  /* HANDSHAKE */
  deadline = currtime_ms () + DEFAULT_HANDSHAKE_TIMEOUT_MSEC;
  if (tcp_read_fully_deadline (cfd, &net_seq, sizeof (net_seq), deadline) < 0)
    {
      LOG (LG_ERROR, "failed to read sequence number");
      goto error;
    }

  lc->sent_seq = lc->consumed_seq = ntohll (net_seq);
  if (adjust_log_before_lconn (rep, lc->sent_seq) < 0)
    {
      goto error;
    }

  if (aeCreateFileEvent
      (rep->el, cfd, AE_READABLE, local_read_handler, lc) == AE_ERR)
    {
      goto error;
    }

  if (aeCreateFileEvent
      (rep->el, cfd, AE_WRITABLE, local_write_handler, lc) == AE_ERR)
    {
      goto error;
    }

  rep->local_client = lc;
  rep->role = LCONN;
  rep->role_id++;
  return;

error:
  if (lc != NULL)
    {
      free_local_conn (lc);
    }
}

/*
 * message from local backend --> local replicator
 * - <consumed seq>
 */
static void
local_read (localConn * lc, aeEventLoop * el, int fd)
{
  int nr;
  int bsr_avail = 0;
  int bsr_used = 0;
  smrReplicator *rep;

  assert (lc != NULL);
  assert (lc->fd == fd);
  rep = lc->rep;

  if ((nr = stream_write_fd (LOCAL_CONN_IN, lc->ins, fd)) < 0)
    {
      LOG (LG_ERROR, "failed to read from local connection");
      free_local_conn (lc);
      return;
    }
  else if (nr == 0)
    {
      LOG (LG_WARN, "called with zero readable");
      return;
    }

  bsr_avail = io_stream_avail_for_read (lc->ins);
  assert (bsr_avail >= 0);

  while (1)
    {
      char cmd;

      BSR_READ (rep, lc->ins, &cmd, 1, NULL);
      switch (cmd)
	{
	  long long net_seq;
	  long long be_ckpt_seq;

	case SMR_OP_SEQ_CONSUMED:
	  BSR_READ (rep, lc->ins, &net_seq, sizeof (net_seq), NULL);
	  lc->consumed_seq = ntohll (net_seq);
	  BSR_COMMIT (lc->ins);
	  break;

	case SMR_OP_SEQ_CKPTED:
	  BSR_READ (rep, lc->ins, &net_seq, sizeof (net_seq), NULL);
	  be_ckpt_seq = ntohll (net_seq);
	  if (be_ckpt_seq < rep->be_ckpt_seq)
	    {
	      LOG (LG_ERROR,
		   "received SMR_OP_SEQ_CKPTED:%lld that is less than current one:%lld",
		   be_ckpt_seq, rep->be_ckpt_seq);
	    }
	  else
	    {
	      LOG (LG_INFO, "received SMR_OP_SEQ_CKPTED:%lld", be_ckpt_seq);
	      rep->be_ckpt_seq = be_ckpt_seq;
	    }
	  BSR_COMMIT (lc->ins);
	  break;

	default:
	  LOG (LG_ERROR, "invalid protocol command %c", cmd);
	  goto bsr_error;
	}
    }

bsr_done:
  BSR_ROLLBACK (lc->ins);
  return;

bsr_error:
  free_local_conn (lc);
  return;
}

static void
local_write (localConn * lc, aeEventLoop * el, int fd)
{
  int ret;
  int has_more;
  int n_sent = 0;

  has_more = 1;
  ret = fd_write_stream (LOCAL_CONN_OUT, fd, lc->ous, &has_more, &n_sent);
  if (ret < 0)
    {
      LOG (LG_ERROR, "can't write");
      free_local_conn (lc);
      return;
    }

  if (has_more == 0)
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      lc->w_prepared = 0;
    }
}

static void
local_accept_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  SLOWLOG_DECLARE ('l', 'a');

  applog_enter_session (rep, LOCAL_CONN_IN, fd);
  SLOWLOG_ENTER ();
  local_accept (rep, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
local_read_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  localConn *lc = (localConn *) data;
  smrReplicator *rep = lc->rep;
  SLOWLOG_DECLARE ('l', 'r');

  rep->stat.local_nre++;
  applog_enter_session (rep, LOCAL_CONN_IN, fd);
  SLOWLOG_ENTER ();
  local_read (lc, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
local_write_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  localConn *lc = (localConn *) data;
  smrReplicator *rep = lc->rep;
  SLOWLOG_DECLARE ('l', 'w');

  rep->stat.local_nwe++;
  applog_enter_session (rep, LOCAL_CONN_OUT, fd);
  SLOWLOG_ENTER ();
  local_write (lc, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
free_slave_conn_by_nid (smrReplicator * rep, int nid)
{
  dlisth *h;
  slaveConn *sc = NULL;

  assert (rep != NULL);
  assert (nid >= 0);

  for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
    {
      sc = (slaveConn *) h;
      if (sc->nid == nid)
	{
	  break;
	}
      sc = NULL;
    }

  if (sc != NULL)
    {
      free_slave_conn (sc);
    }
}

static void
free_slave_conn (slaveConn * sc)
{
  smrReplicator *rep;
  assert (sc != NULL);

  rep = sc->rep;
  if (sc->head.next != &sc->head)
    {
      dlisth_delete (&sc->head);
      sc->rep->num_slaves--;
    }

  if (sc->fd > 0)
    {
      aeDeleteFileEvent (sc->rep->el, sc->fd, AE_READABLE);
      aeDeleteFileEvent (sc->rep->el, sc->fd, AE_WRITABLE);
      close (sc->fd);
      LOG (LG_INFO, "connection closed %d", sc->fd);
      sc->fd = -1;
    }

  if (sc->ins != NULL)
    {
      io_stream_close (sc->ins);
      sc->ins = NULL;
    }

  if (sc->log_mmap != NULL)
    {
      logmmap_entry_release (sc->rep, sc->log_mmap);
      sc->log_mmap = NULL;
    }
  free (sc);

  rep->cron_need_publish = 1;
}

static int
slave_prepare_write (slaveConn * sc)
{
  assert (sc != NULL);
  assert (sc->fd > 0);

  if (sc->w_prepared == 0)
    {
      if (aeCreateFileEvent
	  (sc->rep->el, sc->fd, AE_WRITABLE, slave_write_handler,
	   sc) == AE_OK)
	{
	  sc->w_prepared = 1;
	  return 0;
	}
      else
	{
	  LOG (LG_ERROR, "create file event (slave_prepare_write) failed");
	  return -1;
	}
    }
  return 0;
}

static void
slave_accept (smrReplicator * rep, aeEventLoop * el, int fd)
{
  slaveConn *sc;
  int cfd = 0;
  char cip[64];
  int cport;
  int duplicated = 0;
  long long net_seq;
  long long min_seq = 0LL, commit_seq = 0LL, max_seq = 0LL, log_seq = 0LL;
  int ret;
  short net_nid, nid;
  char buf[3 * sizeof (long long)];
  char *bp;
  long long deadline;

  assert (fd == rep->slave_lfd);

  cfd = tcp_accept (fd, cip, &cport, rep->ebuf, EBUFSZ);
  if (cfd < 0)
    {
      LOG (LG_WARN, "accept failed: %s", rep->ebuf);
      return;
    }
  LOG (LG_INFO, "connection from %s:%d fd:%d", cip, cport, cfd);

  if (rep->role != MASTER)
    {
      LOG (LG_WARN, "accept request during bad state:%d", rep->role);
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }
  if (rep->num_slaves >= SMR_MAX_SLAVES)
    {
      LOG (LG_WARN, "No more slave is accepted");
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }

  sc = malloc (sizeof (slaveConn));
  if (sc == NULL)
    {
      LOG (LG_ERROR, "memory allocation failed");
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }
  init_slave_conn (sc);
  sc->rep = rep;
  sc->fd = cfd;
  sc->read_cron_count = rep->cron_count;

  sc->ins = io_stream_create (1024);
  if (sc->ins == NULL)
    {
      LOG (LG_ERROR, "failed to create in stream");
      goto error;
    }

  ret = tcp_set_option (cfd, TCP_OPT_NONBLOCK | TCP_OPT_NODELAY);
  SFI_PROBE2 ("accept", SLAVE_CONN_IN, (long) &ret);
  if (ret < 0)
    {
      goto error;
    }

  /* HANDSHAKE */
  // send my min, commit, max sequences
  // receive nid, slave seq request
  min_seq = aseq_get (&rep->min_seq);
  commit_seq = rep->commit_seq;
  log_seq = aseq_get (&rep->log_seq);
  max_seq = log_seq;

  bp = buf;
  net_seq = htonll (min_seq);
  memcpy (bp, &net_seq, sizeof (long long));
  bp += sizeof (long long);
  net_seq = htonll (commit_seq);
  memcpy (bp, &net_seq, sizeof (long long));
  bp += sizeof (long long);
  net_seq = htonll (max_seq);
  memcpy (bp, &net_seq, sizeof (long long));
  bp += sizeof (long long);

  deadline = currtime_ms () + DEFAULT_HANDSHAKE_TIMEOUT_MSEC;
  if (tcp_write_fully_deadline (cfd, buf, sizeof (buf), deadline) < 0)
    {
      LOG (LG_ERROR, "failed to send seqence number and nid");
      goto error;
    }

  if (tcp_read_fully_deadline (cfd, &net_nid, sizeof (net_nid), deadline) < 0)
    {
      LOG (LG_ERROR, "failed to read node id");
      goto error;
    }
  nid = ntohs (net_nid);
  if (check_slave_nid (rep, nid, &duplicated) < 0)
    {
      if (duplicated)
	{
	  LOG (LG_WARN, "duplicated slave nid. close current slave: %d", nid);
	  free_slave_conn_by_nid (rep, nid);
	  free_client_conn_by_nid (rep, nid);
	}
      else
	{
	  LOG (LG_ERROR, "check_slave_nid failed: %d", nid);
	  goto error;
	}
    }
  sc->nid = nid;
  sc->is_quorum_member = nid_is_quorum_member (rep, nid);

  if (tcp_read_fully_deadline (cfd, &net_seq, sizeof (net_seq), deadline) < 0)
    {
      LOG (LG_ERROR, "failed to read sequence number");
      goto error;
    }
  sc->slave_log_seq = ntohll (net_seq);
  sc->sent_log_seq = sc->slave_log_seq;

  if (sc->slave_log_seq < min_seq)
    {
      LOG (LG_ERROR,
	   "received seq %lld is less than my min_seq %lld",
	   sc->slave_log_seq, min_seq);
      goto error;
    }
  else if (sc->slave_log_seq > max_seq)
    {
      LOG (LG_ERROR,
	   "received seq %lld is greater than my max_seq %lld",
	   sc->slave_log_seq, max_seq);
      goto error;
    }
  /* 
   * Note: 
   * new log file can be created during transient state.
   * b.t.w. it seems be safe
   */
  sc->log_mmap = logmmap_entry_get (rep, sc->slave_log_seq, GET_EXIST, 0);
  if (sc->log_mmap == NULL)
    {
      LOG (LG_ERROR, "can't get log file for sequence %ldd",
	   sc->slave_log_seq);
      goto error;
    }

  if (aeCreateFileEvent (rep->el, cfd, AE_READABLE, slave_read_handler, sc)
      == AE_ERR)
    {
      goto error;
    }

  if (aeCreateFileEvent (rep->el, cfd, AE_WRITABLE, slave_write_handler, sc)
      == AE_ERR)
    {
      goto error;
    }

  rep->cron_need_progress = 1;
  rep->progress_seq = aseq_get (&rep->log_seq);

  dlisth_insert_before (&sc->head, &rep->slaves);
  rep->num_slaves++;
  return;

error:
  if (sc != NULL)
    {
      free_slave_conn (sc);
    }
}

static void
slave_read (slaveConn * sc, aeEventLoop * el, int fd)
{
  int nr;
  int bsr_avail = 0;
  int bsr_used = 0;

  assert (sc != NULL);
  assert (sc->fd == fd);

  if ((nr = stream_write_fd (SLAVE_CONN_IN, sc->ins, fd)) < 0)
    {
      LOG (LG_ERROR, "failed to read from connection");
      free_slave_conn (sc);
      return;
    }
  else if (nr == 0)
    {
      LOG (LG_WARN, "read handler called with nothing to read");
      return;
    }

  /* prepare to use bsr_xxxx macros */
  bsr_avail = io_stream_avail_for_read (sc->ins);
  assert (bsr_avail > 0);

  while (1)
    {
      char cmd;
      /* do not commit for later processing */
      BSR_READ (sc->rep, sc->ins, &cmd, 1, NULL);
      switch (cmd)
	{
	  long long net_seq;

	case SMR_OP_SEQ_RECEIVED:
	  BSR_READ (sc->rep, sc->ins, &net_seq, sizeof (long long), NULL);
	  // prepare for network isolation
	  // slave can send a duplicated SMR_OP_SEQ_RECEIVED message for detecting closed connection
	  assert (sc->slave_log_seq <= ntohll (net_seq));
	  sc->slave_log_seq = ntohll (net_seq);
	  BSR_COMMIT (sc->ins);
	  LOG (LG_DEBUG,
	       "%s %lld current commit seq: %lld",
	       get_command_string (cmd), sc->slave_log_seq,
	       sc->rep->commit_seq);
	  break;
	default:
	  LOG (LG_ERROR, "invalid protocol command %c", cmd);
	  goto bsr_error;
	}
    }

bsr_done:
  BSR_ROLLBACK (sc->ins);

  if (publish_commit_or_safe_seq (sc->rep, sc->slave_log_seq) < 0)
    {
      goto bsr_error;
    }
  return;

bsr_error:
  LOG (LG_ERROR, "error during slave input processing");
  free_slave_conn (sc);
  return;
}

/* 
 * send log file to the slave. log file is already orgnized without mess.
 */
static void
slave_write (slaveConn * sc, aeEventLoop * el, int fd)
{
  if (slave_send_log (sc) < 0)
    {
      free_slave_conn (sc);
      return;
    }
}

static void
slave_accept_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  SLOWLOG_DECLARE ('s', 'a');

  applog_enter_session (rep, SLAVE_CONN_IN, fd);
  SLOWLOG_ENTER ();
  slave_accept (rep, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
slave_read_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  slaveConn *sc = (slaveConn *) data;
  smrReplicator *rep = sc->rep;
  SLOWLOG_DECLARE ('s', 'r');

  rep->stat.slave_nre++;
  applog_enter_session (rep, SLAVE_CONN_IN, fd);
  sc->read_cron_count = rep->cron_count;
  SLOWLOG_ENTER ();
  slave_read (sc, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
slave_write_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  slaveConn *sc = (slaveConn *) data;
  smrReplicator *rep = sc->rep;
  SLOWLOG_DECLARE ('s', 'w');

  rep->stat.slave_nwe++;
  applog_enter_session (rep, SLAVE_CONN_OUT, fd);
  SLOWLOG_ENTER ();
  slave_write (sc, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

/*
 * Get directly accessable buffer pointer and remaining log file size
 * Note: this function must not be used headlessly. Caller must know that
 * data is already in the log file
 */
static int
slave_log_get_buf (slaveConn * sc, long long from, long long to, char **buf,
		   int *avail)
{
  logMmapEntry *me;
  int off;
  assert (sc != NULL);
  assert (sc->log_mmap != NULL);
  assert (to > from);

  me = sc->log_mmap;

  /* advance log if curr log is used up */
  if (from >= me->start_seq + SMR_LOG_FILE_DATA_SIZE)
    {
      logMmapEntry *nme = logmmap_entry_get (sc->rep, from, GET_EXIST, 0);
      if (nme == NULL)
	{
	  return -1;
	}
      logmmap_entry_release (sc->rep, me);
      sc->log_mmap = me = nme;
    }

  off = (int) (from - me->start_seq);
  assert (off >= 0 && off < SMR_LOG_FILE_DATA_SIZE);
  *buf = (char *) me->addr->addr + off;

  *avail =
    (to - from) >
    (SMR_LOG_FILE_DATA_SIZE - off) ? (SMR_LOG_FILE_DATA_SIZE -
				      off) : (int) (to - from);
  return 0;
}

static int
slave_send_log (slaveConn * sc)
{
  long long from_seq;
  long long to_seq;
  int tot_write = 0;

  assert (sc != NULL);
  from_seq = sc->sent_log_seq;
  to_seq = aseq_get (&sc->rep->log_seq);

  while (from_seq < to_seq)
    {
      char *buf = NULL;
      int avail = 0;
      int nw;

      /* we know that logs up to commit_seq is there */
      if (slave_log_get_buf (sc, from_seq, to_seq, &buf, &avail) < 0)
	{
	  return -1;
	}

      if (avail > MAX_LOG_SEND_PER_EVENT - tot_write)
	{
	  avail = MAX_LOG_SEND_PER_EVENT - tot_write;
	}

      if (avail <= 0)
	{
	  break;
	}

      nw = write (sc->fd, buf, avail);
      SFI_PROBE2 ("write", SLAVE_CONN_OUT, (long) &nw);
      if (nw == -1)
	{
	  if (errno == EAGAIN)
	    {
	      break;
	    }
	  LOG (LG_ERROR, "Failed to write fd:%d errno:%d", sc->fd, errno);
	  return -1;
	}
      else if (nw == 0)
	{
	  smr_panic
	    ("event handling error (write handler called on non writable fd)",
	     __FILE__, __LINE__);
	}
      else
	{
	  from_seq += nw;
	  tot_write += nw;
	}

      if (nw < avail || tot_write >= MAX_LOG_SEND_PER_EVENT)
	{
	  break;
	}
    }

  LOG (LG_DEBUG,
       "log sent to slave fd:%d prev sent seq:%lld curr sent seq:%lld",
       sc->fd, sc->sent_log_seq, from_seq);

  sc->sent_log_seq = from_seq;
  if (from_seq >= to_seq)
    {
      aeDeleteFileEvent (sc->rep->el, sc->fd, AE_WRITABLE);
      sc->w_prepared = 0;
    }
  return 0;
}

static void
client_accept (smrReplicator * rep, aeEventLoop * el, int fd)
{
  clientConn *cc;
  int cfd = 0;
  char cip[64];
  int cport;
  short net_nid;
  long long net_seq;
  int ret;
  long long deadline;

  assert (fd == rep->client_lfd);

  cfd = tcp_accept (fd, cip, &cport, rep->ebuf, EBUFSZ);
  if (cfd < 0)
    {
      LOG (LG_WARN, "accept failed: %s", rep->ebuf);
      return;
    }
  LOG (LG_INFO, "connection from %s:%d fd:%d", cip, cport, cfd);

  if (rep->role != MASTER)
    {
      LOG (LG_WARN, "accept request during bad state role:%d", rep->role);
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }

  cc = malloc (sizeof (clientConn));
  if (cc == NULL)
    {
      LOG (LG_ERROR, "memory allocation failed");
      close (cfd);
      LOG (LG_INFO, "connection closed %d", cfd);
      return;
    }

  init_client_conn (cc);
  cc->rep = rep;
  cc->fd = cfd;
  cc->ins = io_stream_create (8192);
  if (cc->ins == NULL)
    {
      LOG (LG_ERROR, "failed to create in stream");
      goto error;
    }
  cc->ous = io_stream_create (8192);
  if (cc->ous == NULL)
    {
      LOG (LG_ERROR, "failed to create out stream");
      goto error;
    }

  ret = tcp_set_option (cfd, TCP_OPT_NONBLOCK | TCP_OPT_NODELAY);
  SFI_PROBE2 ("accept", CLIENT_CONN_IN, (long) &ret);
  if (ret < 0)
    {
      LOG (LG_ERROR, "failed to set tcp option");
      goto error;
    }

  /* HANDSHAKE */
  deadline = currtime_ms () + DEFAULT_HANDSHAKE_TIMEOUT_MSEC;
  if (tcp_read_fully_deadline (cfd, &net_nid, sizeof (short), deadline) < 0)
    {
      LOG (LG_ERROR, "failed to read nid");
      goto error;
    }

  cc->nid = ntohs (net_nid);
  if (check_client_nid (rep, cc->nid) < 0)
    {
      LOG (LG_ERROR, "bad nid from new client:%d", cc->nid);
      goto error;
    }

  net_seq = htonll (rep->commit_seq);
  if (tcp_write_fully_deadline (cfd, &net_seq, sizeof (long long), deadline) <
      0)
    {
      LOG (LG_ERROR, "write commit seqence failed: %lld", rep->commit_seq);
      goto error;
    }
  LOG (LG_INFO, "new client: nid:%d fd:%d sent commit seq:%lld", cc->nid, cfd,
       rep->commit_seq);

  if (aeCreateFileEvent (rep->el, cfd, AE_READABLE, client_read_handler, cc)
      == AE_ERR)
    {
      LOG (LG_ERROR, "failed to create read event handler");
      goto error;
    }

  if (aeCreateFileEvent (rep->el, cfd, AE_WRITABLE, client_write_handler, cc)
      == AE_ERR)
    {
      LOG (LG_ERROR, "failed to create read event handler");
      goto error;
    }

  /* link to replicator structure */
  dlisth_insert_after (&cc->head, &rep->clients);
  return;

error:
  if (cc != NULL)
    {
      free_client_conn (cc);
    }
}

static void
free_client_conn (clientConn * cc)
{
  smrReplicator *rep;

  assert (cc != NULL);
  rep = cc->rep;

  LOG (LG_WARN, "close client connection");
  dlisth_delete (&cc->head);

  if (cc->fd > 0)
    {
      aeDeleteFileEvent (rep->el, cc->fd, AE_READABLE);
      aeDeleteFileEvent (rep->el, cc->fd, AE_WRITABLE);
      close (cc->fd);
      LOG (LG_INFO, "connection closed %d", cc->fd);
    }

  if (cc->ins != NULL)
    {
      io_stream_close (cc->ins);
    }

  if (cc->ous != NULL)
    {
      io_stream_close (cc->ous);
    }

  free (cc);
}

static void
free_client_conn_by_nid (smrReplicator * rep, int nid)
{
  dlisth *h;
  clientConn *cc = NULL;

  assert (rep != NULL);
  assert (nid >= 0);

  for (h = rep->clients.next; h != &rep->clients; h = h->next)
    {
      cc = (clientConn *) h;
      if (cc->nid == nid)
	{
	  break;
	}
      cc = NULL;
    }

  if (cc != NULL)
    {
      free_client_conn (cc);
    }
}

static int
client_prepare_write (clientConn * cc)
{
  assert (cc != NULL);
  assert (cc->fd > 0);

  if (cc->w_prepared == 0)
    {
      if (aeCreateFileEvent
	  (cc->rep->el, cc->fd, AE_WRITABLE, client_write_handler,
	   cc) == AE_OK)
	{
	  cc->w_prepared = 1;
	  return 0;
	}
      else
	{
	  LOG (LG_ERROR, "create file event (client_write_handler) failed");
	  return -1;
	}
    }
  return 0;
}

static void
timestamp_peek (struct segPeek *sp)
{
  long long ts;
  long long net_ts;
  char *tsp;
  char *tp = (char *) &net_ts;

  assert (sp->seg[0].sz + sp->seg[1].sz == sizeof (long long));

  /* 
   * check if timestamp value is not zero.
   * Timestamp value zero means client (backed) send this 
   */
  memcpy (tp, sp->seg[0].buf, sp->seg[0].sz);
  if (sp->seg[1].buf != NULL)
    {
      memcpy (tp + sp->seg[0].sz, sp->seg[1].buf, sp->seg[1].sz);
    }
  if (net_ts != 0)
    {
      /* 
       * This can be happened during migration. we respect that value.
       */
      return;
    }

  ts = currtime_ms ();

  /* convert to network byte order */
  ts = htonll (ts);
  tsp = (char *) &ts;

  /* copy to peek segments */
  memcpy (sp->seg[0].buf, tsp, sp->seg[0].sz);
  tsp += sp->seg[0].sz;
  if (sp->seg[1].buf != NULL)
    {
      memcpy (sp->seg[1].buf, tsp, sp->seg[1].sz);
    }
}

/*
 * Messages from client 
 *
 *  <session open>
 *    - session ID : 4 byte
 *  <session close>
 *    - session ID : 4 byte
 *  <session data>
 *    - session ID : 4 byte
 *    - length     : 4 byte
 *    - data   
 */
static int
client_read (clientConn * cc, aeEventLoop * el, int fd, int force)
{
  int nr;
  int bsr_avail = 0;
  int bsr_used = 0;
  int n_safe;
  int length = 0;
  int net_sid;
  int loop_count;
  int org_avail;
  smrReplicator *rep;
  struct segPeek sp;

  assert (cc != NULL);
  assert (cc->rep != NULL);
  assert (cc->fd == fd);
  rep = cc->rep;

  if ((nr = stream_write_fd (CLIENT_CONN_IN, cc->ins, fd)) < 0)
    {
      LOG (LG_ERROR, "failed to read from client connection");
      free_client_conn (cc);
      return -1;
    }
  else if (nr == 0 && !force)
    {
      LOG (LG_WARN, "read handler called with nothing to read");
      return 0;
    }

  /* prepare to use bsr_xxxx macros */
  org_avail = bsr_avail = io_stream_avail_for_read (cc->ins);
  assert (bsr_avail >= 0);

  n_safe = 0;
  loop_count = 0;
  while (1)
    {
      char cmd;

      /* do not commit for later processing */
      BSR_READ (rep, cc->ins, &cmd, 1, NULL);

      switch (cmd)
	{
	case SMR_OP_EXT:
	case SMR_OP_SESSION_CLOSE:
	  net_sid = -1;
	  BSR_READ (rep, cc->ins, &net_sid, sizeof (int), NULL);
	  n_safe = bsr_used;
	  break;

	case SMR_OP_SESSION_DATA:	//sid, hash, timestamp, length, data
	  BSR_READ (rep, cc->ins, NULL, sizeof (int), NULL);
	  BSR_READ (rep, cc->ins, NULL, sizeof (int), NULL);
	  init_seg_peek (&sp);
	  BSR_READ (rep, cc->ins, NULL, sizeof (long long), &sp);
	  BSR_READ (rep, cc->ins, &length, sizeof (int), NULL);
	  length = ntohl (length);
	  BSR_READ (rep, cc->ins, NULL, length, NULL);
	  n_safe = bsr_used;
	  timestamp_peek (&sp);
	  break;
	default:
	  LOG (LG_ERROR,
	       "nr:%d, org_avail:%d, bsr_avail:%d, n_safe:%d, nid:%d bad SMR command %c",
	       nr, org_avail, bsr_avail, n_safe, cc->nid, cmd);
	  goto bsr_error;
	}
      loop_count++;
    }

bsr_done:
  BSR_ROLLBACK (cc->ins);
  assert (org_avail == bsr_avail);
  assert (n_safe <= bsr_avail);

  if (n_safe == 0)
    {
      return 0;
    }

  if (log_write_client_conn (cc, n_safe) < 0)
    {
      goto bsr_error;
    }

  /* send logged ack to the client */
  cc->num_logged += n_safe;
  do
    {
      long long net_bytes;
      char ack_cmd[1 + sizeof (long long)];

      net_bytes = htonll (cc->num_logged);

      ack_cmd[0] = SMR_OP_LOG_ACK;
      memcpy (&ack_cmd[1], &net_bytes, sizeof (long long));

      if (client_prepare_write (cc) < 0
	  || stream_write (cc->ous, ack_cmd, sizeof (ack_cmd)) < 0)
	{
	  return -1;
	}
      LOG (LG_DEBUG,
	   "client ack:%lld is buffered for client", cc->num_logged);
    }
  while (0);

  /* let the stream flows to the slaves */
  if (prepare_slaves_for_write (rep) < 0)
    {
      goto bsr_error;
    }

  if (publish_commit_or_safe_seq (rep, aseq_get (&rep->log_seq)) < 0)
    {
      goto bsr_error;
    }
  return 0;

bsr_error:
  LOG (LG_ERROR, "error during client input processing");
  free_client_conn (cc);
  return -1;
}

static void
client_write (clientConn * cc, aeEventLoop * el, int fd)
{
  int ret;
  int has_more;
  int n_sent = 0;

  has_more = 1;
  ret = fd_write_stream (CLIENT_CONN_OUT, fd, cc->ous, &has_more, &n_sent);
  if (ret < 0)
    {
      LOG (LG_ERROR, "can't write");
      free_client_conn (cc);
      return;
    }

  if (has_more == 0)
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      cc->w_prepared = 0;
    }
}

static void
client_accept_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  SLOWLOG_DECLARE ('c', 'a');

  applog_enter_session (rep, CLIENT_CONN_IN, fd);
  SLOWLOG_ENTER ();
  client_accept (rep, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
client_read_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  clientConn *cc = (clientConn *) data;
  smrReplicator *rep = cc->rep;
  SLOWLOG_DECLARE ('c', 'r');

  rep->stat.client_nre++;
  applog_enter_session (rep, CLIENT_CONN_IN, fd);
  SLOWLOG_ENTER ();
  (void) client_read (cc, el, fd, 0);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
client_write_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  clientConn *cc = (clientConn *) data;
  smrReplicator *rep = cc->rep;
  SLOWLOG_DECLARE ('c', 'w');

  rep->stat.client_nwe++;
  applog_enter_session (rep, CLIENT_CONN_IN, fd);
  SLOWLOG_ENTER ();
  client_write (cc, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
free_master_conn (masterConn * mc)
{
  smrReplicator *rep = mc->rep;
  long long rseq;
  int ret;

  rep->master = NULL;
  if (mc->fd > 0)
    {
      aeDeleteFileEvent (rep->el, mc->fd, AE_READABLE);
      aeDeleteFileEvent (rep->el, mc->fd, AE_WRITABLE);
      close (mc->fd);
      LOG (LG_INFO, "connection closed %d", mc->fd);
    }
  if (mc->ous != NULL)
    {
      io_stream_close (mc->ous);
    }

  /* save rseq */
  rseq = mc->rseq;
  free (mc);

  LOG (LG_ERROR,
       "connection to master closed. move state to LCONN:: cseq:%lld lseq:%lld",
       rep->commit_seq, aseq_get (&rep->log_seq));
  if (do_role_lconn (rep) < 0)
    {
      LOG (LG_ERROR, "do_role_lconn failed. exiting..");
      exit (1);
    }

  // purge partially written log to message boundary and adjust sync_seq
  if (rseq >= 0 && rseq < aseq_get (&rep->log_seq))
    {
      ret = log_rewind_to_cseq (rep, rseq);
      if (ret < 0)
	{
	  LOG (LG_ERROR, "Failed to trim partially written log %lld to %lld",
	       aseq_get (&rep->log_seq), rseq);
	  smr_panic ("Log purge error.", __FILE__, __LINE__);
	}
    }
}

static int
connect_to_master (smrReplicator * rep, char *host, int port,
		   long long local_max_seq, long long *master_max_msgend_seq)
{
  masterConn *mc;
  long long master_min_seq;
  long long net_seq;
  int ret;
  short net_nid;
  logMmapEntry *lme = NULL;
  long long deadline;

  if (rep->master != NULL)
    {
      LOG (LG_ERROR, "master connection exists");
      return -1;
    }

  mc = malloc (sizeof (masterConn));
  init_master_conn (mc);
  mc->rep = rep;
  mc->read_cron_count = rep->cron_count;

  if ((mc->ous = io_stream_create (1024)) == NULL)
    {
      LOG (LG_ERROR, "failed to create output stream structure");
      goto error;
    }

  mc->fd = tcp_connect (host, port, TCP_OPT_KEEPALIVE, rep->ebuf, EBUFSZ);
  if (mc->fd < 0)
    {
      LOG (LG_ERROR, "failed to connect to master %s", rep->ebuf);
      goto error;
    }
  LOG (LG_INFO, "connected to the master %s:%d", host, port);

  if (rep->master_host != NULL)
    {
      free (rep->master_host);
    }

  rep->master_host = strdup (host);
  if (rep->master_host == NULL)
    {
      LOG (LG_ERROR, "memory allocation error");
      return -1;
    }
  rep->master_base_port = port - PORT_OFF_SLAVE;

  ret = tcp_set_option (mc->fd, TCP_OPT_NONBLOCK | TCP_OPT_NODELAY);
  if (ret < 0)
    {
      goto error;
    }

  /* 
   * initial HANDSHAKE
   * 1. get sequences from master
   * 2. check local_max_seq against master sequence numbers and ajust log_seq 
   */
  deadline = currtime_ms () + DEFAULT_HANDSHAKE_TIMEOUT_MSEC;
  if (tcp_read_fully_deadline (mc->fd, &net_seq, sizeof (long long), deadline)
      < 0)
    {
      LOG (LG_ERROR, "failed to read master min sequence number");
      goto error;
    }
  master_min_seq = ntohll (net_seq);

  if (tcp_read_fully_deadline (mc->fd, &net_seq, sizeof (long long), deadline)
      < 0)
    {
      LOG (LG_ERROR, "failed to read master max commit sequence number");
      goto error;
    }
  // master_commit_seq is not used

  if (tcp_read_fully_deadline (mc->fd, &net_seq, sizeof (long long), deadline)
      < 0)
    {
      LOG (LG_ERROR, "failed to read master max sequence number");
      goto error;
    }
  *master_max_msgend_seq = ntohll (net_seq);

  if (master_min_seq > local_max_seq)
    {
      LOG (LG_ERROR,
	   "master min seq %lld is greater than my request seq: %lld",
	   master_min_seq, local_max_seq);
      goto error;
    }
  else if (*master_max_msgend_seq < local_max_seq)
    {
      LOG (LG_ERROR,
	   "master max seq %lld is less than my request seq: %lld",
	   *master_max_msgend_seq, local_max_seq);
      goto error;
    }

  net_nid = htons (rep->nid);
  if (tcp_write_fully_deadline (mc->fd, &net_nid, sizeof (net_nid), deadline)
      < 0)
    {
      LOG (LG_ERROR, "failed to write node id to the master");
      goto error;
    }

  net_seq = htonll (local_max_seq);
  if (tcp_write_fully_deadline
      (mc->fd, &net_seq, sizeof (long long), deadline) < 0)
    {
      LOG (LG_ERROR, "failed to write sequence number to the master");
      goto error;
    }
  mc->rseq = local_max_seq;

  rep->master = mc;
  return 0;

error:
  if (lme != NULL)
    {
      logmmap_entry_release (rep, lme);
    }
  if (mc != NULL)
    {
      free_master_conn (mc);
    }
  return -1;
}


static int
master_prepare_write (masterConn * mc)
{
  assert (mc != NULL);
  assert (mc->fd > 0);

  if (mc->w_prepared == 0)
    {
      if (aeCreateFileEvent
	  (mc->rep->el, mc->fd, AE_WRITABLE, master_write_handler,
	   mc) == AE_OK)
	{
	  mc->w_prepared = 1;
	  return 0;
	}
      else
	{
	  LOG (LG_ERROR, "create file event (master_write_handler) failed");
	  return -1;
	}
    }
  return 0;
}

static int
process_master_input (masterConn * mc, char *buf, int sz)
{
  char *bp;
  int avail;
  long long log_seq;
  long long safe_seq;
  long long buf_seq;		//sequence number corresponds to buf[0]
  long long rseq;
  long long msg_begin_seq;
  smrReplicator *rep = mc->rep;

  /* 
   * masterConn must track log sequence for the slave replicator to be able to 
   * send message-boundary SEQ_RECEIVED sequence number to the master.
   *
   * Message boundary constraint must be hold. When a slave connects to the 
   * master, masterConn structure is created and initialized. There are 
   * sequence tracking related variables (cs_xxx) in the structure.  
   * cs_xxx variables assume that initial master input is aligned to the message 
   * boundary.
   *
   * Structure of master input processing is as follows
   * -------------------------------------*---*-----------------* (smr log)
   *                                      ^   ^                 ^ (log_seq)
   *                                          <-----  buf  -----> (sz)
   *                                          bp>>>       
   *                                      <---> (cs_buf)
   */
  log_seq = aseq_get (&rep->log_seq);
  safe_seq = rep->local_client->sent_seq;
  rseq = -1LL;
  msg_begin_seq = -1LL;
  buf_seq = log_seq - sz;
  bp = buf;
  avail = sz;
  while (avail > 0)
    {
      int used;
      char cmd;

      if (mc->cs_msg_skip > 0)
	{
	  assert (mc->cs_buf_pos == 0);
	  used = mc->cs_msg_skip < avail ? mc->cs_msg_skip : avail;
	  mc->cs_msg_skip -= used;
	  avail -= used;
	  bp += used;
	  if (mc->cs_msg_skip == 0)
	    {
	      /*
	       * (message end)
	       * This rseq calc. logic is needed because below <rseq set statement>
	       * only check message begin position.
	       */
	      rseq = buf_seq + (bp - buf);
	    }
	  continue;
	}
      else if (mc->cs_need > 0)
	{
	  assert (mc->cs_msg_skip == 0);
	  used = mc->cs_need < avail ? mc->cs_need : avail;
	  memcpy (&mc->cs_buf[mc->cs_buf_pos], bp, used);
	  mc->cs_need -= used;
	  avail -= used;
	  mc->cs_buf_pos += used;
	  bp += used;
	  if (mc->cs_need > 0)
	    {
	      continue;
	    }
	}

      cmd = mc->cs_buf_pos > 0 ? mc->cs_buf[0] : *bp;

      /* (message begin) */
      msg_begin_seq = log_seq - sz + (bp - buf) - mc->cs_buf_pos;
      rseq = msg_begin_seq;

      switch (cmd)
	{
	case SMR_OP_EXT:
	case SMR_OP_SESSION_CLOSE:
	  /* seq acked by skip */
	  mc->cs_msg_skip = 1 + sizeof (int);
	  break;
	case SMR_OP_NODE_CHANGE:
	  /* seq acked by skip */
	  mc->cs_msg_skip = 1 + sizeof (short);
	  break;
	case SMR_OP_SESSION_DATA:	//sid, hash, timestamp, length, data
	  /* seq acked by skip */
	  if (mc->cs_buf_pos > 0)
	    {
	      int length;
	      //get length field
	      memcpy (&length,
		      &mc->cs_buf[1 + 2 * sizeof (int) + sizeof (long long)],
		      sizeof (int));
	      length = ntohl (length);
	      mc->cs_msg_skip = length;
	      mc->cs_buf_pos = 0;
	      mc->cs_buf[0] = '\0';
	    }
	  else
	    {
	      mc->cs_need = 1 + 3 * sizeof (int) + sizeof (long long);
	    }
	  break;
	case SMR_OP_SEQ_COMMITTED:
	  /* seq acked here */
	  if (mc->cs_buf_pos > 0)
	    {
	      int net_magic, magic;
	      long long net_seq, seq;
	      unsigned short net_seq_cksum, seq_cksum, calc_cksum;
	      char *tmp = &mc->cs_buf[1];

	      memcpy (&net_magic, tmp, sizeof (int));
	      tmp += sizeof (int);
	      memcpy (&net_seq, tmp, sizeof (long long));
	      tmp += sizeof (long long);
	      memcpy (&net_seq_cksum, tmp, sizeof (unsigned short));
	      tmp += sizeof (unsigned short);

	      magic = ntohl (net_magic);
	      seq = ntohll (net_seq);
	      seq_cksum = ntohs (net_seq_cksum);
	      if (magic != SEQ_COMMIT_MAGIC)
		{
		  LOG (LG_ERROR,
		       "magic check failed: got(%d), expected(%d)", magic,
		       SEQ_COMMIT_MAGIC);
		  return -1;
		}

	      calc_cksum = crc16 ((char *) &net_seq, sizeof (long long), 0);
	      if (seq_cksum != calc_cksum)
		{
		  LOG (LG_ERROR,
		       "checksum is different: got(%d), expected(%d)",
		       seq_cksum, calc_cksum);
		  return -1;
		}

	      mc->cs_buf_pos = 0;
	      mc->cs_buf[0] = '\0';
	      // TODO: consider assertion. seq <= rep->commit_seq can't happen here
	      if (seq > rep->commit_seq)
		{
		  /* it is possible to receive seq < rep->commit_seq.
		   * by the log_rewind */
		  rep->commit_seq = seq;
		}
	      if (seq > safe_seq)
		{
		  safe_seq = seq;
		}

	      rseq = msg_begin_seq + SMR_OP_SEQ_COMMITTED_SZ;
	    }
	  else
	    {
	      /* command itself + magic + sequence number + crc16 */
	      mc->cs_need = 1;
	      mc->cs_need += sizeof (int);
	      mc->cs_need += sizeof (long long);
	      mc->cs_need += sizeof (unsigned short);
	    }
	  break;
	default:
	  LOG (LG_ERROR,
	       "Invalid master input processing state cmd: %d('%c')", cmd,
	       cmd);
	  return -1;
	}
    }

  if (rseq > 0 && rseq > mc->rseq)
    {
      mc->rseq = rseq;
    }

  if (rep->local_client != NULL
      && safe_seq > rep->local_client->sent_seq
      && local_publish_safe_seq (rep->local_client, safe_seq) < 0)
    {
      return -1;
    }

  return 0;
}

#define DEFAULT_READ_BUFSZ (4*1024)
static void
master_read (masterConn * mc, aeEventLoop * el, int fd)
{
  smrReplicator *rep = mc->rep;
  int tr = 0;
  long long saved_rseq;
  logMmapEntry *mme = NULL;

  saved_rseq = mc->rseq;

  /* write to log file */
  while (tr < MAX_LOG_RECV_PER_EVENT)
    {
      char *buf = NULL;
      int avail = 0;
      int nr;

      /* get buffer */
      if (smrlog_get_buf (rep->smrlog, rep->curr_log->addr, &buf, &avail) ==
	  -1)
	{
	  LOG (LG_ERROR, "Failed to get buffer for logging");
	  goto error;
	}
      CHECK_SMRLOG_BUF (rep->curr_log->addr, buf, aseq_get (&rep->log_seq));

      if (avail <= 0)
	{
	  LOG (LG_ERROR, "no avail buffer to the log file (low prob.)");
	  smr_panic ("Internal constraint violation.", __FILE__, __LINE__);
	  goto error;
	}

      /* read from fd */
      nr =
	read (fd, buf,
	      avail > DEFAULT_READ_BUFSZ ? DEFAULT_READ_BUFSZ : avail);
      SFI_PROBE2 ("read", MASTER_CONN_IN, (long) &nr);
      if (nr < 0)
	{
	  if (nr == -1)
	    {
	      if (errno == EAGAIN)
		{
		  break;
		}
	      LOG (LG_ERROR, "Failed to read() fd:%d errno:%d", fd, errno);
	      goto error;
	    }
	}
      else if (nr == 0)
	{
	  LOG (LG_WARN, "Connection closed fd:%d", fd);
	  goto error;
	}

      /* 
       * buf is from rep->curr_log but, rep->curr_log can be advaced during log_write
       * so we add reference count temporarly 
       */
      mme = logmmap_entry_addref (rep, rep->curr_log);
      if (log_write (rep, buf, nr) < 0)
	{
	  goto error;
	}

      /* 
       * process buffer seeking for SMR_OP_SEQ_COMMITTED 
       * process_master_input() should be called right after log_write()
       */
      if (process_master_input (mc, buf, nr) < 0)
	{
	  goto error;
	}
      logmmap_entry_release (rep, mme);
      mme = NULL;

      tr += nr;
    }

  if (tr == 0)
    {
      LOG (LG_WARN, "read handler called with nothing to read");
      return;
    }

  if (saved_rseq < mc->rseq)
    {
      char cmd[1 + sizeof (long long)];
      long long net_seq;

      /* send SMR_OP_SEQ_RECEIVED to master */
      cmd[0] = SMR_OP_SEQ_RECEIVED;
      net_seq = htonll (mc->rseq);
      memcpy (&cmd[1], &net_seq, sizeof (long long));

      if (master_prepare_write (mc) < 0
	  || stream_write (mc->ous, cmd, 1 + sizeof (long long)) < 0)
	{
	  goto error;
	}
      LOG (LG_DEBUG, "%s sent to master: %lld",
	   get_command_string (SMR_OP_SEQ_RECEIVED), ntohll (net_seq));
    }
  return;

error:
  if (mme != NULL)
    {
      logmmap_entry_release (rep, mme);
    }
  free_master_conn (mc);
  return;
}

static void
master_write (masterConn * mc, aeEventLoop * el, int fd)
{
  int ret;
  int has_more;
  int n_sent = 0;

  has_more = 1;
  ret = fd_write_stream (MASTER_CONN_OUT, fd, mc->ous, &has_more, &n_sent);
  if (ret < 0)
    {
      LOG (LG_ERROR, "can't write");
      free_master_conn (mc);
      return;
    }

  if (has_more == 0)
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      mc->w_prepared = 0;
    }
}

static void
master_read_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  masterConn *mc = (masterConn *) data;
  smrReplicator *rep = mc->rep;
  SLOWLOG_DECLARE ('m', 'r');

  mc->read_cron_count = rep->cron_count;
  applog_enter_session (rep, MASTER_CONN_IN, fd);
  SLOWLOG_ENTER ();
  master_read (mc, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
master_write_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  masterConn *mc = (masterConn *) data;
  smrReplicator *rep = mc->rep;
  SLOWLOG_DECLARE ('m', 'w');

  applog_enter_session (rep, MASTER_CONN_OUT, fd);
  SLOWLOG_ENTER ();
  master_write (mc, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static int
mgmt_reply_cstring (mgmtConn * conn, char *fmt, ...)
{
  va_list ap;
  char msg_buf[8196];
  char *buf = &msg_buf[0];
  int buf_sz = 8196;
  int sz;
  int ret = 0;

  assert (conn != NULL);
  assert (fmt != NULL);

  while (1)
    {
      va_start (ap, fmt);
      sz = vsnprintf (msg_buf, sizeof (msg_buf), fmt, ap);
      va_end (ap);

      if (sz < 0)
	{
	  LOG (LG_ERROR, "message format output error:%s", fmt);
	  ret = -1;
	  goto done;
	}
      else if (sz > buf_sz)
	{
	  buf_sz = sz + 1;
	  buf =
	    (buf == &msg_buf[0]) ? malloc (buf_sz) : realloc (buf, buf_sz);
	  if (buf == NULL)
	    {
	      LOG (LG_ERROR, "malloc failed:%d", buf_sz);
	      ret = -1;
	      goto done;
	    }
	  continue;
	}
      else
	{
	  break;
	}
    }

  if (buf[0] == '-')
    {
      LOG (LG_WARN, "%s", buf);
    }
  else if (buf[0] == '+')
    {
      LOG (LG_INFO, "%s", buf);
    }

  if (mgmt_prepare_write (conn) < 0)
    {
      ret = -1;
    }
  else if (stream_write (conn->ous, buf, sz) < 0 ||
	   stream_write (conn->ous, "\r\n", 2) < 0)
    {
      ret = -1;
    }

done:
  if (buf != &msg_buf[0])
    {
      free (buf);
    }
  return ret;
}

static void
free_mgmt_conn (mgmtConn * conn)
{
  assert (conn != NULL);

  if (conn->head.next != &conn->head)
    {
      dlisth_delete (&conn->head);
    }
  if (conn->singleton != NULL)
    {
      free (conn->singleton);
      conn->singleton = NULL;
    }
  if (conn->fd > 0)
    {
      aeDeleteFileEvent (conn->rep->el, conn->fd, AE_READABLE);
      aeDeleteFileEvent (conn->rep->el, conn->fd, AE_WRITABLE);
      close (conn->fd);
      LOG (LG_INFO, "connection closed:%d", conn->fd);
      conn->fd = -1;
    }
  if (conn->ibuf != NULL)
    {
      free (conn->ibuf);
      conn->ibuf = conn->ibuf_p = NULL;
      conn->ibuf_sz = 0;
    }
  if (conn->ous != NULL)
    {
      io_stream_close (conn->ous);
      conn->ous = NULL;
    }

  free (conn);
}

static int
mgmt_prepare_write (mgmtConn * conn)
{
  assert (conn != NULL);
  assert (conn->fd > 0);

  if (conn->w_prepared == 0)
    {
      if (aeCreateFileEvent
	  (conn->rep->el, conn->fd, AE_WRITABLE, mgmt_write_handler,
	   conn) == AE_OK)
	{
	  conn->w_prepared = 1;
	  return 0;
	}
      else
	{
	  LOG (LG_ERROR, "create file event (mgmt_write_handler) failed");
	  return -1;
	}
    }
  return 0;
}

static void
mgmt_accept (smrReplicator * rep, aeEventLoop * el, int fd)
{
  mgmtConn *conn;
  int cfd = 0;
  char cip[64];
  int cport;
  int ret;

  assert (fd == rep->mgmt_lfd);
  cfd = tcp_accept (fd, cip, &cport, rep->ebuf, EBUFSZ);
  if (cfd < 0)
    {
      LOG (LG_WARN, "accept failed: %s", rep->ebuf);
      return;
    }
  LOG (LG_INFO, "connection from %s:%d fd:%d", cip, cport, cfd);

  conn = malloc (sizeof (mgmtConn));
  if (conn == NULL)
    {
      LOG (LG_ERROR, "memory allocation failed");
      close (cfd);
      LOG (LG_INFO, "connection closed:%d", cfd);
      return;
    }

  init_mgmt_conn (conn);
  conn->rep = rep;
  conn->fd = cfd;

  conn->ibuf_p = conn->ibuf = malloc (1024);
  if (conn->ibuf == NULL)
    {
      LOG (LG_ERROR, "failed to create input buffer");
      goto error;
    }
  conn->ibuf_sz = 1024;

  conn->ous = io_stream_create (1024);
  if (conn->ous == NULL)
    {
      LOG (LG_ERROR, "failed to create out stream");
      goto error;
    }

  ret = tcp_set_option (cfd, TCP_OPT_NONBLOCK | TCP_OPT_NODELAY);
  SFI_PROBE2 ("accept", MANAGEMENT_CONN_IN, (long) &ret);
  if (ret < 0)
    {
      goto error;
    }

  if (aeCreateFileEvent
      (rep->el, cfd, AE_READABLE, mgmt_read_handler, conn) == AE_ERR)
    {
      goto error;
    }
  if (aeCreateFileEvent
      (rep->el, cfd, AE_WRITABLE, mgmt_write_handler, conn) == AE_ERR)
    {
      goto error;
    }

  /* link to managers */
  dlisth_insert_after (&conn->head, &rep->managers);
  return;

error:
  if (conn != NULL)
    {
      free_mgmt_conn (conn);
    }
}

static void
mgmt_accept_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  SLOWLOG_DECLARE ('g', 'a');

  applog_enter_session (rep, MANAGEMENT_CONN_IN, fd);
  SLOWLOG_ENTER ();
  mgmt_accept (rep, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
mgmt_read (mgmtConn * conn, aeEventLoop * el, int fd)
{
  char *bp, *ep;

  while (1)
    {
      int nr;
      int avail = conn->ibuf_sz - (conn->ibuf_p - conn->ibuf);

      if (avail <= 0)
	{
	  char *buf = realloc (conn->ibuf, conn->ibuf_sz * 2 + 1);

	  if (buf == NULL)
	    {
	      LOG (LG_ERROR,
		   "Failed to allocate a new input buffer: %d",
		   conn->ibuf_sz * 2);
	      free_mgmt_conn (conn);
	      return;
	    }
	  // adjust pointers
	  conn->ibuf_p = buf + (conn->ibuf_p - conn->ibuf);
	  conn->ibuf = buf;
	  conn->ibuf_sz = conn->ibuf_sz * 2;
	  continue;
	}

      nr = read (fd, conn->ibuf_p, avail);
      SFI_PROBE2 ("read", MANAGEMENT_CONN_IN, (long) &nr);
      if (nr < 0)
	{
	  if (nr == -1)
	    {
	      if (errno == EAGAIN)
		{
		  break;
		}
	      LOG (LG_ERROR, "Failed to read() fd:%d errno:%d", fd, errno);
	      free_mgmt_conn (conn);
	      return;
	    }
	}
      else if (nr == 0)
	{
	  /* connection closed */
	  LOG (LG_WARN, "Connection closed fd:%d", fd);
	  free_mgmt_conn (conn);
	  return;
	}

      LOG (LG_DEBUG, "read %d bytes", nr);
      conn->ibuf_p += nr;
    }

  // single turn of read completed. check a command made...
  bp = conn->ibuf;
  ep = conn->ibuf_p;
  if ((ep - bp) > 2 && *(ep - 2) == '\r' && *(ep - 1) == '\n')
    {
      int ret;

      *(ep - 2) = '\0';
      ret = mgmt_request (conn, bp, ep - 2);
      *(ep - 2) = '\r';

      //reset buffer
      conn->ibuf_p = conn->ibuf;
      if (ret < 0)
	{
	  free_mgmt_conn (conn);
	  return;
	}
    }
}

static void
mgmt_read_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  mgmtConn *conn = (mgmtConn *) data;
  smrReplicator *rep = conn->rep;
  SLOWLOG_DECLARE ('g', 'r');

  rep->stat.client_nre++;
  applog_enter_session (rep, MANAGEMENT_CONN_IN, fd);
  SLOWLOG_ENTER ();
  mgmt_read (conn, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
mgmt_write (mgmtConn * conn, aeEventLoop * el, int fd)
{
  int ret;
  int has_more;
  int n_sent = 0;

  has_more = 1;
  ret =
    fd_write_stream (MANAGEMENT_CONN_OUT, fd, conn->ous, &has_more, &n_sent);
  if (ret < 0)
    {
      LOG (LG_ERROR, "can't write");
      free_mgmt_conn (conn);
      return;
    }

  if (has_more == 0)
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      conn->w_prepared = 0;
    }
}

static void
mgmt_write_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  mgmtConn *conn = (mgmtConn *) data;
  smrReplicator *rep = conn->rep;
  SLOWLOG_DECLARE ('g', 'w');

  applog_enter_session (rep, MANAGEMENT_CONN_OUT, fd);
  SLOWLOG_ENTER ();
  mgmt_write (conn, el, fd);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static long long
cron_count_diff_msec (long long was, long long is)
{
  return (is - was) * MAIN_CRON_INTERVAL;
}

static int
parse_ll (char *tok, long long *seq)
{
  assert (tok != NULL);
  assert (seq != NULL);

  errno = 0;
  *seq = strtoll (tok, NULL, 10);
  if (errno == ERANGE)
    {
      return -1;
    }
  return 0;
}

static int
parse_seq (char *tok, long long *seq)
{
  int ret;
  long long llval;

  ret = parse_ll (tok, &llval);
  if (ret == 0 && llval >= 0)
    {
      *seq = llval;
      return 0;
    }
  return -1;
}

static int
parse_int (char *tok, int *rval)
{
  long long ll;

  if (parse_ll (tok, &ll) != 0)
    {
      return -1;
    }

  /* check overflow underflow */
  if (ll > INT_MAX || ll < INT_MIN)
    {
      return -1;
    }

  *rval = (int) ll;
  return 0;
}

static int
parse_nid (char *tok, short *nid)
{
  int n;

  assert (tok != NULL);
  assert (nid != NULL);

  n = strtol (tok, NULL, 10);
  if (n < 0 || errno == ERANGE)
    {
      return -1;
    }
  if (n > SHRT_MAX)
    {
      return -1;
    }

  *nid = n;
  return 0;
}

static int
parse_quorum_policy (char *pol, int *dest)
{
  int qp;

  if (parse_int (pol, &qp) != 0)
    {
      return -1;
    }

  if (qp < 0 || qp > SMR_MAX_SLAVES)
    {
      return -1;
    }

  if (dest != NULL)
    {
      *dest = qp;
    }
  return 0;
}

static int
parse_quorum_members (short master_nid, char *members[],
		      short dest[SMR_MAX_SLAVES], int *num_members)
{
  int i;
  short nid;

  for (i = 0; i < SMR_MAX_SLAVES && members[i] != NULL; i++)
    {
      if (parse_nid (members[i], &nid) < 0 || nid == master_nid)
	{
	  return -1;
	}
      if (dest != NULL)
	{
	  dest[i] = nid;
	}
    }
  if (num_members != NULL)
    {
      *num_members = i;
    }
  return 0;
}

static int
notify_be_new_master (smrReplicator * rep, char *master_host,
		      int client_port, long long last_cseq, short nid)
{
  localConn *lc;
  int ret;
  char conf_buf[BE_CONF_BUF_SIZE];

  lc = rep->local_client;
  if (lc == NULL)
    {
      LOG (LG_ERROR, "local connection is null");
      return -1;
    }

  ret =
    be_configure_msg (conf_buf, BE_CONF_BUF_SIZE, "new_master %s %d %lld %d",
		      master_host, client_port, last_cseq, nid);
  if (ret < 0)
    {
      return -1;
    }
  else if (local_prepare_write (lc) < 0
	   || stream_write (lc->ous, conf_buf, ret) < 0)
    {
      return -1;
    }
  return 0;
}

static int
notify_be_rckpt (smrReplicator * rep, char *behost, int beport)
{
  localConn *lc;
  int ret;
  char conf_buf[BE_CONF_BUF_SIZE];

  lc = rep->local_client;
  if (lc == NULL)
    {
      LOG (LG_ERROR, "local connection is null");
      return -1;
    }
  ret =
    be_configure_msg (conf_buf, BE_CONF_BUF_SIZE,
		      "rckpt %s %d", behost, beport);

  if (ret < 0)
    {
      LOG (LG_ERROR, "failed to make init configure message");
      return -1;
    }
  else if (local_prepare_write (lc) < 0
	   || stream_write (lc->ous, conf_buf, ret) < 0)
    {
      return -1;
    }
  return 0;
}

static int
notify_be_init (smrReplicator * rep, char *log_dir, char *master_host,
		int master_client_port, long long commit_seq, short nid)
{
  localConn *lc;
  int ret;
  char conf_buf[BE_CONF_BUF_SIZE];

  lc = rep->local_client;
  if (lc == NULL)
    {
      LOG (LG_ERROR, "local connection is null");
      return -1;
    }
  ret =
    be_configure_msg (conf_buf, BE_CONF_BUF_SIZE,
		      "init %s %s %d %lld %d", log_dir,
		      master_host, master_client_port, commit_seq, nid);
  if (ret < 0)
    {
      LOG (LG_ERROR, "failed to make init configure message");
      return -1;
    }
  else if (local_prepare_write (lc) < 0
	   || stream_write (lc->ous, conf_buf, ret) < 0)
    {
      return -1;
    }
  return 0;
}

static int
notify_be_lconn (smrReplicator * rep)
{
  localConn *lc;
  int ret;
  char conf_buf[BE_CONF_BUF_SIZE];

  lc = rep->local_client;
  if (lc == NULL)
    {
      LOG (LG_ERROR, "local connection is null");
      return -1;
    }

  ret = be_configure_msg (conf_buf, BE_CONF_BUF_SIZE, "lconn");
  if (ret < 0)
    {
      LOG (LG_ERROR, "failed to make lconn configure message");
      return -1;
    }
  else if (local_prepare_write (lc) < 0
	   || stream_write (lc->ous, conf_buf, ret) < 0)
    {
      return -1;
    }
  return 0;
}


static int
do_virtual_progress (smrReplicator * rep)
{
  char cmd[1 + sizeof (short)];
  char *cp = &cmd[0];
  short vnode = 0x7FFF;

  *cp++ = SMR_OP_NODE_CHANGE;
  vnode = htons (vnode);
  memcpy (cp, &vnode, sizeof (short));

  if (log_write (rep, cmd, sizeof (cmd)) < 0)
    {
      return -1;
    }
  if (prepare_slaves_for_write (rep) < 0)
    {
      return -1;
    }
  return publish_commit_or_safe_seq (rep, aseq_get (&rep->log_seq));
}

static int
nid_is_quorum_member (smrReplicator * rep, short nid)
{
  int i;

  if (rep->has_quorum_members == 0)
    {
      // no special membership
      return 1;
    }

  for (i = 0; i < SMR_MAX_SLAVES; i++)
    {
      if (rep->quorum_members[i] == nid)
	{
	  return 1;
	}
    }
  return 0;
}

static int
do_role_master (mgmtConn * conn, long long rewind_cseq)
{
  smrReplicator *rep;
  int ret;

  rep = conn->rep;

  if (rep->role != LCONN)
    {
      LOG (LG_ERROR, "do_role_master is called. current role:%d", rep->role);
      return -1;
    }
  assert (rep->local_client != NULL);

  if (rep->curr_log != NULL)
    {
      LOG (LG_ERROR, "Internal state error (curr log is not null)");
      return -1;
    }

  if (rep->need_rckpt)
    {
      LOG (LG_ERROR, "need remote checkpoint");
      return -1;
    }
  else if (rewind_cseq != -1)
    {
      /* check against my sequence number */
      if (rep->local_client->sent_seq > rewind_cseq)
	{
	  LOG (LG_ERROR,
	       "rewind_cseq %lld is less than backend commit_seq %lld",
	       rewind_cseq, rep->local_client->sent_seq);
	  return -1;
	}

      if (log_rewind_to_cseq (rep, rewind_cseq) < 0)
	{
	  LOG (LG_ERROR, "rewind log entry failed");
	  return -1;
	}
      assert (aseq_get (&rep->log_seq) == rewind_cseq);
    }

  rep->master_host = strdup ("localhost");
  if (rep->master_host == NULL)
    {
      LOG (LG_ERROR, "memory allocation error");
      return -1;
    }
  rep->master_base_port = rep->base_port;

  if ((rep->curr_log =
       logmmap_entry_get (rep, aseq_get (&rep->log_seq), GET_CREATE,
			  1)) == NULL)
    {
      LOG (LG_ERROR, "Failed to get log file for %lld",
	   aseq_get (&rep->log_seq));
      goto error;
    }

  if (launch_bio_thread (rep) != 0)
    {
      LOG (LG_ERROR, "Failed to launch bio thread");
      goto error;
    }

  rep->num_trailing_commit_msg = 0;
  rep->last_commit_msg_end = 0LL;

  LOG (LG_INFO, "role changed to MASTER: cseq:%lld lseq:%lld",
       rep->commit_seq, aseq_get (&rep->log_seq));
  rep->role = MASTER;
  rep->role_id++;

  if (!rep->local_client->init_sent)
    {
      ret =
	notify_be_init (rep, rep->log_dir, "localhost",
			rep->base_port + PORT_OFF_CLIENT,
			rep->commit_seq, rep->nid);
      if (ret != 0)
	{
	  goto error;
	}
      rep->local_client->init_sent = 1;

      if (rep->local_client->sent_seq < rep->commit_seq
	  && local_publish_safe_seq (rep->local_client, rep->commit_seq) < 0)
	{
	  goto error;
	}
    }
  else if (notify_be_new_master (rep, "localhost",
				 rep->base_port + PORT_OFF_CLIENT,
				 aseq_get (&rep->log_seq), rep->nid) != 0)
    {
      goto error;
    }

  rep->cron_need_progress = 1;
  rep->progress_seq = aseq_get (&rep->log_seq);

  return 0;

error:
  if (rep->master_host != NULL)
    {
      free (rep->master_host);
      rep->master_host = NULL;
      rep->master_base_port = -1;
    }
  if (rep->nid != 0)
    {
      rep->nid = -1;
    }
  if (rep->curr_log != NULL)
    {
      logmmap_entry_release (rep, rep->curr_log);
      rep->curr_log = NULL;
    }
  return -1;
}

static int
role_master_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep;
  int ret;
  short nid = 0;
  long long rewind_cseq = -1LL;
  int i;
  int commit_quorum = 0;
  short quorum_members[SMR_MAX_SLAVES];
  int num_members = 0;

  rep = conn->rep;

  if (num_tok < 3)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR need <nid> <quorum policy> <rewind cseq> [<quorum members>]");
    }

  /* parse nid */
  if (parse_nid (tokens[0], &nid) < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad nid:%s", tokens[0]);
    }
  rep->nid = nid;

  /* parse quorum policy */
  if (parse_quorum_policy (tokens[1], &commit_quorum) != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad quorum policy:%s",
				 tokens[1]);
    }

  /* parse rewind cseq */
  if (parse_seq (tokens[2], &rewind_cseq) < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad rewind_cseq:%s", tokens[2]);
    }

  /* parse quorum members */
  for (i = 0; i < SMR_MAX_SLAVES; i++)
    {
      quorum_members[i] = -1;
    }
  if (num_tok > 3)
    {
      if (parse_quorum_members
	  (rep->nid, &tokens[3], quorum_members, &num_members) < 0)
	{
	  return mgmt_reply_cstring (conn, "-ERR bad quorum members");
	}
    }

  ret = do_role_master (conn, rewind_cseq);
  if (ret == 0)
    {
      rep->commit_quorum = commit_quorum;
      rep->has_quorum_members = (num_members > 0);
      for (i = 0; i < SMR_MAX_SLAVES; i++)
	{
	  rep->quorum_members[i] = quorum_members[i];
	}
      return mgmt_reply_cstring (conn, "+OK");
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR do_role_master failed");
    }
}

static int
do_role_slave (mgmtConn * conn, const char *host, int slave_port,
	       long long rewind_cseq)
{
  smrReplicator *rep = conn->rep;
  int ret;
  long long master_max_msgend_seq = 0LL;

  if (rep->role != LCONN)
    {
      LOG (LG_ERROR, "do_role_slave is called. current role:%d", rep->role);
      return -1;
    }
  assert (rep->local_client != NULL);

  if (rep->curr_log != NULL)
    {
      LOG (LG_ERROR, "Internal state error (curr log is not null)");
      return -1;
    }

  if (rep->need_rckpt)
    {
      LOG (LG_ERROR, "need remote checkpoint");
      return -1;
    }
  else if (rewind_cseq != -1)
    {
      /* check against my sequence number */
      if (rep->local_client->sent_seq > rewind_cseq)
	{
	  LOG (LG_ERROR,
	       "rewind_cseq %lld is less than backend commit_seq %lld",
	       rewind_cseq, rep->local_client->sent_seq);
	  return -1;
	}

      if (log_rewind_to_cseq (rep, rewind_cseq) < 0)
	{
	  LOG (LG_ERROR, "rewind log entry failed");
	  return -1;
	}
      if (rep->commit_seq > rewind_cseq)
	{
	  // for 1.2 configuration master compatibility
	  rep->commit_seq = rewind_cseq;
	}
      assert (aseq_get (&rep->log_seq) == rewind_cseq);
    }

  if (connect_to_master
      (rep, (char *) host, slave_port, aseq_get (&rep->log_seq),
       &master_max_msgend_seq) < 0)
    {
      LOG (LG_ERROR, "Failed to connect to master %s:%d", host, slave_port);
      return -1;
    }

  if ((rep->curr_log =
       logmmap_entry_get (rep, aseq_get (&rep->log_seq), GET_CREATE,
			  1)) == NULL)
    {
      LOG (LG_ERROR, "Failed to get log file for %lld",
	   aseq_get (&rep->log_seq));
      return -1;
    }

  /* set-up handlers after recovery finished */
  if (aeCreateFileEvent
      (rep->el, rep->master->fd, AE_WRITABLE, master_write_handler,
       rep->master) != AE_OK
      || aeCreateFileEvent (rep->el, rep->master->fd, AE_READABLE,
			    master_read_handler, rep->master) != AE_OK)
    {
      LOG (LG_ERROR, "Failed to create master event handler");
      goto error;
    }

  if (launch_bio_thread (rep) != 0)
    {
      LOG (LG_ERROR, "Failed to launch bio thread");
      goto error;
    }

  LOG (LG_INFO, "role changed to SLAVE: cseq:%lld lseq:%lld", rep->commit_seq,
       aseq_get (&rep->log_seq));
  rep->role = SLAVE;
  rep->role_id++;

  if (!rep->local_client->init_sent)
    {
      ret =
	notify_be_init (rep, rep->log_dir, (char *) host,
			slave_port - PORT_OFF_SLAVE + PORT_OFF_CLIENT,
			rep->commit_seq, rep->nid);
      if (ret != 0)
	{
	  goto error;
	}
      rep->local_client->init_sent = 1;

      if (rep->local_client->sent_seq < rep->commit_seq
	  && local_publish_safe_seq (rep->local_client, rep->commit_seq) < 0)
	{
	  goto error;
	}
    }
  else if (notify_be_new_master (rep, (char *) host,
				 slave_port - PORT_OFF_SLAVE +
				 PORT_OFF_CLIENT, master_max_msgend_seq,
				 rep->nid) != 0)
    {
      goto error;
    }
  return 0;

error:
  if (rep->curr_log != NULL)
    {
      logmmap_entry_release (rep, rep->curr_log);
      rep->curr_log = NULL;
    }

  if (rep->master_host != NULL)
    {
      free (rep->master_host);
      rep->master_host = NULL;
      rep->master_base_port = -1;
    }
  return -1;
}

static int
role_slave_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep;
  char *master_host;
  int base_port;
  int ret;
  short nid = 0;
  long long rewind_cseq = -1LL;

  if (num_tok != 3 && num_tok != 4)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR <nid> <host> <base port> [<rewind cseq>]");
    }
  rep = conn->rep;

  /* parse nid */
  if (parse_nid (tokens[0], &nid) < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad nid:%s", tokens[0]);
    }
  rep->nid = nid;

  master_host = tokens[1];
  base_port = atoi (tokens[2]);
  if (base_port < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad port:%d", base_port);
    }

  /* parse rewind cseq */
  if (num_tok == 4 && parse_seq (tokens[3], &rewind_cseq) < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad rewind_cseq:%s", tokens[2]);
    }

  ret =
    do_role_slave (conn, master_host, base_port + PORT_OFF_SLAVE,
		   rewind_cseq);
  if (ret == 0)
    {
      return mgmt_reply_cstring (conn, "+OK");
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR do_role_slave failed");
    }
}

static int
do_role_lconn (smrReplicator * rep)
{
  if (rep->role == LCONN)
    {
      return 0;
    }

  /* stop threads */
  if (rep->bio_thr)
    {
      rep->bio_interrupted = 1;
      pthread_join (*rep->bio_thr, NULL);
      free (rep->bio_thr);
      rep->bio_thr = NULL;
      rep->bio_interrupted = 0;
      LOG (LG_INFO, "BIO thread stopped");
    }

  if (rep->mig != NULL)
    {
      rep->mig_interrupted = 1;
      pthread_join (*rep->mig_thr, NULL);
      free (rep->mig_thr);
      rep->mig_thr = NULL;
      rep->mig_interrupted = 0;
      LOG (LG_INFO, "MIG thread stopped");
    }

  /* release log */
  if (rep->curr_log != NULL)
    {
      logmmap_entry_release (rep, rep->curr_log);
      rep->curr_log = NULL;
    }

  /* disconnect replication related connections */
  if (rep->master != NULL)
    {
      free_master_conn (rep->master);
    }

  while (!dlisth_is_empty (&rep->clients))
    {
      clientConn *cc = (clientConn *) rep->clients.next;
      free_client_conn (cc);
    }

  while (!dlisth_is_empty (&rep->slaves))
    {
      slaveConn *sc = (slaveConn *) rep->slaves.next;
      free_slave_conn (sc);
    }

  /* reset other fields */
  rep->cron_need_publish = 0;
  rep->cron_need_progress = 0;
  rep->progress_seq = 0LL;

  /* notify local client */
  if (rep->local_client != NULL)
    {
      if ((rep->role == MASTER || rep->role == SLAVE)
	  && notify_be_lconn (rep) < 0)
	{
	  LOG (LG_ERROR, "Failed to notify backend the LCONN state");
	  return -1;
	}
    }

  LOG (LG_INFO, "role changed to LCONN: cseq:%lld lseq:%lld", rep->commit_seq,
       aseq_get (&rep->log_seq));
  rep->role = LCONN;
  rep->role_id++;
  return 0;
}

static int
role_lconn_request (mgmtConn * conn, char **tokens, int num_tok)
{
  if (conn->rep->role != LCONN && do_role_lconn (conn->rep) < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR failed");
    }
  return mgmt_reply_cstring (conn, "+OK");
}

static int
do_role_none (smrReplicator * rep)
{
  int ret;

  if (rep->role == NONE)
    {
      return 0;
    }
  else if ((ret = do_role_lconn (rep)) != 0)
    {
      return ret;
    }

  if (rep->local_client != NULL)
    {
      free_local_conn_with_role_change (rep->local_client, 0);
    }

  LOG (LG_INFO, "role changed to NONE: cseq:%lld lseq:%lld", rep->commit_seq,
       aseq_get (&rep->log_seq));
  rep->need_rckpt = 0;
  rep->role = NONE;
  rep->role_id++;
  return ret;
}

static int
role_none_request (mgmtConn * conn, char **tokens, int num_tok)
{
  if (do_role_none (conn->rep) < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR failed");
    }
  return mgmt_reply_cstring (conn, "+OK");
}

static int
do_rckpt (smrReplicator * rep, char *behost, int beport)
{
  int ret;

  ret = notify_be_rckpt (rep, behost, beport);
  return ret;
}

static int
rckpt_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep;
  char *behost;
  int beport;
  int ret;

  if (num_tok != 2)
    {
      return mgmt_reply_cstring (conn, "-ERR <be host> <be port>");
    }
  rep = conn->rep;

  behost = tokens[0];
  beport = atoi (tokens[1]);
  if (beport < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad port:%s", tokens[1]);
    }

  ret = do_rckpt (rep, behost, beport);
  if (ret == 0)
    {
      return mgmt_reply_cstring (conn, "+OK");
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR %d", ret);
    }
}

static int
migrate_start_request (smrReplicator * rep, char **tokens, int num_tok)
{
  char *dest_host;
  int base_port;
  long long seq = 0;
  int num_part;
  int tps_limit;

  if (rep->mig_id != 0)
    {
      MIG_ERR (rep, "migration :%lld already exists", rep->mig_id);
      return -1;
    }

  if (num_tok < 7)
    {
      MIG_ERR (rep,
	       "usage: <dest host> <base port> <seq> <tps> <num part> <rle>");
      return -1;
    }
  dest_host = tokens[0];
  base_port = atoi (tokens[1]);
  if (base_port < 0)
    {
      MIG_ERR (rep, "invalid <base port>:%s", tokens[1]);
      return -1;
    }

  if (parse_seq (tokens[2], &seq) < 0)
    {
      MIG_ERR (rep, "invalid <seq>:%s", tokens[2]);
      return -1;
    }

  tps_limit = atoi (tokens[3]);
  if (tps_limit <= 0)
    {
      MIG_ERR (rep, "invalid <tps limit>:%s", tokens[3]);
      return -1;
    }

  num_part = atoi (tokens[4]);
  if (num_part < 0)
    {
      MIG_ERR (rep, "invalid <num part>:%s", tokens[4]);
      return -1;
    }

  if (launch_mig_thread
      (rep, dest_host, base_port, seq, num_part, tokens + 5, num_tok - 5,
       tps_limit) < 0)
    {
      return -1;
    }
  return 0;
}

#ifdef SFI_ENABLED
static SFI_CB_RET
fi_delay_callback (const char *name, sfi_probe_arg * pargs, void *arg)
{
  fiDelay *s = (fiDelay *) arg;

  if (s->count-- > 0)
    {
      int ret;

      if (s->per_sleep_msec / 1000 > 0)
	{
	  sleep (s->per_sleep_msec / 1000);
	}

      ret = usleep ((s->per_sleep_msec % 1000) * 1000);
      if (ret == 0)
	{
	  return CB_OK_HAS_MORE;
	}
      else
	{
	  return CB_ERROR;
	}
    }
  else
    {
      return CB_OK_DONE;
    }
}

static SFI_CB_RET
fi_rw_delay_callback (const char *name, sfi_probe_arg * pargs, void *arg)
{
  fiRwDelay *s = (fiRwDelay *) arg;

  if ((connType) pargs->arg1 != s->conn_type)
    {
      return CB_OK_HAS_MORE;
    }

  if (s->count-- > 0)
    {
      int ret;

      if (s->per_sleep_msec / 1000 > 0)
	{
	  sleep (s->per_sleep_msec / 1000);
	}

      ret = usleep ((s->per_sleep_msec % 1000) * 1000);
      if (ret == 0)
	{
	  return CB_OK_HAS_MORE;
	}
      else
	{
	  return CB_ERROR;
	}
    }
  else
    {
      return CB_OK_DONE;
    }
}

static SFI_CB_RET
fi_rw_return_callback (const char *name, sfi_probe_arg * pargs, void *arg)
{
  fiRwReturn *s = (fiRwReturn *) arg;
  int *nr;

  if ((connType) pargs->arg1 != s->conn_type)
    {
      return CB_OK_HAS_MORE;
    }

  nr = (int *) pargs->arg2;

  *nr = s->rval;
  errno = s->erno;
  return CB_OK_DONE;
}
#endif

/*
 * delay [log | sync | sleep] <count> <per sleep msec.>
 */
static int
fi_delay_request (mgmtConn * conn, char **tokens, int num_tok)
{
#ifdef SFI_ENABLED
  int count = 0;
  int per_sleep_msec = 0;
  fiDelay *s = NULL;
  char *probe;

  if (num_tok != 3)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR [log | sync | sleep] <count> <per sleep msec>");
    }

  probe = tokens[0];
  if (strcasecmp (probe, "log") != 0 && strcasecmp (probe, "sync") != 0
      && strcasecmp (probe, "sleep") != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR [log | sync | sleep] expected");
    }
  else if (parse_int (tokens[1], &count) != 0
	   || parse_int (tokens[2], &per_sleep_msec) != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad argument");
    }
  s = malloc (sizeof (fiDelay));
  if (s == NULL)
    {
      LOG (LG_ERROR, "malloc failed");
      return -1;
    }
  s->count = count;
  s->per_sleep_msec = per_sleep_msec;

  sfi_enable (probe, fi_delay_callback, free, s);
  return mgmt_reply_cstring (conn, "+OK");
#else
  return mgmt_reply_cstring (conn, "-ERR not supported");
#endif
}

/*
 * [read|write|accept] <conn type> delay <count> <per sleep msec.>
 * [read|write|accept] <conn type> return <return value> <errno>
 */
static int
fi_rw_request (char *rw, mgmtConn * conn, char **tokens, int num_tok)
{
#ifdef SFI_ENABLED
  int conn_type = NO_CONN;

  if (num_tok != 4)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR <conn type> [delay <count> <per sleep msec.> | return <val> <errno>");
    }

  /* check <conn type> */
  if (parse_int (tokens[0], &conn_type) != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad <conn type>");
    }
  else if (conn_type < CONN_TYPE_BEGIN || conn_type > CONN_TYPE_END)
    {
      return mgmt_reply_cstring (conn, "-ERR bad <conn type> value");
    }

  if (strcasecmp (tokens[1], "delay") == 0)
    {
      int count;
      int per_sleep_msec;
      fiRwDelay *s;

      if (parse_int (tokens[2], &count) != 0
	  || parse_int (tokens[3], &per_sleep_msec) != 0)
	{
	  return mgmt_reply_cstring (conn, "-ERR bad argument");
	}

      s = malloc (sizeof (fiRwDelay));
      if (s == NULL)
	{
	  LOG (LG_ERROR, "malloc failed");
	  return -1;
	}
      s->conn_type = conn_type;
      s->count = count;
      s->per_sleep_msec = per_sleep_msec;

      sfi_enable (rw, fi_rw_delay_callback, free, s);
    }
  else if (strcasecmp (tokens[1], "return") == 0)
    {
      int rval;
      int erno;
      fiRwReturn *s;

      if (parse_int (tokens[2], &rval) != 0
	  || parse_int (tokens[3], &erno) != 0)
	{
	  return mgmt_reply_cstring (conn, "-ERR bad argument");
	}

      s = malloc (sizeof (fiRwReturn));
      if (s == NULL)
	{
	  LOG (LG_ERROR, "malloc failed");
	  return -1;
	}
      s->conn_type = conn_type;
      s->rval = rval;
      s->erno = erno;
      sfi_enable (rw, fi_rw_return_callback, free, s);
    }
  else
    {
      assert (0);
      return mgmt_reply_cstring (conn,
				 "-ERR 'delay' or 'return' is expected");
    }

  return mgmt_reply_cstring (conn, "+OK");
#else
  return mgmt_reply_cstring (conn, "-ERR not supported");
#endif
}

static int
fi_request (mgmtConn * conn, char **tokens, int num_tok)
{
  if (num_tok < 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  if (strcasecmp (tokens[0], "delay") == 0)
    {
      return fi_delay_request (conn, tokens + 1, num_tok - 1);
    }
  else if (strcasecmp (tokens[0], "read") == 0
	   || strcasecmp (tokens[0], "write") == 0
	   || strcasecmp (tokens[0], "accept") == 0)
    {
      return fi_rw_request (tokens[0], conn, tokens + 1, num_tok - 1);
    }
  else if (strcasecmp (tokens[0], "clear") == 0)
    {
#ifdef SFI_ENABLED
      sfi_disable_all ();
      return mgmt_reply_cstring (conn, "+OK");
#else
      return mgmt_reply_cstring (conn, "-ERR not supported");
#endif
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR bad token:%s", tokens[0]);
    }
  return 0;
}


static int
role_request (mgmtConn * conn, char **tokens, int num_tok)
{
  if (num_tok < 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }
  else if (strcasecmp (tokens[0], "master") == 0)
    {
      if (conn->rep->role != LCONN)
	{
	  goto bad_state;
	}
      return role_master_request (conn, tokens + 1, num_tok - 1);
    }
  else if (strcasecmp (tokens[0], "slave") == 0)
    {
      if (conn->rep->role != LCONN)
	{
	  goto bad_state;
	}
      return role_slave_request (conn, tokens + 1, num_tok - 1);
    }
  else if (strcasecmp (tokens[0], "lconn") == 0)
    {
      if (conn->rep->role == NONE)
	{
	  goto bad_state;
	}
      return role_lconn_request (conn, tokens + 1, num_tok - 1);
    }
  else if (strcasecmp (tokens[0], "none") == 0)
    {
      return role_none_request (conn, tokens + 1, num_tok - 1);
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR bad token:%s", tokens[0]);
    }
  return 0;

bad_state:
  return mgmt_reply_cstring (conn, "-ERR bad state curr_role:%d",
			     conn->rep->role);
}

static int
migrate_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;
  char msg_buf[128];
  char *msg;
  int ret;

  if (rep->role != MASTER)
    {
      return mgmt_reply_cstring (conn, "-ERR bad state:%d", rep->role);
    }
  else if (num_tok < 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  msg = &msg_buf[0];
  *msg = '\0';
  ret = 0;
  if (strcasecmp (tokens[0], "start") == 0)
    {
      ret = migrate_start_request (rep, tokens + 1, num_tok - 1);
      if (ret == 0)
	{
	  snprintf (msg_buf, sizeof (msg_buf), "%lld", rep->mig_id);
	}
      goto done;
    }

  /* below commands are valid when a migratoin is started */
  if (rep->mig_id == 0)
    {
      MIG_ERR (rep, "no migration exists");
      ret = -1;
      goto done;
    }
  assert (rep->mig != NULL);

  if (strcasecmp (tokens[0], "rate") == 0)
    {
      int tps_limit = 0;

      if (num_tok != 2)
	{
	  return mgmt_reply_cstring (conn, "-ERR rate <tps limit>");
	}

      tps_limit = atoi (tokens[1]);
      if (tps_limit < 0)
	{
	  return mgmt_reply_cstring (conn, "-ERR bad <tps limt>:%s",
				     tokens[1]);
	}
      rep->mig->tps_limit = tps_limit;
    }
  else if (strcasecmp (tokens[0], "interrupt") == 0)
    {
      if (rep->mig_thr)
	{
	  rep->mig_interrupted = 1;
	  pthread_join (*rep->mig_thr, NULL);
	  free (rep->mig_thr);
	  rep->mig_thr = NULL;
	  rep->mig_interrupted = 0;
	}
    }
  else if (strcasecmp (tokens[0], "info") == 0)
    {
      snprintf (msg_buf, sizeof (msg_buf),
		"log:%lld mig:%lld buf:%lld sent:%lld acked:%lld",
		aseq_get (&rep->log_seq),
		aseq_get (&rep->mig_seq), aseq_get (&rep->mig->buffered),
		aseq_get (&rep->mig->sent), aseq_get (&rep->mig->acked));
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR bad <sub command>:%s",
				 tokens[0]);
    }

done:
  if (ret < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR %s", rep->mig_ebuf);
    }
  else
    {
      return mgmt_reply_cstring (conn, "+OK %s", msg);
    }
}

static int
getseq_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;
  int flag = 0;			//1: log, 2 slave
  char buf[8192];
  char *bp, *cp;
  int sz;

  if (num_tok != 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  if (strcasecmp (tokens[0], "all") == 0)
    {
      flag = 1 | 2;
    }
  else if (strcasecmp (tokens[0], "log") == 0)
    {
      flag = 1;
    }
  else if (strcasecmp (tokens[0], "slave") == 0)
    {
      flag = 2;
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR bad token:%s", tokens[0]);
    }

  bp = &buf[0];
  cp = bp;

  /* 
   * Make response: assuumes that buf size is sufficient 
   */
  if (flag & 1)
    {
      long long max_seq;

      if (rep->role == SLAVE)
	{
	  max_seq = rep->master->rseq;
	}
      else
	{
	  max_seq = aseq_get (&rep->log_seq);
	}

      sz =
	sprintf (cp, " log min:%lld commit:%lld max:%lld",
		 aseq_get (&rep->min_seq), rep->commit_seq, max_seq);
      cp += sz;

      if (rep->local_client)
	{
	  sz = sprintf (cp, " be_sent:%lld", rep->local_client->sent_seq);
	}
      cp += sz;
    }

  if (flag & 2)
    {
      dlisth *h;

      for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
	{
	  slaveConn *sc = (slaveConn *) h;

	  sz =
	    sprintf (cp, " slave nid:%d sent:%lld acked:%lld", sc->nid,
		     sc->sent_log_seq, sc->slave_log_seq);
	  cp += sz;
	}
    }

  *cp = 0;
  return mgmt_reply_cstring (conn, "+OK%s", buf);
}

static int
setquorum_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;
  int i;
  int commit_quorum = 0;
  short quorum_members[SMR_MAX_SLAVES];
  int num_members = 0;
  dlisth *h;

  if (num_tok < 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  if (parse_quorum_policy (tokens[0], &commit_quorum) != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad quorum policy:%s",
				 tokens[0]);
    }

  for (i = 0; i < SMR_MAX_SLAVES; i++)
    {
      quorum_members[i] = -1;
    }
  if (num_tok > 1)
    {
      if (parse_quorum_members
	  (rep->nid, &tokens[1], quorum_members, &num_members) < 0)
	{
	  return mgmt_reply_cstring (conn, "-ERR bad quorum members");
	}
    }

  rep->commit_quorum = commit_quorum;
  rep->has_quorum_members = (num_members > 0);
  for (i = 0; i < SMR_MAX_SLAVES; i++)
    {
      rep->quorum_members[i] = quorum_members[i];
    }
  // reset quorum membership
  for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
    {
      slaveConn *sc = (slaveConn *) h;
      sc->is_quorum_member = nid_is_quorum_member (rep, sc->nid);
    }
  rep->cron_need_publish = 1;
  return mgmt_reply_cstring (conn, "+OK");
}

static int
getquorum_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;

  if (num_tok != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  return mgmt_reply_cstring (conn, "%d", rep->commit_quorum);
}

static int
getquorumv_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;
  gpbuf_t gp;
  int ret;
  char tmpbuf[1024];

  if (num_tok != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  gpbuf_init (&gp, tmpbuf, sizeof (tmpbuf));

  ret = gpbuf_printf (&gp, "%d", rep->commit_quorum);
  CHECK (ret);

  if (rep->has_quorum_members)
    {
      int i;
      for (i = 0; i < SMR_MAX_SLAVES; i++)
	{
	  if (rep->quorum_members[i] < 0)
	    {
	      break;
	    }
	  ret = gpbuf_printf (&gp, " %d", rep->quorum_members[i]);
	  CHECK (ret);
	}
    }
  gp.cp = '\0';
  ret = mgmt_reply_cstring (conn, "+OK %s", gp.bp);
  gpbuf_cleanup (&gp);
  return ret;

error:
  ret = mgmt_reply_cstring (conn, "-ERR internal %d", ret);
  gpbuf_cleanup (&gp);
  return ret;
}

static int
singleton_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;
  dlisth *h;
  char *singleton = NULL;

  if (num_tok != 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  singleton = strdup (tokens[0]);
  if (singleton == NULL)
    {
      return -1;
    }

  // close other mgmt connections with same singleton value. except me
  for (h = rep->managers.next; h != &rep->managers;)
    {
      mgmtConn *mc = (mgmtConn *) h;
      h = h->next;
      if (mc != conn && mc->singleton != NULL
	  && strcmp (singleton, mc->singleton) == 0)
	{
	  free_mgmt_conn (mc);
	}
    }
  if (conn->singleton)
    {
      free (conn->singleton);
    }
  conn->singleton = singleton;

  return mgmt_reply_cstring (conn, "+OK", rep->commit_quorum);
}

#define CONF_SLAVE_IDLE_TIMEOUT_MSEC "slave_idle_timeout_msec"
static int
confget_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;

  if (num_tok != 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  if (strcasecmp (tokens[0], CONF_SLAVE_IDLE_TIMEOUT_MSEC) == 0)
    {
      return mgmt_reply_cstring (conn, "+OK %d", rep->slave_idle_to);
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR unsupported confset item:%s",
				 tokens[0]);
    }

  return 0;
}

static int
confset_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;
  int ret;

  if (num_tok < 1)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  if (strcasecmp (tokens[0], CONF_SLAVE_IDLE_TIMEOUT_MSEC) == 0)
    {
      int slave_idle_to;

      if (num_tok != 2)
	{
	  goto invalid_arguments;
	}

      ret = parse_int (tokens[1], &slave_idle_to);
      if (ret < 0 || slave_idle_to < 0)
	{
	  goto invalid_value;
	}

      rep->slave_idle_to = slave_idle_to;
      return mgmt_reply_cstring (conn, "+OK");
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR unsupported confset item:%s",
				 tokens[0]);
    }
  return 0;

invalid_arguments:
  return mgmt_reply_cstring (conn,
			     "-ERR invalid number of arguments:%d for item:%s",
			     num_tok - 1, tokens[0]);
invalid_value:
  return mgmt_reply_cstring (conn,
			     "-ERR invalid value:%s for item:%s", tokens[1],
			     tokens[0]);
}


static char *
get_role_string (repRole role)
{
  switch (role)
    {
    case NONE:
      return "NONE";
    case LCONN:
      return "LCONN";
    case MASTER:
      return "MASTER";
    case SLAVE:
      return "SLAVE";
    default:
      return "BADSTATE";
    }
}

static int
info_replicator (smrReplicator * rep, gpbuf_t * gp)
{
  int ret;
  dlisth *h;

  ret = gpbuf_printf (gp, "#Replicator\r\n");
  CHECK (ret);
  ret = gpbuf_printf (gp, "role:%s\r\n", get_role_string (rep->role));
  CHECK (ret);
  ret = gpbuf_printf (gp, "nid:%d\r\n", rep->nid);
  CHECK (ret);
  ret =
    gpbuf_printf (gp,
		  "port:base=%d local(+0 %d) client(+1 %d) slave(+2 %d) mgmt(+3 %d)\r\n",
		  rep->base_port, rep->local_lfd, rep->client_lfd,
		  rep->slave_lfd, rep->mgmt_lfd);
  CHECK (ret);
  ret =
    gpbuf_printf (gp,
		  "log:min=%lld be_ckpt=%lld commit=%lld sync=%lld log=%lld\r\n",
		  aseq_get (&rep->min_seq), rep->be_ckpt_seq, rep->commit_seq,
		  aseq_get (&rep->sync_seq), aseq_get (&rep->log_seq));
  CHECK (ret);

  // quorum related
  if (rep->role == MASTER)
    {
      ret =
	gpbuf_printf (gp, "quorum_policy:quorum=%d has_members=%s",
		      rep->commit_quorum,
		      rep->has_quorum_members ? "y" : "n");
      CHECK (ret);
      if (rep->has_quorum_members)
	{
	  int i;
	  for (i = 0; i < SMR_MAX_SLAVES; i++)
	    {
	      if (rep->quorum_members[i] < 0)
		{
		  break;
		}
	      ret = gpbuf_printf (gp, " %d", rep->quorum_members[i]);
	      CHECK (ret);
	    }
	}
      ret = gpbuf_printf (gp, "\r\n");
      CHECK (ret);
    }

  // local backend
  if (rep->local_client != NULL)
    {
      localConn *lc = rep->local_client;
      ret =
	gpbuf_printf (gp,
		      "local_backend:init_sent=%d fd=%d consumed_seq=%lld sent_seq=%lld\r\n",
		      lc->init_sent, lc->fd, lc->consumed_seq, lc->sent_seq);
      CHECK (ret);
    }

  // master
  if (rep->master != NULL)
    {
      masterConn *mc = rep->master;
      ret =
	gpbuf_printf (gp,
		      "master:host=%s base_port=%d fd=%d rseq=%lld\r\n",
		      rep->master_host ? rep->master_host : "",
		      rep->master_base_port, mc->fd, mc->rseq);
      CHECK (ret);
    }

  // clients 
  if (!dlisth_is_empty (&rep->clients))
    {
      ret = gpbuf_printf (gp, "#client\r\n");
      CHECK (ret);
      for (h = rep->clients.next; h != &rep->clients; h = h->next)
	{
	  clientConn *cc = (clientConn *) h;
	  ret = gpbuf_printf (gp,
			      "client_%d:nid=%d fd=%d num_logged=%lld\r\n",
			      cc->nid, cc->nid, cc->fd, cc->num_logged);
	  CHECK (ret);
	}
    }

  // slaves
  if (!dlisth_is_empty (&rep->slaves))
    {
      ret = gpbuf_printf (gp, "#slave\r\n");
      CHECK (ret);
      for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
	{
	  slaveConn *sc = (slaveConn *) h;
	  ret = gpbuf_printf (gp,
			      "slave_%d:nid=%d fd=%d slave_log_seq=%lld sent_log_seq=%lld idle_msec=%lld\r\n",
			      sc->nid, sc->nid, sc->fd, sc->slave_log_seq,
			      sc->sent_log_seq,
			      cron_count_diff_msec (sc->read_cron_count,
						    rep->cron_count));
	  CHECK (ret);
	}
    }

  // management clients
  if (!dlisth_is_empty (&rep->managers))
    {
      ret = gpbuf_printf (gp, "#management\r\n");
      CHECK (ret);
      for (h = rep->managers.next; h != &rep->managers; h = h->next)
	{
	  mgmtConn *mc = (mgmtConn *) h;
	  ret =
	    gpbuf_printf (gp, "mgmt_%d:fd=%d singleton=[%s]\r\n", mc->fd,
			  mc->fd, mc->singleton ? mc->singleton : "");
	  CHECK (ret);
	}
    }
  return 0;

error:
  return -1;
}

/* slowlog map function that gathers birief slow log info */
static int
info_slowlog_brief (slowLogEntry * e, void *arg)
{
  slowBrief *b = (slowBrief *) arg;
  int i;

  assert (b != NULL);
  for (i = 0; i < b->num_subs; i++)
    {
      if (b->subs[i].sub1 == e->sub1 && b->subs[i].sub2 == e->sub2)
	{
	  b->subs[i].count++;
	  return 1;
	}
    }
  if (b->num_subs >= MAX_SLOW_SUB)
    {
      LOG (LG_WARN, "Maximum number of slow log subject exceeded");
      return -1;
    }

  i = b->num_subs;
  b->subs[i].sub1 = e->sub1;
  b->subs[i].sub2 = e->sub2;
  b->subs[i].count = 1;
  b->num_subs++;
  return 1;
}

static int
info_slowlog (smrReplicator * rep, gpbuf_t * gp)
{
  int i;
  int ret;

  ret = gpbuf_printf (gp, "#Slowlog\r\n");
  CHECK (ret);

  for (i = 0; i < DEF_SLOWLOG_NUM; i++)
    {
      slowLogStat stat;
      slowLog *sl = rep->slow[i];
      slowBrief brief;
      int j;

      ret = slowlog_stat (sl, &stat);
      CHECK (ret);

      ret =
	gpbuf_printf (gp,
		      "slowlog_%d: cap=%d bar=%d tot_count=%lld sum_duration=%lld count=%d",
		      sl->bar, sl->cap, sl->bar, stat.tot_count,
		      stat.sum_duration, stat.count);
      CHECK (ret);

      init_slow_brief (&brief);
      ret = slowlog_map (rep->slow[i], info_slowlog_brief, &brief, 1);
      CHECK (ret);

      for (j = 0; j < brief.num_subs; j++)
	{
	  ret =
	    gpbuf_printf (gp, " %c%c=%d", brief.subs[j].sub1,
			  brief.subs[j].sub2, brief.subs[j].count);
	  CHECK (ret);
	}

      ret = gpbuf_printf (gp, "\r\n");
      CHECK (ret);
    }

  return 0;
error:
  return -1;
}

/* 
 * This function returns result data in redis bulk string format.
 * - A "$" byte followed by the number of bytes composing the string (a prefixed length), terminated by CRLF.
 * - The actual string data.
 * - A final CRLF.
 */
static int
info_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep;
  int is_all = 0;
  int is_default = 0;
  char *section = NULL;
  char tmpbuf[8192];
  gpbuf_t gp;
  int info_len;
  int ret;

  if (num_tok == 0)
    {
      is_default = 1;
      section = "";
    }
  else if (num_tok == 1)
    {
      section = tokens[0];
      is_all = strcasecmp (section, "all") == 0;
      is_default = strcasecmp (section, "default") == 0;
    }
  else
    {
      return mgmt_reply_cstring (conn, "-ERR invalid number of arguments:%d",
				 num_tok);
    }

  rep = conn->rep;
  gpbuf_init (&gp, tmpbuf, sizeof (tmpbuf));

  ret = gpbuf_printf (&gp, "");
  CHECK (ret);

  if (is_all || is_default || !strcasecmp (section, "replicator"))
    {
      ret = info_replicator (rep, &gp);
      CHECK (ret);
    }
  if (is_all || !strcasecmp (section, "slowlog"))
    {
      ret = info_slowlog (rep, &gp);
      CHECK (ret);
    }

  gp.cp = '\0';
  info_len = strlen (gp.bp);
  ret = mgmt_reply_cstring (conn, "$%d\r\n%s", info_len, gp.bp);
  gpbuf_cleanup (&gp);
  return ret;

error:
  ret = mgmt_reply_cstring (conn, "-ERR internal %d", ret);
  gpbuf_cleanup (&gp);
  return ret;
}

static int
slowlog_mapf (slowLogEntry * e, void *arg)
{
  int ret;
  slowLogQuery *q = (slowLogQuery *) arg;
  int match = 0;

  if (q->curr_count >= q->count)
    {
      return 0;
    }

  if (q->is_index)
    {
      match = (q->curr_index >= q->index.index);
    }
  else
    {
      match =
	q->asc ? (q->index.ts < e->start_ms) : (q->index.ts > e->start_ms);
    }

  if (match)
    {
      ret =
	gpbuf_printf (q->gp, "%lld %c%c %lld %d\r\n", e->id, e->sub1, e->sub2,
		      e->start_ms, e->duration);
      CHECK (ret);
      q->curr_count++;
    }

  q->curr_index++;
  return 1;

error:
  return -1;
}

/*
 * slowlog
 * <bar> <index> <count>
 *
 * argument: <index> 
 * case 1)
 * if abs(index) <= 100000 it denotes array index
 *   0     1           n-1
 *   e(1)  e(2)  ....  e(n)
 *   -n    -n+1        -1
 * otherwise)
 * abs(index) denotes timestamp in msec.
 * negative value means ascending order.
 */
static int
slowlog_request (mgmtConn * conn, char **tokens, int num_tok)
{
  smrReplicator *rep = conn->rep;
  slowLogQuery query;
  int i, ret;
  int bar;
  long long index;
  int count;
  int asc;
  int info_len;
  gpbuf_t gp;
  char tmpbuf[8192];



  /* check argument */
  if (num_tok != 3)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR invalid number of arguments:%d",
				 num_tok);
    }

  ret = parse_int (tokens[0], &bar);
  if (ret < 0 || bar < 0)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR invalid value for argument <bar>:%d",
				 bar);
    }
  ret = parse_ll (tokens[1], &index);
  if (ret < 0)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR invalid value for argument:<index>:%lld",
				 index);
    }
  ret = parse_int (tokens[2], &count);
  if (ret < 0 || count < 1)
    {
      return mgmt_reply_cstring (conn,
				 "-ERR invalid value for argument:<count>:%d",
				 count);
    }

  /* init query structure */
  gpbuf_init (&gp, tmpbuf, sizeof (tmpbuf));

  init_slowlog_query (&query);
  asc = (index >= 0);
  query.asc = asc;
  if (asc)
    {
      if (index <= SLOWLOG_QUERY_INDEX_UB)
	{
	  query.index.index = (int) index;
	  query.is_index = 1;
	}
      else
	{
	  query.index.ts = index;
	  query.is_index = 0;
	}
    }
  else
    {
      if (-1 * index < SLOWLOG_QUERY_INDEX_UB)
	{
	  // adjust curr_index semantic to zero base
	  query.index.index = -1 * (index + 1);
	  query.is_index = 1;
	}
      else
	{
	  query.index.ts = -1 * index;
	  query.is_index = 0;
	}
    }
  query.count = count;
  query.gp = &gp;

  ret = gpbuf_printf (&gp, "");
  CHECK (ret);

  for (i = 0; i < DEF_SLOWLOG_NUM; i++)
    {
      slowLog *sl = rep->slow[i];
      if (sl->bar == bar)
	{
	  ret = slowlog_map (sl, slowlog_mapf, &query, asc);
	  CHECK (ret);
	}
    }

  gp.cp = '\0';
  info_len = strlen (gp.bp);
  ret = mgmt_reply_cstring (conn, "$%d\r\n%s", info_len, gp.bp);
  gpbuf_cleanup (&gp);
  return 0;

error:
  ret = mgmt_reply_cstring (conn, "-ERR internal %d", ret);
  gpbuf_cleanup (&gp);
  return ret;
}

static int
smrversion_request (mgmtConn * conn, char **tokens, int num_tok)
{
  if (num_tok != 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad number of token:%d",
				 num_tok);
    }

  return mgmt_reply_cstring (conn, "+OK %d", SMR_VERSION);
}

static int
mgmt_request (mgmtConn * conn, char *bp, char *ep)
{
  char **tokens = NULL;
  int arity;
  int ret = 0;
  smrReplicator *rep;

  assert (conn != NULL);
  assert (bp != NULL);
  assert (ep != NULL);
  assert (ep > bp);

  rep = conn->rep;

  while (*bp && isspace (*bp))
    {
      bp++;
    }
  /* handle ping request (to avoid a bunch of logs) */
  if (strcasecmp (bp, "ping") == 0)
    {
      int pre_log_level = rep->log_level;
      rep->log_level = LG_WARN;
      ret = mgmt_reply_cstring (conn, "+OK %d %lld", rep->role, rep->role_id);
      rep->log_level = pre_log_level;
      goto done;
    }
  else if (strcasecmp (bp, "quit") == 0)
    {
      ret = -1;
      goto done;
    }
  LOG (LG_INFO, "%s", bp);

  /* smrmp_parse_msg modifies input string (use strtok_r) */
  if (smrmp_parse_msg (bp, ep - bp, &tokens) < 0)
    {
      return mgmt_reply_cstring (conn, "-ERR bad request format");
    }

  arity = 0;
  while (tokens[arity] != NULL)
    {
      arity++;
    }

  if (arity == 0)
    {
      ret = mgmt_reply_cstring (conn, "-ERR bad request: no token");
    }

  if (strcasecmp (tokens[0], "rckpt") == 0)
    {
      if (rep->role != LCONN)
	{
	  ret = mgmt_reply_cstring (conn, "-ERR bad state:%d", rep->role);
	}
      else
	{
	  ret = rckpt_request (conn, tokens + 1, arity - 1);
	}
    }
  else if (strcasecmp (tokens[0], "role") == 0)
    {
      ret = role_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "migrate") == 0)
    {
      ret = migrate_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "getseq") == 0)
    {
      ret = getseq_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "setquorum") == 0)
    {
      if (rep->role == MASTER)
	{
	  ret = setquorum_request (conn, tokens + 1, arity - 1);
	}
      else
	{
	  ret = mgmt_reply_cstring (conn, "-ERR bad state:%d", rep->role);
	}
    }
  else if (strcasecmp (tokens[0], "getquorum") == 0)
    {
      if (rep->role == MASTER)
	{
	  ret = getquorum_request (conn, tokens + 1, arity - 1);
	}
      else
	{
	  ret = mgmt_reply_cstring (conn, "-ERR bad state:%d", rep->role);
	}
    }
  else if (strcasecmp (tokens[0], "getquorumv") == 0)
    {
      if (rep->role == MASTER)
	{
	  ret = getquorumv_request (conn, tokens + 1, arity - 1);
	}
      else
	{
	  ret = mgmt_reply_cstring (conn, "-ERR bad state:%d", rep->role);
	}
    }
  else if (strcasecmp (tokens[0], "singleton") == 0)
    {
      ret = singleton_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "confget") == 0)
    {
      ret = confget_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "confset") == 0)
    {
      ret = confset_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "info") == 0)
    {
      ret = info_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "slowlog") == 0)
    {
      ret = slowlog_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "smrversion") == 0)
    {
      ret = smrversion_request (conn, tokens + 1, arity - 1);
    }
  else if (strcasecmp (tokens[0], "fi") == 0)
    {
      ret = fi_request (conn, tokens + 1, arity - 1);
    }
  else
    {
      ret =
	mgmt_reply_cstring (conn, "-ERR bad request: unsupported cmd %s",
			    tokens[0]);
    }

done:
  smrmp_free_msg (tokens);
  return ret;
}

static void
sig_handler (int sig)
{
  //Note: do not LOG as it is not async. signal safe
  s_Server.interrupted++;
}

static void
setup_sigterm_handler (void)
{
  struct sigaction act;
  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sig_handler;
  sigaction (SIGTERM, &act, NULL);
}

static void
setup_sigint_handler (void)
{
  struct sigaction act;
  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sig_handler;
  sigaction (SIGINT, &act, NULL);
}

static void
free_mig_conn (migStruct * m, int interrupt)
{
  smrReplicator *rep;
  assert (m != NULL);

  rep = m->rep;
  if (m->fd > 0 && m->el != NULL)
    {
      aeDeleteFileEvent (m->el, m->fd, AE_READABLE);
      aeDeleteFileEvent (m->el, m->fd, AE_WRITABLE);
      close (m->fd);
      m->fd = -1;
    }

  if (interrupt)
    {
      rep->mig_interrupted = 1;
    }
}

static void
free_mig_struct (migStruct * m)
{
  smrReplicator *rep;
  assert (m != NULL);

  rep = m->rep;

  free_mig_conn (m, 0);

  if (m->teid != -1 && m->el != NULL)
    {
      aeDeleteTimeEvent (m->el, m->teid);
    }

  if (m->el != NULL)
    {
      aeDeleteEventLoop (m->el);
    }

  if (m->dest_host != NULL)
    {
      free (m->dest_host);
      m->dest_host = NULL;
    }

  if (m->filter != NULL)
    {
      destroy_part_filter (m->filter);
      m->filter = NULL;
    }

  if (m->ous != NULL)
    {
      io_stream_close (m->ous);
      m->ous = NULL;
    }

  if (m->log != NULL)
    {
      logmmap_entry_release (rep, m->log);
      m->log = NULL;
    }

  free (m);
  rep->mig = NULL;
  rep->mig_id = 0;
  rep->mig_interrupted = 0;
  return;
}

static int
launch_mig_thread (smrReplicator * rep, char *dest_host,
		   int dest_base_port, long long from_seq,
		   int num_part, char **tokens, int num_tok, int tps_limit)
{
  struct timeval tv;
  migStruct *m = NULL;

  assert (dest_host != NULL);
  assert (dest_base_port > 0);
  assert (tokens != NULL);
  assert (num_tok > 0);
  assert (from_seq >= 0);
  assert (tps_limit > 0);

  /* sanity check */
  if (rep->mig != NULL)
    {
      MIG_ERR (rep, "Another migration is running.");
      return -1;
    }
  else if (rep->role != MASTER)
    {
      MIG_ERR (rep, "Only master can launch migration thread");
      return -1;
    }

  gettimeofday (&tv, NULL);

  /* set migration argument structure */
  m = malloc (sizeof (migStruct));
  if (m == NULL)
    {
      MIG_ERR (rep, "malloc mig connection failed");
      return -1;
    }
  init_mig_conn (m, rep);
  rep->mig_id = tv.tv_sec * 1000000 + tv.tv_usec;
  rep->mig_ret = 0;
  rep->mig_ebuf[0] = '\0';

  if ((m->dest_host = strdup (dest_host)) == NULL)
    {
      goto error;
    }
  m->dest_base_port = dest_base_port;
  m->from_seq = from_seq;
  aseq_set (&rep->mig_seq, from_seq);
  aseq_set (&m->buffered, 0LL);
  aseq_set (&m->sent, 0LL);
  aseq_set (&m->acked, 0LL);
  m->filter = create_part_filter (num_part);
  if (m->filter == NULL)
    {
      MIG_ERR (rep, "failed to create filter:%d", num_part);
      goto error;
    }
  else if (part_filter_set_rle_tokens (m->filter, tokens, num_tok) != 0)
    {
      MIG_ERR (rep, "Failed to parse filter");
      goto error;
    }
  m->num_part = num_part;
  m->tps_limit = tps_limit;

  m->ibp = &m->ibuf[0];
  m->ous = io_stream_create (8192);
  if (m->ous == NULL)
    {
      MIG_ERR (rep, "failed to allocate migration output stream");
      goto error;
    }
  rep->mig = m;

  /* launch thread */
  if ((rep->mig_thr = malloc (sizeof (pthread_t))) == NULL)
    {
      MIG_ERR (rep, "Failed to allocate memory: %d",
	       (int) sizeof (pthread_t));
      goto error;
    }

  if (pthread_create (rep->mig_thr, NULL, mig_thread, rep) != 0)
    {
      MIG_ERR (rep, "Failed to create bio thread");
      goto error;
    }

  LOG (LG_INFO, "migration thread lauched: %s:%d from:%lld tps_limit:%d ",
       dest_host, dest_base_port, from_seq, tps_limit);
  return 0;

error:
  if (m != NULL)
    {
      free_mig_struct (m);
    }
  return -1;
}

static void
mig_read_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  SLOWLOG_DECLARE ('M', 'r');

  applog_enter_session (rep, MIG_CONN_IN, fd);
  SLOWLOG_ENTER ();
  mig_read (el, fd, data, mask);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

static void
mig_read (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  char *bp, *cp, *ep;
  int avail;
  migStruct *mig;

  mig = rep->mig;
  assert (mig != NULL);

  bp = &mig->ibuf[0];
  cp = mig->ibp;
  ep = bp + sizeof (mig->ibuf);

  /* read data into buffer */
  avail = ep - cp;
  while (avail > 0)
    {
      int nr;

      nr = read (mig->fd, cp, avail);
      SFI_PROBE2 ("read", MIG_CONN_IN, (long) &nr);
      if (nr < 0)
	{
	  if (nr == -1 && errno == EAGAIN)
	    {
	      break;
	    }
	  else
	    {
	      MIG_ERR (rep, "failed to read");
	      free_mig_conn (mig, 1);
	      return;
	    }
	}
      else if (nr == 0)
	{
	  MIG_ERR (rep, "connectin closed");
	  free_mig_conn (mig, 1);
	  return;
	}
      cp += nr;
      avail -= nr;
    }

  /* 
   * process SMR_OP_LOG_ACK messages 
   * we can skip preceding message (latest message always wins)
   * [bp, cp) contains message
   */
  avail = cp - bp;
  if (avail >= 2 * SMR_OP_LOG_ACK_SZ)
    {
      bp += (avail / SMR_OP_LOG_ACK_SZ - 1) * SMR_OP_LOG_ACK_SZ;
      avail = cp - bp;
    }

  assert (avail < 2 * SMR_OP_LOG_ACK_SZ);
  if (avail >= SMR_OP_LOG_ACK_SZ)
    {
      long long ack;
      if (*bp != SMR_OP_LOG_ACK)
	{
	  MIG_ERR (rep, "invalid smr operation:%d", *bp);
	  free_mig_conn (mig, 1);
	  return;
	}
      memcpy (&ack, bp + 1, sizeof (long long));
      ack = ntohll (ack);
      if (ack <= aseq_get (&mig->acked))
	{
	  MIG_ERR (rep, "acked seq: %lld is l.e. than prev acked:%lld",
		   aseq_get (&mig->acked), ack);
	  free_mig_conn (mig, 1);
	  return;
	}
      aseq_set (&mig->acked, ack);
      bp += SMR_OP_LOG_ACK_SZ;
    }

  /* readjust buffer pointers */
  if (bp != &mig->ibuf[0])
    {
      memmove (&mig->ibuf[0], bp, cp - bp);
      cp = &mig->ibuf[0] + (cp - bp);
      bp = &mig->ibuf[0];
    }
  mig->ibp = cp;
  return;
}

static int
mig_prepare_write (migStruct * mig)
{
  smrReplicator *rep = mig->rep;

  if (mig->w_prepared == 0)
    {
      if (aeCreateFileEvent
	  (mig->el, mig->fd, AE_WRITABLE, mig_write_handler, rep) == AE_OK)
	{
	  mig->w_prepared = 1;
	  return 0;
	}
      else
	{
	  MIG_ERR (rep, "create file event (mig_write) failed");
	  return -1;
	}
    }
  return 0;
}

static int
mig_log_get_buf (migStruct * mig, long long seq, long long limit,
		 char **buf, int *buf_sz)
{
  long long seq_;
  int off;

  assert (seq >= 0);
  assert (seq <= limit);

  seq_ = seq_round_down (seq);
  if (seq_ != mig->fseq)
    {
      if (mig->log != NULL)
	{
	  logmmap_entry_release (mig->rep, mig->log);
	  mig->log = NULL;
	}
      mig->fseq = -1LL;
    }

  if (mig->log == NULL)
    {
      mig->log = logmmap_entry_get (mig->rep, seq_, GET_EXIST, 0);
      if (mig->log == NULL)
	{
	  return -1;
	}
      mig->fseq = seq_;
    }

  off = (int) (seq - mig->fseq);
  *buf = (char *) mig->log->addr->addr + off;
  *buf_sz =
    (limit - seq) >
    (SMR_LOG_FILE_DATA_SIZE - off) ? (SMR_LOG_FILE_DATA_SIZE -
				      off) : (int) (limit - seq);
  return 0;
}

static int
mig_process_buf (migStruct * mig, char *buf, int buf_sz, int max_msg,
		 int *bytes_used, int *msg_sent, int *n_buffered)
{
  smrReplicator *rep = mig->rep;
  char *bp;
  int avail;
  int tot_used = 0;
  int tot_sent = 0;
  int tot_buffered = 0;

  bp = buf;
  avail = buf_sz;

  while (avail > 0 && tot_sent < max_msg)
    {
      int used;
      char cmd;

      if (mig->n_skip > 0)
	{
	  assert (mig->tbuf_pos == 0);
	  used = mig->n_skip < avail ? mig->n_skip : avail;
	  if (mig->is_msg)
	    {
	      if (stream_write (mig->ous, bp, used) < 0)
		{
		  return -1;
		}
	      tot_buffered += used;
	    }
	  mig->n_skip -= used;
	  tot_used += used;
	  avail -= used;
	  bp += used;
	  continue;
	}
      else if (mig->n_need > 0)
	{
	  assert (mig->n_skip == 0);
	  used = mig->n_need < avail ? mig->n_need : avail;
	  memcpy (&mig->tbuf[mig->tbuf_pos], bp, used);
	  mig->tbuf_pos += used;
	  mig->n_need -= used;
	  tot_used += used;
	  avail -= used;
	  bp += used;
	  if (mig->n_need > 0)
	    {
	      continue;
	    }
	}

      if (mig->is_msg)
	{
	  tot_sent++;
	  mig->is_msg = 0;
	}
      cmd = mig->tbuf_pos > 0 ? mig->tbuf[0] : *bp;
      switch (cmd)
	{
	case SMR_OP_EXT:
	case SMR_OP_SESSION_CLOSE:
	  mig->n_skip = 1 + sizeof (int);
	  break;
	case SMR_OP_NODE_CHANGE:
	  mig->n_skip = 1 + sizeof (short);
	  break;
	case SMR_OP_SEQ_COMMITTED:
	  mig->n_skip = SMR_OP_SEQ_COMMITTED_SZ;
	  break;
	case SMR_OP_SESSION_DATA:	//sid, hash, timestamp, length, data
	  if (mig->tbuf_pos > 0)
	    {
	      int hash;
	      int length;
	      int ret;

	      memcpy (&hash, &mig->tbuf[1 + sizeof (int)], sizeof (int));
	      hash = ntohl (hash);
	      memcpy (&length,
		      &mig->tbuf[1 + 2 * sizeof (int) + sizeof (long long)],
		      sizeof (int));
	      length = ntohl (length);
	      mig->n_skip = length;

	      if (hash == SMR_SESSION_DATA_HASH_ALL)
		{
		  ret = 1;
		}
	      else if (hash == SMR_SESSION_DATA_HASH_NONE)
		{
		  ret = 0;
		}
	      else
		{
		  assert (hash >= 0);
		  ret = part_filter_get (mig->filter, hash % mig->num_part);
		}

	      if (ret < 0)
		{
		  return -1;
		}
	      else if (ret == 1)
		{
		  /* send header */
		  if (stream_write
		      (mig->ous, mig->tbuf,
		       1 + 3 * sizeof (int) + sizeof (long long)) < 0)
		    {
		      return -1;
		    }
		  tot_buffered += (1 + 3 * sizeof (int) + sizeof (long long));
		  mig->is_msg = 1;
		}
	      mig->tbuf_pos = 0;
	      mig->tbuf[0] = '\0';
	    }
	  else
	    {
	      mig->n_need = 1 + 3 * sizeof (int) + sizeof (long long);
	    }
	  break;
	default:
	  MIG_ERR (rep,
		   "Invalid master input processing state cmd: %d('%c')",
		   cmd, cmd);
	  return -1;
	}
    }
  *bytes_used = tot_used;
  *msg_sent = tot_sent;
  *n_buffered = tot_buffered;
  return 0;
}

static int
mig_process_log (migStruct * mig, int max_msg)
{
  long long log_seq;
  long long mig_seq;
  long long avail;		// helper macro variable

  assert (mig != NULL);

  mig_seq = aseq_get (&mig->rep->mig_seq);
  log_seq = aseq_get (&mig->rep->log_seq);
  assert (mig_seq <= log_seq);

  avail = log_seq - mig_seq;
  while (avail > 0)
    {
      char *buf = NULL;
      int buf_sz = 0;
      int bytes_used = 0;
      int msg_sent = 0;
      int n_buffered = 0;

      if (mig_log_get_buf (mig, mig_seq, log_seq, &buf, &buf_sz) < 0)
	{
	  return -1;
	}

      if (mig_process_buf
	  (mig, buf, buf_sz, max_msg, &bytes_used, &msg_sent,
	   &n_buffered) < 0)
	{
	  return -1;
	}
      aseq_add (&mig->buffered, n_buffered);
      mig_seq += bytes_used;
      avail -= bytes_used;
      max_msg -= msg_sent;

      if (bytes_used == 0 || msg_sent == 0 || max_msg == 0)
	{
	  break;
	}
    }
  aseq_set (&mig->rep->mig_seq, mig_seq);
  return 0;
}

static void
mig_write_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  SLOWLOG_DECLARE ('M', 'w');

  applog_enter_session (rep, MIG_CONN_OUT, fd);
  SLOWLOG_ENTER ();
  mig_write (el, fd, data, mask);
  SLOWLOG_LEAVE (rep);
  applog_leave_session (rep);
}

/* migration write. It regulates write tps */
static void
mig_write (aeEventLoop * el, int fd, void *data, int mask)
{
  smrReplicator *rep = (smrReplicator *) data;
  migStruct *mig;
  int has_more = 1;
  int n_sent = 0;
  struct timeval prev_time, curr_time;
  double alloc_msec;
  int max_send;
  int n_pending;
  int need_prepare;

  mig = rep->mig;
  assert (mig != NULL);

  /* send out stream */
  if (fd_write_stream (MIG_CONN_OUT, fd, mig->ous, &has_more, &n_sent) < 0)
    {
      MIG_ERR (rep, "failed fd_write_stream");
      free_mig_conn (mig, 1);
      return;
    }

  aseq_add (&mig->sent, n_sent);
  if (has_more == 0)
    {
      need_prepare = 0;
    }
  else
    {
      need_prepare = 1;
      /* If there is output to be sent, just return */
      n_pending = io_stream_avail_for_read (mig->ous);
      if (n_pending > 1024 * 1024)
	{
	  goto done;
	}
    }

  /* update time and get tps allocation w.r.t. the time difference */
  prev_time = mig->curr_time;
  gettimeofday (&curr_time, NULL);

  /* get target tps */
  alloc_msec = (double) tv_diff (&curr_time, &prev_time);
  if (alloc_msec > 1000.0 || alloc_msec < 0.0)
    {
      /* mid value */
      alloc_msec = 500;
    }
  max_send = (int) (mig->tps_limit * (alloc_msec / 1000.0));

  if (mig->tps_limit > 0 && max_send == 0)
    {
      /* tps is lost due to mig_cron Hz */
      mig->curr_time = prev_time;
    }
  else
    {
      if (mig_process_log (mig, max_send) < 0)
	{
	  free_mig_conn (mig, 1);
	  return;
	}
      mig->curr_time = curr_time;
    }
  /* 
   * two cases are possible. In both case write event is pended until mig_cron enables.
   * 1. there is no log to send
   * 2. already used up my tps allocation
   */
  need_prepare = 0;

done:
  if (need_prepare && mig->w_prepared == 0)
    {
      if (mig_prepare_write (mig) < 0)
	{
	  free_mig_conn (mig, 1);
	  return;
	}
    }
  else if (need_prepare == 0 && mig->w_prepared)
    {
      aeDeleteFileEvent (mig->el, mig->fd, AE_WRITABLE);
      mig->w_prepared = 0;
    }
  return;
}

static int
mig_cron (struct aeEventLoop *eventLoop, long long id, void *clientData)
{
  smrReplicator *rep = (smrReplicator *) clientData;
  migStruct *mig;

  mig = rep->mig;
  assert (mig != NULL);

  if (rep->interrupted || rep->mig_interrupted)
    {
      aeStop (mig->el);
      return MIG_CRON_INTERVAL;
    }

  if (mig->w_prepared == 0)
    {
      if (mig_prepare_write (mig) < 0)
	{
	  rep->mig_interrupted = 1;
	}
    }

  return MIG_CRON_INTERVAL;
}

static int
mig_dest_connect (smrReplicator * rep)
{
  short net_nid;
  long long net_seq = 0LL;
  int fd = -1;
  migStruct *mig;
  long long deadline;

  assert (rep != NULL);
  mig = rep->mig;

  fd =
    tcp_connect (mig->dest_host, mig->dest_base_port + PORT_OFF_CLIENT,
		 TCP_OPT_KEEPALIVE, mig->rep->mig_ebuf,
		 sizeof (mig->rep->mig_ebuf));
  if (fd < 0)
    {
      return -1;
    }

  if (tcp_set_option (fd, TCP_OPT_NODELAY | TCP_OPT_NONBLOCK) < 0)
    {
      MIG_ERR (rep, "tcp_sert_option failed");
      goto error;
    }

  /* HANDSHAKE */
  deadline = currtime_ms () + DEFAULT_HANDSHAKE_TIMEOUT_MSEC;
  net_nid = htons (rep->nid);
  if (tcp_write_fully_deadline (fd, &net_nid, sizeof (short), deadline) < 0)
    {
      MIG_ERR (rep, "handshake error: failed to write nid");
      goto error;
    }

  if (tcp_read_fully_deadline (fd, &net_seq, sizeof (long long), deadline) <
      0)
    {
      MIG_ERR (rep,
	       "handshake error: failed to read commit sequence from dest");
      goto error;
    }
  // unused master_cseq

  return fd;
error:
  if (fd > 0)
    {
      close (fd);
    }
  return -1;
}

/*
 * migration thread
 */
static void *
mig_thread (void *arg)
{
  smrReplicator *rep = (smrReplicator *) arg;
  migStruct *mig = rep->mig;
  sigset_t sigset;

  /* disable other signal for the main thread get the signal */
  sigemptyset (&sigset);
  sigaddset (&sigset, SIGALRM);
  if (pthread_sigmask (SIG_BLOCK, &sigset, NULL) != 0)
    {
      MIG_ERR (rep, "mig thread: pthread_sigmask failed");
      goto finalization;
    }

  /* create event loop of the migration thread */
  mig->el = aeCreateEventLoop (1024 + 1024);

  /* connect to the master and setup read/write handlers */
  if ((mig->fd = mig_dest_connect (rep)) < 0)
    {
      goto finalization;
    }

  if (aeCreateFileEvent
      (mig->el, mig->fd, AE_READABLE, mig_read_handler, rep) == AE_ERR)
    {
      MIG_ERR (rep, "failed to setup mig_read handler");
      goto finalization;
    }

  if (aeCreateFileEvent
      (mig->el, mig->fd, AE_WRITABLE, mig_write_handler, rep) == AE_ERR)
    {
      MIG_ERR (rep, "failed to setup mig_write handler");
      goto finalization;
    }
  mig->w_prepared = 1;

  /* setup server cron */
  mig->teid = aeCreateTimeEvent (mig->el, 1, mig_cron, rep, NULL);
  if (mig->teid == -1LL)
    {
      MIG_ERR (rep, "failed to setup timer event");
      goto finalization;
    }

  /* enter event loop */
  aeMain (mig->el);

finalization:
  if (mig)
    {
      free_mig_struct (mig);
    }
  return NULL;
}

static int
launch_bio_thread (smrReplicator * rep)
{

  if (rep->bio_thr != NULL)
    {
      LOG (LG_ERROR, "State error: bio thread alreay started!");
      return -1;
    }
  if ((rep->bio_thr = malloc (sizeof (pthread_t))) == NULL)
    {
      LOG (LG_ERROR, "Failed to allocate memory: %d", sizeof (pthread_t));
      return -1;
    }
  if (pthread_create (rep->bio_thr, NULL, bio_thread, rep) != 0)
    {
      LOG (LG_ERROR, "Failed to create bio thread");
      return -1;
    }
  return 0;
}


static void
cron_update_est_min_seq (smrReplicator * rep)
{
  long long seq;
  dlisth *h;

  seq = aseq_get (&rep->log_seq);

  if (rep->local_client != NULL)
    {
      if (rep->local_client->consumed_seq < seq)
	{
	  seq = rep->local_client->consumed_seq;
	}
    }

  if (rep->mig != NULL && rep->mig_thr != NULL)
    {
      long long mig_seq = aseq_get (&rep->mig_seq);
      if (mig_seq < seq)
	{
	  seq = mig_seq;
	}
    }

  for (h = rep->slaves.next; h != &rep->slaves; h = h->next)
    {
      slaveConn *sc = (slaveConn *) h;

      if (sc->sent_log_seq < seq)
	{
	  seq = sc->sent_log_seq;
	}
    }

  if (seq > 0)
    {
      seq = seq_round_down (seq);
      if (seq >= seq_round_down (aseq_get (&rep->min_seq)))
	{
	  aseq_set (&rep->cron_est_min_seq, seq);
	}
    }
}

static void
cron_update_stat (smrReplicator * rep, repStat * prev_stat,
		  time_t * prev_time)
{
  if (*prev_time == 0)
    {
      memset (prev_stat, 0, sizeof (repStat));
    }

  gettimeofday (&rep->stat_time, NULL);
  if (rep->stat_time.tv_sec > *prev_time + 4)
    {
      repStat *c = &rep->stat;
      repStat *p = prev_stat;

      LOG (LG_DEBUG,
	   "EVENT COUNT: %lld C:%lld %lld S:%lld %lld L:%lld %lld",
	   c->nev - p->nev,
	   c->client_nre - p->client_nre,
	   c->client_nwe - p->client_nwe,
	   c->slave_nre - p->slave_nre,
	   c->slave_nwe - p->slave_nwe,
	   c->local_nre - p->local_nre, c->local_nwe - p->local_nwe);

      *prev_time = rep->stat_time.tv_sec;
      *prev_stat = rep->stat;
    }
}

#define run_with_period(_ms_) if ((_ms_ <= MAIN_CRON_INTERVAL) || (rep->cron_count%((_ms_)/MAIN_CRON_INTERVAL)) == 0)
static int
server_cron (struct aeEventLoop *eventLoop, long long id, void *clientData)
{
  static time_t prev_time = 0;
  static repStat prev_stat;
  smrReplicator *rep = (smrReplicator *) clientData;
  int ret;

  assert (rep != NULL);
  rep->cron_count++;

  /* 
   * This ensures the progress without additional client request.
   *
   * - Master election occurred.
   * - Backend of the master or slave may have pending messages that has not committed
   *   + sent but not committed due to failure
   *   + it retransmits uncommitted messages when it knows reconfiguration is completed, 
   *   + it knows the completion by comparing current sequence number with the sequence number 
   *     got at the time of master hand-shake 
   * - There can be no progress if there is no additional client message
   * - Master make dummy message to make sure previous messages are eventually committed 
   */
  if (rep->cron_need_progress)
    {
      assert (rep->role == MASTER);
      if (aseq_get (&rep->log_seq) <= rep->progress_seq)
	{
	  ret = do_virtual_progress (rep);
	  if (ret < 0)
	    {
	      LOG (LG_ERROR,
		   "do_virtual_progress failed at server cron (stop sevice)");
	      aeStop (rep->el);
	      goto done;
	    }
	}
      rep->cron_need_progress = 0;
      rep->progress_seq = 0LL;
    }

  /* 
   * Scenario.
   * 1) master with quorum policy 1
   * 2) slave down
   *    --> client requests are not committed
   * 3) setquroum 0 by management
   * ==> publishing commit sequence is stopped until new client request is made
   *
   */
  if (rep->cron_need_publish)
    {
      if (rep->role == MASTER
	  && publish_commit_or_safe_seq (rep, aseq_get (&rep->log_seq)) < 0)
	{
	  LOG (LG_ERROR,
	       "publish_commit_or_safe_seq failed at server cron (stop sevice)");
	  aeStop (rep->el);
	  goto done;
	}
      rep->cron_need_publish = 0;
    }

  run_with_period (100)
  {
    cron_update_est_min_seq (rep);
    if (rep->log_level >= LG_DEBUG)
      {
	cron_update_stat (rep, &prev_stat, &prev_time);
      }
  }

  run_with_period (100)
  {
    // Send SEQ_RECEIVED message to the master if there is no interaction for 1000 msec.
    // This redundant message sending is needed to detect dead master quickly
    if (rep->role == SLAVE)
      {
	masterConn *mc = rep->master;

	if (mc != NULL
	    && ((rep->cron_count - mc->read_cron_count) * MAIN_CRON_INTERVAL >
		1000))
	  {
	    char cmd[1 + sizeof (long long)];
	    long long net_seq;

	    cmd[0] = SMR_OP_SEQ_RECEIVED;
	    net_seq = htonll (mc->rseq);
	    memcpy (&cmd[1], &net_seq, sizeof (long long));

	    if (master_prepare_write (mc) < 0
		|| stream_write (mc->ous, cmd, 1 + sizeof (long long)) < 0)
	      {
		LOG (LG_ERROR,
		     "sending %s failed at server cron (stop sevice)",
		     get_command_string (SMR_OP_SEQ_RECEIVED));
		aeStop (rep->el);
	      }
	    else
	      {
		mc->read_cron_count = rep->cron_count;
		LOG (LG_DEBUG, "%s sent to master: %lld",
		     get_command_string (SMR_OP_SEQ_RECEIVED),
		     ntohll (net_seq));
	      }
	  }
      }
    else if (rep->role == MASTER && rep->slave_idle_to > 0)
      {
	dlisth *h;

	for (h = rep->slaves.next; h != &rep->slaves;)
	  {
	    slaveConn *sc = (slaveConn *) h;
	    h = h->next;

	    if ((rep->cron_count -
		 sc->read_cron_count) * MAIN_CRON_INTERVAL >
		rep->slave_idle_to)
	      {
		LOG (LG_ERROR,
		     "slave idle timeout (close slave connection): nid:%d",
		     sc->nid);
		free_slave_conn (sc);
	      }
	  }
      }
  }

  if (rep->interrupted)
    {
      LOG (LG_WARN, "server interrupted...");
      aeStop (rep->el);
    }

done:
  return MAIN_CRON_INTERVAL;
}

static void
before_sleep_handler (struct aeEventLoop *eventLoop)
{
  s_Server.stat.nev++;
  SFI_PROBE1 ("sleep", s_Server.stat.nev);
}

static void
print_usage (char *prog)
{
  printf (_usage, prog);
}

static int
initialize_services (smrReplicator * rep)
{
  /* local */
  rep->local_lfd = tcp_server (NULL, rep->local_port, rep->ebuf, EBUFSZ);
  if (rep->local_lfd < 0)
    {
      LOG (LG_ERROR, "Failed to open local listen port: %s", rep->ebuf);
      return -1;
    }

  if (aeCreateFileEvent
      (rep->el, rep->local_lfd, AE_READABLE, local_accept_handler,
       rep) == AE_ERR)
    {
      LOG (LG_ERROR, "Failed to create local accept handler");
      return -1;
    }

  /* client */
  rep->client_lfd = tcp_server (NULL, rep->client_port, rep->ebuf, EBUFSZ);
  if (rep->client_lfd < 0)
    {
      LOG (LG_ERROR, "Failed to open client listen port: %s", rep->ebuf);
      return -1;
    }
  if (aeCreateFileEvent
      (rep->el, rep->client_lfd, AE_READABLE, client_accept_handler,
       rep) == AE_ERR)
    {
      LOG (LG_ERROR, "Failed to create client accept handler");
      return -1;
    }

  /* slave */
  rep->slave_lfd = tcp_server (NULL, rep->slave_port, rep->ebuf, EBUFSZ);
  if (rep->slave_lfd < 0)
    {
      LOG (LG_ERROR, "Failed to open slave listen port:%s", rep->ebuf);
      return -1;
    }

  if (aeCreateFileEvent
      (rep->el, rep->slave_lfd, AE_READABLE, slave_accept_handler,
       rep) == AE_ERR)
    {
      LOG (LG_ERROR, "Failed to create slave accept handler");
      return -1;
    }

  /* management */
  rep->mgmt_lfd = tcp_server (NULL, rep->mgmt_port, rep->ebuf, EBUFSZ);
  if (rep->slave_lfd < 0)
    {
      LOG (LG_ERROR, "Failed to open management listen port:%s", rep->ebuf);
      return -1;
    }

  if (aeCreateFileEvent
      (rep->el, rep->mgmt_lfd, AE_READABLE, mgmt_accept_handler,
       rep) == AE_ERR)
    {
      LOG (LG_ERROR, "Failed to create management accept handler");
      return -1;
    }
  return 0;

}

static int
initialize_slowlog (smrReplicator * rep)
{
  int i;
  int cap = 1000;
  int bar;
  slowLog *esc;


  bar = 0;
  for (i = 0; i < DEF_SLOWLOG_NUM; i++)
    {
      if (bar == 0)
	{
	  bar = 1;
	}
      else
	{
	  bar = bar * 10;
	}
    }

  esc = NULL;
  for (i = DEF_SLOWLOG_NUM - 1; i >= 0; i--, bar = bar / 10)
    {
      rep->slow[i] = new_slowlog (cap, bar, esc);
      if (rep->slow[i] == NULL)
	{
	  return -1;
	}
      esc = rep->slow[i];
    }

  return 0;
}

/* ---- */
/* MAIN */
/* ---- */
int
main (int argc, char *argv[])
{
  char log_dir_buf[PATH_MAX];
  int ret;
  repRole role = NONE;
  int base_port = 1900;
  char *log_file_dir = NULL;
  int log_level = LG_INFO;
  char *app_log_prefix = NULL;
  int run_as_daemon = 0;
  int log_delete_gap = 0;
  int slave_idle_to = DEF_SLAVE_IDLE_TO;
  long long min_seq = 0LL;
  long long max_seq = 0LL;
  long long msg_min_seq = 0LL;
  long long msg_max_seq = 0LL;
  long long max_commit_seq = 0LL;
  smrReplicator *rep;
  smrLog *smrlog = NULL;

  /* fault injection initialization */
  sfi_init ();

  /* parse argument */
  while ((ret = getopt (argc, argv, "d:b:v:l:x:D")) >= 0)
    {
      switch (ret)
	{
	case 'd':
	  log_file_dir = realpath (optarg, log_dir_buf);
	  break;
	case 'b':
	  base_port = atoi (optarg);
	  if (base_port <= 0)
	    {
	      goto arg_error;
	    }
	  break;
	case 'v':
	  log_level = atoi (optarg);
	  if (log_level < LG_ERROR || log_level > LG_MAX_LEVEL)
	    {
	      goto arg_error;
	    }
	  break;
	case 'l':
	  app_log_prefix = optarg;
	  break;
	case 'x':
	  log_delete_gap = atoi (optarg);
	  if (log_delete_gap < 0)
	    {
	      goto arg_error;
	    }
	  break;
	case 'k':
	  slave_idle_to = atoi (optarg);
	  if (slave_idle_to < 0)
	    {
	      goto arg_error;
	    }
	  break;
	case 'D':
	  run_as_daemon = 1;
	  break;
	default:
	  goto arg_error;
	}
    }

  if (log_file_dir == NULL || (smrlog = smrlog_init (log_file_dir)) == NULL)
    {
      printf ("Failed to initialize on log dir: %s errno=%d\n",
	      log_file_dir == NULL ? "null" : log_file_dir, errno);
      exit (1);
    }
  else if (base_port < 0)
    {
      printf ("Bad base port:%d\n", base_port);
      goto arg_error;
    }

  printf ("%s", _logo);
  printf ("log directory :%s\n", log_file_dir);
  printf ("base port     :%d\n", base_port);
  printf ("daemonize     :%s\n", run_as_daemon ? "true" : "false");
  if (app_log_prefix != NULL)
    {
      printf ("app log prefix: %s\n", app_log_prefix);
    }

  /* initialize server structure */
  memset (&s_Server, 0, sizeof (smrReplicator));
  rep = &s_Server;
  init_replicator (rep, role, smrlog, base_port, log_level, slave_idle_to);
  if (log_delete_gap > 0)
    {
      rep->log_delete_gap = log_delete_gap;
    }
  if (app_log_prefix != NULL)
    {
      if (strlen (app_log_prefix) * 2 > sizeof (rep->app_log_buf))
	{
	  printf ("app log prefix is too long");
	  exit (1);
	}
      rep->app_log_prefix_size = strlen (app_log_prefix);
      strcpy (rep->app_log_buf, app_log_prefix);
    }
  else
    {
      rep->app_log_fp = stdout;
    }

  if (run_as_daemon)
    {
      int fd;

      if (fork () != 0)
	{
	  exit (0);		/* parent exits */
	}

      if ((fd = open ("/dev/null", O_RDWR, 0)) != -1)
	{
	  dup2 (fd, STDIN_FILENO);
	  dup2 (fd, STDOUT_FILENO);
	  dup2 (fd, STDERR_FILENO);
	  if (fd > STDERR_FILENO)
	    {
	      close (fd);
	    }
	}
    }

  ret = applog_init (rep);
  if (ret < 0)
    {
      printf ("Failed to iniitlize log\n");
      exit (1);
    }

  /* from now you can use LOG macro */
  rep->log_dir = strdup (log_file_dir);
  if (rep->log_dir == NULL)
    {
      LOG (LG_ERROR, "Memory failure");
      exit (1);
    }

  /* local restart recovery (logical) and set log related fields */
  ret =
    smrlog_recover (rep->smrlog, &min_seq, &max_seq, &msg_min_seq,
		    &msg_max_seq, &max_commit_seq);
  if (ret != 0)
    {
      LOG (LG_ERROR,
	   "Restart recovery failed. purge and retry... ret:%d errno:%d", ret,
	   errno);

      ret = smrlog_purge_all (rep->smrlog);
      if (ret != 0)
	{
	  LOG (LG_ERROR, "Even purge failed.");
	  exit (1);
	}

      ret =
	smrlog_recover (rep->smrlog, &min_seq, &max_seq, &msg_min_seq,
			&msg_max_seq, &max_commit_seq);
      if (ret != 0)
	{
	  LOG (LG_ERROR, "Restart recovery after purge failed..");
	  exit (1);
	}
    }
  LOG (LG_INFO,
       "Log recovery finished. min:%lld, max:%lld, msgmin:%lld, msgmax:%lld, maxcseq: %lld",
       min_seq, max_seq, msg_min_seq, msg_max_seq, max_commit_seq);

  /* set other fields */
  rep->commit_seq = max_commit_seq;
  if (min_seq == 0)
    {
      aseq_set (&rep->min_seq, min_seq);
    }
  else
    {
      aseq_set (&rep->min_seq, msg_min_seq);
    }
  aseq_set (&rep->log_seq, msg_max_seq);
  aseq_set (&rep->sync_seq, msg_max_seq);

  /* setup signal handlers */
  signal (SIGHUP, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);
  setup_sigterm_handler ();
  setup_sigint_handler ();

  /* create event loop */
  rep->el = aeCreateEventLoop (1024 + 1024);
  if (rep->el == NULL)
    {
      LOG (LG_ERROR, "Failed to create event loop");
      exit (1);
    }
  /* setup server cron */
  rep->teid = aeCreateTimeEvent (rep->el, 1, server_cron, rep, NULL);

  /* create servers */
  if (initialize_services (rep) < 0)
    {
      goto finalization;
    }

  /* create slow log */
  if (initialize_slowlog (rep) < 0)
    {
      goto finalization;
    }

  /* enter event loop */
  aeSetBeforeSleepProc (rep->el, before_sleep_handler);
  aeMain (rep->el);

finalization:
  /* graceful shutdown (in harmony with other nodes) */
  if (rep->bio_thr != NULL)
    {
      pthread_join (*rep->bio_thr, NULL);
      free (rep->bio_thr);
      rep->bio_thr = NULL;
    }

  if (rep->mig_thr != NULL)
    {
      pthread_join (*rep->mig_thr, NULL);
      free (rep->mig_thr);
      rep->mig_thr = NULL;
    }
  term_replicator (rep);

  /* fault injection finalization */
  sfi_term ();
  return 0;

arg_error:
  print_usage (argv[0]);
  exit (1);
}

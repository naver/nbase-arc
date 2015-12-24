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

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include "tcp.h"
#include "stream.h"
#include "smr.h"
#include "smr_be.h"
#include "smr_log.h"
#include "crc16.h"

#define ERRNO_FILE_ID BE_FILE_ID

/* session data log reference */
struct smrData
{
  int ref_count;
  int need_free;
  int size;
  char *data;
};

/* memory mapped file info (size is fixed: 1 << SMR_LOG_FILE_DATA_BITS) */
struct mmInfo
{
  long long seq;
  smrLogAddr *addr;
};

/* configuration sub-command related structure */
typedef struct confCommand
{
  int cmd;
} confCommand;
#define CONF_COMMAND_INIT       0
#define CONF_COMMAND_NEW_MASTER 1
#define CONF_COMMAND_RCKPT      2
#define CONF_COMMAND_LCONN      3

typedef struct confInit
{
  int cmd;			//must be the first number
  char *logdir;
  char *mhost;
  int mport;
  long long commit_seq;
  short nid;
} confInit;

typedef struct confNewMaster
{
  int cmd;			//must be the first member
  char *mhost;
  int mport;
  long long after_seq;
  short nid;
} confNewMaster;

typedef struct confRckpt
{
  int cmd;			//must be the first number
  char *behost;
  int beport;
} confRckpt;

typedef struct confLconn
{
  int cmd;
} confLconn;

#define INSTATE_BUFSZ   1024
/* state used to handle stream from the local replicator */
#define MAX_MM_BITS     10
#define MAX_MM          (1<<MAX_MM_BITS)
typedef struct inState
{
  smrLog *smrlog;		// smr log
  short curr_nid;		// current node id
  long long seq;		// sequence number where to process
  long long mm_seq_min;		// minimum sequence number resides in mm (inclusive)
  long long mm_seq_max;		// maximum sequence number (exclusive)
  struct mmInfo mm[MAX_MM];	// array of opened mmap files (ring buffer)
  char buf_[INSTATE_BUFSZ];
  char *bp;			// &buf_[0] (incluseive)
  char *cp;			// curr buffer position
  char *ep;			// bp + INSTATE_BUFSZ
} inState;

/* connector to the replicator */
typedef struct smrConfigure
{
  char *logdir;			// smr log directory
  char *mhost;			// master host
  int mport;			// master port for client
  long long minseq;		// minimum sequence number available
  long long commit_seq;		// maximum commit sequence number
  short nid;			// node id
} smrConfigure;

#define EP_NONE  0
#define EP_READ  1
#define EP_WRITE 2
struct smrConnector
{
  long long ckpt_seq;		// sequence number requested by back-end
  smrConfigure init;		// CONFIGURE "init" data
  long long master_cseq;	// commit sequence number got from master at handshake phase
  inState istate;		// inState (from local replicator)
  ioStream *lous;		// out stream (to local)
  ioStream *mous;		// out state (to master)
  long long curr_last_cseq;	// if not -1, log greater than this seq must got from new master
  int trac_enabled;		// tracking enabled flag
  int trac_sent;		// total number of bytes sent to the master
  int trac_rcvd;		// total number of bytes received from the master
  int cb_enabled;		// callback endabled flag
  smrCallback *cb;		// callback
  void *arg;			// arg
  int epfd;			// return value of epoll_create
  int mfd;			// connection to the master replicator
  int lfd;			// connection to the local replicator
  int mfd_mask;			// epoll mask for mfd
  int lfd_mask;			// epoll mask for lfd
  confNewMaster *new_master;	// unhandled new master request
};

struct cbArg
{
  smrCallback *cb;
  void *arg;
  short nid;
  int sid;
  int hash;
  long long timestamp;
};

static inline long long
min_ll (long long a, long long b)
{
  return a < b ? a : b;
}

static inline long long
mm_round_down (long long seq)
{
  return ((seq) & ~((long long) SMR_LOG_FILE_DATA_SIZE - 1));
}

static inline long long
mm_round_up (long long seq)
{
  return mm_round_down (seq + SMR_LOG_FILE_DATA_SIZE);
}

static inline int
mm_seq_to_idx (long long seq)
{
  return ((seq >> SMR_LOG_FILE_DATA_BITS) & (MAX_MM - 1));
}

static inline void *
mm_seq_to_addr (inState * is, long long seq)
{
  return ((char *) is->mm[mm_seq_to_idx (seq)].addr->addr +
	  (int) ((seq) - mm_round_down (seq)));
}

static inline int
mm_consec_size (inState * is, long long seq)
{
  return min_ll (is->mm_seq_max, mm_round_up (seq)) - seq;
}



/* Forward delclaration of local functions  */
static int check_callback (smrCallback * cb);
static smrConnector *new_connector (smrCallback * cb, void *arg,
				    long long ckpt_seq);
static int master_connect (smrConnector * connector);

/* ---------------------- */
/* configuration commands */
/* ---------------------- */
// init command
static int cfg_copy_configure (smrConnector * connector, confInit * cmd);
static void cfg_free_init (confInit * cmd);
static confCommand *cfg_parse_init (char *tokens[], int num_token);
// new_master command
static void cfg_free_new_master (confNewMaster * cmd);
static confCommand *cfg_parse_new_master (char *tokens[], int num_token);
// rckpt command
static void cfg_free_rckpt (confRckpt * cmd);
static void cfg_free_lconn (confLconn * cmd);
static confCommand *cfg_parse_rckpt (char *tokens[], int num_token);
static confCommand *cfg_parse_lconn (char *tokens[], int num_token);


// general
static int cfg_parse_seq (char *tok, long long *seq);
static int cfg_parse_nid (char *tok, short *nid);
static int cfg_parse_port (char *tok, int *port);
static void cfg_free (confCommand * cmd);
static confCommand *cfg_parse (char *conf_str);

/* --------------- */
/* reconfiguration */
/* --------------- */
static int change_master (smrConnector * connector);

/* ----- */
/* local */
/* ----- */
static int local_handshake (smrConnector * connector, char *host, int port,
			    long long seq);
static int read_input_stream (smrConnector * connector);
static int istate_mi_map (smrLog * smrlog, struct mmInfo *mi, long long seq);
static void istate_mi_unmap (smrLog * smrlog, struct mmInfo *mi);
static int istate_mm_map (inState * is, long long seq);
static int istate_mm_map_range (inState * is, long long seq_, long long size);
static int istate_mm_unmap_range (inState * is, long long seq_,
				  long long size);
static int istate_read (inState * is, void *buf_, int size, long long seq);
static int istate_read_cb (inState * is, struct cbArg *cb, int size,
			   long long seq);
static int process_input_state (smrConnector * connector, smrCallback * cb,
				void *arg);
static int process_input_state_range (smrConnector * connector,
				      smrCallback * cb, void *arg,
				      long long avail, long long *used);
static int read_input_state (smrConnector * connector, int *nconsumed);
static int write_to_stream (ioStream * ous, void *buf_, int size);

static int create_file_event (smrConnector * connector, int fd, int mask,
			      int *org_mask, int permit_badfd);
static int delete_file_event (smrConnector * connector, int fd, int mask,
			      int *org_mask);
static int local_readable (smrConnector * connector);
static int local_writable (smrConnector * connector);
static int master_readable (smrConnector * connector);
static int master_writable (smrConnector * connector);

/* ---------------- */
/* smr data related */
/* ---------------- */
static char *smr_data_get_data_ (smrData * data);
static int smr_data_get_size_ (smrData * data);
static int smr_data_ref_ (smrData * data);
static int smr_data_unref_ (smrData * data);

static int smr_process_internal (smrConnector * connector, int timeout);
/* ----------------------------- */
/* Local function implementation */
/* ----------------------------- */
static int
check_callback (smrCallback * cb)
{
  if (cb == NULL || cb->session_close == NULL
      || cb->session_data == NULL || cb->noti_rckpt == NULL
      || cb->noti_ready == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}


static smrConnector *
new_connector (smrCallback * cb, void *arg, long long ckpt_seq)
{
  smrConnector *connector;
  int i;

  connector = malloc (sizeof (smrConnector));
  if (connector == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  connector->ckpt_seq = ckpt_seq;
  connector->init.mhost = NULL;
  connector->init.mport = -1;
  connector->init.minseq = -1LL;
  connector->init.commit_seq = -1LL;
  connector->init.nid = -1;
  connector->init.logdir = NULL;
  connector->master_cseq = 0LL;
  connector->epfd = -1;
  connector->mfd = -1;
  connector->lfd = -1;
  connector->mfd_mask = EP_NONE;
  connector->lfd_mask = EP_NONE;

  /* input state */
  connector->istate.smrlog = NULL;
  connector->istate.curr_nid = -1;
  connector->istate.seq = 0LL;
  connector->istate.mm_seq_min = -1LL;
  connector->istate.mm_seq_max = 0LL;
  for (i = 0; i < MAX_MM; i++)
    {
      connector->istate.mm[i].seq = -1LL;
      connector->istate.mm[i].addr = NULL;
    }
  connector->istate.bp = &connector->istate.buf_[0];
  connector->istate.cp = connector->istate.bp;
  connector->istate.ep = connector->istate.bp + INSTATE_BUFSZ;

  /* output to local */
  connector->lous = io_stream_create (1024);
  if (connector->lous == NULL)
    {
      goto error;
    }

  /* output state */
  connector->mous = io_stream_create (32 * 1024);
  if (connector->mous == NULL)
    {
      goto error;
    }
  connector->curr_last_cseq = -1LL;
  connector->trac_enabled = 0;
  connector->trac_sent = 0;
  connector->trac_rcvd = 0;

  /* check connector */
  connector->cb_enabled = 1;	// initially enabled
  connector->cb = cb;
  connector->arg = arg;

  /* create epoll sub-system */
  connector->epfd = epoll_create (2);
  if (connector->epfd == -1)
    {
      goto error;
    }

  connector->new_master = NULL;
  return connector;

error:
  if (connector != NULL)
    {
      smr_disconnect (connector);
    }
  ERRNO_POINT ();
  return NULL;
}

static int
master_connect (smrConnector * connector)
{
  char ebuf[512];
  char *host;
  int port;
  short net_nid;
  long long net_seq = 0LL;

  host = connector->init.mhost;
  port = connector->init.mport;

  assert (connector->mfd == -1);
  connector->mfd =
    tcp_connect (host, port, TCP_OPT_NODELAY | TCP_OPT_KEEPALIVE, ebuf, 512);
  if (connector->mfd == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  /* HANDSHAKE */
  net_nid = htons (connector->init.nid);
  if (tcp_write_fully (connector->mfd, &net_nid, sizeof (short)) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  /*
   * get last cseq from master (for catchup)
   */
  if (tcp_read_fully (connector->mfd, &net_seq, sizeof (long long)) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }
  connector->master_cseq = ntohll (net_seq);

  if (tcp_set_option (connector->mfd, TCP_OPT_NONBLOCK) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (create_file_event
      (connector, connector->mfd, EP_READ, &connector->mfd_mask, 0) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (create_file_event
      (connector, connector->mfd, EP_WRITE, &connector->mfd_mask, 0) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  return 0;
}


/* 
 * This function copies the init command to connector->init for lazy initialization
 */
static int
cfg_copy_configure (smrConnector * connector, confInit * cmd)
{
  assert (connector != NULL);
  assert (connector->init.logdir == NULL);
  assert (connector->init.mhost == NULL);
  assert (cmd != NULL);

  connector->init.logdir = strdup (cmd->logdir);
  if (connector->init.logdir == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  connector->init.mhost = strdup (cmd->mhost);
  if (connector->init.mhost == NULL)
    {
      ERRNO_POINT ();
      return -1;
    }
  connector->init.mport = cmd->mport;
  connector->init.commit_seq = cmd->commit_seq;
  connector->init.nid = cmd->nid;

  return 0;
}

static void
cfg_free_init (confInit * cmd)
{
  assert (cmd != NULL);

  if (cmd->logdir != NULL)
    {
      free (cmd->logdir);
    }

  if (cmd->mhost != NULL)
    {
      free (cmd->mhost);
    }

  free (cmd);
}

static confCommand *
cfg_parse_init (char *tokens[], int num_token)
{
  confInit *init_cmd = NULL;

  if (num_token != 5)
    {
      ERRNO_POINT ();
      return NULL;
    }

  init_cmd = calloc (1, sizeof (confInit));
  if (init_cmd == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  init_cmd->cmd = CONF_COMMAND_INIT;

  init_cmd->logdir = tokens[0];
  tokens[0] = NULL;

  init_cmd->mhost = tokens[1];
  tokens[1] = NULL;

  if (cfg_parse_port (tokens[2], &init_cmd->mport) == -1)
    {
      goto error;
    }

  if (cfg_parse_seq (tokens[3], &init_cmd->commit_seq) == -1)
    {
      goto error;
    }

  if (cfg_parse_nid (tokens[4], &init_cmd->nid) == -1)
    {
      goto error;
    }

  return (confCommand *) init_cmd;

error:
  if (init_cmd != NULL)
    {
      cfg_free ((confCommand *) init_cmd);
    }
  ERRNO_POINT ();
  return NULL;
}

static void
cfg_free_new_master (confNewMaster * cmd)
{
  assert (cmd != NULL);

  if (cmd->mhost != NULL)
    {
      free (cmd->mhost);
    }

  free (cmd);
}

static confCommand *
cfg_parse_new_master (char *tokens[], int num_token)
{
  confNewMaster *nm = NULL;

  if (num_token != 4)
    {
      ERRNO_POINT ();
      return NULL;
    }

  nm = calloc (1, sizeof (confNewMaster));
  if (nm == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  nm->cmd = CONF_COMMAND_NEW_MASTER;

  nm->mhost = tokens[0];
  tokens[0] = NULL;

  if (cfg_parse_port (tokens[1], &nm->mport) == -1)
    {
      goto error;
    }

  if (cfg_parse_seq (tokens[2], &nm->after_seq) == -1)
    {
      goto error;
    }

  if (cfg_parse_nid (tokens[3], &nm->nid) == -1)
    {
      goto error;
    }

  return (confCommand *) nm;

error:
  if (nm != NULL)
    {
      cfg_free ((confCommand *) nm);
    }
  ERRNO_POINT ();
  return NULL;
}

static void
cfg_free_rckpt (confRckpt * cmd)
{
  assert (cmd != NULL);

  if (cmd->behost != NULL)
    {
      free (cmd->behost);
    }

  free (cmd);
}

static void
cfg_free_lconn (confLconn * cmd)
{
  assert (cmd != NULL);
  free (cmd);
}

static confCommand *
cfg_parse_rckpt (char *tokens[], int num_token)
{
  confRckpt *ckpt = NULL;

  if (num_token != 2)
    {
      ERRNO_POINT ();
      return NULL;
    }

  ckpt = calloc (1, sizeof (confRckpt));
  if (ckpt == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  ckpt->cmd = CONF_COMMAND_RCKPT;

  ckpt->behost = tokens[0];
  tokens[0] = NULL;

  if (cfg_parse_port (tokens[1], &ckpt->beport) == -1)
    {
      goto error;
    }

  return (confCommand *) ckpt;

error:
  if (ckpt != NULL)
    {
      cfg_free ((confCommand *) ckpt);
    }
  ERRNO_POINT ();
  return NULL;
}

static confCommand *
cfg_parse_lconn (char *tokens[], int num_token)
{
  confLconn *lconn = NULL;

  if (num_token != 0)
    {
      ERRNO_POINT ();
      return NULL;
    }

  lconn = calloc (1, sizeof (confLconn));
  if (lconn == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }
  lconn->cmd = CONF_COMMAND_LCONN;
  return (confCommand *) lconn;
}


static int
cfg_parse_seq (char *tok, long long *seq)
{
  assert (tok != NULL);
  assert (seq != NULL);

  errno = 0;
  *seq = strtoll (tok, NULL, 10);
  if (*seq < 0 || errno == ERANGE)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static int
cfg_parse_nid (char *tok, short *nid)
{
  int n;

  assert (tok != NULL);
  assert (nid != NULL);

  errno = 0;
  n = strtol (tok, NULL, 10);
  if (n < 0 || errno == ERANGE)
    {
      ERRNO_POINT ();
      return -1;
    }
  if (n > SHRT_MAX)
    {
      ERRNO_POINT ();
      return -1;
    }

  *nid = n;
  return 0;
}

static int
cfg_parse_port (char *tok, int *port)
{
  assert (tok != NULL);
  assert (port != NULL);

  errno = 0;
  *port = strtol (tok, NULL, 10);
  if (*port < 0 || errno == ERANGE)
    {
      ERRNO_POINT ();
      return -1;
    }
  return 0;
}

static void
cfg_free (confCommand * cmd)
{
  assert (cmd != NULL);

  switch (cmd->cmd)
    {
    case CONF_COMMAND_INIT:
      cfg_free_init ((confInit *) cmd);
      break;

    case CONF_COMMAND_NEW_MASTER:
      cfg_free_new_master ((confNewMaster *) cmd);
      break;

    case CONF_COMMAND_RCKPT:
      cfg_free_rckpt ((confRckpt *) cmd);
      break;

    case CONF_COMMAND_LCONN:
      cfg_free_lconn ((confLconn *) cmd);
      break;

    default:
      ERRNO_POINT ();
      assert (0);
    }
}

#define DELM " \t\r\n"
static confCommand *
cfg_parse (char *conf_str)
{
  char *ep;
  char *token;
  char *tokens[256];
  int num_token;
  int i;
  confCommand *cmd = NULL;

  assert (conf_str != NULL);

  /* check format */
  ep = strstr (conf_str, "\r\n");
  if (ep == NULL || *(ep + 2) != '\0')
    {
      ERRNO_POINT ();
      return NULL;
    }

  /* tokenize */
  num_token = 0;
  token = strtok (conf_str, DELM);
  if (token == NULL)
    {
      ERRNO_POINT ();
      goto error;
    }

  tokens[0] = strdup (token);
  if (tokens[0] == NULL)
    {
      ERRNO_POINT ();
      goto error;
    }
  num_token = 1;

  while ((token = strtok (NULL, DELM)) != NULL)
    {
      if (num_token > 256)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      tokens[num_token] = strdup (token);
      if (tokens[num_token] == NULL)
	{
	  ERRNO_POINT ();
	  goto error;
	}
      num_token++;
    }

  /* make command */
  if (!strcmp (tokens[0], "init"))
    {
      cmd = cfg_parse_init (tokens + 1, num_token - 1);
    }
  else if (!strcmp (tokens[0], "new_master"))
    {
      cmd = cfg_parse_new_master (tokens + 1, num_token - 1);
    }
  else if (!strcmp (tokens[0], "rckpt"))
    {
      cmd = cfg_parse_rckpt (tokens + 1, num_token - 1);
    }
  else if (!strcmp (tokens[0], "lconn"))
    {
      cmd = cfg_parse_lconn (tokens + 1, num_token - 1);
    }
  else
    {
      ERRNO_POINT ();
      goto error;
    }

  /* clean intermediate resource */
  for (i = 0; i < num_token; i++)
    {
      if (tokens[i] != NULL)
	{
	  free (tokens[i]);
	}
    }
  return cmd;

error:
  for (i = 0; i < num_token; i++)
    {
      if (tokens[i] != NULL)
	{
	  free (tokens[i]);
	}
    }

  if (cmd != NULL)
    {
      cfg_free (cmd);
    }

  ERRNO_POINT ();
  return NULL;
}

static int
change_master (smrConnector * connector)
{
  confNewMaster *nm = connector->new_master;
  assert (nm != NULL);

  /* close connection to the master */
  if (connector->mfd > 0)
    {
      if ((connector->mfd_mask & EP_WRITE) != 0
	  && delete_file_event (connector, connector->mfd, EP_WRITE,
				&connector->mfd_mask) == 1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      else if ((connector->mfd_mask & EP_READ) != 0
	       && delete_file_event (connector, connector->mfd, EP_READ,
				     &connector->mfd_mask) == 1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      close (connector->mfd);
      connector->mfd = -1;
    }

  /* rewind buffer */
  if (io_stream_reset_read (connector->mous) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  /* connect to new master */
  if (connector->init.mhost != NULL)
    {
      free (connector->init.mhost);
    }
  connector->init.mhost = nm->mhost;
  nm->mhost = NULL;
  connector->init.mport = nm->mport;
  connector->init.nid = nm->nid;

  if (master_connect (connector) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }
  cfg_free_new_master (nm);
  connector->new_master = NULL;
  connector->curr_last_cseq = -1LL;

  return 0;
}

static int
local_handshake (smrConnector * connector, char *host, int port,
		 long long seq)
{
  char ebuf[512];
  long long net_seq;

  assert (connector != NULL);
  assert (connector->init.nid == -1);

  connector->lfd =
    tcp_connect (host, port, TCP_OPT_NODELAY | TCP_OPT_KEEPALIVE, ebuf, 512);
  if (connector->lfd == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  net_seq = htonll (seq);
  if (tcp_write_fully (connector->lfd, &net_seq, sizeof (long long)) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (tcp_set_option (connector->lfd, TCP_OPT_NONBLOCK) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (create_file_event
      (connector, connector->lfd, EP_READ, &connector->lfd_mask, 0) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  return 0;
}

static int
read_input_stream (smrConnector * connector)
{
  inState *is;
  int avail;

  assert (connector != NULL);
  is = &connector->istate;

  avail = is->ep - is->cp;
  while (avail > 0)
    {
      int nr;

      nr = read (connector->lfd, is->cp, avail);
      if (nr < 0)
	{
	  if (nr == -1)
	    {
	      if (errno == EAGAIN)
		{
		  break;
		}
	      ERRNO_POINT ();
	      return -1;
	    }
	}
      else if (nr == 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}

      is->cp += nr;
      avail -= nr;
    }
  return 0;
}

static int
istate_mi_map (smrLog * smrlog, struct mmInfo *mi, long long seq)
{
  assert (mi != NULL);
  assert (mi->addr == NULL);

  if (mi->addr == NULL)
    {
      seq = mm_round_down (seq);
      mi->addr = smrlog_read_mmap (smrlog, seq);
      if (mi->addr == NULL)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      mi->seq = seq;
      return 0;
    }
  else
    {
      // already mapped
      ERRNO_POINT ();
      return -1;
    }
}

static void
istate_mi_unmap (smrLog * smrlog, struct mmInfo *mi)
{
  assert (mi != NULL);

  if (mi->addr != NULL)
    {
      smrlog_munmap (smrlog, mi->addr);
      //replicator does.. (void) smrlog_os_decache (smrlog, mi->seq);
      mi->seq = -1LL;
      mi->addr = NULL;
    }
}

static int
istate_mm_map (inState * is, long long seq)
{
  struct mmInfo *mi;

  mi = &is->mm[mm_seq_to_idx (seq)];

  // fast return
  if (mi->seq == mm_round_down (seq))
    {
      return 0;
    }
  else if (mi->seq > 0)
    {
      // alreay mapped it means mmap ring buffer out of range
      ERRNO_POINT ();
      return -1;
    }

  return istate_mi_map (is->smrlog, mi, seq);
}

/* mmaps memory mapped files from mm_round_down (seq_) to mm_round_up (seq_ + size) */
static int
istate_mm_map_range (inState * is, long long seq_, long long size)
{
  long long seq;

  for (seq = mm_round_down (seq_); seq < mm_round_up (seq_ + size);
       seq = seq + SMR_LOG_FILE_DATA_SIZE)
    {
      if (istate_mm_map (is, seq) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}

      if (is->mm_seq_min == -1LL)
	{
	  is->mm_seq_min = seq;
	}
    }
  return 0;
}

/* unmaps memory mapped files from mm_round_down (seq_) to mm_round_down (seq_ + size) */
static int
istate_mm_unmap_range (inState * is, long long seq_, long long size)
{
  long long seq;

  for (seq = mm_round_down (seq_); seq < mm_round_down (seq_ + size);
       seq = seq + SMR_LOG_FILE_DATA_SIZE)
    {
      istate_mi_unmap (is->smrlog, &is->mm[mm_seq_to_idx (seq)]);

      if (seq == is->mm_seq_min)
	{
	  is->mm_seq_min = is->mm_seq_min + SMR_LOG_FILE_DATA_SIZE;
	}
    }
  return 0;
}

static int
istate_read (inState * is, void *buf_, int size, long long seq)
{
  int once;
  char *buf = buf_;

  assert (is != NULL);
  assert (buf_ != NULL);
  assert (size > 0);

  while (size > 0 && size >= (once = mm_consec_size (is, seq)))
    {
      if (buf != NULL)
	{
	  char *bp = mm_seq_to_addr (is, seq);

	  memcpy (buf, bp, once);
	  buf += once;
	}
      size -= once;
      seq += once;
    }

  if (size > 0)
    {
      if (buf != NULL)
	{
	  char *bp = mm_seq_to_addr (is, seq);

	  memcpy (buf, bp, size);
	}
      seq = seq + size;
    }
  return 0;
}

static int
istate_read_cb (inState * is, struct cbArg *arg, int size, long long seq)
{
  int once;
  int ret;
  smrCallback *cb;
  int need_merge;
  long long org_seq = seq;
  int org_size = size;
  smrData *smr_data = NULL;
  char *sdp = NULL;

  assert (is != NULL);
  assert (arg != NULL);
  cb = arg->cb;
  assert (cb != NULL);
  assert (size > 0);

  need_merge = (cb != NULL) && (mm_consec_size (is, seq) < size);
  if (cb != NULL)
    {
      smr_data = malloc (sizeof (smrData));
      if (smr_data == NULL)
	{
	  errno = ENOMEM;
	  ERRNO_POINT ();
	  return -1;
	}
      smr_data->ref_count = 1;
      smr_data->need_free = need_merge;
      smr_data->size = size;
      smr_data->data = NULL;
      if (need_merge)
	{
	  smr_data->data = malloc (size);
	  errno = ENOMEM;
	  ERRNO_POINT ();
	  if (smr_data->data == NULL)
	    {
	      smr_data_unref_ (smr_data);
	      ERRNO_POINT ();
	      return -1;
	    }
	  sdp = smr_data->data;
	}
    }

  while (size > 0 && size >= (once = mm_consec_size (is, seq)))
    {
      if (cb != NULL)
	{
	  char *bp = mm_seq_to_addr (is, seq);

	  if (!need_merge)
	    {
	      smr_data->data = bp;
	      ret =
		cb->session_data (arg->arg, seq, arg->timestamp, arg->nid,
				  arg->sid, arg->hash, smr_data, once);
	      smr_data_unref_ (smr_data);
	      if (ret == -1)
		{
		  ERRNO_POINT ();
		  return -1;
		}
	    }
	  else
	    {
	      memcpy (sdp, bp, once);
	      sdp += once;
	    }
	}
      size -= once;
      seq += once;
    }

  if (size > 0)
    {
      if (cb != NULL)
	{
	  char *bp = mm_seq_to_addr (is, seq);

	  if (!need_merge)
	    {
	      smr_data->data = bp;
	      ret =
		cb->session_data (arg->arg, seq, arg->timestamp, arg->nid,
				  arg->sid, arg->hash, smr_data, size);
	      smr_data_unref_ (smr_data);
	      if (ret == -1)
		{
		  ERRNO_POINT ();
		  return -1;
		}
	    }
	  else
	    {
	      memcpy (sdp, bp, size);
	      sdp += size;
	    }
	}
      seq = seq + size;
    }

  if (need_merge)
    {
      ret =
	cb->session_data (arg->arg, org_seq, arg->timestamp, arg->nid,
			  arg->sid, arg->hash, smr_data, org_size);
      smr_data_unref_ (smr_data);
      if (ret == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }
  return 0;
}

static int
process_input_state (smrConnector * connector, smrCallback * cb, void *arg)
{
  inState *is;
  int ret = 0;
  long long saved_seq;
  long long map_range = 10 * SMR_LOG_FILE_DATA_SIZE;

  is = &connector->istate;
  saved_seq = is->seq;

  while (1)
    {
      long long avail = is->mm_seq_max - is->seq;
      long long used = 0;
      int more_input = 0;

      if (avail > map_range)
	{
	  avail = map_range;
	  more_input = 1;
	}
      else if (avail <= 0)
	{
	  break;
	}

      // unmap range is deferred to the user ('smr_release_seq_upto')
      if (istate_mm_map_range (is, is->seq, avail) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}

      ret = process_input_state_range (connector, cb, arg, avail, &used);
      if (ret < 0)
	{
	  return ret;
	}

      if (!more_input)
	{
	  break;
	}
      else if (used == 0)
	{
	  /* 
	   * This case can be happend when a message size exceed map_range.
	   * We extend the mapping range.
	   */
	  map_range = map_range * 2;
	}
    }

  if (saved_seq < is->seq)
    {
      char buf[1 + sizeof (long long)];
      long long net_seq;

      buf[0] = SMR_OP_SEQ_CONSUMED;
      net_seq = htonll (is->seq);
      memcpy (&buf[1], &net_seq, sizeof (long long));
      if (write_to_stream (connector->lous, buf, sizeof (buf)) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      if (create_file_event (connector, connector->lfd, EP_WRITE,
			     &connector->lfd_mask, 0) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }
  return ret;
}

#define mm_read(buf,_sz) do {        \
  int ret;                           \
  if(tsz < _sz) { goto done; }       \
  ret = istate_read(is,buf,_sz,seq); \
  if(ret == -1) {                    \
    ERRNO_POINT();                   \
    return -1;                       \
  }                                  \
  tsz = tsz - _sz;                   \
  seq = seq + _sz;                   \
} while(0)

#define mm_read_cb(_sz, arg) do {       \
  int ret;                              \
  if(tsz < _sz) { goto done; }          \
  ret = istate_read_cb(is,arg,_sz,seq); \
  if(ret == -1) {                       \
    ERRNO_POINT();                      \
    return -1;                          \
  }                                     \
  tsz = tsz - _sz;                      \
  seq = seq + _sz;                      \
} while(0)

#define mm_commit() do {   \
  saved_tsz = tsz;         \
  saved_seq = seq;         \
} while(0)

#define mm_rollback() do { \
  tsz = saved_tsz;         \
  seq = saved_seq;         \
} while(0)

static int
process_input_state_range (smrConnector * connector, smrCallback * cb,
			   void *arg, long long avail, long long *used)
{
  inState *is;
  long long seq, saved_seq;
  long long tsz, saved_tsz;
  char cmd;

  is = &connector->istate;
  saved_tsz = tsz = avail;
  saved_seq = seq = is->seq;

  while (tsz > 1)		// 1: no command with no data
    {
      short nid;
      int sid, length, hash;
      int seq_magic;
      unsigned short seq_crc16, net_crc16;
      long long net_seq;
      long long timestamp;
      struct cbArg cb_arg;;

      mm_commit ();
      mm_read (&cmd, 1);

      switch (cmd)
	{
	  /* -------------------- */
	case SMR_OP_EXT:
	case SMR_OP_SESSION_CLOSE:
	  /* -------------------- */
	  mm_read (&sid, sizeof (int));
	  sid = ntohl (sid);
	  mm_commit ();
	  if (connector->trac_enabled && is->curr_nid == connector->init.nid)
	    {
	      connector->trac_rcvd += (1 + sizeof (int));
	    }

	  if (cmd == SMR_OP_SESSION_CLOSE
	      && cb->session_close (arg, seq, is->curr_nid, sid) != 0)
	    {
	      mm_rollback ();
	      ERRNO_POINT ();
	      return -1;
	    }

	  break;
	  /* ------------------- */
	case SMR_OP_SESSION_DATA:	//sid, hash, timestamp, length, data
	  /* ------------------- */
	  mm_read (&sid, sizeof (int));
	  sid = ntohl (sid);
	  mm_read (&hash, sizeof (int));
	  hash = ntohl (hash);
	  mm_read (&timestamp, sizeof (long long));
	  timestamp = ntohll (timestamp);
	  mm_read (&length, sizeof (int));
	  length = ntohl (length);
	  cb_arg.cb = cb;
	  cb_arg.arg = arg;
	  cb_arg.nid = is->curr_nid;
	  cb_arg.sid = sid;
	  cb_arg.hash = hash;
	  cb_arg.timestamp = timestamp;
	  mm_read_cb (length, &cb_arg);
	  mm_commit ();
	  if (connector->trac_enabled && is->curr_nid == connector->init.nid)
	    {
	      connector->trac_rcvd +=
		(1 + 3 * sizeof (int) + sizeof (long long) + length);
	    }
	  break;
	  /* ------------------ */
	case SMR_OP_NODE_CHANGE:
	  /* ------------------ */
	  mm_read (&nid, sizeof (short));
	  mm_commit ();
	  nid = ntohs (nid);
	  is->curr_nid = nid;
	  break;
	  /* -------------------- */
	case SMR_OP_SEQ_COMMITTED:
	  /* -------------------- */
	  mm_read (&seq_magic, sizeof (int));
	  seq_magic = ntohl (seq_magic);
	  if (seq_magic != SEQ_COMMIT_MAGIC)
	    {
	      mm_rollback ();
	      ERRNO_POINT ();
	      return -1;
	    }
	  mm_read (&net_seq, sizeof (long long));
	  net_crc16 = crc16 ((char *) &net_seq, sizeof (long long), 0);
	  mm_read (&seq_crc16, sizeof (unsigned short));
	  seq_crc16 = ntohs (seq_crc16);
	  if (net_crc16 != seq_crc16)
	    {
	      mm_rollback ();
	      ERRNO_POINT ();
	      return -1;
	    }
	  mm_commit ();
	  break;
	default:
	  mm_rollback ();
	  ERRNO_POINT ();
	  return -1;
	}

      if (connector->trac_enabled && connector->trac_rcvd > 0)
	{
	  int diff;

	  assert (connector->trac_sent >= connector->trac_rcvd);
	  diff = connector->trac_rcvd;
	  if (io_stream_purge (connector->mous, diff) == -1)
	    {
	      ERRNO_POINT ();
	      return -1;
	    }
	  connector->trac_sent -= diff;
	  connector->trac_rcvd = 0;

	}
    }

  if (connector->curr_last_cseq != -1 && connector->curr_last_cseq <= seq)
    {
      assert (connector->curr_last_cseq <= seq);
      assert (connector->new_master != NULL);
      if (change_master (connector) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

done:
  mm_rollback ();
  *used = seq - is->seq;
  is->seq = seq;
  return 0;
}

static int
read_input_state (smrConnector * connector, int *nconsumed)
{
  smrCallback *cb;
  int ret;
  inState *is;
  char *cp;
  char *ep;

  assert (connector != NULL);
  assert (nconsumed != NULL);

  cb = connector->cb;
  is = &connector->istate;

  cp = is->bp;
  ep = is->cp;
  while (cp < ep)
    {
      char cmd = *cp;

      if (cmd == SMR_OP_SEQ_SAFE)
	{
	  long long seq;

	  if (ep - cp < (1 + sizeof (long long)))
	    {
	      /* partial command */
	      break;
	    }
	  cp++;
	  memcpy (&seq, cp, sizeof (long long));
	  cp += sizeof (long long);
	  seq = ntohll (seq);
	  assert (seq >= is->mm_seq_max);
	  is->mm_seq_max = seq;
	  continue;
	}
      else if (cmd == SMR_OP_CONFIGURE)
	{
	  int header_len = 1 + sizeof (int);
	  int data_len = 0;
	  char *end;
	  confCommand *cmd;
	  char *conf;

	  /* check all command data is received */
	  if (ep - cp < header_len)
	    {
	      break;
	    }
	  memcpy (&data_len, cp + 1, sizeof (int));
	  data_len = ntohl (data_len);
	  assert (data_len > 0);

	  if (ep - cp < header_len + data_len)
	    {
	      break;
	    }

	  end = cp + header_len + data_len;
	  if (*(end - 2) != '\r' || *(end - 1) != '\n')
	    {
	      ERRNO_POINT ();
	      return -1;
	    }

	  /* parse */
	  conf = malloc (data_len + 1);
	  if (conf == NULL)
	    {
	      ERRNO_POINT ();
	      return -1;
	    }
	  memcpy (conf, cp + header_len, data_len);
	  conf[data_len] = '\0';
	  if ((cmd = cfg_parse (conf)) == NULL)
	    {
	      free (conf);
	      ERRNO_POINT ();
	      return -1;
	    }
	  free (conf);

	  if (cmd->cmd == CONF_COMMAND_NEW_MASTER)
	    {
	      confNewMaster *new_master = (confNewMaster *) cmd;

	      if (connector->new_master != NULL)
		{
		  /* reconfiguration during reconfiguration is not permitted */
		  cfg_free (cmd);
		  ERRNO_POINT ();
		  return -1;
		}
	      connector->curr_last_cseq = new_master->after_seq;
	      connector->new_master = new_master;

	      /* this can be happend when master election occurred */
	      if (is->seq >= new_master->after_seq)
		{
		  if (change_master (connector) == -1)
		    {
		      ERRNO_POINT ();
		      return -1;
		    }
		}
	    }
	  else if (cmd->cmd == CONF_COMMAND_INIT)
	    {

	      if (cfg_copy_configure (connector, (confInit *) cmd) == -1)
		{
		  cfg_free (cmd);
		  ERRNO_POINT ();
		  return -1;
		}
	      cfg_free (cmd);

	      if (connector->init.commit_seq < connector->ckpt_seq)
		{
		  errno = ERANGE;
		  ERRNO_POINT ();
		  return -1;
		}
	      connector->istate.seq = connector->ckpt_seq;
	      connector->istate.mm_seq_max = connector->init.commit_seq;

	      /* init log system */
	      if (connector->istate.smrlog != NULL)
		{
		  ERRNO_POINT ();
		  return -1;
		}
	      if ((connector->istate.smrlog =
		   smrlog_init (connector->init.logdir)) == NULL)
		{
		  ERRNO_POINT ();
		  return -1;
		}

	      /* connect to master */
	      if (master_connect (connector) == -1)
		{
		  ERRNO_POINT ();
		  return -1;
		}

	      ret = cb->noti_ready (connector->arg);
	      if (ret != 0)
		{
		  ERRNO_POINT ();
		  return -1;
		}
	    }
	  else if (cmd->cmd == CONF_COMMAND_RCKPT)
	    {
	      confRckpt *ckpt = (confRckpt *) cmd;

	      ret =
		cb->noti_rckpt (connector->arg, ckpt->behost, ckpt->beport);
	      if (ret != 0)
		{
		  ERRNO_POINT ();
		  return -1;
		}
	    }
	  else if (cmd->cmd == CONF_COMMAND_LCONN)
	    {
	      /* 
	       * local replicator is not a replication member. we must disconnect to the
	       * master to disable replication message to the master (if exists) 
	       */
	      if (connector->mfd > 0)
		{
		  (void) delete_file_event (connector, connector->mfd,
					    EP_READ, &connector->mfd_mask);
		  (void) delete_file_event (connector, connector->mfd,
					    EP_WRITE, &connector->mfd_mask);
		  close (connector->mfd);
		  connector->mfd = -1;
		}
	    }
	  else
	    {
	      // not supported
	      cfg_free (cmd);
	      ERRNO_POINT ();
	      return -1;
	    }
	  cp += header_len + data_len;
	  continue;
	}
      else
	{
	  /* unknown command */
	  ERRNO_POINT ();
	  return -1;
	}
    }
  *nconsumed = cp - is->bp;

  /* adjust the buffer */
  if (cp > is->bp)
    {
      int remain = is->cp - cp;
      if (remain > 0)
	{
	  memmove (is->bp, cp, remain);
	}
      is->cp = is->bp + remain;
    }
  return 0;
}

static int
write_to_stream (ioStream * ous, void *buf_, int size)
{
  int remain = size;
  char *bp = buf_;
  while (remain > 0)
    {
      char *buf = NULL;
      int avail = 0;
      int nw;
      if (io_stream_peek_write_buffer (ous, &buf, &avail) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}

      nw = avail > remain ? remain : avail;
      memcpy (buf, bp, nw);
      remain -= nw;
      bp += nw;
      if (io_stream_commit_write (ous, nw) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }
  return 0;
}

static int
create_file_event (smrConnector * connector, int fd, int mask, int *org_mask,
		   int permit_badfd)
{
  struct epoll_event ee;
  int op;
  assert (connector != NULL);
  assert (org_mask != NULL);
  /* fast return */
  if ((*org_mask & mask) == mask)
    {
      return 0;
    }
  else if (fd < 0)
    {
      if (permit_badfd)
	{
	  return 0;
	}
      else
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

  op = (*org_mask == EP_NONE) ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
  ee.events = 0;
  mask |= *org_mask;
  if (mask & EP_READ)
    {
      ee.events |= EPOLLIN;
    }
  if (mask & EP_WRITE)
    {
      ee.events |= EPOLLOUT;
    }
  ee.data.u64 = 0LL;
  ee.data.fd = fd;
  if (epoll_ctl (connector->epfd, op, fd, &ee) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }
  *org_mask = mask;
  return 0;
}

static int
delete_file_event (smrConnector * connector, int fd,
		   int del_mask, int *org_mask)
{
  struct epoll_event ee;
  int mask;
  int ret = 0;
  assert (connector != NULL);
  assert (fd > 0);
  assert (org_mask != NULL);
  /* fast return */
  if ((*org_mask & del_mask) == 0)
    {
      return 0;
    }

  mask = *org_mask & (~del_mask);
  ee.events = 0;
  if (mask & EP_READ)
    {
      ee.events |= EPOLLIN;
    }
  if (mask & EP_WRITE)
    {
      ee.events |= EPOLLOUT;
    }
  ee.data.u64 = 0LL;
  ee.data.fd = fd;
  if (mask != EP_NONE)
    {
      ret = epoll_ctl (connector->epfd, EPOLL_CTL_MOD, fd, &ee);
    }
  else
    {
      ret = epoll_ctl (connector->epfd, EPOLL_CTL_DEL, fd, &ee);
    }

  if (ret == 0)
    {
      *org_mask = mask;
    }
  return ret;
}

static int
local_readable (smrConnector * connector)
{
  int nconsumed = 0;
  assert (connector != NULL);
  if (read_input_stream (connector) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (read_input_state (connector, &nconsumed) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  if (nconsumed == 0)
    {
      return 0;
    }

  if (!connector->cb_enabled)
    {
      return 0;
    }
  else
    {
      return process_input_state (connector, connector->cb, connector->arg);
    }
}

static int
local_writable (smrConnector * connector)
{
  ioStream *ous;
  int fd;

  assert (connector != NULL);
  ous = connector->lous;
  fd = connector->lfd;

  while (1)
    {
      int ret;
      char *buf = NULL;
      int bufsz = 0;
      int nw;
      int used;

      ret = io_stream_peek_read_buffer (ous, &buf, &bufsz);
      if (ret == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      else if (bufsz == 0)
	{
	  break;
	}

      nw = write (fd, buf, bufsz);
      used = nw < 0 ? 0 : nw;
      if ((bufsz - used) > 0
	  && io_stream_rewind_readpos (ous, bufsz - used) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}

      if (nw == -1)
	{
	  if (errno == EAGAIN)
	    {
	      return 0;
	    }
	  ERRNO_POINT ();
	  return -1;
	}
      else if (nw == 0)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      else
	{
	  if (io_stream_purge (ous, nw) == -1)
	    {
	      ERRNO_POINT ();
	      return -1;
	    }
	  if (nw < bufsz)
	    {
	      return 0;
	    }
	}
    }
  return delete_file_event (connector, fd, EP_WRITE, &connector->lfd_mask);
}

/* Just consume for now. It is not used at backend side */
static int
master_readable (smrConnector * connector)
{
  char buf[128];
  int fd;

  assert (connector != NULL);
  fd = connector->mfd;

  while (1)
    {
      int nr;

      nr = read (fd, buf, 128);
      if (nr < 0)
	{
	  if (nr == -1)
	    {
	      if (errno == EAGAIN)
		{
		  break;
		}
	      goto master_dead;
	    }
	}
      else if (nr == 0)
	{
	  goto master_dead;
	}
    }
  return 0;

master_dead:
  /* master dead. wait for master election from local replicator */
  (void) delete_file_event (connector, fd, EP_READ, &connector->mfd_mask);
  (void) delete_file_event (connector, fd, EP_WRITE, &connector->mfd_mask);
  close (connector->mfd);
  connector->mfd = -1;
  return 0;
}


static int
master_writable (smrConnector * connector)
{
  ioStream *ous;
  int fd;
  assert (connector != NULL);
  ous = connector->mous;
  fd = connector->mfd;
  while (1)
    {
      int ret;
      char *buf = NULL;
      int bufsz = 0;
      int nw;
      int used;

      ret = io_stream_peek_read_buffer (ous, &buf, &bufsz);
      if (ret == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
      else if (bufsz == 0)
	{
	  break;
	}

      nw = write (fd, buf, bufsz);
      used = nw < 0 ? 0 : nw;
      if ((bufsz - used) > 0
	  && io_stream_rewind_readpos (ous, bufsz - used) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}

      if (nw == -1)
	{
	  if (errno == EAGAIN)
	    {
	      return 0;
	    }
	  goto master_dead;
	}
      else if (nw == 0)
	{
	  goto master_dead;
	}
      else
	{
	  if (!connector->trac_enabled)
	    {
	      if (io_stream_purge (ous, nw) == -1)
		{
		  ERRNO_POINT ();
		  return -1;
		}
	      if (nw < bufsz)
		{
		  return 0;
		}
	    }
	}
    }

  return delete_file_event (connector, fd, EP_WRITE, &connector->mfd_mask);
master_dead:
  /* master dead. wait for master election from local replicator */
  (void) delete_file_event (connector, fd, EP_READ, &connector->mfd_mask);
  (void) delete_file_event (connector, fd, EP_WRITE, &connector->mfd_mask);
  close (connector->mfd);
  connector->mfd = -1;
  return 0;
}

/* ------------------ */
/* Exported functions */
/* ------------------ */
smrConnector *
smr_connect_tcp (int local_port, long long ckpt_seq, smrCallback * cb,
		 void *cb_arg)
{
  smrConnector *connector;

  errno = 0;
  if (check_callback (cb) == -1)
    {
      ERRNO_POINT ();
      return NULL;
    }

  connector = new_connector (cb, cb_arg, ckpt_seq);
  if (connector == NULL)
    {
      ERRNO_POINT ();
      return NULL;
    }

  /* connect to local replicator */
  if (local_handshake (connector, "localhost", local_port, ckpt_seq) == -1)
    {
      smr_disconnect (connector);
      ERRNO_POINT ();
      return NULL;
    }

  return connector;
}

void
smr_disconnect (smrConnector * connector)
{
  int i;
  assert (connector != NULL);
  errno = 0;
  if (connector == NULL)
    {
      return;
    }

  if (connector->mfd > 0)
    {
      close (connector->mfd);
    }

  if (connector->lfd > 0)
    {
      close (connector->lfd);
    }

  for (i = 0; i < MAX_MM; i++)
    {
      istate_mi_unmap (connector->istate.smrlog, &connector->istate.mm[i]);
    }

  if (connector->istate.smrlog != NULL)
    {
      smrlog_destroy (connector->istate.smrlog);
    }

  if (connector->lous != NULL)
    {
      io_stream_close (connector->lous);
    }

  if (connector->mous != NULL)
    {
      io_stream_close (connector->mous);
    }

  if (connector->epfd > 0)
    {
      close (connector->epfd);
    }

  if (connector->init.logdir != NULL)
    {
      free (connector->init.logdir);
    }

  if (connector->init.mhost != NULL)
    {
      free (connector->init.mhost);
    }

  free (connector);
}

short
smr_get_nid (smrConnector * connector)
{
  assert (connector != NULL);
  errno = 0;
  if (connector == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  return connector->init.nid;
}

int
smr_session_close (smrConnector * connector, int sid)
{
  char buf[1 + sizeof (int)];

  if (connector == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  else if (!connector->trac_enabled)
    {
      ERRNO_POINT ();
      return -1;
    }

  errno = 0;
  buf[0] = SMR_OP_SESSION_CLOSE;
  sid = htonl (sid);
  memcpy (&buf[1], &sid, sizeof (int));
  if (write_to_stream (connector->mous, buf, 1 + sizeof (int)) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }
  connector->trac_sent += sizeof (buf);

  return create_file_event (connector, connector->mfd, EP_WRITE,
			    &connector->mfd_mask, 1);
}

int
smr_session_data (smrConnector * connector, int sid, int hash,
		  char *data, int size)
{
  int length;
  char buf[1 + 3 * sizeof (int) + sizeof (long long)];

  if (connector == NULL || data == NULL || size <= 0)
    {
      errno = EINVAL;
      return -1;
    }
  else if (!connector->trac_enabled)
    {
      ERRNO_POINT ();
      return -1;
    }

  errno = 0;
  buf[0] = SMR_OP_SESSION_DATA;
  sid = htonl (sid);
  memcpy (&buf[1], &sid, sizeof (int));
  hash = htonl (hash);
  memcpy (&buf[1 + sizeof (int)], &hash, sizeof (int));
  memset (&buf[1 + 2 * sizeof (int)], 0, sizeof (long long));
  length = htonl (size);
  memcpy (&buf[1 + 2 * sizeof (int) + sizeof (long long)], &length,
	  sizeof (int));
  if (write_to_stream (connector->mous, buf, sizeof (buf)) == -1
      || write_to_stream (connector->mous, data, size) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }
  connector->trac_sent += (sizeof (buf) + size);

  return create_file_event (connector, connector->mfd, EP_WRITE,
			    &connector->mfd_mask, 1);
}

int
smr_seq_ckpted (smrConnector * connector, long long seq)
{
  char buf[1 + sizeof (long long)];
  long long net_seq;

  if (connector == NULL || seq < 0)
    {
      errno = EINVAL;
      return -1;
    }

  buf[0] = SMR_OP_SEQ_CKPTED;
  net_seq = htonll (seq);
  memcpy (&buf[1], &net_seq, sizeof (long long));
  if (write_to_stream (connector->lous, buf, sizeof (buf)) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }
  return create_file_event (connector, connector->lfd, EP_WRITE,
			    &connector->lfd_mask, 0);
}

/* Note: 'seq' is exclusive */
int
smr_release_seq_upto (smrConnector * connector, long long seq)
{
  inState *is;

  assert (connector != NULL);
  assert (seq > 0);
  is = &connector->istate;
  if (is->mm_seq_min == -1LL || is->mm_seq_min >= seq)
    {
      return 0;
    }
  if (seq > is->mm_seq_max)
    {
      seq = is->mm_seq_max;
    }

  return istate_mm_unmap_range (is, is->mm_seq_min, seq - is->mm_seq_min);
}

static char *
smr_data_get_data_ (smrData * data)
{
  return data->data;
}

char *
smr_data_get_data (smrData * data)
{
  assert (data != NULL);
  errno = 0;
  if (data == NULL)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return NULL;
    }

  return smr_data_get_data_ (data);
}

static int
smr_data_get_size_ (smrData * data)
{
  return data->size;
}

int
smr_data_get_size (smrData * data)
{
  assert (data != NULL);
  errno = 0;
  if (data == NULL)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return -1;
    }
  return smr_data_get_size_ (data);
}

static int
smr_data_ref_ (smrData * data)
{
  data->ref_count++;
  return 0;
}

int
smr_data_ref (smrData * data)
{
  assert (data != NULL);
  errno = 0;
  if (data == NULL || data->ref_count <= 0)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return -1;
    }
  return smr_data_ref_ (data);
}

static int
smr_data_unref_ (smrData * data)
{
  data->ref_count--;
  if (data->ref_count == 0)
    {
      if (data->need_free)
	{
	  if (data->data != NULL)
	    {
	      free (data->data);
	    }
	}
      free (data);
    }
  return 0;
}

int
smr_data_unref (smrData * data)
{
  assert (data != NULL);
  errno = 0;
  if (data == NULL || data->ref_count <= 0)
    {
      errno = EINVAL;
      ERRNO_POINT ();
      return -1;
    }
  return smr_data_unref_ (data);
}


int
smr_catchup (smrConnector * connector)
{
  assert (connector != NULL);

  errno = 0;
  if (connector == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  /* local catchup */
  if (process_input_state (connector, connector->cb, connector->arg) == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  /* remote catchup */
  while (connector->istate.seq < connector->master_cseq)
    {
      if (smr_process_internal (connector, 1) == -1)
	{
	  ERRNO_POINT ();
	  return -1;
	}
    }

  connector->trac_enabled = 1;
  return 0;
}

int
smr_get_poll_fd (smrConnector * connector)
{
  assert (connector != NULL);
  errno = 0;
  if (connector != NULL)
    {
      return connector->epfd;
    }
  errno = EINVAL;
  return -1;
}

static int
smr_process_internal (smrConnector * connector, int timeout)
{
  struct epoll_event events[2];
  int num_events;
  int i;
  int ret;
  int mfd;

  errno = 0;
  assert (connector != NULL);
  num_events = epoll_wait (connector->epfd, events, 2, timeout);
  if (num_events == -1)
    {
      ERRNO_POINT ();
      return -1;
    }

  mfd = connector->mfd;

  for (i = 0; i < num_events; i++)
    {
      int mask = 0;
      if (events[i].events & EPOLLIN)
	{
	  mask |= EP_READ;
	}
      else if (events[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP))
	{
	  mask |= EP_WRITE;
	}

      /* call back */
      ret = 0;
      if (events[i].data.fd == connector->mfd)
	{
	  if (mask & EP_READ)
	    {
	      ret = master_readable (connector);
	    }

	  if (ret != -1 && connector->mfd != -1 && (mask & EP_WRITE))
	    {
	      ret = master_writable (connector);
	    }
	}
      else if (events[i].data.fd == connector->lfd)
	{
	  if (mask & EP_READ)
	    {
	      ret = local_readable (connector);
	    }

	  if (ret != -1 && (mask & EP_WRITE))
	    {
	      ret = local_writable (connector);
	    }
	}
      else
	{
	  /* 
	   * this can be happended when local processing closed the fd
	   * see 'lconn' command processing' 
	   */
	  if (events[i].data.fd == mfd)
	    {
	      ret = 0;
	    }
	  else
	    {
	      ERRNO_POINT ();
	      return -1;
	    }
	}

      if (ret == -1)
	{
	  return ret;
	}
    }
  return num_events;
}

int
smr_process (smrConnector * connector)
{
  return smr_process_internal (connector, 0);
}

int
smr_enable_callback (smrConnector * connector, int enable)
{
  assert (connector != NULL);
  errno = 0;
  if (connector != NULL)
    {
      connector->cb_enabled = (enable != 0);
    }
  errno = EINVAL;
  return -1;
}

long long
smr_get_seq (smrConnector * connector)
{
  assert (connector != NULL);
  return connector->istate.seq;
}

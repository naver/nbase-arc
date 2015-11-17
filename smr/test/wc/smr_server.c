#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <time.h>
#include <stdarg.h>
#include <limits.h>

#include "dlist.h"
#include "ae.h"
#include "tcp.h"
#include "stream.h"
#include "smr.h"
#include "smr_be.h"
#include "smr_log.h"
#include "crc16.h"

/* -------- */
/* Typedefs */
/* -------- */
#define DEF_CHUNK_SIZE 7500
#define SYNC_CLOSE_MARKER ((smrClient *)(-1))
typedef struct smrClient
{
  int fd;			// socket fd
  // input stream
  char *ibuf;			// input gathering bufer
  int ibuf_sz;			// ibuf size
  char *icp;			// current input position over ibuf
  // output stream
  ioStream *ous;
  int w_prepared;		// writer prepared
  char *buf;			// buffer curreltly being sent
  int buf_sz;			// buffer size
  char *buf_cp;			// current position in buffer
} smrClient;

typedef struct serverStat
{
  //event loop count
  long long n_evloop;		// number of event loop
  //receive -----
  long long n_rb;		// number of bytes received from client
  long long n_rqst;		// number of request
  //response ----
  long long n_wb;		// number of bytes sent to client
  long long n_resp;		// number of response

} serverStat;
#define init_server_stat(ss) do { \
  (ss)->n_evloop = 0LL;           \
  (ss)->n_rb = 0LL;               \
  (ss)->n_rqst = 0LL;             \
  (ss)->n_wb = 0LL;               \
  (ss)->n_resp = 0LL;             \
} while(0)


#define EBUFSZ 512
typedef struct wcServer
{
  FILE *log_fp;
  int port;			// listen port
  int fd;			// listen socket fd
  int max_clients;		// maximum allowed number of clients
  int num_clients;		// number of active clients
  int interrupted;		// SIGTERM received
  struct timeval cron_time;	// time set by serverCron
  long long cron_teid;		// timer event id of serverCron
  serverStat stat;		// server stat
  smrClient *curr_client;	// current client
  int clients_sz;
  smrClient **clients;		// array of smrClient
  aeEventLoop *el;		// event loop
  char ebuf[EBUFSZ];		// error buffer
  /* SMR initialization related */
  int is_ready;
  int need_rckpt;
  char *ckpt_host;
  int ckpt_port;
  /* SMR replication related */
  smrConnector *smr_conn;	// SMR connector
  short nid;			// nid
  long long curr_seq;
  /* keyspace related */
  int keyspacelen;
  unsigned short *keyspace;
} wcServer;
#define CKPT_FILE "wc.ckpt"
#define CKPT_TMP_FILE "wc.ckpt.tmp"
#define FAKE_CLIENT ((smrClient *)(-1))

/* utility */
static void log_msg (char *fmt, ...);

/* recovery and chekpoint related */
static int read_checkpoint (char *filename, long long *seq);
static int checkpoint_to_file (char *filename, long long seq);

static void sigHandler (int sig);
static void setupSigtermHandler (void);
static void setupSigintHandler (void);
static void beforeSleepHandler (struct aeEventLoop *eventLoop);

/* asynchronous event processor */
static void acceptHandler (aeEventLoop * el, int fd, void *data, int mask);
static int serverCron (struct aeEventLoop *eventLoop, long long id,
		       void *clientData);

/* client structure management */
static smrClient *createClient (int fd);
static void freeClient (smrClient * client, int is_sync);

/* client query processing */
static void requestHandlerWrap (aeEventLoop * el, int fd, void *data,
				int mask);
static void requestHandler (aeEventLoop * el, int fd, void *data, int mask);
static int prepareWrite (smrClient * client);
static int writefResponse (smrClient * client, char *fmt, ...);
static int writeResponseRaw (smrClient * client, char *data, size_t len);
static void responseHandlerWrap (aeEventLoop * el, int fd, void *data,
				 int mask);
static void responseHandler (aeEventLoop * el, int fd, void *data, int mask);

/* actual processing */
static int processQuery (smrClient * client, char *buf, int bufsz);

/* SMR related functions */
static int cb_session_close (void *arg, long long seq, short nid, int sid);
static int cb_session_data (void *arg, long long seq, long long timestamp,
			    short nid, int sid, int hash, smrData * data,
			    int size);
static int cb_noti_rckpt (void *arg, char *be_host, int be_port);
static int cb_noti_ready (void *arg);
static void smrHandlerWrap (aeEventLoop * el, int fd, void *data, int mask);
static void smrHandler (aeEventLoop * el, int fd, void *data, int mask);


/* -------------- */
/* Local variable */
/* -------------- */
static wcServer server;		//server instance

static smrCallback smrCb = {
  cb_session_close,
  cb_session_data,
  cb_noti_rckpt,
  cb_noti_ready
};

/* --------------- */
/* Local functions */
/* --------------- */

static smrClient *
createClient (int fd)
{
  smrClient *client = calloc (1, sizeof (smrClient));
  if (client == NULL)
    {
      return NULL;
    }

  // init client structure
  client->fd = fd;
  client->ibuf = malloc (8192);
  if (client->ibuf == NULL)
    {
      freeClient (client, 0);
      return NULL;
    }
  client->ibuf_sz = 8192;
  client->icp = client->ibuf;

  client->ous = io_stream_create (DEF_CHUNK_SIZE);
  if (client->ous == NULL)
    {
      freeClient (client, 0);
      return NULL;
    }
  client->w_prepared = 0;
  client->buf = NULL;
  client->buf_sz = 0;
  client->buf_cp = NULL;

  // link to server
  server.clients[fd] = client;
  server.num_clients++;

  // setup asynchronous event processor
  if (aeCreateFileEvent
      (server.el, fd, AE_READABLE, requestHandlerWrap, client) == AE_ERR)
    {
      freeClient (client, 0);
      return NULL;
    }
  return client;
}

static void
freeClient (smrClient * client, int is_sync)
{
  int ret;
  assert (client != NULL);
  assert (client->fd >= 0);

  server.num_clients--;
  if (server.curr_client == client)
    {
      server.curr_client = NULL;
    }

  aeDeleteFileEvent (server.el, client->fd, AE_READABLE);
  aeDeleteFileEvent (server.el, client->fd, AE_WRITABLE);

  // unlink to server structure
  if (is_sync && client->fd > 0)
    {
      ret = smr_session_close (server.smr_conn, client->fd);
      assert (ret == 0);
      server.clients[client->fd] = SYNC_CLOSE_MARKER;
    }
  else
    {
      if (client->fd > 0)
	{
	  close (client->fd);
	}
      server.clients[client->fd] = NULL;
    }


  if (client->ibuf != NULL)
    {
      free (client->ibuf);
    }

  if (client->ous != NULL)
    {
      io_stream_close (client->ous);
    }

  free (client);
  return;
}

static void
requestHandlerWrap (aeEventLoop * el, int fd, void *data, int mask)
{
  requestHandler (el, fd, data, mask);
}


static void
requestHandler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrClient *client = (smrClient *) data;
  int avail;
  int tot_nr = 0;
  int nr;

  assert (client != NULL);

  server.curr_client = client;

  avail = client->ibuf_sz - (client->icp - client->ibuf);
  assert (avail > 0);
  while ((nr = read (client->fd, client->icp, avail)) > 0)
    {
      avail -= nr;
      client->icp += nr;
      tot_nr += nr;
      if (avail == 0)
	{
	  client->ibuf = realloc (client->ibuf, client->ibuf_sz * 2);
	  if (client->ibuf == NULL)
	    {
	      freeClient (client, 1);
	      return;
	    }
	  avail = client->ibuf_sz;
	  client->icp = client->ibuf + client->ibuf_sz;
	  client->ibuf_sz = client->ibuf_sz * 2;
	}
    }
  if (nr == 0)
    {
      log_msg ("fd:%d read returns 0. tot_nr:%d avail:%d errno:%d", fd,
	       tot_nr, avail, errno);
      freeClient (client, 1);
      return;
    }
  else if (nr == -1)
    {
      if (errno != EAGAIN)
	{
	  freeClient (client, 1);
	  return;
	}
    }

  // check a request is made
  if ((client->icp - client->ibuf) > 2 && *(client->icp - 2) == '\r'
      && *(client->icp - 1) == '\n')
    {
      int ret;

      if (strncasecmp (client->ibuf, "QUIT\r\n", 6) == 0)
	{
	  freeClient (client, 1);
	  server.curr_client = NULL;
	  return;
	}
      else if (strncasecmp (client->ibuf, "PING\r\n", 6) == 0)
	{
	  ret = writefResponse (client, "0");
	}
      else if (strncasecmp (client->ibuf, "CKPT\r\n", 6) == 0)
	{
	  ret = checkpoint_to_file (CKPT_TMP_FILE, server.curr_seq);
	  if (ret < 0)
	    {
	      log_msg ("failed to checkpoint to temp file:%s", CKPT_TMP_FILE);
	    }
	  ret = rename (CKPT_TMP_FILE, CKPT_FILE);
	  if (ret == -1)
	    {
	      log_msg ("failed to rename %s to %s", CKPT_TMP_FILE, CKPT_FILE);
	    }
	  ret = writefResponse (client, "0");
	}
      else
	{
	  ret =
	    smr_session_data (server.smr_conn, fd, SMR_SESSION_DATA_HASH_ALL,
			      client->ibuf, client->icp - client->ibuf);
	}
      assert (ret != -1);
      server.stat.n_rb += client->icp - client->ibuf;
      client->icp = client->ibuf;
    }
  server.curr_client = NULL;
  return;
}

static int
prepareWrite (smrClient * client)
{
  assert (client != NULL);

  if (client->fd > 0 && client->w_prepared == 0)
    {
      if (aeCreateFileEvent
	  (server.el, client->fd, AE_WRITABLE, responseHandlerWrap,
	   client) == AE_OK)
	{
	  client->w_prepared = 1;
	  return 0;
	}
      else
	{
	  return -1;
	}
    }

  return 0;
}

static int
writefResponse (smrClient * client, char *fmt, ...)
{
  va_list ap;
  char buf[8196];
  char *bp, *cp, *tmp = NULL;
  size_t need = 0;
  int ret;

  if (client == FAKE_CLIENT)
    {
      return 0;
    }
  va_start (ap, fmt);
  need = vsnprintf (buf, 0, fmt, ap);
  va_end (ap);

  va_start (ap, fmt);
  if (need + 3 <= sizeof (buf))
    {
      vsnprintf (buf, sizeof (buf), fmt, ap);
      bp = buf;
    }
  else
    {
      tmp = malloc (need + 3);
      if (tmp == NULL)
	{
	  return -1;
	}
      vsnprintf (tmp, need + 3, fmt, ap);
      bp = tmp;
    }
  va_end (ap);
  cp = bp + need;
  *cp++ = '\r';
  *cp++ = '\n';

  ret = writeResponseRaw (client, bp, cp - bp);
  if (tmp != NULL)
    {
      free (tmp);
    }
  return ret;
}

static int
writeResponseRaw (smrClient * client, char *data, size_t len)
{
  char *bp;
  int remain;

  assert (client != NULL);
  assert (data != NULL);

  if (prepareWrite (client) == -1)
    {
      assert (0);
      return -1;
    }

  bp = data;
  remain = (int) len;

  while (remain > 0)
    {
      char *buf = NULL;
      int avail = 0;
      int nw;

      if (io_stream_peek_write_buffer (client->ous, &buf, &avail) == -1)
	{
	  return -1;
	}

      nw = avail > remain ? remain : avail;
      memcpy (buf, bp, nw);
      remain -= nw;

      if (io_stream_commit_write (client->ous, nw) == -1)
	{
	  return -1;
	}
      server.stat.n_wb += nw;
    }
  return 0;
}


static void
responseHandlerWrap (aeEventLoop * el, int fd, void *data, int mask)
{
  responseHandler (el, fd, data, mask);
}

/**
 * Send response to the client
 *
 * This function is output stream consumer.
 * It deletes ioChunks when sent.
 *
 */
static void
responseHandler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrClient *client = (smrClient *) data;
  char *buf = NULL;
  int bufsz = 0;
  int ret;


  while (1)
    {
      int nw, avail;

      if (client->buf == NULL)
	{
	  ret = io_stream_peek_read_buffer (client->ous, &buf, &bufsz);
	  if (ret == -1)
	    {
	      return;
	    }
	  else if (bufsz == 0)
	    {
	      break;
	    }
	  client->buf = buf;
	  client->buf_cp = buf;
	  client->buf_sz = bufsz;
	}

      avail = client->buf_sz - (client->buf_cp - client->buf);
      nw = write (client->fd, client->buf_cp, avail);
      if (nw == -1)
	{
	  if (errno == EAGAIN)
	    {
	      if (io_stream_purge (client->ous, 0) == -1)
		{
		  freeClient (client, 1);
		  return;
		}
	      return;
	    }
	  else
	    {
	      freeClient (client, 1);
	      return;
	    }
	}
      else if (nw == 0)
	{
	  freeClient (client, 1);
	  return;
	}
      else
	{
	  if (io_stream_purge (client->ous, nw) == -1)
	    {
	      freeClient (client, 1);
	      return;
	    }

	  if (nw == avail)
	    {
	      client->buf = NULL;
	      client->buf_sz = 0;
	      client->buf_cp = NULL;
	      continue;
	    }
	  else
	    {
	      client->buf_cp += nw;
	    }
	}
    }

  // assert: nothing to send for now..
  aeDeleteFileEvent (server.el, client->fd, AE_WRITABLE);
  client->w_prepared = 0;
  return;
}

/*
 * Process input buffer. 
 *
 * This function is input stream consumser.
 * It deltes ioChunks these are no more used.
 */
static int
processQuery (smrClient * client, char *buf, int bufsz)
{
  char *bp, *ep, *curr;
  int i, ret = 0;

  /* client can be NULL, when restartd recovery is processed */
  assert (buf != NULL);
  assert (bufsz > 2);

  assert (*(buf + bufsz - 2) == '\r');
  assert (*(buf + bufsz - 1) == '\n');
  bp = buf;
  ep = buf + bufsz - 2;
  curr = bp;

  // set <key> <data>
  if (strncasecmp (curr, "SET ", 4) == 0)
    {
      int key;
      char *kp, *dp;

      curr += 4;
      assert (isdigit (*curr));

      kp = curr;
      while (isdigit (*curr))
	{
	  curr++;
	}
      assert (isspace (*curr));

      key = atoi (kp);
      assert (key >= 0 && key < server.keyspacelen);

      dp = ++curr;
      server.keyspace[key] = crc16 (dp, ep - dp, server.keyspace[key]);
      ret = writefResponse (client, "%d", server.keyspace[key]);
    }
  //get <key>
  else if (strncasecmp (curr, "GET ", 4) == 0)
    {
      int key;
      char *kp;

      curr += 4;		//skip command and a space
      assert (isdigit (*curr));
      kp = curr;
      while (isdigit (*curr))
	{
	  curr++;
	}
      assert (isspace (*curr));

      key = atoi (kp);
      assert (key >= 0 && key < server.keyspacelen);

      ret = writefResponse (client, "%d", server.keyspace[key]);
    }
  else if (strncasecmp (curr, "RESET\r\n", 7) == 0)
    {
      for (i = 0; i < server.keyspacelen; i++)
	{
	  server.keyspace[i] = 0;
	}
      ret = writefResponse (client, "0");
    }
  else
    {
      return writefResponse (client, "-ERR bad request");
    }

  server.stat.n_rqst++;
  server.stat.n_resp++;
  return ret;
}

static int
cb_session_close (void *arg, long long seq, short nid, int sid)
{
  if (nid == server.nid && sid > 0)
    {
      assert (server.clients[sid] == SYNC_CLOSE_MARKER);
      close (sid);
      log_msg ("session close nid:%d sid:%d", nid, sid);
      server.clients[sid] = NULL;
    }
  return 0;
}

static int
cb_session_data (void *arg, long long seq, long long timestamp, short nid,
		 int sid, int hash, smrData * smr_data, int size)
{
  char *data;
  smrClient *client;
  int ret;

  if (seq > server.curr_seq)
    {
      server.curr_seq = seq + size;
    }
  if (nid != server.nid)
    {
      client = FAKE_CLIENT;
    }
  else
    {
      client = server.clients[sid];
    }
  data = smr_data_get_data (smr_data);
  ret = processQuery (client, data, size);
  assert (ret != -1);
  smr_release_seq_upto (server.smr_conn, seq);
  return ret;
}

static int
cb_noti_rckpt (void *arg, char *be_host, int be_port)
{
  assert (be_host != NULL);
  assert (be_port > 0);

  server.need_rckpt = 1;
  server.ckpt_host = strdup (be_host);
  assert (server.ckpt_host != NULL);
  server.ckpt_port = be_port;
  aeStop (server.el);
  return 0;
}

static int
cb_noti_ready (void *arg)
{
  server.is_ready = 1;
  aeStop (server.el);
  return 0;
}

static void
smrHandlerWrap (aeEventLoop * el, int fd, void *data, int mask)
{
  smrHandler (el, fd, data, mask);
}

static void
smrHandler (aeEventLoop * el, int fd, void *data, int mask)
{
  int ret;

  if (server.interrupted)
    {
      return;
    }

  ret = smr_process (server.smr_conn);
  if (ret == -1)
    {
      log_msg ("Failed to smr_process: errno:%d", errno);
      server.interrupted++;
      aeStop (server.el);
    }
}

/* 
 * checkpoint file format 
 *
 * - seqeunce number
 * - keyspace             //string
 * - keyspace# checksum   //string
 * - keyspace# ckecksum   //string
 *   ....
 *   ....
 * - ckecksum of checksum (checksum in network byte order)
 */
static int
read_checkpoint (char *filename, long long *seq)
{
  char line[2048];
  FILE *fp = NULL;
  int keyspacelen = 0;
  unsigned short cksum_tot = 0;
  int line_num = 0;
  int i;
  long long ll;

  if (filename == NULL || seq == NULL)
    {
      return -1;
    }

  errno = 0;
  fp = fopen (filename, "r");
  if (fp == NULL)
    {
      log_msg ("checkpoint file does not exist: %s", filename);
      errno = ENOENT;
      goto error;
    }

  /* get sequence number */
  line_num++;
  if (fgets (line, 2048, fp) == NULL)
    {
      log_msg ("failed to read line: %d", line_num);
      goto error;
    }
  else
    {
      ll = strtoll (line, NULL, 10);
      if (ll < 0)
	{
	  log_msg ("failed to read sequence");
	  goto error;
	}
      *seq = ll;
    }

  /* get keyspacelen */
  line_num++;
  if (fgets (line, 2048, fp) == NULL)
    {
      log_msg ("failed to read line: %d", line_num);
      goto error;
    }
  else
    {
      keyspacelen = atoi (line);
      if (keyspacelen != server.keyspacelen)
	{
	  log_msg ("keyspace mismatch %d %d", keyspacelen,
		   server.keyspacelen);
	  goto error;
	}
    }

  for (i = 0; i < keyspacelen; i++)
    {
      line_num++;
      if (fgets (line, 2048, fp) == NULL)
	{
	  log_msg ("failed to read line: %d", line_num);
	  goto error;
	}
      else
	{
	  char *sp = strchr (line, ' ');
	  int key, ti;
	  unsigned short cksum;

	  if (sp == NULL)
	    {
	      log_msg ("invalid checkpoint file line:%s", line);
	      goto error;
	    }

	  key = atoi (line);
	  if (key < 0 || key >= keyspacelen)
	    {
	      log_msg ("invalid key:%d", key);
	      goto error;
	    }

	  ti = atoi (sp + 1);
	  if (ti < 0 || ti >= USHRT_MAX)
	    {
	      log_msg ("invalid checksum:%d", ti);
	      goto error;
	    }

	  cksum = (unsigned short) ti;
	  server.keyspace[key] = cksum;
	  cksum = htons (cksum);
	  cksum_tot =
	    crc16 ((char *) &cksum, sizeof (unsigned short), cksum_tot);
	}
    }

  /* get checksum of checksum */
  line_num++;
  if (fgets (line, 2048, fp) == NULL)
    {
      log_msg ("failed to read line: %d", line_num);
      goto error;
    }
  else
    {
      int ti;
      unsigned short tcksum;

      ti = atoi (line);
      if (ti < 0 || ti >= USHRT_MAX)
	{
	  log_msg ("invalid checksum:%d", ti);
	  goto error;
	}

      tcksum = (unsigned short) ti;
      if (tcksum != cksum_tot)
	{
	  log_msg ("checksum verification failed %d %d", tcksum, cksum_tot);
	  goto error;
	}
    }

  fclose (fp);
  return 0;

error:
  if (fp)
    {
      fclose (fp);
    }
  return -1;
}

static int
checkpoint_to_file (char *filename, long long seq)
{
  FILE *fp;
  int i;
  int ret;
  unsigned short cksum_tot = 0;

  fp = fopen (filename, "w");
  if (fp == NULL)
    {
      log_msg ("Fail to open file for write: %s", filename);
      return -1;
    }

  fprintf (fp, "%lld\n", seq);
  fprintf (fp, "%d\n", server.keyspacelen);
  for (i = 0; i < server.keyspacelen; i++)
    {
      unsigned short ns = htons (server.keyspace[i]);
      fprintf (fp, "%d %d\n", i, server.keyspace[i]);
      cksum_tot = crc16 ((char *) &ns, sizeof (unsigned short), cksum_tot);
    }

  fprintf (fp, "%d\n", cksum_tot);
  fflush (fp);

  ret = smr_seq_ckpted (server.smr_conn, seq);
  assert (ret == 0);

  fclose (fp);
  return 0;
}

static void
log_msg (char *fmt, ...)
{
  va_list ap;
  char msg_buf[8196];
  char time_buf[128];
  struct timeval tv;
  int off;

  assert (fmt != NULL);

  if (server.log_fp == NULL)
    {
      return;
    }
  va_start (ap, fmt);
  vsnprintf (msg_buf, sizeof (msg_buf), fmt, ap);
  va_end (ap);

  gettimeofday (&tv, NULL);
  off =
    strftime (time_buf, sizeof (time_buf), "%d %b %H:%M:%S.",
	      localtime (&tv.tv_sec));
  snprintf (time_buf + off, sizeof (time_buf) - off, "%03d",
	    (int) tv.tv_usec / 1000);
  fprintf (server.log_fp, "[smr-server]%s %s\n", time_buf, msg_buf);
}

static void
sigHandler (int sig)
{
  log_msg ("signal received: %d", sig);
  log_msg ("aborting...");
  server.interrupted++;
  if (server.el != NULL)
    {
      aeStop (server.el);
    }
}

static void
setupSigHupHandler ()
{
  struct sigaction act;

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sigHandler;
  sigaction (SIGHUP, &act, NULL);
}

static void
setupSigtermHandler (void)
{
  struct sigaction act;

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sigHandler;
  sigaction (SIGTERM, &act, NULL);
}


static void
setupSigintHandler (void)
{
  struct sigaction act;

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = sigHandler;
  sigaction (SIGINT, &act, NULL);
}

static void
beforeSleepHandler (struct aeEventLoop *eventLoop)
{
  server.stat.n_evloop++;
}

static void
acceptHandler (aeEventLoop * el, int fd, void *data, int mask)
{
  smrClient *client;
  int cfd;
  char cip[64];
  int cport;
  int ret;

  cfd = tcp_accept (fd, cip, &cport, server.ebuf, EBUFSZ);
  if (cfd == -1)
    {
      log_msg ("Accept failed: %s", server.ebuf);
      return;
    }

  client = createClient (cfd);
  if (client == NULL)
    {
      return;
    }

  ret = tcp_set_option (cfd, TCP_OPT_NONBLOCK | TCP_OPT_NODELAY);
  if (ret == -1)
    {
      freeClient (client, 0);
      return;
    }
}

/**
 * update timer
 */
static int
serverCron (struct aeEventLoop *eventLoop, long long id, void *clientData)
{
#if 0
  static time_t prev_time = 0;
  static serverStat prev_stat;

  gettimeofday (&server.cron_time, NULL);

  // Server performance monitor
  if (server.cron_time.tv_sec > prev_time)
    {
      // report performance
      log_msg
	("[%d]client:%d rb:%lld wb:%lld rqst:%lld resp:%lld n_evloop:%lld",
	 server.port, server.num_clients, server.stat.n_rb - prev_stat.n_rb,
	 server.stat.n_wb - prev_stat.n_wb,
	 server.stat.n_rqst - prev_stat.n_rqst,
	 server.stat.n_resp - prev_stat.n_resp,
	 server.stat.n_evloop - prev_stat.n_evloop);

      prev_time = server.cron_time.tv_sec;
      prev_stat = server.stat;
    }
#endif
  if (server.interrupted)
    {
      aeStop (server.el);
    }

  //300 Hz
  return 1000 / 300;
}

static const char *_usage =
  "smrwc-server                                           \n"
  "Usage: %s <options>                                    \n"
  "option:                                                \n"
  "    -p <replicator port>                               \n"
  "    -s <service port>                                  \n";

int
main (int argc, char *argv[])
{
  int local_replicator_port = 0;
  int service_port = 0;
  int keyspacelen = 1024;
  smrConnector *smr_conn = NULL;
  long long ckpt_seq;
  int i, ret;
  struct timeval bt;
  struct timeval et;

  // argument check
  while ((ret = getopt (argc, argv, "p:s:")) > 0)
    {
      switch (ret)
	{
	case 'p':
	  local_replicator_port = atoi (optarg);
	  if (local_replicator_port <= 0)
	    {
	      goto arg_error;
	    }
	  break;
	case 's':
	  service_port = atoi (optarg);
	  if (service_port <= 0)
	    {
	      goto arg_error;
	    }
	  break;
	default:
	  goto arg_error;
	}

    }

  if (local_replicator_port <= 0 || service_port <= 0)
    {
      goto arg_error;
    }

  // setup signal handlers
  signal (SIGHUP, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);
  setupSigHupHandler ();
  setupSigtermHandler ();
  setupSigintHandler ();

  // initialize server structure
  server.log_fp = stdout;
  server.port = service_port;
  server.fd = -1;
  server.max_clients = 1024;
  server.num_clients = 0;
  server.interrupted = 0;
  gettimeofday (&server.cron_time, NULL);
  server.cron_teid = 0LL;
  init_server_stat (&server.stat);
  server.curr_client = NULL;
  server.clients_sz = server.max_clients + 1024;
  server.clients = calloc (server.clients_sz, sizeof (smrClient *));
  if (server.clients == NULL)
    {
      log_msg ("Failed to allocate clients array");
      exit (1);
    }
  server.el = NULL;
  server.ebuf[0] = '\0';
  server.is_ready = 0;
  server.need_rckpt = 0;
  server.ckpt_host = NULL;
  server.ckpt_port = 0;
  server.smr_conn = NULL;
  server.nid = -1;
  server.curr_seq = 0LL;
  server.keyspacelen = keyspacelen;
  server.keyspace = calloc (keyspacelen, sizeof (unsigned short));
  if (server.keyspace == NULL)
    {
      log_msg ("Failed to allocate keyspace structure");
      exit (2);
    }

  // restart recovery
  ckpt_seq = 0LL;
  if (read_checkpoint ("wc.ckpt", &ckpt_seq) == -1 && errno != ENOENT)
    {
      exit (3);
    }
  server.curr_seq = ckpt_seq;

  // create event loop
  server.el = aeCreateEventLoop (server.max_clients + 1024);

  // smr connection
  errno = 0;
  server.smr_conn = smr_conn =
    smr_connect_tcp (local_replicator_port, ckpt_seq, &smrCb, NULL);
  if (server.smr_conn == NULL)
    {
      log_msg ("Failed to connect to smr errno(%d)", errno);
      exit (4);
    }

  // setup smr handler for initial notification
  if (aeCreateFileEvent (server.el, smr_get_poll_fd (smr_conn),
			 AE_READABLE, smrHandlerWrap, NULL) == AE_ERR)
    {
      log_msg ("Failed to register smrHandler handler");
      smr_disconnect (smr_conn);
      exit (5);
    }

  // wait for nitofication from replicator.
  aeMain (server.el);

  if (server.need_rckpt)
    {
      log_msg ("Need remote checkpoint from %s:%d", server.ckpt_host,
	       server.ckpt_port);
      goto finalization;
    }
  else if (!server.is_ready)
    {
      log_msg ("Invalid initiailzation state");
      goto finalization;
    }
  server.interrupted = 0;

  // catch up
  gettimeofday (&bt, NULL);
  errno = 0;
  if (smr_catchup (server.smr_conn) == -1)
    {
      log_msg ("Failed to catchup errno(%d)", errno);
      goto finalization;
    }
  gettimeofday (&et, NULL);
  do
    {
      int millis =
	(et.tv_sec - bt.tv_sec) * 1000 + (et.tv_usec - bt.tv_usec) / 1000;
      log_msg ("catchup finished in %d millis", millis);
    }
  while (0);

  // get SMR node id
  server.nid = smr_get_nid (server.smr_conn);
  if (server.nid == -1)
    {
      log_msg ("Failed to get node id from smr");
      goto finalization;
    }

  // setup server
  server.fd = tcp_server (NULL, server.port, server.ebuf, EBUFSZ);
  if (server.fd == -1)
    {
      log_msg ("%s", server.ebuf);
      goto finalization;
    }

  // setup accept handler and smr handler
  if (aeCreateFileEvent
      (server.el, server.fd, AE_READABLE, acceptHandler, NULL) == AE_ERR)
    {
      log_msg ("Failed to register event handler");
      smr_disconnect (server.smr_conn);
      server.smr_conn = NULL;
      goto finalization;
    }

  // setup application timer
  server.cron_teid = aeCreateTimeEvent (server.el, 1, serverCron, NULL, NULL);

  aeSetBeforeSleepProc (server.el, beforeSleepHandler);
  aeMain (server.el);

finalization:

  // delete malloced data
  if (server.ckpt_host != NULL)
    {
      free (server.ckpt_host);
    }

  if (server.keyspace != NULL)
    {
      free (server.keyspace);
    }

  // disconnect SMR
  if (server.smr_conn != NULL)
    {
      smr_disconnect (smr_conn);
    }

  // close listen socket
  if (server.fd != -1)
    {
      close (server.fd);
      server.fd = -1;
    }

  // delete this timer event
  if (server.cron_teid != -1)
    {
      aeDeleteTimeEvent (server.el, server.cron_teid);
      server.cron_teid = -1;
    }

  // close clients
  for (i = 0; i < server.clients_sz; i++)
    {
      if (server.clients[i] == SYNC_CLOSE_MARKER)
	{
	  close (i);
	}
      else if (server.clients[i] != NULL)
	{
	  freeClient (server.clients[i], 0);
	}
    }
  free (server.clients);

  aeDeleteEventLoop (server.el);
  server.el = NULL;

  return 0;

arg_error:
  printf (_usage, argv[0]);
  exit (6);
}

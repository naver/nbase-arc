#ifdef NBASE_ARC
#include "arc_internal.h"

#include <assert.h>
#include "smr.h"

/* ------------------ */
/* Local declarations */
/* ------------------ */
static void set_error_reason (client * c, char *err);
static void set_error_reasonf (client * c, const char *fmt, ...);
static void set_protocol_error (client * c, int pos);
static void reset_client (client * c);
static int process_inline_buffer (client * c);
static int process_multibulk_buffer (client * c);
static int get_key_hash (client * c);
static void replace_parsed_args (client * c, robj * o);
static int process_command (client * c);
static int is_client_from_lconn (client * c);
static void free_smr_argv (client * c);
static void init_client (client * c);
static void process_input_buffer (client * c);
static void read_query_from_client (aeEventLoop * el, int fd, void *privdata,
				    int mask);

/* --------------- */
/* Local functions */
/* --------------- */
static void
set_error_reason (client * c, char *err)
{
  sdsfree (c->smr->protocol_error_reply);
  c->smr->protocol_error_reply = sdsnew (err);
}

static void
set_error_reasonf (client * c, const char *fmt, ...)
{
  size_t l, j;
  va_list ap;
  va_start (ap, fmt);
  sds s = sdscatvprintf (sdsempty (), fmt, ap);
  va_end (ap);
  /* Make sure there are no newlines in the string, otherwise invalid protocol
   * is emitted. */
  l = sdslen (s);
  for (j = 0; j < l; j++)
    {
      if (s[j] == '\r' || s[j] == '\n')
	{
	  s[j] = ' ';
	}
    }
  set_error_reason (c, s);
  sdsfree (s);
}

static void
set_protocol_error (client * c, int pos)
{
  callbackInfo *cb;

  if (server.verbosity >= LL_VERBOSE)
    {
      sds cl = catClientInfoString (sdsempty (), c);
      serverLog (LL_VERBOSE, "Protocol error from client: %s", cl);
      sdsfree (cl);
    }
  c->smr->flags |= ARC_SMR_CLIENT_CLOSING;
  sdsrange (c->smr->querybuf, pos, -1);
  smr_session_close (arc.smr_conn, c->fd);

  if (c->smr->argv)
    {
      free_smr_argv (c);
      zfree (c->smr->argv);
      c->smr->argv = NULL;
    }

  cb = zmalloc (sizeof (callbackInfo));
  dlisth_insert_before (&cb->global_head, &arc.global_callbacks);
  dlisth_insert_before (&cb->client_head, &c->smr->client_callbacks);
  cb->client = c;
  cb->argc = 0;
  cb->argv = NULL;
}

/* resetClient prepare the client to process the next command */
static void
reset_client (client * c)
{
  if (c->smr->argv)
    {
      free_smr_argv (c);
      zfree (c->smr->argv);
      c->smr->argv = NULL;
    }

  sdsrange (c->smr->querybuf, c->smr->querylen, -1);
  c->smr->querylen = 0;
  c->smr->reqtype = 0;
  c->smr->multibulklen = 0;
  c->smr->bulklen = -1;
}

/* modified from original source from networking.c
 * 1. defer applying sdsrange() to querybuf 
 * 2. modify protocol error handling */
static int
process_inline_buffer (client * c)
{
  char *newline;
  int argc, j, has_cr = 0;
  sds *argv, aux;
  size_t querylen;

  /* Search for end of line */
  newline = strchr (c->smr->querybuf, '\n');

  /* Nothing to do without a \r\n */
  if (newline == NULL)
    {
      if (sdslen (c->smr->querybuf) > PROTO_INLINE_MAX_SIZE)
	{
	  set_error_reason (c, "Protocol error: too big inline request");
	  set_protocol_error (c, 0);
	}
      return C_ERR;
    }

  /* Handle the \r\n case. */
  if (newline && newline != c->smr->querybuf && *(newline - 1) == '\r')
    {
      newline--;
      has_cr = 1;
    }

  /* Split the input buffer up to the \r\n */
  querylen = newline - (c->smr->querybuf);
  aux = sdsnewlen (c->smr->querybuf, querylen);
  argv = sdssplitargs (aux, &argc);
  sdsfree (aux);
  if (argv == NULL)
    {
      set_error_reason (c, "Protocol error: unbalanced quotes in request");
      set_protocol_error (c, 0);
      return C_ERR;
    }

  /* Leave data after the first line of the query in the buffer */
  /* NBASE_ARC bug fix: Check carriage return */
  c->smr->querylen = querylen + 1 + has_cr;

  /* Setup argv array on client structure */
  assert (!c->smr->argv);
  c->smr->argv = zmalloc (sizeof (robj *) * argc);

  /* Create redis objects for all arguments. */
  for (c->smr->argc = 0, j = 0; j < argc; j++)
    {
      if (sdslen (argv[j]))
	{
	  c->smr->argv[c->smr->argc] = createObject (OBJ_STRING, argv[j]);
	  c->smr->argc++;
	}
      else
	{
	  sdsfree (argv[j]);
	}
    }
  zfree (argv);
  return C_OK;
}

/* modified from original source from networking.c
 * 1. defer applying sdsrange() to querybuf 
 * 2. modify protocol error handling 
 * 3. remove optimization */
static int
process_multibulk_buffer (client * c)
{
  char *newline = NULL;
  int pos = c->smr->querylen, ok;
  long long ll;

  if (c->smr->multibulklen == 0)
    {
      /* The client should have been reset */
      serverAssertWithInfo (c, NULL, c->smr->argc == 0);

      /* Multi bulk length cannot be read without a \r\n */
      newline = strchr (c->smr->querybuf, '\r');
      if (newline == NULL)
	{
	  if (sdslen (c->smr->querybuf) > PROTO_INLINE_MAX_SIZE)
	    {
	      set_error_reason (c,
				"Protocol error: too big mbulk count string");
	      set_protocol_error (c, 0);
	    }
	  return C_ERR;
	}

      /* Buffer should also contain \n */
      if (newline - (c->smr->querybuf) >
	  ((signed) sdslen (c->smr->querybuf) - 2))
	{
	  return C_ERR;
	}

      /* We know for sure there is a whole line since newline != NULL,
       * so go ahead and find out the multi bulk length. */
      serverAssertWithInfo (c, NULL, c->smr->querybuf[0] == '*');
      ok =
	string2ll (c->smr->querybuf + 1, newline - (c->smr->querybuf + 1),
		   &ll);
      if (!ok || ll > 1024 * 1024)
	{
	  set_error_reason (c, "Protocol error: invalid multibulk length");
	  set_protocol_error (c, pos);
	  return C_ERR;
	}

      pos = (newline - c->smr->querybuf) + 2;
      if (ll <= 0)
	{
	  /* argc == 0, reset in process_input_buffer */
	  c->smr->querylen = pos;
	  return C_OK;
	}

      c->smr->multibulklen = ll;

      /* Setup argv array on client structure */
      assert (!c->smr->argv);
      c->smr->argv = zmalloc (sizeof (robj *) * c->smr->multibulklen);
    }

  serverAssertWithInfo (c, NULL, c->smr->multibulklen > 0);
  while (c->smr->multibulklen)
    {
      /* Read bulk length if unknown */
      if (c->smr->bulklen == -1)
	{
	  newline = strchr (c->smr->querybuf + pos, '\r');
	  if (newline == NULL)
	    {
	      if (sdslen (c->smr->querybuf) - pos > PROTO_INLINE_MAX_SIZE)
		{
		  set_error_reason (c,
				    "Protocol error: too big bulk count string");
		  set_protocol_error (c, 0);
		}
	      break;
	    }

	  /* Buffer should also contain \n */
	  if (newline - (c->smr->querybuf) >
	      ((signed) sdslen (c->smr->querybuf) - 2))
	    break;

	  if (c->smr->querybuf[pos] != '$')
	    {
	      set_error_reasonf (c,
				 "Protocol error: expected '$', got '%c'",
				 c->smr->querybuf[pos]);
	      set_protocol_error (c, pos);
	      return C_ERR;
	    }

	  ok =
	    string2ll (c->smr->querybuf + pos + 1,
		       newline - (c->smr->querybuf + pos + 1), &ll);
	  if (!ok || ll < 0 || ll > 512 * 1024 * 1024)
	    {
	      set_error_reason (c, "Protocol error: invalid bulk length");
	      set_protocol_error (c, pos);
	      return C_ERR;
	    }

	  pos += newline - (c->smr->querybuf + pos) + 2;
	  c->smr->bulklen = ll;
	}

      /* Read bulk argument */
      if (sdslen (c->smr->querybuf) - pos < (unsigned) (c->smr->bulklen + 2))
	{
	  /* Not enough data (+2 == trailing \r\n) */
	  break;
	}
      else
	{
	  c->smr->argv[c->smr->argc++] =
	    createStringObject (c->smr->querybuf + pos, c->smr->bulklen);
	  pos += c->smr->bulklen + 2;
	  c->smr->bulklen = -1;
	  c->smr->multibulklen--;
	}
    }

  /* Save pos */
  c->smr->querylen = pos;

  /* We're done when c->multibulk == 0 */
  if (c->smr->multibulklen == 0)
    return C_OK;

  /* Still not read to process the command */
  return C_ERR;
}

static int
get_key_hash (client * c)
{
  /* If the query is readonly type or invalid, replicating log isn't necessary.
   * So, keyhash is not requried */
  if (!c->smr->cmd || c->smr->cmd->flags & CMD_READONLY)
    {
      return SMR_SESSION_DATA_HASH_NONE;
    }
  else if ((c->smr->cmd->arity > 0 && c->smr->cmd->arity != c->smr->argc) ||
	   (c->smr->argc < -c->smr->cmd->arity))
    {
      return SMR_SESSION_DATA_HASH_NONE;
    }

  if (c->smr->cmd->firstkey == c->smr->cmd->lastkey)
    {
      /* If the query is single key type, keyhash of the key is required for migration */
      sds key = c->smr->argv[c->smr->cmd->firstkey]->ptr;
      return crc16 (key, sdslen (key));
    }
  else
    {
      /* If the query is multi key type, this query should always be copied for migration */
      return SMR_SESSION_DATA_HASH_ALL;
    }
}

static void
replace_parsed_args (client * c, robj * o)
{
  free_smr_argv (c);
  zfree (c->smr->argv);
  c->smr->argv = NULL;

  c->smr->argv = zmalloc (sizeof (robj *) * 2);
  incrRefCount (shared.addreply_through_smr);
  c->smr->argv[0] = shared.addreply_through_smr;
  c->smr->argv[1] = o;
  c->smr->argc = 2;
  c->smr->cmd = NULL;
}

static int
process_command (client * c)
{
  int keyhash = 0, cmdflags = 0;
  int sid;
  callbackInfo *cb;

  c->smr->cmd = lookupCommand (c->smr->argv[0]->ptr);

  /* c->smr->cmd == NULL is permitted. because responses for the pipelined
   * requests must be sent in order. (NO SUCH COMMAND should be included) */
  if (c->smr->cmd != NULL)
    {
      if (c->smr->cmd->proc == bpingCommand)
	{
	  bpingCommand (c);
	  return C_OK;
	}
      else if (c->smr->cmd->proc == quitCommand)
	{
	  quitCommand (c);
	}
      cmdflags = c->smr->cmd->flags;

#ifndef COVERAGE_TEST
      /* if the command is not permitted in cluster environment,
       * act as if this command does not exist */
      if (c->smr->cmd->flags & CMD_NOCLUSTER)
	{
	  replace_parsed_args (c,
			       createStringObject
			       ("-ERR Unsupported Command\r\n", 26));
	  cmdflags = 0;
	}
#endif
    }

  /* If this partition group is in OOM state, reply oom error message. 
   * The message has to be in order through smr log stream.
   *
   * we can use mstime of local machine for checking oom, 
   * because message is not inserted to smr log, yet. */
  if (mstime () < arc.smr_oom_until && (cmdflags & CMD_DENYOOM))
    {
      incrRefCount (shared.oomerr);
      replace_parsed_args (c, shared.oomerr);
      cmdflags = 0;
    }

  keyhash = get_key_hash (c);
  assert (c->fd < 65535);
  sid = (0xFFFF0000 & (cmdflags << 16)) | (0x0000FFFF & c->fd);

  /* save parsed argv to reuse after smr callback is invoked */
  cb = zmalloc (sizeof (callbackInfo));
  dlisth_insert_before (&cb->global_head, &arc.global_callbacks);
  dlisth_insert_before (&cb->client_head, &c->smr->client_callbacks);
  cb->client = c;
  cb->argc = c->smr->argc;
  cb->argv = c->smr->argv;
  cb->hash = keyhash;
  c->smr->argc = 0;
  c->smr->argv = NULL;

  if (cmdflags & CMD_WRITE)
    {
      smr_session_data (arc.smr_conn, sid, keyhash, c->smr->querybuf,
			c->smr->querylen);
    }
  else
    {
      smr_session_data (arc.smr_conn, sid, keyhash, "R", 1);
    }

  return C_OK;
}

static int
is_client_from_lconn (client * c)
{

  char ip[NET_IP_STR_LEN];
  int port;
  int retval;

  if (c->flags & CLIENT_UNIX_SOCKET)
    {
      /* Unix socket client. */
      return 1;
    }
  else
    {
      /* Tcp client. */
      retval = anetPeerToString (c->fd, ip, sizeof (ip), &port);
      return (retval == -1) ? 0 : arcx_is_local_ip_addr (arc.local_ip_addrs,
							 ip);
    }
}

static void
free_smr_argv (client * c)
{
  int j;
  for (j = 0; j < c->smr->argc; j++)
    {
      decrRefCount (c->smr->argv[j]);
    }
  c->smr->argc = 0;
  c->smr->cmd = NULL;
}

static void
init_client (client * c)
{
  /* If this is a fake client(fd == -1) for executing smr log or lua script
   * , smr data structure is not necessary.*/
  if (c->fd == -1)
    {
      return;
    }
  serverAssert (c->fd > 0);
  c->smr = zmalloc (sizeof (struct arcClient));
  c->smr->querybuf = sdsempty ();
  c->smr->querybuf_peak = 0;
  c->smr->querylen = 0;
  c->smr->argc = 0;
  c->smr->argv = NULL;
  dlisth_init (&c->smr->client_callbacks);
  c->smr->cmd = NULL;
  c->smr->reqtype = 0;
  c->smr->multibulklen = 0;
  c->smr->bulklen = -1;
  c->smr->flags = 0;
  c->smr->protocol_error_reply = NULL;
  c->smr->ckptdbfd = -1;
  c->smr->ckptdboff = 0;
  c->smr->ckptdbsize = 0;
}

static void
process_input_buffer (client * c)
{
  /* Keep processing while there is something in the input buffer */
  while (sdslen (c->smr->querybuf))
    {
      /* Immediately abort if the client is in the middle of something. */
      if (c->flags & CLIENT_BLOCKED)
	{
	  return;
	}

      /* ARC_SMR_CLIENT_CLOSING closes the connection once the reply is
       * written to the client. Make sure to not let the reply grow after
       * this flag has been set (i.e. don't process more commands). */
      if (c->smr->flags & ARC_SMR_CLIENT_CLOSING)
	{
	  return;
	}

      /* Determine request type when unknown. */
      if (!c->smr->reqtype)
	{
	  if (c->smr->querybuf[0] == '*')
	    {
	      c->smr->reqtype = PROTO_REQ_MULTIBULK;
	    }
	  else
	    {
	      c->smr->reqtype = PROTO_REQ_INLINE;
	    }
	}

      if (c->smr->reqtype == PROTO_REQ_INLINE)
	{
	  if (process_inline_buffer (c) != C_OK)
	    break;
	}
      else if (c->smr->reqtype == PROTO_REQ_MULTIBULK)
	{
	  if (process_multibulk_buffer (c) != C_OK)
	    break;
	}
      else
	{
	  serverPanic ("Unknown request type");
	}

      /* Multibulk processing could see a <= 0 length. */
      if (c->smr->argc > 0)
	{
	  process_command (c);
	}
      reset_client (c);
    }
}

static void
read_query_from_client (aeEventLoop * el, int fd, void *privdata, int mask)
{
  client *c = (client *) privdata;
  int nread, readlen;
  size_t qblen;
  UNUSED (el);
  UNUSED (mask);

  server.current_client = c;
  readlen = PROTO_IOBUF_LEN;

  qblen = sdslen (c->smr->querybuf);
  if (c->smr->querybuf_peak < qblen)
    {
      c->smr->querybuf_peak = qblen;
    }
  c->smr->querybuf = sdsMakeRoomFor (c->smr->querybuf, readlen);
  nread = read (fd, c->smr->querybuf + qblen, readlen);
  if (nread == -1)
    {
      if (errno == EAGAIN)
	{
	  nread = 0;
	}
      else
	{
	  serverLog (LL_VERBOSE, "Reading from client: %s", strerror (errno));
	  freeClient (c);
	  return;
	}
    }
  else if (nread == 0)
    {
      serverLog (LL_VERBOSE, "Client closed connection");
      freeClient (c);
      return;
    }
  if (nread)
    {
      sdsIncrLen (c->smr->querybuf, nread);
      c->lastinteraction = server.unixtime;
    }
  else
    {
      server.current_client = NULL;
      return;
    }
  if (sdslen (c->smr->querybuf) > server.client_max_querybuf_len)
    {
      sds ci = catClientInfoString (sdsempty (), c), bytes = sdsempty ();

      bytes = sdscatrepr (bytes, c->smr->querybuf, 64);
      serverLog (LL_WARNING,
		 "Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)",
		 ci, bytes);
      sdsfree (ci);
      sdsfree (bytes);
      freeClient (c);
      return;
    }
  process_input_buffer (c);
  server.current_client = NULL;
}

/* ---------------------- */
/* Exported arc functions */
/* ---------------------- */
void
arc_smrc_create (client * c)
{
  c->smr = NULL;
  // supersede read handler
  if (arc.cluster_mode)
    {
      // hack: previous call was successful in createClient. no error check is needed
      (void) aeCreateFileEvent (server.el, c->fd, AE_READABLE,
				read_query_from_client, c);
      init_client (c);
    }
}

void
arc_smrc_free (client * c)
{
  if (c == arc.checkpoint_client)
    {
      arc.checkpoint_client = NULL;
    }

  if (c->smr != NULL)
    {
      sdsfree (c->smr->querybuf);
      c->smr->querybuf = NULL;

      free_smr_argv (c);
      zfree (c->smr->argv);
      c->smr->argv = NULL;

      while (!dlisth_is_empty (&c->smr->client_callbacks))
	{
	  dlisth *node = c->smr->client_callbacks.next;
	  dlisth_delete (node);

	  /* offset from client_head */
	  callbackInfo *cb =
	    (callbackInfo *) ((char *) node -
			      offsetof (callbackInfo, client_head));
	  cb->client = arc.smrlog_client;	/* change to fake client for smrlog */
	}

      sdsfree (c->smr->protocol_error_reply);
      c->smr->protocol_error_reply = NULL;

      if (c->smr->ckptdbfd != -1)
	{
	  close (c->smr->ckptdbfd);
	  c->smr->ckptdbfd = -1;
	}

      zfree (c->smr);
      c->smr = NULL;
    }
}

void
arc_smrc_accept_bh (client * c)
{
  if (arc.cluster_mode && is_client_from_lconn (c))
    {
      c->flags |= CLIENT_LOCAL_CONN;
    }
}

void
arc_smrc_set_protocol_error (client * c)
{
  if (arc.cluster_mode)
    {
      serverLog (LL_WARNING, "Protocol error query len:%ld buf:%s",
		 sdslen (c->querybuf), c->querybuf);
      serverPanic ("Protocol error from replication log");
    }
}

void
arc_smrc_try_process (client * c)
{
  if (arc.cluster_mode)
    {
      if (c->smr->querybuf && sdslen (c->smr->querybuf) > 0)
	{
	  process_input_buffer (c);
	  server.current_client = NULL;
	}
    }
}

/* ----------------------- */
/* Exported Redis commands */
/* ----------------------- */

/* Backend Ping command which is not replicated in smr */
void
bpingCommand (client * c)
{
  addReply (c, shared.pong);
}

/* To block input after this command */
void
quitCommand (client * c)
{
  c->smr->flags |= ARC_SMR_CLIENT_CLOSING;
}

#else
// make compiler happy
int arc_networking_is_not_used = 1;
#endif

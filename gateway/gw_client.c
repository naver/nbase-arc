#include "gw_client.h"

// Forward declaration
static void cli_read_handler (aeEventLoop * el, int fd, void *privdata,
			      int mask);
static void cli_send_handler (aeEventLoop * el, int fd, void *privdata,
			      int mask);
static void rqst_free (client_request * rqst);
static void close_client (client * cli);

static int
cli_cron (aeEventLoop * el, long long id, void *privdata)
{
  client_pool *pool = privdata;
  int *hz = &pool->hz;
  int *cronloops = &pool->cronloops;
  GW_NOTUSED (el);
  GW_NOTUSED (id);

  (*cronloops)++;
  return 1000 / *hz;
}

client_pool *
cli_pool_create (aeEventLoop * el, command_manager * cmd_mgr,
		 long long rqst_timeout, long long conn_inactive_timeout,
		 sbuf_hdr * shared_stream, sbuf_mempool * mp_sbuf)
{
  client_pool *pool;

  pool = zmalloc (sizeof (client_pool));
  pool->el = el;
  pool->cron_teid = aeCreateTimeEvent (el, 1000, cli_cron, pool, NULL);
  pool->hz = GW_DEFAULT_HZ;
  pool->cronloops = 0;
  pool->cmd_mgr = cmd_mgr;
  pool->nclient = 0;
  TAILQ_INIT (&pool->clients);
  pool->shared_stream = shared_stream;
  pool->rqst_timeout = rqst_timeout;
  pool->conn_inactive_timeout = conn_inactive_timeout;

  // Memory pool
  pool->mp_client_request =
    mempool_create (sizeof (client_request), MEMPOOL_DEFAULT_POOL_SIZE);
  pool->mp_parse_ctx = createParserMempool ();
  pool->mp_sbuf = mp_sbuf;

  return pool;
}

void
cli_pool_destroy (client_pool * pool)
{
  client *cli, *tvar;

  aeDeleteTimeEvent (pool->el, pool->cron_teid);
  TAILQ_FOREACH_SAFE (cli, &pool->clients, client_tqe, tvar)
  {
    close_client (cli);
  }
  mempool_destroy (pool->mp_client_request);
  destroyParserMempool (pool->mp_parse_ctx);
  zfree (pool);
}

int
cli_add_client (client_pool * pool, int fd)
{
  client *cli;

  assert (fd != -1);

  cli = zmalloc (sizeof (client));
  anetNonBlock (NULL, fd);
  anetEnableTcpNoDelay (NULL, fd);
  anetKeepAlive (NULL, fd, GW_DEFAULT_TCP_KEEPALIVE);

  if (aeCreateFileEvent (pool->el, fd, AE_READABLE, cli_read_handler, cli) ==
      AE_ERR)
    {
      zfree (cli);
      return ERR;
    }

  cli->my_pool = pool;
  cli->fd = fd;
  cli->stream = stream_hdr_create (SBUF_DEFAULT_PAGESIZE, pool->mp_sbuf);
  cli->parse_ctx = createParseContext (pool->mp_parse_ctx, cli->stream);
  cli->nrqstq = 0;
  TAILQ_INIT (&cli->rqst_q);
  cli->close_after_reply = 0;

  pool->nclient++;
  TAILQ_INSERT_TAIL (&pool->clients, cli, client_tqe);

  return OK;
}

static void
close_client (client * cli)
{
  client_pool *pool;
  client_request *rqst, *tvar;

  pool = cli->my_pool;
  aeDeleteFileEvent (pool->el, cli->fd, AE_WRITABLE | AE_READABLE);
  close (cli->fd);
  cli->fd = -1;

  pool->nclient--;
  TAILQ_REMOVE (&pool->clients, cli, client_tqe);
  stream_delegate_data_and_free (pool->shared_stream, cli->stream);
  deleteParseContext (cli->parse_ctx);

  TAILQ_FOREACH_SAFE (rqst, &cli->rqst_q, request_tqe, tvar)
  {
    rqst_free (rqst);
  }

  zfree (cli);
}

static client_request *
rqst_create (client * cli)
{
  client_pool *pool = cli->my_pool;
  client_request *rqst;

  rqst = mempool_alloc (pool->mp_client_request);
  rqst->my_client = cli;
  rqst->cmd_ctx = NULL;
  rqst->reply = NULL;
  rqst->reply_received = 0;

  cli->nrqstq++;
  TAILQ_INSERT_TAIL (&cli->rqst_q, rqst, request_tqe);

  return rqst;
}

static void
rqst_free (client_request * rqst)
{
  client *cli = rqst->my_client;
  client_pool *pool = cli->my_pool;

  cli = rqst->my_client;

  assert (cli->nrqstq > 0);
  cli->nrqstq--;
  TAILQ_REMOVE (&cli->rqst_q, rqst, request_tqe);

  if (!rqst->reply_received)
    {
      cmd_cancel (rqst->cmd_ctx);
      rqst->cmd_ctx = NULL;
    }
  else
    {
      assert (rqst->reply != NULL);
      sbuf_free (rqst->reply);
      rqst->reply = NULL;
    }
  mempool_free (pool->mp_client_request, rqst);
}

static int
client_reply_ready (client * cli)
{
  client_request *rqst;

  if (TAILQ_EMPTY (&cli->rqst_q))
    {
      return 0;
    }
  rqst = TAILQ_FIRST (&cli->rqst_q);
  if (!rqst->reply_received)
    {
      return 0;
    }
  return 1;
}

static void
enable_cli_send_handler (client * cli)
{
  client_pool *pool;

  pool = cli->my_pool;

  if (!client_reply_ready (cli))
    {
      return;
    }

  if (!(aeGetFileEvents (pool->el, cli->fd) & AE_WRITABLE))
    {
      if (aeCreateFileEvent
	  (pool->el, cli->fd, AE_WRITABLE, cli_send_handler, cli) == AE_ERR)
	{
	  close_client (cli);
	}
    }
}

static void
cli_reply_handler (sbuf * reply, void *cbarg)
{
  client_request *rqst = cbarg;
  client *cli;

  cli = rqst->my_client;
  rqst->reply = reply;
  rqst->reply_received = 1;

  if (cmd_is_close_after_reply (rqst->cmd_ctx))
    {
      cli->close_after_reply = 1;
    }
  rqst->cmd_ctx = NULL;

  enable_cli_send_handler (cli);
}

static void
close_after_reply_error (client * cli, sds err)
{
  client_request *rqst;
  sbuf *reply;
  size_t l, i;

  // Make sure there are no newlines in string
  l = sdslen (err);
  for (i = 0; i < l; i++)
    {
      if (err[i] == '\r' || err[i] == '\n')
	err[i] = ' ';
    }

  reply =
    stream_create_sbuf_printf (cli->my_pool->shared_stream, "-ERR %s\r\n",
			       err);
  assert (reply);

  rqst = rqst_create (cli);
  rqst->reply = reply;
  rqst->reply_received = 1;
  cli->close_after_reply = 1;

  enable_cli_send_handler (cli);
}

static void
cli_read_handler (aeEventLoop * el, int fd, void *privdata, int mask)
{
  client *cli = privdata;
  client_pool *pool;
  sbuf *query;
  client_request *rqst;
  ssize_t nread;
  sds err;
  int ret;
  GW_NOTUSED (el);
  GW_NOTUSED (mask);

  pool = cli->my_pool;

  nread = stream_append_read (cli->stream, fd);
  if (nread <= 0)
    {
      if (nread == -1 && errno == EAGAIN)
	{
	  return;
	}
      goto close_cli;
    }

  if (cli->close_after_reply)
    return;

  do
    {
      ret = requestParser (cli->parse_ctx, cli->stream, &query, &err);
      if (ret == PARSE_COMPLETE)
	{
	  if (getArgumentCount (cli->parse_ctx) == 0)
	    {
	      // Empty request, reply nothing
	      sbuf_free (query);
	      resetParseContext (cli->parse_ctx, cli->stream);
	      continue;
	    }

	  rqst = rqst_create (cli);
	  rqst->cmd_ctx =
	    cmd_create_ctx (pool->cmd_mgr, query, cli->parse_ctx,
			    cli_reply_handler, rqst);
	  cli->parse_ctx =
	    createParseContext (pool->mp_parse_ctx, cli->stream);

	  cmd_send_command (rqst->cmd_ctx);
	  if (cli->close_after_reply)
	    {
	      break;
	    }
	}
      else if (ret == PARSE_ERROR)
	{
	  close_after_reply_error (cli, err);
	  sdsfree (err);
	  break;
	}
    }
  while (ret != PARSE_INSUFFICIENT_DATA);

  return;

close_cli:
  close_client (cli);
}

static void
cli_send_handler (aeEventLoop * el, int fd, void *privdata, int mask)
{
  client *cli = privdata;
  client_request *rqst, *tvar;
  sbuf *bufv[SBUF_IOV_MAX];
  ssize_t nwritten;
  int nbuf;
  GW_NOTUSED (mask);

  nbuf = 0;
  TAILQ_FOREACH (rqst, &cli->rqst_q, request_tqe)
  {
    if (nbuf == SBUF_IOV_MAX)
      {
	break;
      }
    if (!rqst->reply_received)
      {
	break;
      }
    bufv[nbuf++] = rqst->reply;
  }
  assert (nbuf > 0);

  nwritten = sbuf_writev (bufv, nbuf, fd);
  if (nwritten <= 0)
    {
      if (nwritten == -1 && errno == EAGAIN)
	{
	  return;
	}
      goto close_cli;
    }

  TAILQ_FOREACH_SAFE (rqst, &cli->rqst_q, request_tqe, tvar)
  {
    if (!rqst->reply || sbuf_check_write_finish (rqst->reply) == ERR)
      {
	break;
      }
    rqst_free (rqst);
  }

  if (!client_reply_ready (cli))
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
    }
  if (cli->close_after_reply && TAILQ_EMPTY (&cli->rqst_q))
    {
      goto close_cli;
    }
  return;

close_cli:
  close_client (cli);
}

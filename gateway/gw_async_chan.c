#include "gw_async_chan.h"

static void async_receive_event (aeEventLoop * el, int fd, void *privdata,
				 int mask);
static void async_notify_event (aeEventLoop * el, int fd, void *privdata,
				int mask);

async_chan *
async_create (aeEventLoop * el, void *chan_data)
{
  async_chan *chan;
  int ret;

  chan = zmalloc (sizeof (async_chan));
  chan->el = el;
  if (pipe (chan->pipefd) == -1)
    {
      zfree (chan);
      return NULL;
    }
  anetNonBlock (NULL, chan->pipefd[ASYNC_PIPE_READ]);
  anetNonBlock (NULL, chan->pipefd[ASYNC_PIPE_WRITE]);

  if (aeCreateFileEvent
      (el, chan->pipefd[ASYNC_PIPE_READ], AE_READABLE, async_receive_event,
       chan) == AE_ERR)
    {
      close (chan->pipefd[ASYNC_PIPE_READ]);
      close (chan->pipefd[ASYNC_PIPE_WRITE]);
      zfree (chan);
      return NULL;
    }

  ret = pthread_mutex_init (&chan->q_lock, NULL);
  assert (ret == 0);
  TAILQ_INIT (&chan->event_q);

  chan->chan_data = chan_data;

  return chan;
}

void
async_destroy (async_chan * chan)
{
  aeDeleteFileEvent (chan->el, chan->pipefd[ASYNC_PIPE_READ], AE_READABLE);
  aeDeleteFileEvent (chan->el, chan->pipefd[ASYNC_PIPE_WRITE], AE_WRITABLE);
  close (chan->pipefd[ASYNC_PIPE_READ]);
  close (chan->pipefd[ASYNC_PIPE_WRITE]);
  pthread_mutex_destroy (&chan->q_lock);
  zfree (chan);
}

void
async_send_event (aeEventLoop * my_el, async_chan * target,
		  async_exec_proc * exec, void *arg)
{
  async_event *event;
  int notify;

  event = zmalloc (sizeof (async_event));
  event->exec = exec;
  event->arg = arg;

  notify = 0;

  pthread_mutex_lock (&target->q_lock);
  if (TAILQ_EMPTY (&target->event_q))
    {
      notify = 1;
    }
  TAILQ_INSERT_TAIL (&target->event_q, event, async_event_tqe);
  pthread_mutex_unlock (&target->q_lock);

  if (notify)
    {
      if (aeCreateFileEvent
	  (my_el, target->pipefd[ASYNC_PIPE_WRITE], AE_WRITABLE,
	   async_notify_event, NULL) == AE_ERR)
	{
	  assert (0);
	}
    }
}

static void
async_receive_event (aeEventLoop * el, int fd, void *privdata, int mask)
{
  async_chan *chan = privdata;
  async_event *event;
  char dummy[ASYNC_IOBUF_SIZE];
  ssize_t nread;
  int empty;
  GW_NOTUSED (el);
  GW_NOTUSED (mask);

  nread = read (fd, dummy, ASYNC_IOBUF_SIZE);
  if (nread == 0 || (nread == -1 && errno != EAGAIN))
    {
      assert (0);
    }

  empty = 0;
  do
    {
      pthread_mutex_lock (&chan->q_lock);
      if (TAILQ_EMPTY (&chan->event_q))
	{
	  empty = 1;
	}
      else
	{
	  event = TAILQ_FIRST (&chan->event_q);
	  TAILQ_REMOVE (&chan->event_q, event, async_event_tqe);
	}
      pthread_mutex_unlock (&chan->q_lock);

      if (!empty)
	{
	  event->exec (event->arg, chan->chan_data);
	  zfree (event);
	}
    }
  while (!empty);
}

static void
async_notify_event (aeEventLoop * el, int fd, void *privdata, int mask)
{
  char dummy = 0;
  ssize_t nwritten;
  GW_NOTUSED (privdata);
  GW_NOTUSED (mask);

  nwritten = write (fd, &dummy, 1);
  if (nwritten <= 0)
    {
      if (nwritten == -1 && errno == EAGAIN)
	{
	  return;
	}
      assert (0);
    }

  aeDeleteFileEvent (el, fd, AE_WRITABLE);
}

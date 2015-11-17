#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include "stream.h"


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

      if (io_stream_peek_write_buffer (ios, &buf, &avail) == -1)
	{
	  return -1;
	}

      nw = avail > remain ? remain : avail;
      memcpy (buf, bp, nw);
      remain -= nw;
      bp += nw; 

      if (io_stream_commit_write (ios, nw) == -1)
	{
	  return -1;
	}
    }
  return 0;
}

static int
stream_read (ioStream * ios, void *buf_, int size, int purge)
{
  int remain = size;
  char *bp = buf_;

  while (remain > 0)
    {
      char *buf;
      int avail = 0;
      int nw;

      if (io_stream_peek_read_buffer (ios, &buf, &avail) == -1)
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

      if (purge && io_stream_purge (ios, nw) == -1)
	{
	  return -1;
	}

      if (remain == 0 && (avail - nw) > 0
	  && io_stream_rewind_readpos (ios, avail - nw) == -1)
	{
	  return -1;
	}
    }

  assert (remain == 0);
  return size - remain;
}

static int
network_mimic_test(int bufsz, int out_bufsz, int in_bufsz)
{
  char *send_buf = NULL;
  char *recv_buf = NULL;
  ioStream *ous = NULL;
  ioStream *ins = NULL;
  int i;
  char *bp;
  char *ep;
  int ret;

  /* send random seed */
  srand(100);

  /* make buffers and streams */
  send_buf = malloc(bufsz);
  if(send_buf == NULL)
    {
      goto error;
    }

  recv_buf = malloc(bufsz);
  if(recv_buf == NULL)
    {
      goto error;
    }

  ous = io_stream_create(out_bufsz);
  if(ous == NULL)
    {
      goto error;
    }

  ins = io_stream_create(in_bufsz);
  if(ins == NULL)
    {
      goto error;
    }

  /* make send_buf (deterministic) */
  for(i = 0; i < 1024*1024; i++)
    {
      send_buf[i] = "0123456789"[i % 10];
    }

  /* send only */
  bp = send_buf;
  ep = send_buf + bufsz;

  while(bp < ep)
    {
      int max_send = ep - bp;
      int to_send = rand() % 50 + 50; // [50, 100)

      to_send = max_send < to_send ? max_send : to_send;
      ret = stream_write(ous, bp, to_send);
      assert(ret == 0);
      bp += to_send;
    }

  bp = recv_buf;
  ep = recv_buf + bufsz;
  while(bp < ep)
    {
      int max_recv = ep - bp;
      int to_recv = rand() % 50 + 50; // [50, 100)

      to_recv = max_recv < to_recv ? max_recv : to_recv;
      ret = stream_read(ous, bp, to_recv, 0);
      assert(ret == to_recv);
      bp += to_recv;
    }

  for(i = 0; i < bufsz; i++)
    {
      assert(send_buf[i] == recv_buf[i]);
    }

  free(send_buf);
  free(recv_buf);
  io_stream_close(ins);
  io_stream_close(ous);
  return 0;

error:
  if(send_buf != NULL)
    {
      free(send_buf);
    }
  if(recv_buf != NULL)
    {
      free(recv_buf);
    }
  if(ins != NULL)
    {
      io_stream_close(ins);
    }
  if(ous != NULL)
    {
      io_stream_close(ous);
    }
  return -1;
}

int
main (int argc, char *argv[])
{
  ioStream *stream;
  int ret;
  char *buff, *buff2;
  int avail, avail2, i;

  stream = io_stream_create (8000);
  assert (stream != NULL);

  // invalid argument
  ret = io_stream_peek_write_buffer (NULL, NULL, NULL);
  assert (ret == -1);
  assert (errno == EINVAL);

  // should get all
  ret = io_stream_peek_write_buffer (stream, &buff, &avail);
  assert (ret == 0);
  assert (avail == 8000);

  // double peek must be a error
  ret = io_stream_peek_write_buffer (stream, &buff2, &avail2);
  assert (ret == -1);
  assert (errno == EACCES);

  //commit 100 bytes
  memset (buff, 1, 100);
  ret = io_stream_commit_write (stream, 100);
  assert (ret == 0);

  // peek and rewind should be idempotent
  ret = io_stream_peek_read_buffer (stream, &buff, &avail);
  assert (ret == 0);
  assert (avail == 100);

  ret = io_stream_rewind_readpos(stream, avail);
  assert (ret == 0);

  // read buff should return 100 bytes
  ret = io_stream_peek_read_buffer (stream, &buff, &avail);
  assert (ret == 0);
  assert (avail == 100);
  for (i = 0; i < 100; i++)
    {
      if (buff[i] != (char) 1)
	{
	  break;
	}
    }
  assert (i == 100);

  // purge 
  ret = io_stream_purge (stream, 0);
  assert (ret == 0);
  ret = io_stream_purge (stream, 50);
  assert (ret == 0);
  ret = io_stream_purge (stream, 50);
  assert (ret == 0);

  // all purged and no pending write means new (reset) stream
  ret = io_stream_peek_write_buffer (stream, &buff, &avail);
  assert (ret == 0);
  assert (avail == 8000);

  // meaningless purge 
  ret = io_stream_purge (stream, 50);
  assert (ret == -1 && errno == EACCES);
  ret = io_stream_purge (stream, 100000);
  assert (ret == -1 && errno == EACCES);

  ret = io_stream_commit_write (stream, 0);
  assert (ret == 0);

  // test reset read and avail for read
  for (i = 0; i < 1000; i++)
    {
      int j;

      buff2 = NULL;
      for (j = 0; j <= i; j++)
	{
	  ret = io_stream_peek_write_buffer (stream, &buff, &avail);
	  assert (ret == 0);
	  assert (avail >= 100);	//8000 is multiple of 100
	  memset (buff, 2, 100);
	  if (buff2 == NULL)
	    {
	      buff2 = buff;
	    }

	  ret = io_stream_commit_write (stream, 100);
	  assert (ret == 0);
	}
      avail = io_stream_avail_for_read(stream);
      assert(avail == (i+1)*100);

      for (j = 0; j <= i; j++)
	{
	  int av, av2;

	  ret = io_stream_peek_read_buffer (stream, &buff, &av);
	  assert (ret == 0);
	  assert (av >= 0);

	  avail -= av;
	  av2 = io_stream_avail_for_read(stream);
	  assert(av2 == avail);
	  if (av == 0)
	    {
	      break;
	    }
	}
      assert (j > 0);

      ret = io_stream_reset_read (stream);
      assert (ret == 0);
      ret = io_stream_peek_read_buffer (stream, &buff, &avail);
      assert (ret == 0);
      assert (buff = buff2);

      //reset all
      io_stream_close (stream);
      stream = io_stream_create (8000);
      assert (stream != NULL);
    }

  // close()
  io_stream_close (stream);

  // TEST for more realistic example
  ret = network_mimic_test(1024*1024, 128, 1024);
  assert(ret == 0);
  ret = network_mimic_test(1024*1024, 8192, 1024);
  assert(ret == 0);

  return 0;
}

#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include "dlist.h"
#include "stream.h"

/*
 * Simple buffered stream implementatino.
 *
 * Following constraints are always true
 *   C-1) stream->read_pos <= chunk->used <= stream->chunk_size
 *   C-2) (purge_chunk, purge_pos) <= (read_chunk, read_pos)
 *
 */

typedef struct ioChunk
{
  dlisth head;			// must be the first member for save type casting
  int used;			// number of used bytes
  char data[];			// data buffer
} ioChunk;

struct ioStream
{
  dlisth head;			// list header that links ioChunk
  int chunk_size;		// chunk data size (common)
  int num_chunk;		// number of chunks in the list
  ioChunk *read_chunk;		// current read chunk
  int read_pos;			// position in read chunk
  ioChunk *purge_chunk;		// current purge chunk
  int purge_pos;		// position in purge chunk
  int peeked;			// set to 1 if last chunk position is peeked for write
};

static ioChunk *
append_chunk (ioStream * stream)
{
  ioChunk *chunk;

  assert (stream != NULL);

  chunk = malloc (sizeof (ioChunk) + stream->chunk_size);
  if (chunk == NULL)
    {
      free (stream);
      errno = ENOMEM;
      return NULL;
    }

  dlisth_init (&chunk->head);
  chunk->used = 0;

  /* link the chunk to stream end */
  dlisth_insert_before (&chunk->head, &stream->head);
  stream->num_chunk++;

  return chunk;
}

ioStream *
io_stream_create (int chunk_size)
{
  ioStream *stream;
  ioChunk *chunk;

  assert (chunk_size > 0);

  stream = malloc (sizeof (ioStream));
  if (stream == NULL)
    {
      errno = ENOMEM;
      return NULL;
    }

  dlisth_init (&stream->head);
  stream->chunk_size = chunk_size;
  stream->num_chunk = 0;
  if ((chunk = append_chunk (stream)) == NULL)
    {
      free (stream);
      return NULL;
    }
  stream->read_chunk = chunk;
  stream->read_pos = 0;
  stream->purge_chunk = chunk;
  stream->purge_pos = 0;
  stream->peeked = 0;

  return stream;
}


void
io_stream_close (ioStream * stream)
{
  if (stream == NULL)
    {
      return;
    }

  while (!dlisth_is_empty (&stream->head))
    {
      ioChunk *chunk = (ioChunk *) stream->head.next;
      dlisth_delete (&chunk->head);
      free (chunk);
    }
  free (stream);
}

int
io_stream_peek_write_buffer (ioStream * stream, char **buf, int *avail)
{
  ioChunk *chunk;
  int av;

  if (stream == NULL || buf == NULL || avail == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  else if (stream->peeked == 1)
    {
      errno = EACCES;
      return -1;
    }
  assert (stream->num_chunk > 0);

  chunk = (ioChunk *) stream->head.prev;
  av = stream->chunk_size - chunk->used;
  if (av == 0)
    {
      if ((chunk = append_chunk (stream)) == NULL)
	{
	  return -1;
	}
      av = stream->chunk_size - chunk->used;
    }

  *buf = &chunk->data[chunk->used];
  *avail = av;
  stream->peeked = 1;
  return 0;
}


int
io_stream_commit_write (ioStream * stream, int sz)
{
  ioChunk *chunk;
  int avail;

  if (stream == NULL || sz < 0)
    {
      errno = EINVAL;
      return -1;
    }
  else if (stream->peeked == 0)
    {
      errno = EACCES;
      return -1;
    }

  assert (!dlisth_is_empty (&stream->head));
  assert (stream->peeked == 1);

  chunk = (ioChunk *) stream->head.prev;
  avail = stream->chunk_size - chunk->used;
  assert (avail >= sz);
  chunk->used += sz;
  stream->peeked = 0;
  return 0;
}

/*
 * Peek next consective data written.
 * returns the buffer position and the size available by out paramater.
 * available size 0 means no data available for now.
 */
int
io_stream_peek_read_buffer (ioStream * stream, char **buf, int *avail)
{
  ioChunk *chunk;

  if (stream == NULL || buf == NULL || avail == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  else if (stream->peeked == 1)
    {
      errno = EACCES;
      return -1;
    }

  chunk = stream->read_chunk;
  assert (chunk != NULL);

  if (stream->read_pos < stream->chunk_size
      || chunk->head.next == &stream->head)
    {
      *buf = &chunk->data[stream->read_pos];
      *avail = chunk->used - stream->read_pos;
      stream->read_pos = chunk->used;
      return 0;
    }

  chunk = stream->read_chunk = (ioChunk *) chunk->head.next;
  *buf = &chunk->data[0];
  *avail = chunk->used;
  stream->read_pos = chunk->used;
  return 0;
}

extern int
io_stream_rewind_readpos (ioStream * stream, int size)
{
  int remain;
  ioChunk *chunk;
  int base;
  int pos;

  if (stream == NULL || size < 0)
    {
      errno = EINVAL;
      return -1;
    }
  else if (stream->peeked == 1)
    {
      errno = EACCES;
      return -1;
    }

  remain = size;
  chunk = stream->read_chunk;
  base = (chunk == stream->purge_chunk ? stream->purge_pos : 0);
  pos = stream->read_pos;

  while (remain > 0)
    {
      if (remain <= (pos - base))
	{
	  stream->read_chunk = chunk;
	  stream->read_pos = pos - remain;
	  return 0;
	}

      /* check range */
      if (chunk->head.prev == &stream->head)
	{
	  errno = ERANGE;
	  return -1;
	}

      /* go to prev */
      remain -= (pos - base);
      chunk = (ioChunk *) chunk->head.prev;
      base = (chunk == stream->purge_chunk ? stream->purge_pos : 0);
      pos = chunk->used;
      assert (pos == stream->chunk_size);
    }

  return 0;
}

/*
 * Purges the leading buffer upto given bytes.
 * It does not purge bytes beyond current read position.
 */
int
io_stream_purge (ioStream * stream, int bytes)
{
  int remain;
  int max_purge;

  if (stream == NULL || bytes < 0)
    {
      errno = EINVAL;
      return -1;
    }
  else if (stream->peeked == 1)
    {
      errno = EACCES;
      return -1;
    }
  assert (stream->num_chunk > 0);

  remain = bytes;
  while (stream->purge_chunk != stream->read_chunk)
    {
      max_purge = stream->purge_chunk->used - stream->purge_pos;
      if (remain >= max_purge)
	{
	  ioChunk *chunk = stream->purge_chunk;
	  ioChunk *next = (ioChunk *) chunk->head.next;

	  /* delete chunk */
	  dlisth_delete (&chunk->head);
	  free (chunk);
	  stream->num_chunk--;
	  assert (stream->num_chunk > 0);

	  /* update */
	  remain -= max_purge;
	  stream->purge_chunk = next;
	  stream->purge_pos = 0;
	  continue;
	}
      else
	{
	  stream->purge_pos += remain;
	  return 0;
	}
    }

  /*
   * At this point, purge_chunk == read_chunk
   * we can not purge beyond read_pos
   */
  max_purge = stream->read_pos - stream->purge_pos;
  if (remain > max_purge)
    {
      remain = max_purge;
    }
  stream->purge_pos += remain;

  /*
   * Do some optimization
   * If there is only one chunk and there is no remaing data to read and all read data purged
   * We can reset the chunk
   */
  if (stream->purge_pos == stream->read_pos)
    {
      if (stream->head.prev == (dlisth *) stream->read_chunk
	  && stream->read_chunk->used == stream->read_pos)
	{
	  stream->read_chunk->used = 0;
	  stream->read_pos = stream->purge_pos = 0;
	}
    }
  return 0;
}

int
io_stream_reset_read (ioStream * stream)
{
  if (stream == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  else if (stream->peeked == 1)
    {
      errno = EACCES;
      return -1;
    }

  stream->read_chunk = stream->purge_chunk;
  stream->read_pos = stream->purge_pos;
  return 0;
}

int
io_stream_avail_for_read (ioStream * stream)
{
  ioChunk *chunk;
  int avail = 0;

  if (stream == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  else if (stream->peeked == 1)
    {
      errno = EACCES;
      return -1;
    }

  avail = stream->chunk_size * stream->num_chunk;
  /* cancel out already read chunks */
  for (chunk = (ioChunk *) stream->head.next; chunk != stream->read_chunk;
       chunk = (ioChunk *) chunk->head.next)
    {
      avail -= stream->chunk_size;
    }

  /* read chunk */
  assert (chunk == stream->read_chunk);
  avail = avail - stream->read_pos;

  /* write chunk */
  chunk = (ioChunk *) stream->head.prev;
  avail = avail - (stream->chunk_size - chunk->used);
  assert (avail >= 0);

  return avail;
}

int
io_stream_num_chunk (ioStream * stream)
{
  if (stream == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  return stream->num_chunk;
}

void
io_stream_dump (ioStream * stream, FILE * fp, int width)
{
  dlisth *h;
  int chunk_off;

  if (stream == NULL || fp == NULL)
    {
      return;
    }

  fprintf (fp, "chunk size : %d\n", stream->chunk_size);
  fprintf (fp, "num chunk  : %d\n", stream->num_chunk);
  fprintf (fp, "read chunk : %p\n", (void *) stream->read_chunk);
  fprintf (fp, "read pos   : %d\n", stream->read_pos);
  fprintf (fp, "purge chunk: %p\n", (void *) stream->purge_chunk);
  fprintf (fp, "purge pos  : %d\n", stream->purge_pos);
  fprintf (fp, "peeked     : %d\n", stream->peeked);
  fprintf (fp, "============================================\n");
  for (h = stream->head.next; h != &stream->head; h = h->next)
    {
      ioChunk *chunk = (ioChunk *) h;
      int remain = stream->chunk_size;
      int i;
      char *cp = chunk->data;

      chunk_off = 0;
      fprintf (fp, "chunk: %p, used:%d\n", (void *) chunk, chunk->used);
      while (remain > 0)
	{
	  fprintf (fp, "%010d", chunk_off);
	  for (i = 0; i < (width > remain ? remain : width); i++, cp++)
	    {
	      if (i % 10 == 0)
		{
		  fprintf (fp, "|");
		}
	      fprintf (fp, "%c", isprint (*cp) ? *cp : '.');
	      chunk_off++;
	    }
	  fprintf (fp, "\n");
	  remain = remain - i;
	}
    }
}

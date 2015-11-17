#ifndef _STREAM_H_
#define _STREAM_H_

#include <stdio.h>

typedef struct ioStream ioStream;

extern ioStream *io_stream_create (int chunk_size);
extern void io_stream_close (ioStream * stream);
extern int io_stream_peek_write_buffer (ioStream * stream, char **buf,
					int *avail);
extern int io_stream_commit_write (ioStream * stream, int sz);
extern int io_stream_peek_read_buffer (ioStream * stream, char **buf,
				       int *avail);
extern int io_stream_rewind_readpos (ioStream * stream, int size);
extern int io_stream_purge (ioStream * stream, int bytes);
extern int io_stream_reset_read (ioStream * stream);
extern int io_stream_avail_for_read (ioStream * stream);
extern int io_stream_num_chunk (ioStream * stream);
extern void io_stream_dump (ioStream * stream, FILE * fp, int width);

#endif

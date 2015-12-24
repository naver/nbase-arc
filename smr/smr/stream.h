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

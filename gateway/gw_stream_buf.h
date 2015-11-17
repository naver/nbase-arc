#ifndef _GW_STREAM_BUF_H_
#define _GW_STREAM_BUF_H_

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <stdarg.h>

#include "gw_config.h"
#include "gw_mem_pool.h"
#include "queue.h"
#include "zmalloc.h"

#define SBUF_IOV_MAX 128
#define SBUF_DEFAULT_PAGESIZE (16*1024)

typedef struct sbuf_hdr sbuf_hdr;
typedef struct sbuf sbuf;
typedef struct sbuf_pos sbuf_pos;
typedef struct sbuf_page sbuf_page;
typedef struct sbuf_mempool sbuf_mempool;

struct sbuf_pos
{
  sbuf_page *page;
  size_t offset;
};

#define SBUF_POS_EQUAL(p1, p2) ((p1)->page == (p2)->page && (p1)->offset == (p2)->offset)

sbuf_mempool *sbuf_mempool_create (size_t page_size);
void sbuf_mempool_destroy (sbuf_mempool * mp);
sbuf_hdr *stream_hdr_create (size_t page_size, sbuf_mempool * mp);
void stream_hdr_free (sbuf_hdr * hdr);
void stream_delegate_data_and_free (sbuf_hdr * dst, sbuf_hdr * src);
int stream_append_copy_buf (sbuf_hdr * hdr, void *buf, size_t len);
int stream_append_copy_str (sbuf_hdr * hdr, const char *str);
int stream_append_vprintf (sbuf_hdr * hdr, const char *fmt, va_list ap);
int stream_append_printf (sbuf_hdr * hdr, const char *fmt, ...);
int stream_append_copy_sbuf_pos (sbuf_hdr * hdr, sbuf_pos start,
				 sbuf_pos last, size_t * ret_len);
int stream_append_copy_sbuf_pos_len (sbuf_hdr * hdr, sbuf_pos start,
				     size_t len);
int stream_append_copy_sbuf (sbuf_hdr * hdr, sbuf * buf);
ssize_t stream_append_read (sbuf_hdr * hdr, int fd);
void stream_discard_appended (sbuf_hdr * hdr);
sbuf_pos stream_start_pos (sbuf_hdr * hdr);
sbuf_pos stream_last_pos (sbuf_hdr * hdr);
size_t stream_mem_used (sbuf_hdr * hdr);
size_t stream_mem_alloc (sbuf_hdr * hdr);
float stream_fragmentation_ratio (sbuf_hdr * hdr);
sbuf *stream_create_sbuf (sbuf_hdr * hdr, sbuf_pos last_pos);
sbuf *stream_create_sbuf_buf (sbuf_hdr * hdr, void *buf, size_t len);
sbuf *stream_create_sbuf_str (sbuf_hdr * hdr, const char *str);
sbuf *stream_create_sbuf_printf (sbuf_hdr * hdr, const char *fmt, ...);
int sbuf_copy_buf (void *s, sbuf_pos start, size_t n);
int sbuf_offset (sbuf_pos start, sbuf_pos last, size_t offset,
		 sbuf_pos * ret_pos);
ssize_t sbuf_offset_len (sbuf_pos start, sbuf_pos last);
int sbuf_offset_char (sbuf_pos start, sbuf_pos last, size_t offset,
		      char *ret_char);
int sbuf_memchr (sbuf_pos start, sbuf_pos last, int c, sbuf_pos * ret_pos,
		 size_t * ret_offset);
ssize_t sbuf_writev (sbuf ** bufv, ssize_t nbuf, int fd);
void sbuf_reset_write_marker (sbuf * buf);
int sbuf_check_write_none (sbuf * buf);
int sbuf_check_write_finish (sbuf * buf);
sbuf_pos sbuf_start_pos (sbuf * buf);
sbuf_pos sbuf_last_pos (sbuf * buf);
size_t sbuf_len (sbuf * buf);
char sbuf_char (sbuf_pos pos);
void sbuf_next_pos (sbuf_pos * pos);
void sbuf_free (sbuf * buf);
unsigned short sbuf_crc16 (sbuf_pos start_pos, size_t len, unsigned short sd);
int sbuf_string2ll (sbuf_pos start_pos, size_t slen, long long *value);
#endif

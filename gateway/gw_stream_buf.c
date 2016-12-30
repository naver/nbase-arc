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

#include "gw_stream_buf.h"

/*
 *   sbuf_hdr   sbuf_page ...          (fixed size page)
 *   |          |
 *   v          v
 *   +------------------------------------------------------+
 *   |          |          |          |          |          |
 *   +------------------------------------------------------+
 *   |        |      .          |           |               |
 *   +------------------------------------------------------+
 *   ^        ^      ^
 *   |        |      |
 *   sbuf_hdr sbuf   sbuf_pos          (logical buffer)
 *
 */

/* Macros for min/max.  */
#define	MIN(a,b) (((a)<(b))?(a):(b))
#define	MAX(a,b) (((a)>(b))?(a):(b))

struct sbuf_page
{
  TAILQ_ENTRY (sbuf_page) page_tqe;
  sbuf_hdr *my_hdr;
  size_t size;
  size_t len;
  char data[1];
};

struct sbuf
{
  TAILQ_ENTRY (sbuf) sbuf_tqe;
  sbuf_hdr *my_hdr;
  sbuf_pos start;
  sbuf_pos last;
  sbuf_pos write_marker;
  ssize_t len;
};

struct sbuf_mempool
{
  mempool_hdr_t *mp_sbuf;
  mempool_hdr_t *mp_sbuf_page;
};

TAILQ_HEAD (page_tqh, sbuf_page);
TAILQ_HEAD (sbuf_tqh, sbuf);

struct sbuf_hdr
{
  struct page_tqh page_q;	/* List of fixed size buffer pages */
  struct sbuf_tqh sbuf_q;	/* List of logical buffers which span multiple pages */
  sbuf_pos stream_start;
  sbuf_pos stream_last;
  size_t sbuf_used;
  size_t num_sbuf;
  size_t page_size;
  size_t num_page;
  mempool_hdr_t *mp_sbuf;
  mempool_hdr_t *mp_sbuf_page;
};

static inline size_t
page_avail_size (sbuf_page * page)
{
  return page->size - page->len;
}

static sbuf_page *
hdr_add_tail_page (sbuf_hdr * hdr)
{
  sbuf_page *page;

  page = mempool_alloc (hdr->mp_sbuf_page);
  if (!page)
    {
      errno = ENOMEM;
      return NULL;
    }
  TAILQ_INSERT_TAIL (&hdr->page_q, page, page_tqe);
  page->my_hdr = hdr;
  page->size = hdr->page_size;
  page->len = 0;
  page->data[0] = '\0';
  hdr->num_page++;
  return page;
}

static sbuf_page *
page_get_appendable_start (sbuf_hdr * hdr)
{
  return hdr->stream_last.page;
}

static sbuf_page *
page_get_appendable_next (sbuf_page * page)
{
  sbuf_page *next;

  next = TAILQ_NEXT (page, page_tqe);
  if (next)
    {
      return next;
    }

  next = hdr_add_tail_page (page->my_hdr);
  if (next)
    {
      return next;
    }
  errno = ENOMEM;
  return NULL;
}

static size_t
page_append_buf (sbuf_page * page, void *buf, size_t len)
{
  size_t copy_len;

  copy_len = MIN (page_avail_size (page), len);

  memcpy (page->data + page->len, buf, copy_len);
  page->len += copy_len;
  page->data[page->len] = '\0';
  return copy_len;
}

static void
hdr_remove_page (sbuf_page * page)
{
  sbuf_hdr *hdr;

  hdr = page->my_hdr;
  TAILQ_REMOVE (&hdr->page_q, page, page_tqe);
  hdr->num_page--;
  mempool_free (hdr->mp_sbuf_page, page);
}

static void
hdr_remove_sbuf (sbuf * buf)
{
  sbuf_hdr *hdr;

  hdr = buf->my_hdr;
  TAILQ_REMOVE (&hdr->sbuf_q, buf, sbuf_tqe);
  hdr->sbuf_used -= buf->len;
  hdr->num_sbuf--;
  mempool_free (hdr->mp_sbuf, buf);
}

sbuf_mempool *
sbuf_mempool_create (size_t page_size)
{
  sbuf_mempool *mp;

  mp = zmalloc (sizeof (sbuf_mempool));
  mp->mp_sbuf = mempool_create (sizeof (sbuf), MEMPOOL_DEFAULT_POOL_SIZE);

  // The size of sbuf_page includes meta header, data buffer, and a null termination byte
  mp->mp_sbuf_page =
    mempool_create (sizeof (sbuf_page) + page_size,
		    MEMPOOL_DEFAULT_POOL_SIZE);
  return mp;
}

void
sbuf_mempool_destroy (sbuf_mempool * mp)
{
  mempool_destroy (mp->mp_sbuf);
  mempool_destroy (mp->mp_sbuf_page);
  zfree (mp);
}

sbuf_hdr *
stream_hdr_create (size_t page_size, sbuf_mempool * mp)
{
  sbuf_hdr *hdr;
  sbuf_page *page;

  hdr = zmalloc (sizeof (sbuf_hdr));
  if (!hdr)
    {
      errno = ENOMEM;
      return NULL;
    }
  TAILQ_INIT (&hdr->page_q);
  TAILQ_INIT (&hdr->sbuf_q);
  hdr->sbuf_used = 0;
  hdr->num_sbuf = 0;
  hdr->page_size = page_size;
  hdr->num_page = 0;
  hdr->mp_sbuf = mp->mp_sbuf;
  hdr->mp_sbuf_page = mp->mp_sbuf_page;

  page = hdr_add_tail_page (hdr);
  if (!page)
    {
      errno = ENOMEM;
      zfree (hdr);
      return NULL;
    }
  hdr->stream_start.page = page;
  hdr->stream_start.offset = 0;
  hdr->stream_last.page = page;
  hdr->stream_last.offset = 0;

  return hdr;
}

void
stream_hdr_free (sbuf_hdr * hdr)
{
  sbuf_page *page, *page_tvar;
  sbuf *buf, *buf_tvar;

  TAILQ_FOREACH_SAFE (page, &hdr->page_q, page_tqe, page_tvar)
  {
    hdr_remove_page (page);
  }

  TAILQ_FOREACH_SAFE (buf, &hdr->sbuf_q, sbuf_tqe, buf_tvar)
  {
    hdr_remove_sbuf (buf);
  }

  zfree (hdr);
}

void
stream_delegate_data_and_free (sbuf_hdr * dst, sbuf_hdr * src)
{
  sbuf_page *page;
  sbuf *buf;

  // To insert all entries from src in front of first entry of dst, we first
  // append dst entries to src, then swap queue of dst and src.

  // Discard all unnecessary data in src
  stream_discard_appended (src);

  // Change header info of all page in src
  TAILQ_FOREACH (page, &src->page_q, page_tqe)
  {
    page->my_hdr = dst;
    dst->num_page++;
  }

  // Append all dst pages to src, so pages from dst are appended after pages
  // from src.
  TAILQ_CONCAT (&src->page_q, &dst->page_q, page_tqe);

  // Swap page_q of src and dst, so page_q of dst has all src and dst pages.
  TAILQ_INIT (&dst->page_q);
  TAILQ_SWAP (&src->page_q, &dst->page_q, sbuf_page, page_tqe);

  // Changer header info of all sbuf in src
  TAILQ_FOREACH (buf, &src->sbuf_q, sbuf_tqe)
  {
    buf->my_hdr = dst;
    dst->sbuf_used += buf->len;
    dst->num_sbuf++;
  }

  // Append all dst sbufs to src, so sbufs from dst are appended after sbufs
  // from src.
  TAILQ_CONCAT (&src->sbuf_q, &dst->sbuf_q, sbuf_tqe);

  // Swap sbuf_q of src and dst, so sbuf_q of dst has all src and dst sbufs.
  TAILQ_INIT (&dst->sbuf_q);
  TAILQ_SWAP (&src->sbuf_q, &dst->sbuf_q, sbuf, sbuf_tqe);

  zfree (src);
}

int
stream_append_copy_buf (sbuf_hdr * hdr, void *buf, size_t len)
{
  sbuf_page *page;
  size_t nwritten, pos;

  pos = 0;
  page = page_get_appendable_start (hdr);
  while (pos < len)
    {
      nwritten = page_append_buf (page, ((char *) buf) + pos, len - pos);
      pos += nwritten;
      if (page_avail_size (page) == 0)
	{
	  page = page_get_appendable_next (page);
	  if (!page)
	    {
	      errno = ENOMEM;
	      return ERR;
	    }
	}
    }
  hdr->stream_last.page = page;
  hdr->stream_last.offset = page->len;
  hdr->sbuf_used += len;
  return OK;
}

int
stream_append_copy_str (sbuf_hdr * hdr, const char *str)
{
  return stream_append_copy_buf (hdr, (void *) str, strlen (str));
}

int
stream_append_vprintf (sbuf_hdr * hdr, const char *fmt, va_list ap)
{
  va_list cpy;
  char staticbuf[1024], *buf = staticbuf;
  size_t buflen = strlen (fmt) * 2;
  int len, ret;

  if (buflen > sizeof (staticbuf))
    {
      buf = zmalloc (buflen);
      if (buf == NULL)
	{
	  errno = ENOMEM;
	  return ERR;
	}
    }
  else
    {
      buflen = sizeof (staticbuf);
    }

  while (1)
    {
      buf[buflen - 2] = '\0';
      va_copy (cpy, ap);
      len = vsnprintf (buf, buflen, fmt, cpy);
      if (buf[buflen - 2] != '\0')
	{
	  if (buf != staticbuf)
	    zfree (buf);
	  buflen *= 2;
	  buf = zmalloc (buflen);
	  if (buf == NULL)
	    {
	      errno = ENOMEM;
	      return ERR;
	    }
	  continue;
	}
      break;
    }

  ret = stream_append_copy_buf (hdr, buf, len);
  if (buf != staticbuf)
    zfree (buf);

  return ret;
}

int
stream_append_printf (sbuf_hdr * hdr, const char *fmt, ...)
{
  va_list ap;
  int ret;

  va_start (ap, fmt);
  ret = stream_append_vprintf (hdr, fmt, ap);
  va_end (ap);

  return ret;
}

int
stream_append_copy_sbuf_pos (sbuf_hdr * hdr, sbuf_pos start, sbuf_pos last,
			     size_t * ret_len)
{
  sbuf_page *page;
  size_t copy_len, copy_offset;
  int ret;

  *ret_len = 0;
  page = start.page;
  TAILQ_FOREACH_FROM (page, &hdr->page_q, page_tqe)
  {
    copy_len = page->size;
    if (page == start.page)
      {
	copy_len -= start.offset;
	copy_offset = start.offset;
      }
    else
      {
	copy_offset = 0;
      }
    if (page == last.page)
      {
	copy_len -= page->size - last.offset;
      }
    ret = stream_append_copy_buf (hdr, page->data + copy_offset, copy_len);
    if (ret == ERR)
      {
	return ERR;
      }
    *ret_len += copy_len;
    if (page == last.page)
      {
	/* Find last page */
	return OK;
      }
  }
  /* Can't find last page */
  return ERR;
}

int
stream_append_copy_sbuf_pos_len (sbuf_hdr * hdr, sbuf_pos start, size_t len)
{
  sbuf_hdr *src;
  sbuf_page *page;
  size_t copy_len, copy_offset, total_len;
  int ret;

  src = start.page->my_hdr;
  total_len = 0;
  page = start.page;
  TAILQ_FOREACH_FROM (page, &src->page_q, page_tqe)
  {
    copy_len = page->size;
    if (page == start.page)
      {
	copy_len -= start.offset;
	copy_offset = start.offset;
      }
    else
      {
	copy_offset = 0;
      }
    if (total_len + copy_len > len)
      {
	copy_len = len - total_len;
      }
    ret = stream_append_copy_buf (hdr, page->data + copy_offset, copy_len);
    if (ret == ERR)
      {
	return ERR;
      }
    total_len += copy_len;
    if (total_len == len)
      {
	return OK;
      }
  }
  return ERR;
}

int
stream_append_copy_sbuf (sbuf_hdr * hdr, sbuf * buf)
{
  size_t len;

  return stream_append_copy_sbuf_pos (hdr, buf->start, buf->last, &len);
}

ssize_t
stream_append_read (sbuf_hdr * hdr, int fd)
{
  sbuf_page *page;
  ssize_t nread;

  page = page_get_appendable_start (hdr);
  assert (page_avail_size (page) != 0);
  nread = read (fd, page->data + page->len, page_avail_size (page));
  if (nread == 0 || nread == -1)
    {
      return nread;
    }
  page->len += nread;
  page->data[page->len] = '\0';
  if (page_avail_size (page) == 0)
    {
      page = page_get_appendable_next (page);
      if (!page)
	{
	  errno = ENOMEM;
	  return -1;
	}
    }
  hdr->stream_last.page = page;
  hdr->stream_last.offset = page->len;
  hdr->sbuf_used += nread;
  return nread;
}

void
stream_discard_appended (sbuf_hdr * hdr)
{
  sbuf_page *page, *tvar;

  page = hdr->stream_start.page;
  assert (page != NULL);
  page->len = hdr->stream_start.offset;
  hdr->stream_last = hdr->stream_start;

  TAILQ_FOREACH_FROM_SAFE (page, &hdr->page_q, page_tqe, tvar)
  {
    if (page == hdr->stream_start.page)
      {
	/* Skip */
	continue;
      }
    hdr_remove_page (page);
  }
}

sbuf_pos
stream_start_pos (sbuf_hdr * hdr)
{
  return hdr->stream_start;
}

sbuf_pos
stream_last_pos (sbuf_hdr * hdr)
{
  return hdr->stream_last;
}

size_t
stream_mem_used (sbuf_hdr * hdr)
{
  return hdr->sbuf_used;
}

size_t
stream_mem_alloc (sbuf_hdr * hdr)
{
  return hdr->page_size * hdr->num_page;
}

float
stream_fragmentation_ratio (sbuf_hdr * hdr)
{
  // http://grouper.ieee.org/groups/754/faq.html#exceptions
  // Division by floating point 0.0 is valid and yields +/-inf by IEEE 754
  return (float) hdr->page_size * hdr->num_page / hdr->sbuf_used;
}

sbuf *
stream_create_sbuf (sbuf_hdr * hdr, sbuf_pos last_pos)
{
  sbuf *buf;

  buf = mempool_alloc (hdr->mp_sbuf);
  if (!buf)
    {
      errno = ENOMEM;
      return NULL;
    }

  buf->my_hdr = hdr;
  buf->start = hdr->stream_start;
  buf->last = last_pos;
  buf->write_marker = buf->start;
  buf->len = sbuf_offset_len (buf->start, buf->last);
  if (buf->len == -1)
    {
      mempool_free (hdr->mp_sbuf, buf);
      return NULL;
    }

  TAILQ_INSERT_TAIL (&hdr->sbuf_q, buf, sbuf_tqe);
  hdr->stream_start = last_pos;
  hdr->num_sbuf++;

  return buf;
}

sbuf *
stream_create_sbuf_buf (sbuf_hdr * hdr, void *buf, size_t len)
{
  int ret;

  ret = stream_append_copy_buf (hdr, buf, len);
  if (ret == ERR)
    {
      return NULL;
    }
  return stream_create_sbuf (hdr, stream_last_pos (hdr));
}

sbuf *
stream_create_sbuf_str (sbuf_hdr * hdr, const char *str)
{
  return stream_create_sbuf_buf (hdr, (void *) str, strlen (str));
}

sbuf *
stream_create_sbuf_printf (sbuf_hdr * hdr, const char *fmt, ...)
{
  va_list ap;
  int ret;

  va_start (ap, fmt);
  ret = stream_append_vprintf (hdr, fmt, ap);
  va_end (ap);

  if (ret == ERR)
    {
      return NULL;
    }

  return stream_create_sbuf (hdr, stream_last_pos (hdr));
}

int
sbuf_copy_buf (void *s, sbuf_pos start, size_t n)
{
  sbuf_hdr *hdr;
  sbuf_page *page;
  size_t copy_len, copy_offset, pos;

  hdr = start.page->my_hdr;

  pos = 0;
  page = start.page;
  TAILQ_FOREACH_FROM (page, &hdr->page_q, page_tqe)
  {
    copy_len = page->size;
    if (page == start.page)
      {
	copy_len -= start.offset;
	copy_offset = start.offset;
      }
    else
      {
	copy_offset = 0;
      }
    copy_len = MIN (copy_len, n - pos);
    memcpy (((char *) s) + pos, page->data + copy_offset, copy_len);
    pos += copy_len;
    if (pos == n)
      {
	return OK;
      }
  }
  return ERR;
}

int
sbuf_offset (sbuf_pos start, sbuf_pos last, size_t offset, sbuf_pos * ret_pos)
{
  sbuf_hdr *hdr;
  sbuf_page *page;
  size_t len, pos;

  hdr = start.page->my_hdr;

  pos = 0;
  page = start.page;
  TAILQ_FOREACH_FROM (page, &hdr->page_q, page_tqe)
  {
    len = page->size;
    if (page == start.page)
      {
	len -= start.offset;
      }
    if (pos + len <= offset)
      {
	pos += len;
      }
    else
      {
	ret_pos->page = page;
	ret_pos->offset = offset - pos;
	if (page == start.page)
	  {
	    ret_pos->offset += start.offset;
	  }
	/* Check pos is in range between start and last */
	if (page == last.page && ret_pos->offset > last.offset)
	  {
	    break;
	  }
	return OK;
      }
    if (page == last.page)
      {
	break;
      }
  }
  ret_pos->page = NULL;
  ret_pos->offset = 0;
  return ERR;
}

ssize_t
sbuf_offset_len (sbuf_pos start, sbuf_pos last)
{
  sbuf_hdr *hdr;
  sbuf_page *page;
  ssize_t len;

  hdr = start.page->my_hdr;

  len = 0;
  page = start.page;
  TAILQ_FOREACH_FROM (page, &hdr->page_q, page_tqe)
  {
    len += page->size;
    if (page == start.page)
      {
	len -= start.offset;
      }
    if (page == last.page)
      {
	/* Find 'last' page */
	len -= page->size - last.offset;
	return len;
      }
  }
  /* Can't find 'last' page */
  return -1;
}

int
sbuf_offset_char (sbuf_pos start, sbuf_pos last, size_t offset,
		  char *ret_char)
{
  sbuf_pos find;
  int ret;

  ret = sbuf_offset (start, last, offset, &find);
  if (ret == ERR)
    {
      return ERR;
    }
  *ret_char = *(find.page->data + find.offset);
  return OK;
}

int
sbuf_strchr (sbuf_pos start, sbuf_pos last, int c, sbuf_pos * ret_pos,
	     size_t * ret_offset)
{
  sbuf_hdr *hdr;
  sbuf_page *page;
  size_t len, pos, offset;
  char *ptr;

  hdr = start.page->my_hdr;

  pos = 0;
  page = start.page;
  TAILQ_FOREACH_FROM (page, &hdr->page_q, page_tqe)
  {
    len = page->size;
    if (page == start.page)
      {
	len -= start.offset;
	offset = start.offset;
      }
    else
      {
	offset = 0;
      }
    if (page == last.page)
      {
	len -= page->size - last.offset;
      }
    ptr = strchrnul (page->data + offset, c);
    // Make sure ptr doesn't cross the boundary of next buffer.
    if (*ptr != '\0' && ptr < page->data + offset + len)
      {
	ret_pos->page = page;
	ret_pos->offset = ptr - page->data;
	*ret_offset = pos + ptr - (page->data + offset);
	return OK;
      }
    // ptr is null terminated if it's inside the page boundary and the pointed value is null.
    if (ptr < page->data + offset + len)
      {
	break;
      }
    if (page == last.page)
      {
	break;
      }
    pos += len;
  }
  return ERR;
}

ssize_t
sbuf_writev (sbuf ** bufv, ssize_t nbuf, int fd)
{
  struct iovec iov[SBUF_IOV_MAX];
  size_t niov;
  sbuf_hdr *hdr;
  sbuf *buf;
  sbuf_page *page;
  ssize_t nwritten, len;
  int i;

  niov = 0;
  for (i = 0; i < nbuf; i++)
    {
      buf = bufv[i];
      if (sbuf_check_write_finish (buf) == OK)
	{
	  continue;
	}

      hdr = buf->my_hdr;
      page = buf->write_marker.page;
      TAILQ_FOREACH_FROM (page, &hdr->page_q, page_tqe)
      {
	if (page == buf->write_marker.page)
	  {
	    iov[niov].iov_base = page->data + buf->write_marker.offset;
	    iov[niov].iov_len = page->size - buf->write_marker.offset;
	  }
	else
	  {
	    iov[niov].iov_base = page->data;
	    iov[niov].iov_len = page->size;
	  }
	if (page == buf->last.page)
	  {
	    iov[niov].iov_len -= page->size - buf->last.offset;
	  }
	niov++;
	if (page == buf->last.page || niov == SBUF_IOV_MAX)
	  {
	    break;
	  }
      }
      if (niov == SBUF_IOV_MAX)
	{
	  break;
	}
    }

  nwritten = writev (fd, iov, niov);
  if (nwritten == 0 || nwritten == -1)
    {
      return nwritten;
    }

  len = 0;
  for (i = 0; i < nbuf; i++)
    {
      sbuf_pos new_marker;
      ssize_t offset_len;
      int ret;

      buf = bufv[i];
      if (sbuf_check_write_none (buf) == OK)
	{
	  offset_len = buf->len;
	}
      else
	{
	  offset_len = sbuf_offset_len (buf->write_marker, buf->last);
	}
      if (len + offset_len < nwritten)
	{
	  buf->write_marker = buf->last;
	  len += offset_len;
	  continue;
	}

      ret =
	sbuf_offset (buf->write_marker, buf->last, nwritten - len,
		     &new_marker);
      if (ret == ERR)
	{
	  /* Fatal Error */
	  assert (0);
	}
      buf->write_marker = new_marker;
      break;
    }
  return nwritten;
}

void
sbuf_reset_write_marker (sbuf * buf)
{
  buf->write_marker = buf->start;
}

int
sbuf_check_write_none (sbuf * buf)
{
  if (SBUF_POS_EQUAL (&buf->write_marker, &buf->start))
    {
      return OK;
    }
  return ERR;
}

int
sbuf_check_write_finish (sbuf * buf)
{
  if (SBUF_POS_EQUAL (&buf->write_marker, &buf->last))
    {
      return OK;
    }
  return ERR;
}

sbuf_pos
sbuf_start_pos (sbuf * buf)
{
  return buf->start;
}

sbuf_pos
sbuf_last_pos (sbuf * buf)
{
  return buf->last;
}

size_t
sbuf_len (sbuf * buf)
{
  return buf->len;
}

char
sbuf_char (sbuf_pos pos)
{
  return *(pos.page->data + pos.offset);
}

void
sbuf_next_pos (sbuf_pos * pos)
{
  pos->offset++;
  if (pos->offset == pos->page->size)
    {
      pos->page = TAILQ_NEXT (pos->page, page_tqe);
      pos->offset = 0;
      assert (pos->page);
    }
}

void
sbuf_free (sbuf * buf)
{
  sbuf_hdr *hdr;
  sbuf *prev_buf, *next_buf;
  sbuf_page *from, *to, *page, *tvar;

  if (!buf)
    {
      return;
    }
  hdr = buf->my_hdr;

  prev_buf = TAILQ_PREV (buf, sbuf_tqh, sbuf_tqe);
  if (prev_buf)
    {
      from = prev_buf->last.page;
    }
  else
    {
      from = TAILQ_FIRST (&hdr->page_q);
    }
  next_buf = TAILQ_NEXT (buf, sbuf_tqe);
  if (next_buf)
    {
      to = next_buf->start.page;
    }
  else
    {
      to = hdr->stream_start.page;
    }

  if (from != to)
    {
      page = TAILQ_NEXT (from, page_tqe);
      TAILQ_FOREACH_FROM_SAFE (page, &hdr->page_q, page_tqe, tvar)
      {
	if (page == to)
	  {
	    /* Finish */
	    break;
	  }
	hdr_remove_page (page);
      }
      if (!prev_buf)
	{
	  hdr_remove_page (from);
	}
      if (page != to)
	{
	  /* Fatal Error */
	  assert (0);
	}
    }

  hdr_remove_sbuf (buf);
}

/* CRC16 implementation acording to CCITT standards.
 *
 * Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
 * following parameters:
 *
 * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
 * Width                      : 16 bit
 * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
 * Initialization             : 0000
 * Reflect Input byte         : False
 * Reflect Output CRC         : False
 * Xor constant to output CRC : 0000
 * Output for "123456789"     : 31C3
 */

static const unsigned short crc16tab[256] = {
  0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
  0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
  0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
  0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
  0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
  0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
  0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
  0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
  0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
  0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
  0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
  0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
  0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
  0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
  0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
  0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
  0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
  0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
  0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
  0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
  0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
  0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
  0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
  0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
  0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
  0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
  0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
  0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
  0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
  0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
  0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
  0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

unsigned short
sbuf_crc16 (sbuf_pos start_pos, size_t len, unsigned short sd)
{
  sbuf_pos pos = start_pos;
  int counter;
  unsigned short crc = sd;

  for (counter = 0; counter < (signed) len; counter++)
    {
      crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ sbuf_char (pos)) & 0x00FF];
      sbuf_next_pos (&pos);
    }
  return crc;
}

int
sbuf_string2ll (sbuf_pos start_pos, size_t slen, long long *value)
{
  sbuf_pos p = start_pos;
  char c;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen)
    return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && sbuf_char (p) == '0')
    {
      if (value != NULL)
	*value = 0;
      return 1;
    }

  if (sbuf_char (p) == '-')
    {
      negative = 1;
      sbuf_next_pos (&p);
      plen++;

      /* Abort on only a negative sign. */
      if (plen == slen)
	return 0;
    }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  c = sbuf_char (p);
  if (c >= '1' && c <= '9')
    {
      v = c - '0';
      sbuf_next_pos (&p);
      plen++;
    }
  else if (c == '0' && slen == 1)
    {
      *value = 0;
      return 1;
    }
  else
    {
      return 0;
    }

  c = sbuf_char (p);
  while (plen < slen && c >= '0' && c <= '9')
    {
      if (v > (ULLONG_MAX / 10))	/* Overflow. */
	return 0;
      v *= 10;

      if (v > (ULLONG_MAX - (c - '0')))	/* Overflow. */
	return 0;
      v += c - '0';

      sbuf_next_pos (&p);
      c = sbuf_char (p);
      plen++;
    }

  /* Return if not all bytes were used. */
  if (plen < slen)
    return 0;

  if (negative)
    {
      if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1))	/* Overflow. */
	return 0;
      if (value != NULL)
	*value = -v;
    }
  else
    {
      if (v > LLONG_MAX)	/* Overflow. */
	return 0;
      if (value != NULL)
	*value = v;
    }
  return 1;
}

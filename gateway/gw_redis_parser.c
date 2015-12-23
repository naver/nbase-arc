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

#include "gw_redis_parser.h"

/* 
 * (Example)
 * *2\r\n$3\r\nGET\r\n$10\r\nXXXXXXXXXX\r\n
 *                           ^         ^
 *                           |         |
 *                           |         end
 *                           start
 */
typedef struct ArgPos
{
  sbuf_pos start;
  ssize_t len;
} ArgPos;

#define INIT_ARRAY_COUNT 100
ARRAY_HEAD (argpos_array, ArgPos, INIT_ARRAY_COUNT);

/* 
 * (Example)
 * *2\r\n$3\r\nGET\r\n$10\r\nXXXXXXXXXX\r\n
 * ^                                       ^
 * |                                       |
 * |                                       end
 * start
 */
struct ParseContext
{
  int type;
  sbuf_pos pos;
  int multibulklen;
  long bulklen;
  struct argpos_array args;
  mempool_hdr_t *mp_parse_ctx;
};

static int multibulkParser (ParseContext * ctx, sbuf_hdr * stream,
			    sbuf ** query, sds * err);

static int
getReplyType (char c)
{
  switch (c)
    {
    case '-':
    case '+':
    case ':':
      return TYPE_INLINE;
    case '$':
      return TYPE_BULK;
    case '*':
      return TYPE_MULTIBULK;
    default:
      return TYPE_UNKNOWN;
    }
}

static int
getRequestType (char c)
{
  switch (c)
    {
    case '*':
      return TYPE_MULTIBULK;
    default:
      return TYPE_INLINE;
    }
}

static void
appendArgposToArray (ParseContext * ctx, sbuf_pos start, ssize_t len)
{
  ArgPos a;

  a.start = start;
  a.len = len;

  ARRAY_PUSH (&ctx->args, a);
}

static int
parseLine (sbuf_pos start, sbuf_pos last, sbuf_pos * ret_next,
	   ArgPos * ret_arg)
{
  sbuf_pos newline;
  size_t offset;
  int ret;

  ret = sbuf_memchr (start, last, '\r', &newline, &offset);
  if (ret == ERR)
    {
      return PARSE_INSUFFICIENT_DATA;
    }

  ret = sbuf_offset (newline, last, 2, ret_next);
  if (ret == ERR)
    {
      return PARSE_INSUFFICIENT_DATA;
    }

  ret_arg->start = start;
  ret_arg->len = offset;
  return PARSE_COMPLETE;
}

static int
parseNumber (sbuf_pos start, sbuf_pos last, sbuf_pos * ret_next,
	     long long *ret_ll)
{
  ArgPos arg;
  int ret, ok;

  ret = parseLine (start, last, ret_next, &arg);
  if (ret == PARSE_INSUFFICIENT_DATA)
    {
      return PARSE_INSUFFICIENT_DATA;
    }

  if (arg.len < 1)
    {
      return PARSE_ERROR;
    }

  sbuf_next_pos (&arg.start);	// Skip '$' character
  ok = sbuf_string2ll (arg.start, arg.len - 1, ret_ll);
  if (!ok)
    {
      return PARSE_ERROR;
    }

  return PARSE_COMPLETE;
}

static int
parseBulkStr (sbuf_pos start, sbuf_pos last, size_t bulklen,
	      sbuf_pos * ret_next)
{
  int ret;

  ret = sbuf_offset (start, last, bulklen + 2, ret_next);
  if (ret == ERR)
    {
      return PARSE_INSUFFICIENT_DATA;
    }
  return PARSE_COMPLETE;
}

static sds
makeSdsFromSbuf (sbuf * buf)
{
  sds s;
  int ret;

  s = sdsMakeRoomFor (sdsempty (), sbuf_len (buf) + 1);
  ret = sbuf_copy_buf (s, sbuf_start_pos (buf), sbuf_len (buf));
  assert (ret == OK);
  sdsIncrLen (s, sbuf_len (buf));
  return s;
}

static int
inlineParser (ParseContext * ctx, sbuf_hdr * stream, sbuf ** query, sds * err)
{
  sbuf_pos last, newline, next_pos;
  sbuf *tmp_sbuf;
  size_t len;
  sds aux, remaining, *argv;
  int ret, argc, i, count;

  last = stream_last_pos (stream);

  ret = sbuf_memchr (ctx->pos, last, '\n', &newline, &len);
  if (ret == ERR)
    {
      if (sbuf_offset_len (ctx->pos, last) > PARSE_INLINE_MAX_SIZE)
	{
	  *err = sdsnew ("Protocol error: too big inline request");
	  return PARSE_ERROR;
	}
      return PARSE_INSUFFICIENT_DATA;
    }

  ret = sbuf_offset (newline, last, 1, &next_pos);
  if (ret == ERR)
    {
      return PARSE_INSUFFICIENT_DATA;
    }

  // Copy parsed data to sds and free the data from the stream.
  tmp_sbuf = stream_create_sbuf (stream, next_pos);
  aux = makeSdsFromSbuf (tmp_sbuf);
  sbuf_free (tmp_sbuf);

  argv = sdssplitargs (aux, &argc);
  sdsfree (aux);
  if (!argv)
    {
      *err = sdsnew ("Protocol error: unbalanced quotes in request");
      return PARSE_ERROR;
    }

  // Copy remaining data to sds and free the data from the stream.
  tmp_sbuf = stream_create_sbuf (stream, stream_last_pos (stream));
  remaining = makeSdsFromSbuf (tmp_sbuf);
  sbuf_free (tmp_sbuf);

  // Rewrite parsed data in a multibulk format and append to the stream.
  count = 0;
  for (i = 0; i < argc; i++)
    {
      if (sdslen (argv[i]))
	{
	  count++;
	}
    }

  ret = stream_append_printf (stream, "*%d\r\n", count);
  assert (ret == OK);
  for (i = 0; i < argc; i++)
    {
      if (sdslen (argv[i]))
	{
	  ret = stream_append_printf (stream, "$%ld\r\n", sdslen (argv[i]));
	  assert (ret == OK);
	  ret = stream_append_copy_buf (stream, argv[i], sdslen (argv[i]));
	  assert (ret == OK);
	  ret = stream_append_copy_buf (stream, (void *) "\r\n", 2);
	  assert (ret == OK);
	}
    }
  sdsfreesplitres (argv, argc);

  // Append remaining data saved previously to the stream.
  ret = stream_append_copy_buf (stream, remaining, sdslen (remaining));
  sdsfree (remaining);

  resetParseContext (ctx, stream);
  ret = multibulkParser (ctx, stream, query, err);
  assert (ret == PARSE_COMPLETE);

  return PARSE_COMPLETE;
}

static int
multibulkParser (ParseContext * ctx, sbuf_hdr * stream, sbuf ** query,
		 sds * err)
{
  sbuf_pos last, next_pos;
  long long ll;
  int ret;

  last = stream_last_pos (stream);

  /* Get Multibulk Length */
  if (ctx->multibulklen == 0)
    {
      ret = parseNumber (ctx->pos, last, &next_pos, &ll);
      if (ret == PARSE_INSUFFICIENT_DATA)
	{
	  if (sbuf_offset_len (ctx->pos, last) > PARSE_INLINE_MAX_SIZE)
	    {
	      *err = sdsnew ("Protocol error: too big mbulk count string");
	      return PARSE_ERROR;
	    }
	  return PARSE_INSUFFICIENT_DATA;
	}
      if (ret == PARSE_ERROR || (ret == PARSE_COMPLETE && ll > 1024 * 1024))
	{
	  *err = sdsnew ("Protocol error: invalid multibulk length");
	  return PARSE_ERROR;
	}
      ctx->pos = next_pos;
      ctx->multibulklen = ll;

      /* Null Bulk */
      if (ll <= 0)
	{
	  *query = stream_create_sbuf (stream, ctx->pos);
	  return PARSE_COMPLETE;
	}
    }

  while (ctx->multibulklen)
    {
      /* Get Bulk Length */
      if (ctx->bulklen == -1)
	{
	  ret = parseNumber (ctx->pos, last, &next_pos, &ll);
	  if (ret == PARSE_INSUFFICIENT_DATA)
	    {
	      if (sbuf_offset_len (ctx->pos, last) > PARSE_INLINE_MAX_SIZE)
		{
		  *err = sdsnew ("Protocol error: too big bulk count string");
		  return PARSE_ERROR;
		}
	      return PARSE_INSUFFICIENT_DATA;
	    }
	  if (sbuf_char (ctx->pos) != '$')
	    {
	      *err =
		sdscatprintf (sdsempty (),
			      "Protocol error: expected '$', got '%c'",
			      sbuf_char (ctx->pos));
	      return PARSE_ERROR;
	    }
	  if (ret == PARSE_ERROR
	      || (ret == PARSE_COMPLETE
		  && (ll < 0 || ll > 512 * 1024 * 1024)))
	    {
	      *err = sdsnew ("Protocol error: invalid bulk length");
	      return PARSE_ERROR;
	    }
	  ctx->pos = next_pos;
	  ctx->bulklen = ll;
	}

      /* Get Bulk String */
      ret = parseBulkStr (ctx->pos, last, ctx->bulklen, &next_pos);
      if (ret == PARSE_INSUFFICIENT_DATA)
	{
	  return PARSE_INSUFFICIENT_DATA;
	}
      appendArgposToArray (ctx, ctx->pos, ctx->bulklen);
      ctx->pos = next_pos;
      ctx->bulklen = -1;
      ctx->multibulklen--;
    }

  *query = stream_create_sbuf (stream, ctx->pos);
  return PARSE_COMPLETE;
}

static int
replyParserLoop (ParseContext * ctx, sbuf_hdr * stream, sbuf ** query,
		 sds * err)
{
  ArgPos arg;
  sbuf_pos last, next_pos;
  long long ll;
  int type, ret;

  last = stream_last_pos (stream);

  while (ctx->multibulklen)
    {
      if (ctx->bulklen == -1)
	{
	  if (SBUF_POS_EQUAL (&ctx->pos, &last))
	    {
	      return PARSE_INSUFFICIENT_DATA;
	    }
	  type = getReplyType (sbuf_char (ctx->pos));

	  switch (type)
	    {
	    case TYPE_INLINE:
	      ret = parseLine (ctx->pos, last, &next_pos, &arg);
	      if (ret == PARSE_INSUFFICIENT_DATA)
		{
		  return PARSE_INSUFFICIENT_DATA;
		}
	      appendArgposToArray (ctx, arg.start, arg.len);
	      ctx->pos = next_pos;
	      ctx->multibulklen--;
	      break;
	    case TYPE_BULK:
	      ret = parseNumber (ctx->pos, last, &next_pos, &ll);
	      if (ret == PARSE_INSUFFICIENT_DATA)
		{
		  return PARSE_INSUFFICIENT_DATA;
		}
	      else if (ret == PARSE_ERROR)
		{
		  *err = sdsnew ("Protocol error: invalid bulk length");
		  return PARSE_ERROR;
		}
	      ctx->pos = next_pos;
	      ctx->bulklen = ll;

	      /* Null Bulk */
	      if (ll < 0)
		{
		  appendArgposToArray (ctx, ctx->pos, ll);
		  ctx->bulklen = -1;
		  ctx->multibulklen--;
		}
	      break;
	    default:
	      *err =
		sdscatprintf (sdsempty (),
			      "Protocol error, got %c as reply type byte",
			      sbuf_char (ctx->pos));
	      return PARSE_ERROR;
	    }
	}

      if (ctx->bulklen == -1)
	{
	  continue;
	}
      ret = parseBulkStr (ctx->pos, last, ctx->bulklen, &next_pos);
      if (ret == PARSE_INSUFFICIENT_DATA)
	{
	  return PARSE_INSUFFICIENT_DATA;
	}
      appendArgposToArray (ctx, ctx->pos, ctx->bulklen);
      ctx->pos = next_pos;
      ctx->bulklen = -1;
      ctx->multibulklen--;
    }
  *query = stream_create_sbuf (stream, ctx->pos);
  return PARSE_COMPLETE;
}

mempool_hdr_t *
createParserMempool (void)
{
  return mempool_create (sizeof (struct ParseContext),
			 MEMPOOL_DEFAULT_POOL_SIZE);
}

void
destroyParserMempool (mempool_hdr_t * mp_parse_ctx)
{
  mempool_destroy (mp_parse_ctx);
}

int
getArgumentPosition (ParseContext * ctx, int index, sbuf_pos * ret_start,
		     ssize_t * ret_len)
{
  ArgPos arg;

  if (ARRAY_N (&ctx->args) <= index)
    {
      return ERR;
    }
  arg = ARRAY_GET (&ctx->args, index);

  *ret_start = arg.start;
  *ret_len = arg.len;
  return OK;
}

int
getArgumentCount (ParseContext * ctx)
{
  return ARRAY_N (&ctx->args);
}

int
getParsedStr (ParseContext * ctx, int idx, char **ret_str)
{
  sbuf_pos start_pos;
  ssize_t len;
  char *val;
  int ret;

  if (getArgumentCount (ctx) <= idx)
    {
      return ERR;
    }

  ret = getArgumentPosition (ctx, idx, &start_pos, &len);
  assert (ret == OK);

  val = zmalloc (len + 1);
  ret = sbuf_copy_buf (val, start_pos, len);
  assert (ret == OK);
  val[len] = '\0';

  *ret_str = val;
  return OK;
}

int
getParsedNumber (ParseContext * ctx, int idx, long long *ret_ll)
{
  sbuf_pos start_pos;
  ssize_t len;
  int ret;

  if (getArgumentCount (ctx) <= idx)
    {
      return ERR;
    }

  ret = getArgumentPosition (ctx, idx, &start_pos, &len);
  assert (ret == OK);
  ret = sbuf_string2ll (start_pos, len, ret_ll);
  if (!ret)
    {
      return ERR;
    }
  return OK;
}

ParseContext *
createParseContext (mempool_hdr_t * mp_parse_ctx, sbuf_hdr * stream)
{
  ParseContext *ctx;

  ctx = mempool_alloc (mp_parse_ctx);
  if (!ctx)
    {
      return NULL;
    }

  ctx->type = 0;
  ctx->pos = stream_start_pos (stream);
  ctx->multibulklen = 0;
  ctx->bulklen = -1;
  ARRAY_INIT (&ctx->args);
  ctx->mp_parse_ctx = mp_parse_ctx;
  return ctx;
}

void
resetParseContext (ParseContext * ctx, sbuf_hdr * stream)
{
  ctx->pos = stream_start_pos (stream);
  ctx->type = 0;
  ctx->multibulklen = 0;
  ctx->bulklen = -1;
  ARRAY_CLEAR (&ctx->args);
}

void
deleteParseContext (ParseContext * ctx)
{
  ARRAY_FINALIZE (&ctx->args);
  mempool_free (ctx->mp_parse_ctx, ctx);
}

int
requestParser (ParseContext * ctx, sbuf_hdr * stream, sbuf ** query,
	       sds * err)
{
  sbuf_pos last;

  last = stream_last_pos (stream);

  if (SBUF_POS_EQUAL (&ctx->pos, &last))
    {
      return PARSE_INSUFFICIENT_DATA;
    }
  if (ctx->type == 0)
    {
      ctx->type = getRequestType (sbuf_char (ctx->pos));
    }
  if (ctx->type == TYPE_MULTIBULK)
    {
      return multibulkParser (ctx, stream, query, err);
    }
  assert (ctx->type == TYPE_INLINE);
  return inlineParser (ctx, stream, query, err);
}

int
replyParser (ParseContext * ctx, sbuf_hdr * stream, sbuf ** query, sds * err)
{
  sbuf_pos last, next_pos;
  long long ll;
  int ret;

  last = stream_last_pos (stream);

  if (SBUF_POS_EQUAL (&ctx->pos, &last))
    {
      return PARSE_INSUFFICIENT_DATA;
    }
  if (ctx->type == 0)
    {
      ctx->type = getReplyType (sbuf_char (ctx->pos));
      if (ctx->type == TYPE_UNKNOWN)
	{
	  *err =
	    sdscatprintf (sdsempty (),
			  "Protocol error, got %c as reply type byte",
			  sbuf_char (ctx->pos));
	  return PARSE_ERROR;
	}
    }

  /* Get Multibulklen */
  switch (ctx->type)
    {
    case TYPE_MULTIBULK:
      if (ctx->multibulklen == 0)
	{
	  ret = parseNumber (ctx->pos, last, &next_pos, &ll);
	  if (ret == PARSE_INSUFFICIENT_DATA)
	    {
	      return PARSE_INSUFFICIENT_DATA;
	    }
	  else if (ret == PARSE_ERROR)
	    {
	      *err = sdsnew ("Protocol error: invalid multibulk length");
	      return PARSE_ERROR;
	    }

	  ctx->pos = next_pos;
	  ctx->multibulklen = ll;
	}

      /* Null Bulk */
      if (ctx->multibulklen <= 0)
	{
	  *query = stream_create_sbuf (stream, ctx->pos);
	  return PARSE_COMPLETE;
	}
      break;
    case TYPE_INLINE:
    case TYPE_BULK:
      ctx->multibulklen = 1;
      break;
    case TYPE_UNKNOWN:
      *err =
	sdscatprintf (sdsempty (),
		      "Protocol error, got %c as reply type byte",
		      sbuf_char (ctx->pos));
      return PARSE_ERROR;
    default:
      break;
    }
  return replyParserLoop (ctx, stream, query, err);
}

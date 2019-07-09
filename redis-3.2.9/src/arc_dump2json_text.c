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

#ifdef NBASE_ARC

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <assert.h>
#include "arc_dump_plugin.h"

static long long num_s3collections = 0;

static void
fwrite_repr (char *p, size_t len, FILE * fp)
{
  int index, n;
  char s[8192];

  index = 0;
  s[index++] = '"';
  while (len--)
    {
      switch (*p)
	{
	case '"':		/* quotation mark */
	  s[index++] = '\\';
	  s[index++] = '\"';
	  break;
	case '\\':		/* reverse solidus */
	  s[index++] = '\\';
	  s[index++] = '\\';
	  break;
	case '/':		/* solidus */
	  s[index++] = '\\';
	  s[index++] = '/';
	  break;
	case '\b':		/* backspace */
	  s[index++] = '\\';
	  s[index++] = 'b';
	  break;
	case '\f':		/* formfeed */
	  s[index++] = '\\';
	  s[index++] = 'f';
	  break;
	case '\n':		/* newline */
	  s[index++] = '\\';
	  s[index++] = 'n';
	  break;
	case '\r':		/* carriage return */
	  s[index++] = '\\';
	  s[index++] = 'r';
	  break;
	case '\t':		/* horizontal tab */
	  s[index++] = '\\';
	  s[index++] = 't';
	  break;
	default:
	  if (isprint (*p))
	    {
	      s[index++] = *p;
	    }
	  else
	    {
	      n = snprintf (s + index, 16, "\\u%04x", (unsigned char) *p);
	      index += n;
	    }
	  break;
	}
      p++;
      if (index > 8000)
	{
	  fwrite (s, index, 1, fp);
	  index = 0;
	}
    }

  s[index++] = '"';
  fwrite (s, index, 1, fp);

}

static int
initialize (int argc, char **argv, void **pctx)
{
  FILE *fp;

  if (argc != 1)
    return RET_ERR;

  fp = fopen (argv[0], "w");
  if (fp == NULL)
    return RET_ERR;

  *pctx = fp;
  return RET_OK;
}

static int
dumpinfo (void *ctx, int rdbver, long long smr_seqnum, long long smr_mstime)
{
  printf ("Dump Start, rdbver:%d, smr_seqnum:%lld, smr_mstime:%lld\r\n",
	  rdbver, smr_seqnum, smr_mstime);
  return RET_OK;
}

static int
begin_key (void *ctx, int type, char *key, int keylen, long long expiretime)
{
  FILE *fp = ctx;

  fputs ("{\"key\":", fp);
  fwrite_repr (key, keylen, fp);
  fprintf (fp, " ,\"expire\":%lld,", expiretime);

  switch (type)
    {
    case PLUGIN_RDB_TYPE_STRING:
      fputs ("\"type\":\"string\",\"value\":", fp);
      break;
    case PLUGIN_RDB_TYPE_LIST:
      fputs ("\"type\":\"list\",\"value\":[", fp);
      break;
    case PLUGIN_RDB_TYPE_SET:
      fputs ("\"type\":\"set\",\"value\":[", fp);
      break;
    case PLUGIN_RDB_TYPE_ZSET:
      fputs ("\"type\":\"zset\",\"value\":[", fp);
      break;
    case PLUGIN_RDB_TYPE_HASH:
      fputs ("\"type\":\"hash\",\"value\":[", fp);
      break;
    case PLUGIN_RDB_TYPE_SSS:
      fputs ("\"type\":\"sss\",\"value\":[", fp);
      num_s3collections = 0;
      break;
    }
  return RET_OK;
}

static int
string_val (void *ctx, char *val, int vallen)
{
  FILE *fp = ctx;

  fwrite_repr (val, vallen, fp);
  return RET_OK;
}

static int
list_val (void *ctx, char *val, int vallen)
{
  FILE *fp = ctx;

  fwrite_repr (val, vallen, fp);
  fputc (',', fp);
  return RET_OK;
}

static int
set_val (void *ctx, char *val, int vallen)
{
  FILE *fp = ctx;

  fwrite_repr (val, vallen, fp);
  fputc (',', fp);
  return RET_OK;
}

static int
zset_val (void *ctx, char *val, int vallen, double score)
{
  FILE *fp = ctx;

  fputs ("{\"data\":", fp);
  fwrite_repr (val, vallen, fp);
  fprintf (fp, ",\"score\":%.17g},", score);
  return RET_OK;
}

static int
hash_val (void *ctx, char *hkey, int hkeylen, char *hval, int hvallen)
{
  FILE *fp = ctx;

  fputs ("{\"hkey\":", fp);
  fwrite_repr (hkey, hkeylen, fp);
  fputs (",\"hval\":", fp);
  fwrite_repr (hval, hvallen, fp);
  fputs ("},", fp);
  return RET_OK;
}

static int
begin_sss_collection (void *ctx, char *ks, int kslen, char *svc, int svclen,
		      char *key, int keylen, int mode)
{
  FILE *fp = ctx;

  fputs ("{\"ks\":", fp);
  fwrite_repr (ks, kslen, fp);
  fputs (",\"svc\":", fp);
  fwrite_repr (svc, svclen, fp);
  fputs (",\"key\":", fp);
  fwrite_repr (key, keylen, fp);
  if (mode == PLUGIN_SSS_KV_LIST)
    {
      fputs (",\"mode\":\"list\",\"value\":[", fp);
    }
  else
    {
      fputs (",\"mode\":\"set\",\"value\":[", fp);
    }

  return RET_OK;
}

static int
sss_val (void *ctx, char *val, int vallen, long long val_expire)
{
  FILE *fp = ctx;

  fputs ("{\"data\":", fp);
  fwrite_repr (val, vallen, fp);
  fprintf (fp, ",\"expire\":%lld},", val_expire);
  return RET_OK;
}

static int
end_sss_collection (void *ctx, int mode)
{
  FILE *fp = ctx;

  fseek (fp, -1, SEEK_CUR);
  fputs ("]},", fp);
  num_s3collections++;

  return RET_OK;
}

static int
end_key (void *ctx, int type)
{
  FILE *fp = ctx;

  if (type != PLUGIN_RDB_TYPE_SSS || num_s3collections > 0)
    {
      // Empty s3 object can exist by active purge during read operation.
      // Empty objects are deleted during s3 GC.
      fseek (fp, -1, SEEK_CUR);
      num_s3collections = 0;
    }
  switch (type)
    {
    case PLUGIN_RDB_TYPE_STRING:
      fputs ("\"}\n", fp);
      break;
    case PLUGIN_RDB_TYPE_LIST:
    case PLUGIN_RDB_TYPE_SET:
    case PLUGIN_RDB_TYPE_ZSET:
    case PLUGIN_RDB_TYPE_HASH:
    case PLUGIN_RDB_TYPE_SSS:
      fputs ("]}\n", fp);
      break;
    }
  return RET_OK;
}

static int
finalize (void *ctx)
{
  FILE *fp = ctx;

  fclose (fp);
  return RET_OK;
}

struct dump_plugin_callback callback = {
  initialize,
  dumpinfo,
  begin_key,
  string_val,
  list_val,
  set_val,
  zset_val,
  hash_val,
  begin_sss_collection,
  sss_val,
  end_sss_collection,
  end_key,
  finalize
};
#endif

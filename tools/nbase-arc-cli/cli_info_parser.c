#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include "dict.h"
#include "sds.h"
#include "crc16.h"

static unsigned int
dictStrCaseHash (const void *key)
{
  return dictGenCaseHashFunction ((unsigned char *) key,
				  strlen ((char *) key));
}

static int
dictSdsKeyCaseCompare (void *privdata, const void *key1, const void *key2)
{
  DICT_NOTUSED (privdata);

  return strcasecmp (key1, key2) == 0;
}

static void
dictSdsDestructor (void *privdata, void *val)
{
  DICT_NOTUSED (privdata);

  sdsfree (val);
}

static dictType infoDictType = {
  dictStrCaseHash,
  NULL,
  NULL,
  dictSdsKeyCaseCompare,
  dictSdsDestructor,
  dictSdsDestructor
};

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
static int
string2ll (const char *s, size_t slen, long long *value)
{
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen)
    return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0')
    {
      if (value != NULL)
	*value = 0;
      return 1;
    }

  if (p[0] == '-')
    {
      negative = 1;
      p++;
      plen++;

      /* Abort on only a negative sign. */
      if (plen == slen)
	return 0;
    }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9')
    {
      v = p[0] - '0';
      p++;
      plen++;
    }
  else if (p[0] == '0' && slen == 1)
    {
      *value = 0;
      return 1;
    }
  else
    {
      return 0;
    }

  while (plen < slen && p[0] >= '0' && p[0] <= '9')
    {
      if (v > (ULLONG_MAX / 10))	/* Overflow. */
	return 0;
      v *= 10;

      if (v > (ULLONG_MAX - (p[0] - '0')))	/* Overflow. */
	return 0;
      v += p[0] - '0';

      p++;
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

static long long
numberAfterCharEqual (sds s, int *ok)
{
  int argc;
  sds *argv;
  int ret;
  long long val;

  argv = sdssplitlen (s, sdslen (s), "=", 1, &argc);
  if (argc != 2)
    {
      if (ok)
	{
	  *ok = 0;
	}
      if (argc > 0)
	{
	  sdsfreesplitres (argv, argc);
	}
      return 0;
    }

  ret = string2ll (argv[1], sdslen (argv[1]), &val);
  sdsfreesplitres (argv, argc);
  if (!ret)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return 0;
    }

  if (ok)
    {
      *ok = 1;
    }

  return val;
}

dict *
getDictFromInfoStr (sds str)
{
  dict *d;
  int linec;
  sds *linev;
  int i;

  d = dictCreate (&infoDictType, NULL);
  linev = sdssplitlen (str, sdslen (str), "\r\n", 2, &linec);
  for (i = 0; i < linec; i++)
    {
      int argc;
      sds *argv;

      argv = sdssplitlen (linev[i], sdslen (linev[i]), ":", 1, &argc);
      if (argc > 3)
	{
	  /* We don't have result of info command with colon twice or more. */
	  sdsfreesplitres (argv, argc);
	  continue;
	}

      if (argc == 1)
	{
	  sdsfreesplitres (argv, argc);
	  continue;
	}

      if (argc == 2)
	{
	  dictAdd (d, sdsdup (argv[0]), sdsdup (argv[1]));
	}
      sdsfreesplitres (argv, argc);
    }
  sdsfreesplitres (linev, linec);

  return d;
}

void
releaseDict (dict * d)
{
  dictRelease (d);
}

long long
fetchLongLong (dict * d, const void *key, int *ok)
{
  sds s = dictFetchValue (d, key);
  long long val;
  int ret;

  if (!s)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return 0;
    }

  ret = string2ll (s, sdslen (s), &val);
  if (!ret)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return 0;
    }

  if (ok)
    {
      *ok = 1;
    }

  return val;
}

sds
fetchSds (dict * d, const void *key, int *ok)
{
  void *s = dictFetchValue (d, key);
  sds val;

  if (!s)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return NULL;
    }
  val = sdsdup (s);
  if (!val)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return NULL;
    }

  if (ok)
    {
      *ok = 1;
    }
  return val;

}

void
getDbstat (dict * d, int *ok, long long *keys, long long *expires,
	   long long *avg_ttl)
{
  const char *key = "db0";
  sds s;
  int _ok;
  int itemc;
  sds *itemv;

  *keys = *expires = *avg_ttl = 0;

  s = fetchSds (d, key, &_ok);
  if (!_ok)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return;
    }

  itemv = sdssplitlen (s, sdslen (s), ",", 1, &itemc);
  sdsfree (s);
  if (itemc != 2 && itemc != 3)
    {
      if (ok)
	{
	  *ok = 0;
	}
      if (itemc > 0)
	{
	  sdsfreesplitres (itemv, itemc);
	}
      return;
    }

  /* keys */
  *keys = numberAfterCharEqual (itemv[0], NULL);
  *expires = numberAfterCharEqual (itemv[1], NULL);
  if (itemc == 3)
    {
      // Old version redis doesn't provide avg_ttl
      *avg_ttl = numberAfterCharEqual (itemv[2], NULL);
    }

  if (ok)
    {
      *ok = 1;
    }
  sdsfreesplitres (itemv, itemc);
  return;
}

void
getRedisInfo (dict * d, const void *key, int *ok, sds * addr,
	      int *port, int *pg_id, int *max_latency, sds * max_cmd)
{
  sds s;
  int _ok, i;
  int itemc;
  sds *itemv;

  s = fetchSds (d, key, &_ok);
  if (!_ok)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return;
    }

  itemv = sdssplitargs (s, &itemc);
  sdsfree (s);
  for (i = 0; i < itemc; i++)
    {
      int argc;
      sds *argv;
      argv = sdssplitlen (itemv[i], sdslen (itemv[i]), "=", 1, &argc);
      if (argc != 2)
	{
	  if (argc > 0)
	    {
	      sdsfreesplitres (argv, argc);
	    }
	  continue;
	}

      if (addr && !strcasecmp (argv[0], "addr"))
	{
	  *addr = sdsdup (argv[1]);
	}
      else if (port && !strcasecmp (argv[0], "port"))
	{
	  long long val;

	  if (!string2ll (argv[1], sdslen (argv[1]), &val))
	    {
	      val = -1;
	    }
	  *port = val;
	}
      else if (pg_id && !strcasecmp (argv[0], "pg_id"))
	{
	  long long val;

	  if (!string2ll (argv[1], sdslen (argv[1]), &val))
	    {
	      val = -1;
	    }
	  *pg_id = val;
	}
      else if (max_latency && !strcasecmp (argv[0], "max_latency"))
	{
	  long long val;

	  if (!string2ll (argv[1], sdslen (argv[1]), &val))
	    {
	      val = -1;
	    }
	  *max_latency = val;
	}
      else if (max_cmd && !strcasecmp (argv[0], "max_cmd"))
	{
	  *max_cmd = sdsdup (argv[1]);
	}
      sdsfreesplitres (argv, argc);
    }
  sdsfreesplitres (itemv, itemc);

  if (ok)
    {
      *ok = 1;
    }
  return;
}

int
getPgMapSize (dict * d, int *ok)
{
  return fetchLongLong (d, "cluster_slot_size", ok);
}

int
getSlotNoOfKey (dict * d, const char *key, int *ok)
{
  int pg_map_size;
  int _ok;

  pg_map_size = getPgMapSize (d, &_ok);
  if (!_ok || pg_map_size == 0)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return 0;
    }

  if (ok)
    {
      *ok = 1;
    }
  return crc16 (key, strlen (key), 0L) % pg_map_size;
}

static int *
getPgMapFromRLE (sds pg_map_rle, int pg_map_size)
{
  int *pg_map;
  int argc, i, j, index;
  sds *argv;

  pg_map = malloc (pg_map_size * sizeof (int));
  if (!pg_map)
    {
      return NULL;
    }

  argv = sdssplitargs (pg_map_rle, &argc);
  if (!argv)
    {
      free (pg_map);
      return NULL;
    }

  if (argc % 2 != 0)
    {
      goto error_return;
    }

  index = 0;
  for (i = 0; i < argc; i += 2)
    {
      long long pg_id, count;
      int ret;

      ret = string2ll (argv[i], sdslen (argv[i]), &pg_id);
      if (!ret)
	{
	  goto error_return;
	}
      ret = string2ll (argv[i + 1], sdslen (argv[i + 1]), &count);
      if (!ret)
	{
	  goto error_return;
	}

      for (j = 0; j < count; j++)
	{
	  if (index >= pg_map_size)
	    {
	      goto error_return;
	    }
	  pg_map[index++] = pg_id;
	}
    }
  sdsfreesplitres (argv, argc);

  return pg_map;

error_return:
  free (pg_map);
  sdsfreesplitres (argv, argc);
  return NULL;
}

int *
getPgMapArray (dict * d, int *ok, int *size)
{
  sds pg_map_rle;
  int pg_map_size;
  int *pg_map;
  int _ok;

  pg_map_size = getPgMapSize (d, &_ok);
  if (!_ok)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return NULL;
    }

  pg_map_rle = fetchSds (d, "pg_map_rle", &_ok);
  if (!_ok)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return NULL;
    }

  pg_map = getPgMapFromRLE (pg_map_rle, pg_map_size);
  sdsfree (pg_map_rle);
  if (!pg_map)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return NULL;
    }

  *size = pg_map_size;

  if (ok)
    {
      *ok = 1;
    }
  return pg_map;
}

int
getPgIdOfKey (dict * d, sds key, int *ok)
{
  int *pg_map;
  int slot_no, pg_map_size, pg_id;
  int _ok;

  slot_no = getSlotNoOfKey (d, key, &_ok);
  if (!_ok)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return 0;
    }

  pg_map = getPgMapArray (d, &_ok, &pg_map_size);
  if (!_ok)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return 0;
    }

  pg_id = pg_map[slot_no];
  free (pg_map);

  if (ok)
    {
      *ok = 1;
    }
  return pg_id;
}

sds *
getRedisIdsOfPg (dict * d, int *ok, int pg_id, int *count)
{
  int argc, _ok, i;
  sds *argv, *redis_ids;
  sds s, key;
  int _pg_id;

  *count = 0;
  s = fetchSds (d, "redis_id_list", &_ok);
  if (!_ok)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return NULL;
    }

  argv = sdssplitargs (s, &argc);
  sdsfree (s);
  if (!argv)
    {
      if (ok)
	{
	  *ok = 0;
	}
      return NULL;
    }

  redis_ids = malloc (argc * sizeof (sds));

  for (i = 0; i < argc; i++)
    {
      key = sdscatprintf (sdsempty (), "REDIS_%s", argv[i]);
      getRedisInfo (d, key, &_ok, NULL, NULL, &_pg_id, NULL, NULL);
      if (!_ok)
	{
	  sdsfree (key);
	  continue;
	}

      if (pg_id == _pg_id)
	{
	  redis_ids[(*count)++] = sdsdup (key);
	}
      sdsfree (key);
    }

  sdsfreesplitres (argv, argc);

  if (ok)
    {
      *ok = 1;
    }

  return redis_ids;
}

void
freeRedisIdsOfPg (sds * tokens, int count)
{
  if (!tokens)
    return;
  while (count--)
    sdsfree (tokens[count]);
  free (tokens);
}

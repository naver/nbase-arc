#include <string.h>

#include "arcci.h"
#include "sds.h"
#include "dict.h"
#include "cli_cmd.h"
#include "cli_info_parser.h"

#define NOTUSED(V) ((void) V)

static dict *
getDictOfInfoAll (arc_t * arc, sds * err, int timeout)
{
  int argc;
  sds *argv;
  sds out;
  unsigned raw_mode = 1;
  dict *d;

  argv = sdssplitargs ("INFO ALL", &argc);
  out = executeCommand (arc, argc, argv, timeout, raw_mode, err);
  sdsfreesplitres (argv, argc);
  if (!out)
    {
      return NULL;
    }

  d = getDictFromInfoStr (out);
  sdsfree (out);

  return d;
}

static sds
whereisCommand (arc_t * arc, int argc, char **argv, int timeout, sds * err)
{
  int pg_id, count, slot_no;
  sds *redis_ids;
  sds key, out;
  dict *d;
  int ok, i;

  if (argc != 2)
    {
      if (err)
	{
	  *err = sdscat (*err, "Invalid format.\n usage:WHEREIS <key>\n");
	  return NULL;
	}
    }

  key = argv[1];

  d = getDictOfInfoAll (arc, err, timeout);
  if (!d)
    {
      return NULL;
    }

  pg_id = getPgIdOfKey (d, key, &ok);
  if (!ok)
    {
      if (err)
	{
	  *err = sdscat (*err, "Can't get PG ID from INFO command\n");
	}
      releaseDict (d);
      return NULL;
    }

  redis_ids = getRedisIdsOfPg (d, &ok, pg_id, &count);
  if (!ok)
    {
      if (err)
	{
	  *err = sdscat (*err, "Can't get Redis IDs from INFO command\n");
	}
      releaseDict (d);
      return NULL;
    }

  slot_no = getSlotNoOfKey (d, key, &ok);
  if (!ok)
    {
      if (err)
	{
	  *err =
	    sdscat (*err, "Can't get Slot No of key from INFO command\n");
	}
      releaseDict (d);
      freeRedisIdsOfPg (redis_ids, count);
      return NULL;
    }

  out = sdsempty ();
  out = sdscat (out, "* REDIS ID  : ");
  for (i = 0; i < count; i++)
    {
      sds addr;
      int port;

      if (i != 0)
	{
	  out = sdscat (out, ", ");
	}

      getRedisInfo (d, redis_ids[i], &ok, &addr, &port, NULL, NULL, NULL);
      out = sdscatprintf (out, "%s(%s:%d)", redis_ids[i], addr, port);
      sdsfree (addr);
    }
  out = sdscat (out, "\n");
  out = sdscatprintf (out, "* PG ID     : %d\n", pg_id);
  out = sdscatprintf (out, "* SLOT No   : %d\n", slot_no);

  releaseDict (d);
  freeRedisIdsOfPg (redis_ids, count);

  return out;
}

int
isCliCommand (char *cmd)
{
#ifdef _ADMIN_BUILD
  if (!strcasecmp (cmd, "WHEREIS"))
    {
      return 1;
    }
#else
  NOTUSED (cmd);
#endif
  return 0;
}

sds
executeCommandCli (arc_t * arc, int argc, char **argv, int timeout, sds * err)
{
#ifdef _ADMIN_BUILD
  if (!strcasecmp (argv[0], "WHEREIS"))
    {
      return whereisCommand (arc, argc, argv, timeout, err);
    }
#else
  NOTUSED (arc);
  NOTUSED (argc);
  NOTUSED (argv);
  NOTUSED (timeout);
  NOTUSED (err);

  NOTUSED (whereisCommand);
#endif
  return NULL;
}

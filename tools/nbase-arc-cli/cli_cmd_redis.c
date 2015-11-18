#include <strings.h>
#include <stdio.h>
#include <signal.h>
#include "sds.h"
#include "cli_cmd.h"
#include "cli_err_arcci.h"
#include "cli_info_parser.h"

#define NOTUSED(V) ((void) V)
static volatile int scan_interrupted;

static dict *
getDictOfInfoRedis (arc_t * arc, sds * err, int timeout)
{
  int argc;
  sds *argv;
  sds out;
  unsigned raw_mode = 1;
  dict *d;

  argv = sdssplitargs ("INFO REDIS", &argc);
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

static arc_t *
connectToRedis (dict * d, sds redis_id)
{
  arc_conf_t conf;
  arc_t *arc;
  sds addr;
  int port;
  int ok;
  sds connection_str;

  getRedisInfo (d, redis_id, &ok, &addr, &port, NULL, NULL, NULL);
  if (!ok)
    {
      return NULL;
    }

  arc_init_conf (&conf);

  connection_str = sdscatprintf (sdsempty (), "%s:%d", addr, port);
  sdsfree (addr);
  arc = arc_new_gw (connection_str, &conf);
  sdsfree (connection_str);

  return arc;
}

static void
scanSigintHandler (int signum)
{
  NOTUSED (signum);

  scan_interrupted = 1;
}

static void
setupScanSigintHandler (struct sigaction *prev_act)
{
  struct sigaction act;

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = scanSigintHandler;
  sigaction (SIGINT, &act, prev_act);
}

static void
restorePrevSigintHandler (struct sigaction *prev_act)
{
  sigaction (SIGINT, prev_act, NULL);
}

static sds
scanCommand (arc_t * arc, int argc, char **argv, int timeout, sds * err)
{
  sds redis_id, pattern, filepath, out, repr_buf;
  dict *d;
  arc_t *redis;
  unsigned long long cursor = 0;
  FILE *fp;
  struct sigaction prev_act;

  if (argc != 4)
    {
      if (err)
	{
	  *err =
	    sdscat (*err,
		    "Invalid format.\n usage:SCAN <redis id> <pattern> <output:stdout or filepath>\n");
	  return NULL;
	}
    }

  redis_id = argv[1];
  pattern = argv[2];
  filepath = argv[3];

  if (!strcasecmp ("STDOUT", filepath))
    {
      fp = stdout;
    }
  else
    {
      fp = fopen (filepath, "w");
    }

  d = getDictOfInfoRedis (arc, err, timeout);
  if (!d)
    {
      if (fp != stdout)
	{
	  fclose (fp);
	}
      return NULL;
    }

  redis = connectToRedis (d, redis_id);
  releaseDict (d);
  if (!redis)
    {
      *err = sdscatperr (*err, "Unable to connect to Redis", 0);
      if (fp != stdout)
	{
	  fclose (fp);
	}
      return NULL;
    }

  scan_interrupted = 0;
  setupScanSigintHandler (&prev_act);

  repr_buf = sdsempty ();
  do
    {
      int argc;
      sds *argv;
      sds query, _err;
      arc_request_t *rqst;
      arc_reply_t *reply;
      int i;

      query =
	sdscatprintf (sdsempty (), "SCAN %llu MATCH %s", cursor, pattern);
      argv = sdssplitargs (query, &argc);
      sdsfree (query);

      reply =
	executeCommandArcReply (redis, argc, argv, timeout, &rqst, &_err);
      sdsfreesplitres (argv, argc);
      if (!reply)
	{
	  if (err)
	    {
	      *err = _err;
	    }
	  out = NULL;
	  goto release_and_return;
	}
      else if (reply->type == ARC_REPLY_ERROR)
	{
	  if (err)
	    {
	      *err = sdscatprintf (*err, "ERROR: %s\n", reply->d.error.str);
	    }
	  out = NULL;
	  arc_free_reply (reply);
	  arc_free_request (rqst);
	  goto release_and_return;
	}

      cursor = strtoull (reply->d.array.elem[0]->d.string.str, NULL, 10);
      for (i = 0; i < reply->d.array.elem[1]->d.array.len; i++)
	{
	  int len;
	  char *buf;

	  len = reply->d.array.elem[1]->d.array.elem[i]->d.string.len;
	  buf = reply->d.array.elem[1]->d.array.elem[i]->d.string.str;
	  sdsclear (repr_buf);
	  repr_buf = sdscatrepr (repr_buf, buf, len);
	  fprintf (fp, "%s\n", repr_buf);
	}
      fflush (fp);

      arc_free_reply (reply);
      arc_free_request (rqst);
    }
  while (!scan_interrupted && cursor != 0);

  if (scan_interrupted)
    {
      out = sdsnew ("\n-ERR SCAN Interrupted\n");
    }
  else
    {
      out = sdsnew ("\n+OK SCAN Completed\n");
    }

release_and_return:
  restorePrevSigintHandler (&prev_act);
  sdsfree (repr_buf);
  if (fp != stdout)
    {
      fclose (fp);
    }
  arc_destroy (redis);

  return out;
}

int
isRedisCommand (char *cmd)
{
#ifdef _ADMIN_BUILD
  if (!strcasecmp (cmd, "SCAN"))
    {
      return 1;
    }
#else
  NOTUSED (cmd);
#endif
  return 0;
}

sds
executeCommandRedis (arc_t * arc, int argc, char **argv,
		     int timeout, sds * err)
{
#ifdef _ADMIN_BUILD
  if (!strcasecmp (argv[0], "SCAN"))
    {
      return scanCommand (arc, argc, argv, timeout, err);
    }
#else
  NOTUSED (arc);
  NOTUSED (argc);
  NOTUSED (argv);
  NOTUSED (timeout);
  NOTUSED (err);

  NOTUSED (scanCommand);
#endif
  return NULL;
}

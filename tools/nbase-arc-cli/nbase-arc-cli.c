#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <limits.h>
#include "linenoise.h"
#include "sds.h"
#include "arcci.h"
#include "cli_cmd.h"
#include "cli_info_parser.h"
#include "cli_err_arcci.h"

#define NOTUSED(V) ((void) V)
#define DEFAULT_TIMEOUT 1000
#define ARC_CONNECTION_INTERVAL_SEC 5

static volatile int repeat_interrupted;

typedef struct
{
  arc_t *arc;

  sds zk_addr;
  sds cluster_name;
  int repeat;
  int interval;

  unsigned allow_non_tty:1;
  unsigned stat_mode:1;
  unsigned latency_mode:1;
  unsigned repeat_mode:1;
  unsigned raw_mode:1;
} cli_context_t;

typedef void printHandler (FILE * fp, sds out);
typedef void printSeperator (FILE * fp, cli_context_t * ctx);

static void statLoop (cli_context_t * ctx, int interval);
static void latencyLoop (cli_context_t * ctx, int interval);

static arc_t *
initArcAPI (sds zk_addr, sds cluster_name)
{
  arc_conf_t conf;

  arc_init_conf (&conf);
  return arc_new_zk (zk_addr, cluster_name, &conf);
}

static void
executeAndPrint (FILE * fp, arc_t * arc, int argc, char **argv,
		 int timeout, unsigned raw_mode, printHandler * handler)
{
  sds out, err;

  err = sdsempty ();
  out = executeCommand (arc, argc, argv, timeout, raw_mode, &err);
  if (!out)
    {
      fprintf (fp, err);
      fflush (fp);
      sdsfree (err);
      return;
    }

  if (!handler)
    {
      fprintf (fp, out);
      if (raw_mode)
	{
	  fprintf (fp, "\n");
	}
      fflush (fp);
    }
  else
    {
      handler (fp, out);
    }

  sdsfree (err);
  sdsfree (out);
}

static void
intro (void)
{
  fprintf (stdout,
	   "    _   ______  ___   _____ ______   ___    ____  ______   ________    ____\n"
	   "   / | / / __ )/   | / ___// ____/  /   |  / __ \\/ ____/  / ____/ /   /  _/\n"
	   "  /  |/ / __  / /| | \\__ \\/ __/    / /| | / /_/ / /      / /   / /    / /  \n"
	   " / /|  / /_/ / ___ |___/ / /___   / ___ |/ _, _/ /___   / /___/ /____/ /   \n"
	   "/_/ |_/_____/_/  |_/____/_____/  /_/  |_/_/ |_|\\____/   \\____/_____/___/   \n"
	   "                                                                           \n");
}

static void
usage (void)
{
  intro ();
#ifdef _ADMIN_BUILD
  fprintf (stdout,
	   "Usage: arc-cli-admin -z <zk addr> -c <cluster name> [OPTIONS]\n"
	   "   -z <zookeeper address>      Zookeeper address (ex: zookeeper.nbasearc.com:2181)\n"
	   "   -c <cluster name>           Cluster name (ex: dev_test)\n"
	   "   -t                          Allow non-tty input\n"
	   "   -s                          stat mode\n"
	   "   -l                          latency mode\n"
	   "   -r <repeat count>           repeat mode\n"
	   "   -i <interval sec>           interval for stat/latency/repeat (default: 5sec)\n"
	   "   -p                          Use raw formatting\n\n"
	   "Special Commands\n"
	   "   STAT [<interval sec>]       Print stats of cluster\n"
	   "   LATENCY [<interval sec>]    Print latencies of cluster\n"
	   "   WHEREIS <key>               Get redis_id of a key.\n"
	   "   SCAN <redis_id> <pattern> <output:\"stdout\" or filepath>    Get scan result of a specific redis \n"
	   "                               in escaped string representation. All the non-printable characters\n"
	   "                               will be escaped in the form of \"\\n\\r\\a....\" or \"\\x<hex-number>\"\n\n");
#else
  fprintf (stdout,
	   "Usage: arc-cli -z <zk addr> -c <cluster name>\n"
	   "   -z <zookeeper address>      Zookeeper address (ex: zookeeper.nbasearc.com:2181)\n"
	   "   -c <cluster name>           Cluster name (ex: dev_test)\n\n"
	   "Special Commands\n"
	   "   STAT [<interval sec>]       Print stats of cluster\n"
	   "   LATENCY [<interval sec>]    Print latencies of cluster\n\n");
#endif
}

static int
getPromptStr (char *prompt, int len, sds cluster_name)
{
  int ret;

  ret = snprintf (prompt, len, "%s> ", cluster_name);
  return ret;
}

#define PROMPT_MAXLEN 128
static void
repl (cli_context_t * ctx)
{
  char prompt[PROMPT_MAXLEN];
  char *line;
  int argc;
  int timeout = DEFAULT_TIMEOUT;
  sds *argv;
  char historyfile[PATH_MAX];

  if (isatty (STDIN_FILENO))
    {
      intro ();
    }
  getPromptStr (prompt, PROMPT_MAXLEN, ctx->cluster_name);

  historyfile[0] = '\0';
  if (getenv ("HOME") != NULL)
    {
      snprintf (historyfile, PATH_MAX, "%s/.nbasearc_cli_history",
		getenv ("HOME"));
      linenoiseHistoryLoad (historyfile);
    }

  while ((line = linenoise (prompt)) != NULL)
    {
      if (line[0] != '\0')
	{
	  argv = sdssplitargs (line, &argc);
	  linenoiseHistoryAdd (line);
	  if (strlen (historyfile) != 0)
	    {
	      linenoiseHistorySave (historyfile);
	    }
	  if (argv == NULL)
	    {
	      fprintf (stdout, "Invalid argument(s)\n");
	      free (line);
	      continue;
	    }

	  if (!strcasecmp (argv[0], "quit") || !strcasecmp (argv[0], "exit"))
	    {
	      free (line);
	      sdsfreesplitres (argv, argc);
	      return;
	    }
	  else if (argc == 1 && !strcasecmp (argv[0], "clear"))
	    {
	      linenoiseClearScreen ();
	    }
	  else if (!strcasecmp (argv[0], "stat"))
	    {
	      if (argc == 1)
		{
		  statLoop (ctx, ctx->interval);
		}
	      else if (argc == 2)
		{
		  int new_interval = atoi (argv[1]);
		  if (new_interval < 1)
		    {
		      fprintf (stdout, "Invalid argument(s)\n");
		    }
		  else
		    {
		      statLoop (ctx, new_interval);
		    }
		}
	      else
		{
		  fprintf (stdout, "Invalid argument(s)\n");
		}
	    }
	  else if (!strcasecmp (argv[0], "latency"))
	    {
	      if (argc == 1)
		{
		  latencyLoop (ctx, ctx->interval);
		}
	      else if (argc == 2)
		{
		  int new_interval = atoi (argv[1]);
		  if (new_interval < 1)
		    {
		      fprintf (stdout, "Invalid argument(s)\n");
		    }
		  else
		    {
		      latencyLoop (ctx, new_interval);
		    }
		}
	      else
		{
		  fprintf (stdout, "Invalid argument(s)\n");
		}
	    }
	  else
	    {
	      unsigned raw_mode = ctx->raw_mode;

	      if (!strcasecmp (argv[0], "INFO"))
		{
		  raw_mode = 1;
		}
	      executeAndPrint (stdout, ctx->arc, argc, argv, timeout,
			       raw_mode, NULL);
	    }
	  sdsfreesplitres (argv, argc);
	}
      free (line);
    }
}

/* Return the UNIX time in microseconds */
static long long
ustime (void)
{
  struct timeval tv;
  long long ust;

  gettimeofday (&tv, NULL);
  ust = ((long long) tv.tv_sec) * 1000000;
  ust += tv.tv_usec;
  return ust;
}

static void
sleepInterval (long long start_usec, int interval_sec)
{
  long long cur_usec;
  long long sleep_usec;

  cur_usec = ustime ();
  sleep_usec = start_usec + interval_sec * 1000000 - cur_usec;

  if (sleep_usec > 0)
    {
      usleep (sleep_usec);
    }
}

static void
repeatSigintHandler (int signum)
{
  NOTUSED (signum);

  repeat_interrupted = 1;
}

static void
setupRepeatSigintHandler (struct sigaction *prev_act)
{
  struct sigaction act;

  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = repeatSigintHandler;
  sigaction (SIGINT, &act, prev_act);
}

static void
restorePrevSigintHandler (struct sigaction *prev_act)
{
  sigaction (SIGINT, prev_act, NULL);
}

/* If repeat parameter is 0, repeating a command indefinitely.*/
static void
repeat (cli_context_t * ctx, int argc, char **argv, int interval,
	printHandler * handler, printSeperator * seperator)
{
  int timeout = DEFAULT_TIMEOUT;
  int count;
  long long start;
  unsigned raw_mode;
  struct sigaction prev_act;

  if (interval < 1)
    return;

  repeat_interrupted = 0;
  setupRepeatSigintHandler (&prev_act);

  count = 0;
  while (!repeat_interrupted && (ctx->repeat == 0 || count < ctx->repeat))
    {
      if (seperator && count % 15 == 0)
	{
	  seperator (stdout, ctx);
	}

      count++;
      start = ustime ();

      raw_mode = ctx->raw_mode;
      if (!strcasecmp (argv[0], "INFO"))
	{
	  raw_mode = 1;
	}

      executeAndPrint (stdout, ctx->arc, argc, argv, timeout, raw_mode,
		       handler);

      if (ctx->repeat == 0 || count < ctx->repeat)
	{
	  sleepInterval (start, interval);
	}
    }

  restorePrevSigintHandler (&prev_act);
}

static void
bytesToHuman (char *s, unsigned long long n)
{
  double d;

  if (n < 1024)
    {
      /* Bytes */
      sprintf (s, "%llu B", n);
      return;
    }
  else if (n < (1024 * 1024))
    {
      d = (double) n / (1024);
      sprintf (s, "%.2f KB", d);
    }
  else if (n < (1024LL * 1024 * 1024))
    {
      d = (double) n / (1024 * 1024);
      sprintf (s, "%.2f MB", d);
    }
  else if (n < (1024LL * 1024 * 1024 * 1024))
    {
      d = (double) n / (1024LL * 1024 * 1024);
      sprintf (s, "%.2f GB", d);
    }
  else
    {
      d = (double) n / (1024LL * 1024 * 1024 * 1024);
      sprintf (s, "%.2f TB", d);
    }
}

static void
numberToHuman (char *s, unsigned long long n)
{
  double d;

  if (n < 1000)
    {
      sprintf (s, "%llu", n);
      return;
    }
  else if (n < (1000 * 1000))
    {
      d = (double) n / (1000);
      sprintf (s, "%.2f K", d);
    }
  else if (n < (1000LL * 1000 * 1000))
    {
      d = (double) n / (1000 * 1000);
      sprintf (s, "%.2f M", d);
    }
  else if (n < (1000LL * 1000 * 1000 * 1000))
    {
      d = (double) n / (1000LL * 1000 * 1000);
      sprintf (s, "%.2f G", d);
    }
  else
    {
      d = (double) n / (1000LL * 1000 * 1000 * 1000);
      sprintf (s, "%.2f T", d);
    }
}

static void
printBytesHuman (FILE * fp, const char *format, long long val)
{
  char s[64];

  bytesToHuman (s, val);
  fprintf (fp, format, s);
}

static void
printNumberHuman (FILE * fp, const char *format, long long val)
{
  char s[64];

  numberToHuman (s, val);
  fprintf (fp, format, s);
}

static size_t
timestr (char *s, size_t n, const char *format)
{
  time_t t;
  struct tm *tmp;
  size_t ret;

  s[0] = '\0';
  ret = 0;
  t = time (NULL);
  tmp = localtime (&t);
  if (tmp)
    {
      ret = strftime (s, n, format, tmp);
    }

  return ret;
}

static void
statPrintSeperator (FILE * fp, cli_context_t * ctx)
{
  char s[128];

  timestr (s, 128, "%F %T");

  fprintf (fp,
	   "+------------------------------------------------------------------------------------------------------+\n");
  fprintf (fp,
	   "| %20s, CLUSTER:%-50s                     |\n", s,
	   ctx->cluster_name);
  fprintf (fp,
	   "+------------------------------------------------------------------------------------------------------+\n");
  fprintf (fp,
	   "|  Time | Redis |  PG  | Connection |    Mem    |   OPS   |   Hits   |  Misses  |   Keys   |  Expires  |\n");
  fprintf (fp,
	   "+------------------------------------------------------------------------------------------------------+\n");
  fflush (fp);
}

static void
printInfoStat (FILE * fp, dict * d)
{
  char s[128];
  long long keys, expires, avg_ttl;

  timestr (s, 128, "%M:%S");
  fprintf (fp, "|%6s ", s);
  fprintf (fp, "|%6lld ",
	   fetchLongLong (d, "redis_instances_available", NULL));
  fprintf (fp, "|%5lld ",
	   fetchLongLong (d, "partition_groups_available", NULL));
  fprintf (fp, "|%11lld ",
	   fetchLongLong (d, "gateway_connected_clients", NULL));
  printBytesHuman (fp, "|%10s ", fetchLongLong (d, "used_memory_rss", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "instantaneous_ops_per_sec", NULL));
  printNumberHuman (fp, "|%9s ", fetchLongLong (d, "keyspace_hits", NULL));
  printNumberHuman (fp, "|%9s ", fetchLongLong (d, "keyspace_misses", NULL));
  getDbstat (d, NULL, &keys, &expires, &avg_ttl);
  printNumberHuman (fp, "|%9s ", keys);
  printNumberHuman (fp, "|%10s |\n", expires);
  fflush (fp);
}

static void
statPrintHandler (FILE * fp, sds out)
{
  dict *d;

  d = getDictFromInfoStr (out);
  printInfoStat (fp, d);

  releaseDict (d);
}

static void
statLoop (cli_context_t * ctx, int interval)
{
  int argc;
  sds *argv;

  argv = sdssplitargs ("INFO ALL", &argc);
  repeat (ctx, argc, argv, interval, statPrintHandler, statPrintSeperator);

  sdsfreesplitres (argv, argc);
}

static void
latencyPrintSeperator (FILE * fp, cli_context_t * ctx)
{
  char s[128];

  timestr (s, 128, "%F %T");

  fprintf (fp,
	   "+-------------------------------------------------------------------------------------------------------------------------------+\n");
  fprintf (fp,
	   "| %20s, CLUSTER:%-50s                                              |\n",
	   s, ctx->cluster_name);
  fprintf (fp,
	   "+-------------------------------------------------------------------------------------------------------------------------------+\n");
  fprintf (fp,
	   "|  Time |  <= 1ms |  <= 2ms |  <= 4ms |  <= 8ms | <= 16ms | <= 32ms | <= 64ms |  <= 128 |  <= 256 |  <= 512 | <= 1024 |  > 1024 |\n");
  fprintf (fp,
	   "+-------------------------------------------------------------------------------------------------------------------------------+\n");
  fflush (fp);
}

static void
printLatency (FILE * fp, dict * d)
{
  char s[128];

  timestr (s, 128, "%M:%S");
  fprintf (fp, "|%6s ", s);
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_1ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_2ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_4ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_8ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_16ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_32ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_64ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_128ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_256ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_512ms", NULL));
  printNumberHuman (fp, "|%8s ",
		    fetchLongLong (d, "less_than_or_equal_to_1024ms", NULL));
  printNumberHuman (fp, "|%8s |\n",
		    fetchLongLong (d, "more_than_1024ms", NULL));
  fflush (fp);
}

static void
latencyPrintHandler (FILE * fp, sds out)
{
  dict *d;

  d = getDictFromInfoStr (out);
  printLatency (fp, d);

  releaseDict (d);
}

static void
latencyLoop (cli_context_t * ctx, int interval)
{
  int argc;
  sds *argv;

  argv = sdssplitargs ("INFO LATENCY", &argc);
  repeat (ctx, argc, argv, interval, latencyPrintHandler,
	  latencyPrintSeperator);

  sdsfreesplitres (argv, argc);
}

static int
parseOption (int argc, char **argv, cli_context_t * ctx, int *next)
{
  int ret;

  ctx->arc = NULL;
  ctx->zk_addr = NULL;
  ctx->cluster_name = NULL;
  ctx->repeat = 0;
  ctx->interval = 5;
  ctx->stat_mode = 0;
  ctx->latency_mode = 0;
  ctx->repeat_mode = 0;
  ctx->raw_mode = 0;
  ctx->allow_non_tty = 0;

  while ((ret = getopt (argc, argv, "z:c:tslr:i:p")) != -1)
    {
      switch (ret)
	{
	case 'z':
	  ctx->zk_addr = sdsnew (optarg);
	  break;
	case 'c':
	  ctx->cluster_name = sdsnew (optarg);
	  break;
#ifdef _ADMIN_BUILD
	case 't':
	  ctx->allow_non_tty = 1;
	  break;
	case 's':
	  ctx->stat_mode = 1;
	  break;
	case 'l':
	  ctx->latency_mode = 1;
	  break;
	case 'r':
	  ctx->repeat_mode = 1;
	  ctx->repeat = atoi (optarg);
	  break;
	case 'i':
	  ctx->interval = atoi (optarg);
	  break;
	case 'p':
	  ctx->raw_mode = 1;
	  break;
#endif
	default:
	  return -1;
	}
    }
  *next = optind;

  if (!ctx->zk_addr || !ctx->cluster_name)
    {
      return -1;
    }
  if (ctx->stat_mode + ctx->latency_mode + ctx->repeat_mode > 1)
    {
      return -1;
    }
  if (ctx->stat_mode || ctx->latency_mode || ctx->repeat_mode)
    {
      if (ctx->interval < 1)
	{
	  return -1;
	}
    }
  if (ctx->stat_mode || ctx->latency_mode)
    {
      if (*next != argc)
	{
	  return -1;
	}
    }
  if (ctx->repeat_mode)
    {
      if (ctx->repeat < 1)
	{
	  return -1;
	}
      if (*next == argc)
	{
	  return -1;
	}
    }

  return 0;
}

static sds
getConnectLogPath (void)
{
  sds connect_log_path;
  if (getenv ("USER") != NULL)
    {
      connect_log_path =
	sdscatprintf (sdsempty (), "/tmp/.nbasearc_connect_%s.log",
		      getenv ("USER"));
    }
  else
    {
      connect_log_path =
	sdscatprintf (sdsempty (), "/tmp/.nbasearc_connect_default.log");
    }

  return connect_log_path;
}

static void
updateConnectionHistory (sds zk_addr, sds cluster_name)
{
  sds connect_log_path;
  FILE *fp;
  char s[128];

  connect_log_path = getConnectLogPath ();
  fp = fopen (connect_log_path, "a+");

  if (!fp)
    {
      sdsfree (connect_log_path);
      return;
    }

  timestr (s, 128, "%F %T");
  fprintf (fp, "[%s] ZK:%s, CLUSTER:%s\n", s, zk_addr, cluster_name);
  fclose (fp);

  chmod (connect_log_path, 0666);
  sdsfree (connect_log_path);
  return;
}

#ifndef _ADMIN_BUILD
#define NOT_ACCESSIBLE -1
#define NOT_EXIST 0
#define SUCCESS 1
static int
getLastConnectionTimeUsec (long long *time_usec)
{
  sds connect_log_path;
  struct stat stat_buf;
  int ret;

  *time_usec = 0;
  connect_log_path = getConnectLogPath ();
  ret = stat (connect_log_path, &stat_buf);
  sdsfree (connect_log_path);

  if (ret == -1)
    {
      if (errno == ENOENT)
	{
	  return NOT_EXIST;
	}
      else
	{
	  return NOT_ACCESSIBLE;
	}
    }

  *time_usec = stat_buf.st_mtime * 1000000;
  return SUCCESS;
}

static void
waitConnectionInterval (int interval)
{
  long long last_connect_utime;
  int ret;

  ret = getLastConnectionTimeUsec (&last_connect_utime);
  if (ret == NOT_ACCESSIBLE)
    {
      /* Because we don't know last connection time, sleep interval from now. */
      sleepInterval (ustime (), interval);
    }
  else if (ret == NOT_EXIST)
    {
      /* Do not sleep */
    }
  else if (ret == SUCCESS)
    {
      sleepInterval (last_connect_utime, interval);
    }
}
#endif

static void
releaseContext (cli_context_t * ctx)
{
  if (ctx->arc)
    {
      arc_destroy (ctx->arc);
    }
  sdsfree (ctx->zk_addr);
  sdsfree (ctx->cluster_name);
}

int
main (int argc, char **argv)
{
  cli_context_t ctx;
  int ret, parsed_argc;

  ret = parseOption (argc, argv, &ctx, &parsed_argc);
  if (ret == -1)
    {
      usage ();
      exit (1);
    }

#ifndef _ADMIN_BUILD
  waitConnectionInterval (ARC_CONNECTION_INTERVAL_SEC);
#endif
  updateConnectionHistory (ctx.zk_addr, ctx.cluster_name);

  ctx.arc = initArcAPI (ctx.zk_addr, ctx.cluster_name);
  if (!ctx.arc)
    {
      perr ("Connect to cluster", 0);
      releaseContext (&ctx);
      return 1;
    }

  /* Stat Mode */
  if (ctx.stat_mode)
    {
      statLoop (&ctx, ctx.interval);
      releaseContext (&ctx);
      return 0;
    }

  /* Latency Mode */
  if (ctx.latency_mode)
    {
      latencyLoop (&ctx, ctx.interval);
      releaseContext (&ctx);
      return 0;
    }

  if (!ctx.allow_non_tty && (!isatty (STDIN_FILENO) || parsed_argc != argc))
    {
      fprintf (stderr, "Non-TTY input is not allowed.\n");
      releaseContext (&ctx);
      return 1;
    }

  /* Repeat Mode */
  if (ctx.repeat_mode)
    {
      int remain = argc - parsed_argc;
      char **remain_argv = argv + parsed_argc;

      repeat (&ctx, remain, remain_argv, ctx.interval, NULL, NULL);
      releaseContext (&ctx);
      return 0;
    }

  /* CLI Mode */
  if (parsed_argc == argc)
    {
      repl (&ctx);
    }
  else
    {
      int remain = argc - parsed_argc;
      char **remain_argv = argv + parsed_argc;
      int timeout = DEFAULT_TIMEOUT;
      unsigned raw_mode = ctx.raw_mode;

      if (!strcasecmp (remain_argv[0], "INFO"))
	{
	  raw_mode = 1;
	}
      executeAndPrint (stdout, ctx.arc, remain, remain_argv, timeout,
		       raw_mode, NULL);
    }
  releaseContext (&ctx);
  return 0;
}

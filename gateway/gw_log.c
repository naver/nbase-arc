#include "gw_log.h"

static struct logger logger;
static const char *log_symbol = ".-*#";

static int
print_msg (FILE * fp, int level, char *timestr, const char *msg)
{
  int size;

#ifdef __linux__
  size =
    fprintf (fp, "[%d|%d] %s %c %s\n", (int) getpid (),
	     (int) syscall (__NR_gettid), timestr, log_symbol[level], msg);
#else
  size =
    fprintf (fp, "[%d] %s %c %s\n", (int) getpid (), timestr,
	     log_symbol[level], msg);
#endif
  fflush (fp);
  return size;
}

static void
log_console (int level, char *timestr, const char *msg)
{
  print_msg (stdout, level, timestr, msg);
}

static void
log_file (int level, char *timestr, const char *msg)
{
  int size;

  pthread_mutex_lock (&logger.logfile_lock);

  if (logger.logfile_fp == NULL)
    {
      struct timeval tv;
      char path[PATH_MAX + 1];
      int off;

      gettimeofday (&tv, NULL);
      strcpy (path, logger.logfile_prefix);
      off = strlen (path);
      strftime (path + off, PATH_MAX - off, ".%Y%m%d_%H%M%S.log",
		localtime (&tv.tv_sec));
      logger.logfile_fp = fopen (path, "a");
      if (logger.logfile_fp == NULL)
	{
	  goto done;
	}
    }

  assert (logger.logfile_fp);
  size = print_msg (logger.logfile_fp, level, timestr, msg);
  logger.logfile_size += size;
  if (logger.logfile_size > logger.logfile_rolling_size)
    {
      fclose (logger.logfile_fp);
      logger.logfile_fp = NULL;
      logger.logfile_size = 0;
    }

done:
  pthread_mutex_unlock (&logger.logfile_lock);
}

static void
log_syslog (int level, const char *msg)
{
  const int syslog_level_map[] =
    { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };

  syslog (syslog_level_map[level], "%s", msg);
}

static void
log_msg (int level, const char *msg)
{
  char timestr[64];
  struct timeval tv;
  int off;

  if ((level & 0xff) < logger.verbosity)
    return;

  gettimeofday (&tv, NULL);
  off =
    strftime (timestr, sizeof (timestr), "%d %b %H:%M:%S.",
	      localtime (&tv.tv_sec));
  snprintf (timestr + off, sizeof (timestr) - off, "%03d",
	    (int) tv.tv_usec / 1000);

  if (logger.console_enabled)
    {
      log_console (level, timestr, msg);
    }
  if (logger.logfile_enabled)
    {
      log_file (level, timestr, msg);
    }
  if (logger.syslog_enabled)
    {
      log_syslog (level, msg);
    }
}

static void
syslog_init (void)
{
  openlog (logger.syslog_ident, LOG_PID | LOG_NDELAY, logger.syslog_facility);
}

static void
syslog_fin (void)
{
  closelog ();
}

void
gwlog_init (char *prefix, int verbosity, int using_console, int using_logfile,
	    int using_syslog)
{
  logger.verbosity = verbosity;

  /* File */
  pthread_mutex_init (&logger.logfile_lock, NULL);
  if (prefix)
    {
      logger.logfile_prefix = prefix;
    }
  else
    {
      logger.logfile_prefix = GWLOG_FILE_PREFIX;
    }
  logger.logfile_fp = NULL;
  logger.logfile_rolling_size = GWLOG_FILE_ROLLING_SIZE;
  logger.logfile_size = 0;

  /* Syslog */
  logger.syslog_ident = GWLOG_SYSLOG_IDENT;
  logger.syslog_facility = GWLOG_SYSLOG_FACILITY;

  /* Flags */
  logger.console_enabled = using_console;
  logger.logfile_enabled = using_logfile;
  logger.syslog_enabled = using_syslog;

  if (using_syslog)
    syslog_init ();
}

void
gwlog_finalize (void)
{
  pthread_mutex_lock (&logger.logfile_lock);
  if (logger.logfile_fp)
    fclose (logger.logfile_fp);
  pthread_mutex_unlock (&logger.logfile_lock);
  pthread_mutex_destroy (&logger.logfile_lock);

  syslog_fin ();
}

void
gwlog (int level, const char *fmt, ...)
{
  va_list ap;
  char msg[GWLOG_MAX_MSG_LEN];

  if ((level & 0xff) < logger.verbosity)
    return;

  va_start (ap, fmt);
  vsnprintf (msg, sizeof (msg), fmt, ap);
  va_end (ap);

  log_msg (level, msg);
}

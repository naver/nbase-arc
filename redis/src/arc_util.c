#ifdef NBASE_ARC
#include "arc_internal.h"

#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>


#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8)	/* 8 MB */

char **
arcx_get_local_ip_addrs ()
{
  struct ifaddrs *ifaddr, *ifa;
  int family, s;
  char ip[NI_MAXHOST];
  char **addrs;
  int count, size = 5;

  if (getifaddrs (&ifaddr) == -1)
    return NULL;

  addrs = zmalloc (sizeof (char *) * (size + 1));
  count = 0;
  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next)
    {
      if (ifa->ifa_addr == NULL)
	continue;

      family = ifa->ifa_addr->sa_family;
      if (family == AF_INET || family == AF_INET6)
	{
	  s = getnameinfo (ifa->ifa_addr,
			   (family ==
			    AF_INET) ? sizeof (struct sockaddr_in) :
			   sizeof (struct sockaddr_in6), ip, NI_MAXHOST,
			   NULL, 0, NI_NUMERICHOST);
	  if (s != 0)
	    {
	      continue;
	    }
	  if (size <= count)
	    {
	      size *= 2;
	      addrs = zrealloc (addrs, sizeof (char *) * size + 1);
	    }
	  addrs[count++] = zstrdup (ip);
	}
    }

  freeifaddrs (ifaddr);

  if (count == 0)
    {
      zfree (addrs);
      return NULL;
    }

  addrs[count] = NULL;
  return addrs;
}

int
arcx_is_local_ip_addr (char **local_ip_list, char *cmp_ip)
{
  char **ip;

  if (local_ip_list == NULL || *local_ip_list == NULL)
    {
      return 0;
    }

  for (ip = local_ip_list; *ip != NULL; ip++)
    {
      if (!strcmp (*ip, cmp_ip))
	{
	  return 1;
	}
    }
  return 0;
}

#define MEMINFO_FILE "/proc/meminfo"

int
arcx_get_memory_usage (unsigned long *total_kb, unsigned long *free_kb,
		       unsigned long *cached_kb)
{
  char buf[2048];
  char *head, *tail;
  int nr;
  int has_total, has_free, has_cached;

#ifdef COVERAGE_TEST
  if (arc.debug_mem_usage_fixed)
    {
      *total_kb = arc.debug_total_mem_kb;
      *free_kb = arc.debug_free_mem_kb;
      *cached_kb = arc.debug_cached_mem_kb;
      return C_OK;
    }
#endif
  if (arc.meminfo_fd == -1
      && (arc.meminfo_fd = open (MEMINFO_FILE, O_RDONLY)) == -1)
    {
      serverLog (LL_WARNING, "Can't open meminfo file: %s", strerror (errno));
      arc.meminfo_fd = -1;
      return C_ERR;
    }
  lseek (arc.meminfo_fd, 0L, SEEK_SET);
  if ((nr = read (arc.meminfo_fd, buf, sizeof (buf) - 1)) < 0)
    {
      serverLog (LL_WARNING, "Reading from meminfo file: %s",
		 strerror (errno));
      close (arc.meminfo_fd);
      arc.meminfo_fd = -1;
      return C_ERR;
    }
  buf[nr] = '\0';

  has_total = has_free = has_cached = 0;
  head = buf;
  for (;;)
    {
      int len;
      tail = strchr (head, ':');
      if (!tail)
	break;
      len = tail - head;

      if (!has_total && len == 8 && !strncmp (head, "MemTotal", 8))
	{
	  head = tail + 1;
	  *total_kb = strtoul (head, &tail, 10);
	  has_total = 1;
	}
      else if (!has_free && len == 7 && !strncmp (head, "MemFree", 7))
	{
	  head = tail + 1;
	  *free_kb = strtoul (head, &tail, 10);
	  has_free = 1;
	}
      else if (!has_cached && len == 6 && !strncmp (head, "Cached", 6))
	{
	  head = tail + 1;
	  *cached_kb = strtoul (head, &tail, 10);
	  has_cached = 1;
	}
      tail = strchr (head, '\n');
      if (!tail)
	break;
      head = tail + 1;
    }

  if (!has_total || !has_free || !has_cached)
    {
      serverLog (LL_WARNING, "Invalid meminfo file format");
      return C_ERR;
    }
  return C_OK;
}

int
arcx_get_dump (char *source_addr, int source_port, char *filename,
	       char *range, int net_limit)
{
  char tmpfile[256];
  int dfd = -1, maxtries = 5;
  int fd = -1;
  char neterr[ANET_ERR_LEN];
  char buf[4096];
  int len;
  long net_limit_bytes_duration, net_avail, last_ts, remain_ts, duration;

  /* connect to source redis server */
  fd = anetTcpConnect (neterr, source_addr, source_port);
  if (fd == ANET_ERR)
    {
      serverLog (LL_WARNING, "Unable to connect to redis server(%s:%d): %s",
		 source_addr, source_port, neterr);
      return C_ERR;
    }

  /* Send the PING */
  syncWrite (fd, "PING\r\n", 6, 100);

  /* Recieve the PONG */
  if (syncReadLine (fd, buf, sizeof (buf),
		    server.repl_syncio_timeout * 1000) == -1)
    {
      serverLog (LL_WARNING,
		 "I/O error reading PING reply from source: %s",
		 strerror (errno));
      goto error;
    }
  if (buf[0] != '-' && buf[0] != '+')
    {
      serverLog (LL_WARNING, "Unexpected reply to PING from source.");
      goto error;
    }
  serverLog (LL_NOTICE, "Source replied to PING, getdump can continue...");

  /* Issue the CHECKPOINT command */
  len =
    sprintf (buf, "*2\r\n$10\r\nCHECKPOINT\r\n$%ld\r\n%s\r\n", strlen (range),
	     range);
  if (syncWrite (fd, buf, len, server.repl_syncio_timeout * 1000) == -1)
    {
      serverLog (LL_WARNING, "I/O error writing to source: %s",
		 strerror (errno));
      goto error;
    }
  if (syncReadLine (fd, buf, 1024, server.repl_syncio_timeout * 1000) == -1)
    {
      serverLog (LL_WARNING,
		 "I/O error reading smr sequence number from source: %s",
		 strerror (errno));
      goto error;
    }
  if (buf[0] != ':')
    {
      serverLog (LL_WARNING,
		 "Bad protocol from source, the first byte is not ':', are you sure the host and port are right?");
      serverLog (LL_WARNING, "Reply from source:%s", buf);
      goto error;
    }
  serverLog (LL_NOTICE, "Checkpoint Sequence Number%s", buf);

  /* Prepare a suitable temp file for bulk transfer */
  while (maxtries--)
    {
      snprintf (tmpfile, 256,
		"temp-%d.%ld.rdb", (int) time (NULL), (long int) getpid ());
      dfd = open (tmpfile, O_CREAT | O_WRONLY | O_EXCL, 0644);
      if (dfd != -1)
	break;
      sleep (1);
    }
  if (dfd == -1)
    {
      serverLog (LL_WARNING,
		 "Opening the temp file needed for SOURCE <-> TARGET synchronization: %s",
		 strerror (errno));
      goto error;
    }

  /* Read the buf length from the source reply */
  if (syncReadLine (fd, buf, 1024, server.repl_syncio_timeout * 1000) == -1)
    {
      serverLog (LL_WARNING,
		 "I/O error reading bulk count from source: %s",
		 strerror (errno));
      goto error;
    }
  if (buf[0] == '-')
    {
      serverLog (LL_WARNING, "Source aborted get dump with an error: %s",
		 buf + 1);
      goto error;
    }
  else if (buf[0] != '$')
    {
      serverLog (LL_WARNING,
		 "Bad protocol from SOURCE, the first byte is not '$', are you sure the host and port are right?");
      goto error;
    }
  server.repl_transfer_size = strtol (buf + 1, NULL, 10);
  server.repl_transfer_read = 0;
  server.repl_transfer_last_fsync_off = 0;
  serverLog (LL_NOTICE,
	     "SOURCE <-> TARGET sync: receiving %ld bytes from source",
	     server.repl_transfer_size);

  /* Set initial network limit and timestamp */
  duration = 1000 / server.hz;
  net_limit_bytes_duration = net_limit * 1024 * 1024 / server.hz;
  net_avail = net_limit_bytes_duration;
  last_ts = mstime ();

  /* Read bulk data */
  do
    {
      off_t left = server.repl_transfer_size - server.repl_transfer_read;
      ssize_t nread, readlen;
      readlen = (left < (signed) sizeof (buf)) ? left : (signed) sizeof (buf);
      nread = read (fd, buf, readlen);
      if (nread <= 0)
	{
	  serverLog (LL_WARNING,
		     "I/O error trying to getdump with source: %s",
		     (nread == -1) ? strerror (errno) : "connection lost");
	  goto error;
	}
      if (write (dfd, buf, nread) != nread)
	{
	  serverLog (LL_WARNING,
		     "Write error or short write writing to the DB dump file needed for TARGET <-> SOURCE synchronization: %s",
		     strerror (errno));
	  goto error;
	}
      server.repl_transfer_read += nread;

      /* Sync data on disk from time to time, otherwise at the end of the transfer
       * we may suffer a big delay as the memory buffers are copied into the
       * actual disk. */
      if (server.repl_transfer_read >=
	  server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
	{
	  off_t sync_size = server.repl_transfer_read -
	    server.repl_transfer_last_fsync_off;
	  rdb_fsync_range (dfd,
			   server.repl_transfer_last_fsync_off, sync_size);
	  server.repl_transfer_last_fsync_off += sync_size;
	}

      /* Receiving from source redis is done. Exit loop */
      if (server.repl_transfer_read == server.repl_transfer_size)
	{
	  break;
	}

      /* Check network limit */
      // Adjust available network limit
      net_avail -= nread;
      if (net_avail <= 0)
	{
	  // Check whether time exceeds duration
	  remain_ts = last_ts + duration - mstime ();
	  if (remain_ts > 0)
	    {
	      // Time doesn't exceed duration, but network limit has been reached.
	      // Sleep remaining time and reset network limit
	      usleep (remain_ts * 1000);
	    }
	  last_ts = mstime ();
	  net_avail = net_limit_bytes_duration;
	}
    }
  while (1);

  if (syncReadLine (fd, buf, 1024, server.repl_syncio_timeout * 1000) == -1)
    {
      serverLog (LL_WARNING,
		 "I/O error reading +OK from source: %s", strerror (errno));
      goto error;
    }

  if (buf[0] != '+' || buf[1] != 'O' || buf[2] != 'K')
    {
      serverLog (LL_WARNING, "Unexpected reply from source.");
      goto error;
    }

  if (rename (tmpfile, filename) == -1)
    {
      serverLog (LL_WARNING,
		 "Failed trying to rename the temp DB into dump.rdb in MASTER <-> SLAVE synchronization: %s",
		 strerror (errno));
      goto error;
    }

  close (fd);
  close (dfd);
  return C_OK;

error:
  if (fd != -1)
    {
      close (fd);
    }
  if (dfd != -1)
    {
      close (dfd);
    }
  return C_ERR;
}


// Note: arcx_dumpscan_xxx functions are basically from rdbLoad in rdb.c

int
arcx_dumpscan_start (dumpScan * ds, char *file)
{
  FILE *fp = NULL;
  char buf[1024];
  int rdbver;

  if ((fp = fopen (file, "r")) == NULL)
    {
      return -1;
    }
  fseeko (fp, 0L, SEEK_END);
  ds->filesz = ftello (fp);
  fseeko (fp, 0L, SEEK_SET);
  rioInitWithFile (&ds->rdb, fp);

  // check header
  if (rioRead (&ds->rdb, buf, 9) == 0)
    {
      goto error;
    }
  buf[9] = '\0';

  if (memcmp (buf, "REDIS", 5) != 0)
    {
      serverLog (LL_WARNING, "Wrong signature trying to load DB from file");
      goto error;
    }
  rdbver = atoi (buf + 5);
  if (rdbver < 1 || rdbver > RDB_VERSION)
    {
      serverLog (LL_WARNING, "Can't handle RDB format version %d", rdbver);
      goto error;
    }

  ds->fp = fp;
  ds->rdbver = rdbver;
  return 0;

error:
  if (fp != NULL)
    {
      fclose (fp);
    }
  return -1;
}


/* returns 1 if ds has iteritem, 0 if no more, -1 error */
int
arcx_dumpscan_iterate (dumpScan * ds)
{
  int type;

  while (1)
    {
      robj *key = NULL, *val = NULL;
      long long expiretime = -1;

      if ((type = rdbLoadType (&ds->rdb)) == -1)
	{
	  return -1;
	}

      if (type == RDB_OPCODE_EXPIRETIME)
	{
	  if ((expiretime = rdbLoadTime (&ds->rdb)) == -1)
	    {
	      goto error;
	    }
	  if ((type = rdbLoadType (&ds->rdb)) == -1)
	    {
	      goto error;
	    }
	  expiretime *= 1000;
	}
      else if (type == RDB_OPCODE_EXPIRETIME_MS)
	{
	  if ((expiretime = rdbLoadMillisecondTime (&ds->rdb)) == -1)
	    {
	      goto error;
	    }
	  if ((type = rdbLoadType (&ds->rdb)) == -1)
	    {
	      goto error;
	    }
	}
      else if (type == RDB_OPCODE_EOF)
	{
	  return 0;
	}
      else if (type == RDB_OPCODE_SELECTDB)
	{
	  uint32_t dbid;

	  if ((dbid = rdbLoadLen (&ds->rdb, NULL)) == RDB_LENERR)
	    {
	      goto error;
	    }
	  if (dbid >= (unsigned) server.dbnum)
	    {
	      serverLog (LL_WARNING,
			 "FATAL: Data file was created with a Redis "
			 "server configured to handle more than %d "
			 "databases. Exiting\n", server.dbnum);
	      exit (1);
	    }
	  ds->dt = ARCX_DUMP_DT_SELECTDB;
	  ds->d.selectdb.dbid = (int) dbid;
	  return 1;
	}
      else if (type == RDB_OPCODE_RESIZEDB)
	{
	  uint32_t db_size, expires_size;
	  if ((db_size = rdbLoadLen (&ds->rdb, NULL)) == RDB_LENERR)
	    {
	      goto error;
	    }
	  if ((expires_size = rdbLoadLen (&ds->rdb, NULL)) == RDB_LENERR)
	    {
	      goto error;
	    }
	  continue;
	}
      else if (type == RDB_OPCODE_AUX)
	{
	  robj *auxkey, *auxval;
	  if ((auxkey = rdbLoadStringObject (&ds->rdb)) == NULL)
	    {
	      goto error;
	    }
	  if ((auxval = rdbLoadStringObject (&ds->rdb)) == NULL)
	    {
	      goto error;
	    }
	  ds->dt = ARCX_DUMP_DT_AUX;
	  ds->d.aux.key = auxkey;
	  ds->d.aux.val = auxval;
	  return 1;
	}

      /* Read key */
      if ((key = rdbLoadStringObject (&ds->rdb)) == NULL)
	{
	  goto error;
	}

      /* Read value */
      (void) arc_rio_peek_key (&ds->rdb, type, key);
      if ((val = rdbLoadObject (type, &ds->rdb)) == NULL)
	{
	  goto error;
	}

      if (ds->rdbver == 6 && arcx_is_auxkey (key))
	{
	  ds->dt = ARCX_DUMP_DT_AUX;
	  ds->d.aux.key = key;
	  ds->d.aux.val = val;
	  return 1;
	}
      else
	{
	  ds->dt = ARCX_DUMP_DT_KV;
	  ds->d.kv.type = val->type;
	  ds->d.kv.expiretime = expiretime;
	  ds->d.kv.key = key;
	  ds->d.kv.val = val;
	  return 1;
	}
    }
error:
  return -1;
}

int
arcx_dumpscan_finish (dumpScan * ds, int will_need)
{
  if (ds->fp != NULL)
    {
      if (!will_need)
	{
	  int fd = fileno (ds->fp);
	  if (fdatasync (fd) == 0)
	    {
	      (void) posix_fadvise (fd, 0, 0, POSIX_FADV_DONTNEED);
	    }
	}
      fclose (ds->fp);
      ds->fp = NULL;
    }

  return 0;
}

#endif

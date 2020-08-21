/* * Copyright 2015 Naver Corp.
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

#include "arc_internal.h"
#include <fcntl.h>
#include <stdarg.h>

#define PLAYDUMP_LOAD       0
#define PLAYDUMP_RANGEDEL   1

struct dumpState
{
  dumpScan *scan;
  rio aof;
  sds readbuf;
  int fd;
  off_t aofpos;
  int tps_hz, opcount;
  long net_limit_bytes_hz, net_avail;
  int ret, emit_done, send_done;
  int cronloops;
  int flags;
  off_t cursize, totalsize;
};


/* ----------------- */
/* Local declaration */
/* ----------------- */
static void exit_with_usage (void);
static int cat_load_command (dumpScan * scan, rio * aof, int *is_eof,
			     int for_mig);
static int cat_del_command (dumpScan * scan, rio * aof, int *is_eof,
			    int for_mig);
static void playdump_write_handler (aeEventLoop * el, int fd, void *data,
				    int mask);
static void playdump_read_handler (aeEventLoop * el, int fd, void *data,
				   int mask);
static int playdump_command (int argc, sds * argv);
static int getdump_command (int argc, char **argv);
static int getandplay_command (int argc, char **argv);
static int rangedel_command (int argc, char **argv);
static int respout_command (int argc, char **argv, int is_load);
static int do_sub_commands (sds options);
static void init_cluster_util ();

/* --------------- */
/* Local variables */
/* --------------- */
static const char *_usage =
  "Usage:                                                                                         \n"
  "  ./cluster-util --playdump <src rdb> <dest addr> <port> <tps> [<rate(MB/s)>]                  \n"
  "  ./cluster-util --getdump <src addr> <port> <dest rdb> <range> [<rate(MB/s)>]                 \n"
  "  ./cluster-util --getandplay <src addr> <port> <dest addr> <port> <range> <tps> [<rate(MB/s)>]\n"
  "  ./cluster-util --rangedel <addr> <port> <range> <tps> [<rate(MB/s)>]                         \n"
  "  ./cluster-util --catdata <src rdb>                                                           \n"
  "  ./cluster-util --deldata <src rdb>                                                           \n"
  "    * If ommited, default rate limit is 30MB/s                                                 \n"
  "    * <range> is INCLUSIVE (e.g. 0-1024 has 1025 partitions)                                   \n"
  "Examples:                                                                                      \n"
  "  ./cluster-util --playdump dump.rdb localhost 6379 30000                                      \n"
  "  ./cluster-util --playdump dump.rdb localhost 6379 30000 30                                   \n"
  "  ./cluster-util --getdump localhost 6379 dump.rdb 0-8191                                      \n"
  "  ./cluster-util --getdump localhost 6379 dump.rdb 0-8191 30                                   \n"
  "  ./cluster-util --getandplay localhost 6379 localhost 7379 0-8191 30000                       \n"
  "  ./cluster-util --getandplay localhost 6379 localhost 7379 0-8191 30000 30                    \n"
  "  ./cluster-util --rangedel localhost 6379 0-1024 30000                                        \n"
  "  ./cluster-util --rangedel localhost 6379 0-1024 30000 30                                     \n";

static int verboselog = 1;
#define SERVERLOG(l, ...) do {               \
  if(verboselog) serverLog(l,__VA_ARGS__);   \
} while(0)

/* --------------- */
/* Local functions */
/* --------------- */

static void
exit_with_usage (void)
{
  fprintf (stderr, _usage);
  exit (1);
}

// returns 0 (EOF), -1 (ERROR), number of op emitted 
static int
cat_load_command (dumpScan * scan, rio * aof, int *is_eof, int for_mig)
{
  robj *key = NULL, *val = NULL;

  *is_eof = 0;
  while (1)
    {
      int ret, count;

      if ((ret = arcx_dumpscan_iterate (scan)) <= 0)
	{
	  if (ret == 0)
	    {
	      *is_eof = 1;
	    }
	  return ret;
	}

      key = val = NULL;
      count = -1;

      if (scan->dt == ARCX_DUMP_DT_SELECTDB)
	{
	  int dbid = scan->d.selectdb.dbid;
	  char cmd[] = "*2\r\n$6\r\nSELECT\r\n";
	  if (!for_mig)
	    {
	      continue;
	    }
	  GOTOIF (err, rioWrite (aof, cmd, sizeof (cmd) - 1) == 0);
	  GOTOIF (err, rioWriteBulkLongLong (aof, dbid) == 0);
	  return 1;
	}
      else if (scan->dt == ARCX_DUMP_DT_KV)
	{
	  long long expiretime = scan->d.kv.expiretime;
	  key = scan->d.kv.key;
	  val = scan->d.kv.val;
	  count = 1;

	  switch (scan->d.kv.type)
	    {
	    case OBJ_STRING:
	      {
		char cmd[] = "*3\r\n$3\r\nSET\r\n";
		GOTOIF (err, rioWrite (aof, cmd, sizeof (cmd) - 1) == 0);
		GOTOIF (err, rioWriteBulkObject (aof, key) == 0);
		GOTOIF (err, rioWriteBulkObject (aof, val) == 0);
	      }
	      break;
	    case OBJ_LIST:
	      GOTOIF (err, rewriteListObject (aof, key, val) == 0);
	      break;
	    case OBJ_SET:
	      GOTOIF (err, rewriteSetObject (aof, key, val) == 0);
	      break;
	    case OBJ_ZSET:
	      GOTOIF (err, rewriteSortedSetObject (aof, key, val) == 0);
	      break;
	    case OBJ_HASH:
	      GOTOIF (err, rewriteHashObject (aof, key, val) == 0);
	      break;
	    case OBJ_SSS:
	      GOTOIF (err, arc_rewrite_sss_object (aof, key, val) == 0);
	      count = arc_sss_type_value_count (val);
	      break;
	    default:
	      SERVERLOG (LL_WARNING, "Unknown object type in rdb file");
	      goto err;
	    }

	  if (expiretime != -1)
	    {
	      if (for_mig)
		{
		  char cmd[] = "*3\r\n$12\r\nMIGPEXPIREAT\r\n";
		  GOTOIF (err, rioWrite (aof, cmd, sizeof (cmd) - 1) == 0);
		}
	      else
		{
		  char cmd[] = "*3\r\n$9\r\nPEXPIREAT\r\n";
		  GOTOIF (err, rioWrite (aof, cmd, sizeof (cmd) - 1) == 0);
		}
	      GOTOIF (err, rioWriteBulkObject (aof, key) == 0);
	      GOTOIF (err, rioWriteBulkLongLong (aof, expiretime) == 0);
	      count += 1;
	    }
	  decrRefCount (key);
	  decrRefCount (val);
	  return count;
	}
      else if (scan->dt == ARCX_DUMP_DT_AUX)
	{
	  decrRefCount (scan->d.aux.key);
	  decrRefCount (scan->d.aux.val);
	  continue;
	}
      else
	{
	  serverPanic ("Unkown iteration type in rdb scan");
	}
    }

err:
  if (key != NULL)
    {
      decrRefCount (key);
    }
  if (val != NULL)
    {
      decrRefCount (val);
    }
  return -1;
}

// returns 0 (EOF), -1 (ERROR), number of op emitted 
static int
cat_del_command (dumpScan * scan, rio * aof, int *is_eof, int for_mig)
{
  robj *key = NULL, *val = NULL;

  *is_eof = 0;
  while (1)
    {
      int ret;

      if ((ret = arcx_dumpscan_iterate (scan)) <= 0)
	{
	  if (ret == 0)
	    {
	      *is_eof = 1;
	    }
	  return ret;
	}

      key = val = NULL;

      if (scan->dt == ARCX_DUMP_DT_SELECTDB)
	{
	  int dbid = scan->d.selectdb.dbid;
	  char cmd[] = "*2\r\n$6\r\nSELECT\r\n";
	  if (!for_mig)
	    {
	      continue;
	    }
	  GOTOIF (err, rioWrite (aof, cmd, sizeof (cmd) - 1) == 0);
	  GOTOIF (err, rioWriteBulkLongLong (aof, dbid) == 0);
	  return 1;
	}
      else if (scan->dt == ARCX_DUMP_DT_KV)
	{
	  char cmd[] = "*2\r\n$3\r\nDEL\r\n";

	  key = scan->d.kv.key;
	  val = scan->d.kv.val;

	  GOTOIF (err, rioWrite (aof, cmd, sizeof (cmd) - 1) == 0);
	  GOTOIF (err, rioWriteBulkObject (aof, key) == 0);

	  decrRefCount (key);
	  decrRefCount (val);
	  return 1;
	}
      else if (scan->dt == ARCX_DUMP_DT_AUX)
	{
	  decrRefCount (scan->d.aux.key);
	  decrRefCount (scan->d.aux.val);
	  continue;
	}
      else
	{
	  serverPanic ("Unkown iteration type in rdb scan");
	}
    }

err:
  if (key != NULL)
    {
      decrRefCount (key);
    }
  if (val != NULL)
    {
      decrRefCount (val);
    }
  return -1;
}

static void
playdump_write_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  struct dumpState *ds = (struct dumpState *) data;
  int ret = 0, nw;
  off_t len;
  UNUSED (mask);


  if (ds->aof.io.buffer.pos == 0)
    {
      int is_eof = 0;

      /* fill buffer */
      do
	{
	  int count;
	  if (ds->flags == PLAYDUMP_LOAD)
	    {
	      ret = count = cat_load_command (ds->scan, &ds->aof, &is_eof, 1);
	    }
	  else if (ds->flags == PLAYDUMP_RANGEDEL)
	    {
	      ret = count = cat_del_command (ds->scan, &ds->aof, &is_eof, 1);
	    }
	  else
	    {
	      goto err;
	    }

	  ds->opcount += count;
	  if (ds->opcount >= ds->tps_hz)
	    {
	      break;
	    }
	}
      while (!is_eof && ret >= 0 && ds->aof.io.buffer.pos < PROTO_IOBUF_LEN);

      if (ret == -1)
	{
	  goto err;
	}
      if (is_eof)
	{
	  char pingcmd[] = "*1\r\n$4\r\nPING\r\n";
	  if (rioWrite (&ds->aof, pingcmd, sizeof (pingcmd) - 1) == 0)
	    {
	      goto err;
	    }
	  ds->emit_done = 1;
	}
    }

  len = ds->aof.io.buffer.pos - ds->aofpos;
  nw =
    write (fd, ds->aof.io.buffer.ptr + ds->aofpos,
	   ds->net_avail < len ? ds->net_avail : len);

  if (nw == -1 && errno == EAGAIN)
    {
      return;
    }
  else if (nw <= 0)
    {
      SERVERLOG (LL_WARNING, "I/O error writing to target, ret:%d, err:%s",
		 nw, strerror (errno));
      goto err;
    }

  ds->aofpos += nw;

  if (ds->aofpos == ds->aof.io.buffer.pos)
    {
      sdsclear (ds->aof.io.buffer.ptr);
      ds->aof.io.buffer.pos = 0;
      ds->aofpos = 0;
      if (ds->opcount >= ds->tps_hz || ds->emit_done)
	{
	  if (ds->emit_done)
	    {
	      ds->send_done = 1;
	    }
	  aeDeleteFileEvent (el, fd, AE_WRITABLE);
	  return;
	}
    }

  ds->net_avail -= nw;
  if (ds->net_avail <= 0)
    {
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
    }

  return;

err:
  ds->ret = C_ERR;
  aeDeleteFileEvent (el, fd, AE_READABLE);
  aeDeleteFileEvent (el, fd, AE_WRITABLE);
  aeStop (el);
}

static void
playdump_read_handler (aeEventLoop * el, int fd, void *data, int mask)
{
  struct dumpState *ds = (struct dumpState *) data;
  int nr;
  UNUSED (mask);

  nr = read (fd, ds->readbuf + sdslen (ds->readbuf), sdsavail (ds->readbuf));
  if (nr == -1 && errno == EAGAIN)
    {
      return;
    }
  else if (nr <= 0)
    {
      ds->ret = C_ERR;
      aeDeleteFileEvent (el, fd, AE_READABLE);
      aeDeleteFileEvent (el, fd, AE_WRITABLE);
      aeStop (el);
      return;
    }

  sdsIncrLen (ds->readbuf, nr);
  if (ds->send_done)
    {
      if (strstr (ds->readbuf, "+PONG\r\n"))
	{
	  aeDeleteFileEvent (el, fd, AE_WRITABLE);
	  aeDeleteFileEvent (el, fd, AE_READABLE);
	  aeStop (el);
	}
      sdsrange (ds->readbuf, -10, -1);
    }
  else
    {
      sdsclear (ds->readbuf);
    }
}

static int
playdump_cron (struct aeEventLoop *el, long long id, void *data)
{
  struct dumpState *ds = (struct dumpState *) data;
  UNUSED (id);

  if (!(ds->cronloops % server.hz))
    {
      ds->cursize = rioTell (&ds->scan->rdb);
      if (ds->flags == PLAYDUMP_LOAD)
	{
	  SERVERLOG (LL_NOTICE, "load %lld%% done",
		     (long long) ds->cursize * 100 / ds->totalsize);
	}
      else if (ds->flags == PLAYDUMP_RANGEDEL)
	{
	  SERVERLOG (LL_NOTICE, "delete %lld%% done",
		     (long long) ds->cursize * 100 / ds->totalsize);
	}
    }

  if (ds->send_done)
    {
      return 1000 / server.hz;
    }

  // Cron resets tps/network limit and enables writable file events.
  // We can simply call aeCreateFileEvent everytime cron executes, because it's
  // possible to call aeCreateFileEvent with fd which is already registered.
  ds->opcount = 0;
  ds->net_avail = ds->net_limit_bytes_hz;
  if (aeCreateFileEvent (el, ds->fd, AE_WRITABLE, playdump_write_handler, ds)
      == AE_ERR)
    {
      ds->ret = C_ERR;
      aeDeleteFileEvent (el, ds->fd, AE_READABLE);
      aeDeleteFileEvent (el, ds->fd, AE_WRITABLE);
      aeStop (el);
    }

  ds->cronloops++;
  return 1000 / server.hz;
}

static int
play_dump (char *filename, char *target_addr, int target_port, int tps,
	   int net_limit, int flags)
{
  sds aofbuf;
  int fd;
  char neterr[ANET_ERR_LEN];
  aeEventLoop *el;
  struct dumpState ds;
  dumpScan scan_s, *scan = NULL;
  long long teid;
  int ret = C_OK;

  /* init dump scan */
  init_dumpscan (&scan_s);
  if (arcx_dumpscan_start (&scan_s, filename) < 0)
    {
      SERVERLOG (LL_WARNING, "Can not open rdb dumpfile(%s)", filename);
      return C_ERR;
    }
  scan = &scan_s;

  /* connect to target redis server */
  fd = anetTcpNonBlockConnect (neterr, target_addr, target_port);
  if (fd == ANET_ERR)
    {
      SERVERLOG (LL_WARNING, "Unable to connect to redis server(%s:%d): %s",
		 target_addr, target_port, neterr);
      arcx_dumpscan_finish (scan, 0);
      return C_ERR;
    }

  /* initialize dumpState */
  ds.scan = scan;
  ds.totalsize = scan->filesz;
  ds.readbuf = sdsempty ();
  ds.readbuf = sdsMakeRoomFor (ds.readbuf, PROTO_IOBUF_LEN);
  aofbuf = sdsempty ();
  aofbuf = sdsMakeRoomFor (aofbuf, PROTO_IOBUF_LEN);
  rioInitWithBuffer (&ds.aof, aofbuf);
  ds.fd = fd;
  ds.tps_hz = tps / server.hz;
  ds.opcount = 0;
  ds.net_limit_bytes_hz = net_limit * 1024 * 1024 / server.hz;
  ds.net_avail = ds.net_limit_bytes_hz;
  ds.aofpos = 0;
  ds.emit_done = 0;
  ds.send_done = 0;
  ds.ret = C_OK;
  ds.cronloops = 0;
  ds.flags = flags;

  el = aeCreateEventLoop (1024);
  if (aeCreateFileEvent (el, fd, AE_WRITABLE, playdump_write_handler, &ds) ==
      AE_ERR
      || aeCreateFileEvent (el, fd, AE_READABLE, playdump_read_handler,
			    &ds) == AE_ERR)
    {
      SERVERLOG (LL_WARNING,
		 "Unable to create file event for playdump command.");
      ret = C_ERR;
      goto return_ret;
    }

  teid = aeCreateTimeEvent (el, 1, playdump_cron, &ds, NULL);
  aeMain (el);
  aeDeleteTimeEvent (el, teid);

  if (ds.ret == C_ERR)
    {
      SERVERLOG (LL_WARNING,
		 "Target redis server is not responding correctly.");
      ret = C_ERR;
      goto return_ret;
    }

return_ret:
  aeDeleteEventLoop (el);
  arcx_dumpscan_finish (scan, 0);
  close (fd);
  sdsfree (ds.readbuf);
  sdsfree (ds.aof.io.buffer.ptr);
  return ret;
}

/*-----------------------------------------------------------------------------
 * Cluster Util Commands
 *----------------------------------------------------------------------------*/

/* Play Dump Command
 * --playdump <rdb filename> <target address> <target port> */
static int
playdump_command (int argc, sds * argv)
{
  char *filename;
  char *target_addr;
  int target_port;
  int tps, net_limit;

  /* parameter */
  filename = (char *) argv[1];
  target_addr = (char *) argv[2];
  target_port = atoi (argv[3]);
  tps = atoi (argv[4]);
  net_limit = ARC_GETDUMP_DEFAULT_NET_LIMIT_MB;
  /* Check bandwidth option. */
  if (argc == 6)
    {
      net_limit = atoi (argv[5]);
    }

  if (target_port <= 0 || tps <= 0 || net_limit <= 0)
    {
      SERVERLOG (LL_WARNING, "Negative number in playdump command arguments");
      return C_ERR;
    }

  return play_dump (filename, target_addr, target_port, tps, net_limit,
		    PLAYDUMP_LOAD);
}

/* get dump Command
 * --getdump <source address> <port> <target rdb filename> <partition range(inclusive)> */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8)	/* 8 MB */
static int
getdump_command (int argc, char **argv)
{
  char *filename;
  char *source_addr;
  int source_port;
  char *range;
  int net_limit;

  /* parameter */
  source_addr = (char *) argv[1];
  source_port = atoi (argv[2]);
  filename = (char *) argv[3];
  range = (char *) argv[4];
  net_limit = ARC_GETDUMP_DEFAULT_NET_LIMIT_MB;	/* Check bandwidth option. */
  if (argc == 6)
    {
      net_limit = atoi (argv[5]);
    }

  if (source_port <= 0)
    {
      SERVERLOG (LL_WARNING,
		 "Negative number of source port in getdump command");
      return C_ERR;
    }

  if (!strchr (range, '-'))
    {
      SERVERLOG (LL_WARNING, "Invalid range format");
      return C_ERR;
    }

  if (net_limit <= 0)
    {
      SERVERLOG (LL_WARNING,
		 "Negative number of bandwidth in playdump command");
      return C_ERR;
    }

  return arcx_get_dump (source_addr, source_port, filename, range, net_limit);
}

/* get and play Command
 * --getandplay <source address> <port> <target address> <port> <partition range(inclusive)> */
static int
getandplay_command (int argc, char **argv)
{
  char filename[256];
  char *source_addr, *target_addr;
  int source_port, target_port;
  int tps, net_limit;
  char *range;

  /* parameter */
  source_addr = (char *) argv[1];
  source_port = atoi (argv[2]);
  target_addr = (char *) argv[3];
  target_port = atoi (argv[4]);
  range = (char *) argv[5];
  tps = atoi (argv[6]);
  net_limit = ARC_GETDUMP_DEFAULT_NET_LIMIT_MB;

  /* Check bandwidth option. */
  if (argc == 8)
    {
      net_limit = atoi (argv[7]);
    }

  if (source_port <= 0 || target_port <= 0)
    {
      SERVERLOG (LL_WARNING, "Negative number of port in getandplay command");
      return C_ERR;
    }

  if (tps <= 0)
    {
      SERVERLOG (LL_WARNING, "Negative number of tps in getandplay command");
      return C_ERR;
    }

  if (net_limit <= 0)
    {
      SERVERLOG (LL_WARNING,
		 "Negative number of bandwidth in playdump command");
      return C_ERR;
    }

  if (!strchr (range, '-'))
    {
      SERVERLOG (LL_WARNING, "Invalid range format");
      return C_ERR;
    }

  /* tempfile */
  snprintf (filename, 256, "temp-getandplay-%d.rdb", (int) getpid ());
  if (arcx_get_dump (source_addr, source_port, filename, range, net_limit) !=
      C_OK)
    {
      unlink (filename);
      return C_ERR;
    }

  if (play_dump
      (filename, target_addr, target_port, tps, net_limit,
       PLAYDUMP_LOAD) != C_OK)
    {
      unlink (filename);
      return C_ERR;
    }

  unlink (filename);
  return C_OK;
}

/* range del Command
 * --rangedel <address> <port> <partition range(inclusive)> <tps> */
static int
rangedel_command (int argc, char **argv)
{
  char filename[256];
  char *addr;
  int port, tps, net_limit;
  char *range;

  /* parameter */
  addr = (char *) argv[1];
  port = atoi (argv[2]);
  range = (char *) argv[3];
  tps = atoi (argv[4]);
  net_limit = ARC_GETDUMP_DEFAULT_NET_LIMIT_MB;
  /* Check bandwidth option. */
  if (argc == 6)
    {
      net_limit = atoi (argv[5]);
    }

  if (port <= 0)
    {
      SERVERLOG (LL_WARNING, "Negative number of port in rangedel command");
      return C_ERR;
    }

  if (tps <= 0)
    {
      SERVERLOG (LL_WARNING, "Negative number of tps in rangedel command");
      return C_ERR;
    }

  if (!strchr (range, '-'))
    {
      SERVERLOG (LL_WARNING, "Invalid range format");
      return C_ERR;
    }

  /* tempfile */
  snprintf (filename, 256, "temp-rangedel-%d.rdb", (int) getpid ());
  if (arcx_get_dump (addr, port, filename, range, net_limit) != C_OK)
    {
      unlink (filename);
      return C_ERR;
    }

  if (play_dump (filename, addr, port, tps, net_limit, PLAYDUMP_RANGEDEL) !=
      C_OK)
    {
      unlink (filename);
      return C_ERR;
    }

  unlink (filename);
  return C_OK;
}

/* cat dump command: emit RESP dump of the rdb to stdout
 * --catdata <src rdb> */
static int
respout_command (int argc, char **argv, int is_load)
{
  char *filename;
  dumpScan scan;
  rio sout;
  int is_eof = 0;
  UNUSED (argc);

  // Init dump scan
  filename = (char *) argv[1];
  init_dumpscan (&scan);
  if (arcx_dumpscan_start (&scan, filename) < 0)
    {
      return C_ERR;
    }
  // Init standard output
  rioInitWithFile (&sout, stdout);
  while (!is_eof)
    {
      int ret;
      if (is_load)
	{
	  ret = cat_load_command (&scan, &sout, &is_eof, 0);
	}
      else
	{
	  ret = cat_del_command (&scan, &sout, &is_eof, 0);
	}
      if (ret < 0)
	{
	  arcx_dumpscan_finish (&scan, 0);
	  return C_ERR;
	}
    }
  arcx_dumpscan_finish (&scan, 0);
  return C_OK;
}

/* Command Executor */
static int
do_sub_commands (sds options)
{
  sds *argv;
  int argc;
  int ret;

  verboselog = 1;
  options = sdstrim (options, " \t\r\n");
  argv = sdssplitargs (options, &argc);
  sdstolower (argv[0]);

  if (!strcasecmp (argv[0], "playdump") && (argc == 5 || argc == 6))
    {
      ret = playdump_command (argc, argv);
    }
  else if (!strcasecmp (argv[0], "getdump") && (argc == 5 || argc == 6))
    {
      ret = getdump_command (argc, argv);
    }
  else if (!strcasecmp (argv[0], "getandplay") && (argc == 7 || argc == 8))
    {
      ret = getandplay_command (argc, argv);
    }
  else if (!strcasecmp (argv[0], "rangedel") && (argc == 5 || argc == 6))
    {
      ret = rangedel_command (argc, argv);
    }
  else if (!strcasecmp (argv[0], "catdata") && argc == 2)
    {
      verboselog = 0;
      ret = respout_command (argc, argv, 1);
    }
  else if (!strcasecmp (argv[0], "deldata") && argc == 2)
    {
      verboselog = 0;
      ret = respout_command (argc, argv, 0);
    }
  else
    {
      exit_with_usage ();
    }
  if (ret == C_OK)
    {
      SERVERLOG (LL_NOTICE, "%s success\n", argv[0]);
    }
  sdsfreesplitres (argv, argc);
  return ret;
}

static void
init_cluster_util ()
{
  createSharedObjects ();
  arc.gc_line = zmalloc (sizeof (dlisth) * arc.gc_num_line);
  for (int j = 0; j < arc.gc_num_line; j++)
    {
      dlisth_init (&arc.gc_line[j]);
    }
}

/* ---- */
/* Main */
/* ---- */
int
arcx_cluster_util_main (int argc, char **argv)
{
  int j = 1;
  sds options;
  int ret;


  init_cluster_util ();

  if (argc < 2 || (argv[j][0] != '-' || argv[j][1] != '-'))
    {
      exit_with_usage ();
    }

  options = sdsempty ();
  while (j != argc)
    {
      if (argv[j][0] == '-' && argv[j][1] == '-')
	{
	  /* Option name */
	  if (sdslen (options))
	    {
	      exit_with_usage ();
	    }
	  options = sdscat (options, argv[j] + 2);
	  options = sdscat (options, " ");
	}
      else
	{
	  /* Option argument */
	  options = sdscatrepr (options, argv[j], strlen (argv[j]));
	  options = sdscat (options, " ");
	}
      j++;
    }

  ret = do_sub_commands (options);
  sdsfree (options);

  if (ret != C_OK)
    {
      exit (1);
    }
  else
    {
      exit (0);
    }
}
#endif /* NBASE_ARC */

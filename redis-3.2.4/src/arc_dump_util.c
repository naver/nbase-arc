#ifdef NBASE_ARC
#include "arc_internal.h"
#include "arc_dump_plugin.h"

#include <dlfcn.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "smr_log.h"

#define SMR_SCAN_CONTINUE 1
#define SMR_SCAN_QUIT 0
#define SMR_SCAN_ERROR -1

struct keyScan
{
  long long target_ts;
  robj *keyset;
  long long count;
};

typedef struct
{
  char *tempfile, tempfile_buf[256];
  rio *rdb, rdb_s;
  FILE *fp;
  struct arcRio *ario, ario_s;
  int error;
} patchState;

#define init_patchstate(p) do {    \
  (p)->tempfile = NULL;            \
  (p)->rdb = NULL;                 \
  (p)->fp = NULL;                  \
  (p)->ario = NULL;                \
  (p)->error = 0;                  \
} while(0)

// shortener
typedef struct dump_plugin_callback pluginCb;

/* returns C_OK or C_ERR */
typedef int (*type_iterator) (robj * o, pluginCb * cb, void *ctx);

/* ------------------ */
/* Local declarations */
/* ------------------ */
/* patch dump related */
static int load_meta_info (char *filename, int will_need);
static sds get_rdb_filename_for (sds rdb_dir, long long target_ts);
static int load_objects_in_keyset (char *filename, robj * keyset);
static void clear_patchstate (patchState * ps);
static int patch_prepare (patchState * ps);
static int patch_write_head (patchState * ps);
static int patch_save_nondirty (patchState * ps, char *in_file, robj * xset);
static int patch_save_fresh (patchState * ps);
static int patch_write_tail (patchState * ps);
static int patch_finish (patchState * ps, char *out_file);
static int make_patch_dump (char *in_file, char *out_file, robj * keyset);
static int dirty_key_scanner (void *arg, long long seq,
			      long long timestamp, int hash,
			      unsigned char *buf, int size);
static int data_scanner (void *arg, long long seq, long long timestamp,
			 int hash, unsigned char *buf, int size);

static int dump_command (int argc, sds * argv);
/* dump plugin interface */
static void bulk_object_peek (robj * o, char *buf, int bufsz, char **rp,
			      int *rsz);
static int bulk_object_callback (robj * o,
				 int (*callback) (void *, char *, int),
				 void *ctx);
static int string_iterator (robj * o, pluginCb * cb, void *ctx);
static int list_iterator (robj * o, pluginCb * cb, void *ctx);
static int set_iterator (robj * o, pluginCb * cb, void *ctx);
static int zset_iterator (robj * o, pluginCb * cb, void *ctx);
static int hash_iterator (robj * o, pluginCb * cb, void *ctx);
static int sss_iterator (robj * o, pluginCb * cb, void *ctx);
static int iterate_kv (robj * key, robj * o, long long expiretime,
		       pluginCb * cb, void *ctx);
static int dump_iterator_command (int argc, sds * argv);
/* info */
static int rdb_info_command (int argc, sds * argv);
/* common */
static void init_dump_util (void);
static void exit_with_usage (void);
static int do_sub_commands (sds options);

/* -------------- */
/* Local variable */
/* -------------- */
static const char *_usage =
  "Usage:                                                                                          \n"
  "  ./dump-util --dump <epochtime in sec> <path/to/smrlog_dir> <path/to/rdb_dir> <output filename>\n"
  "  ./dump-util --dump-iterator <path/to/rdbfile> <path/to/plugin> \"<plugin args>\"              \n"
  "  ./dump-util --rdb-info <rdb dir>                                                              \n"
  "Examples:                                                                                       \n"
  "  ./dump-util --dump 1389766796 ../smr/log ../redis out.rdb                                     \n"
  "  ./dump-util --dump-iterator dump.rdb dump2json.so \"dump.json\"                               \n"
  "  ./dump-util --rdb-info dump.rdb                                                               \n";


/* --------------- */
/* Local functions */
/* --------------- */
static int
load_meta_info (char *filename, int will_need)
{
  dumpScan scan_s, *scan;
  int i, ret, aux_in_aux = 0, count;
  long long now;

  scan = &scan_s;
  init_dumpscan (scan);

  ret = arcx_dumpscan_start (scan, filename);
  if (ret < 0)
    {
      return C_ERR;
    }

  count = 0;
  for (i = 0; count < ARC_RDB_MAX_HEADERS; i++)
    {
      ret = arcx_dumpscan_iterate (scan);
      if (ret == 0)
	{
	  break;
	}
      else if (ret < 0)
	{
	  ret = C_ERR;
	  goto done;
	}

      if (scan->dt == ARCX_DUMP_DT_AUX)
	{
	  arc_rdb_load_aux_fields_hook (scan->d.aux.key, scan->d.aux.val,
					&now);
	  aux_in_aux = 1;
	}
      else if (scan->dt == ARCX_DUMP_DT_KV)
	{
	  if (aux_in_aux)
	    {
	      // means aux fields in right position. can not be here.
	      decrRefCount (scan->d.kv.key);
	      decrRefCount (scan->d.kv.val);
	      break;
	    }
	  // old version has nbase-arc aux fields in leading data portion
	  arc_rdb_load_aux_fields_hook (scan->d.kv.key, scan->d.kv.val, &now);
	  count++;
	}
    }
  ret = C_OK;

done:
  arcx_dumpscan_finish (scan, will_need);
  return ret;
}

static sds
get_rdb_filename_for (sds rdb_dir, long long target_ts)
{
  DIR *dir;
  struct dirent *entry;
  sds rdbname, cand_rdb = NULL;
  long long cand_ts = 0;

  dir = opendir (rdb_dir);
  if (dir == NULL)
    {
      serverLog (LL_WARNING, "Can't not open directory during rdbSave.");
      return NULL;
    }

  rdbname = sdsempty ();
  while ((entry = readdir (dir)) != NULL)
    {
      if (!strncmp (entry->d_name, "dump", 4)
	  && !strncmp (entry->d_name + strlen (entry->d_name) - 4, ".rdb", 4))
	{
	  struct stat sb;

	  sdsclear (rdbname);
	  rdbname = sdscatprintf (rdbname, "%s/%s", rdb_dir, entry->d_name);

	  stat (rdbname, &sb);
	  if (!S_ISREG (sb.st_mode))
	    {
	      continue;
	    }
	  if (load_meta_info (rdbname, 0) == C_ERR)
	    {
	      continue;
	    }

	  if (arc.smr_ts <= target_ts && arc.smr_ts > cand_ts)
	    {
	      cand_ts = arc.smr_ts;
	      if (cand_rdb == NULL)
		{
		  cand_rdb = sdsempty ();
		}
	      cand_rdb = sdscpy (cand_rdb, rdbname);
	    }
	}
    }
  sdsfree (rdbname);
  closedir (dir);
  return cand_rdb;
}

// Note: assumes that aux fields are alreay loaded before (load_meta_info)
static int
load_objects_in_keyset (char *filename, robj * keyset)
{
  dumpScan scan_s, *scan;
  long long now = arc.smr_ts;
  redisDb *db = server.db + 0;
  int ret = C_OK;

  scan = &scan_s;
  init_dumpscan (scan);

  ret = arcx_dumpscan_start (scan, filename);
  if (ret < 0)
    {
      return C_ERR;
    }

  // resize db first
  dictExpand (db->dict, setTypeSize (keyset));

  while (1)
    {
      ret = arcx_dumpscan_iterate (scan);
      if (ret == 0)
	{
	  break;
	}
      else if (ret < 0)
	{
	  ret = C_ERR;
	  goto done;
	}

      if (scan->dt == ARCX_DUMP_DT_AUX)
	{
	  decrRefCount (scan->d.aux.key);
	  decrRefCount (scan->d.aux.val);
	}
      else if (scan->dt == ARCX_DUMP_DT_SELECTDB)
	{
	  db = server.db + scan->d.selectdb.dbid;
	}
      else if (scan->dt == ARCX_DUMP_DT_KV)
	{
	  long long expiretime = scan->d.kv.expiretime;
	  robj *key = scan->d.kv.key;
	  robj *val = scan->d.kv.val;
	  int expired = (expiretime != -1 && expiretime < now);

	  if (expired || !setTypeIsMember (keyset, key))
	    {
	      decrRefCount (key);
	      decrRefCount (val);
	    }
	  else
	    {
	      dbAdd (db, key, val);
	      if (expiretime != -1)
		{
		  setExpire (db, key, expiretime);
		}
	      decrRefCount (key);
	    }
	}
    }
  ret = C_OK;

done:
  arcx_dumpscan_finish (scan, 1);
  return ret;
}

static void
clear_patchstate (patchState * ps)
{
  if (ps->fp != NULL)
    {
      fclose (ps->fp);
    }
  if (ps->tempfile)
    {
      unlink (ps->tempfile);
    }
}

static int
patch_prepare (patchState * ps)
{
  // make temp rio to write
  snprintf (ps->tempfile, 256, "temp-dump-util-%d.rdb", (int) getpid ());
  ps->fp = fopen (ps->tempfile, "w");
  if (!ps->fp)
    {
      serverLog (LL_WARNING,
		 "Failed opening the RDB file %s  "
		 "for saving: %s", ps->tempfile, strerror (errno));
      return C_ERR;
    }
  rioInitWithFile (&ps->rdb_s, ps->fp);
  ps->rdb = &ps->rdb_s;

  // setup flush state
  init_arc_rio (&ps->ario_s);
  ps->ario = &ps->ario_s;
  ps->ario->fd = fileno (ps->fp);
  if (ps->ario->fd < 0)
    {
      return C_ERR;
    }

  ps->rdb->ario = ps->ario;

  return C_OK;
}

static int
patch_write_head (patchState * ps)
{
  char magic[10];

  if (server.rdb_checksum)
    {
      ps->rdb->update_cksum = rioGenericUpdateChecksum;
    }
  snprintf (magic, sizeof (magic), "REDIS%04d", RDB_VERSION);
  RETURNIF (C_ERR, rioWrite (ps->rdb, magic, 9) == 0);
  RETURNIF (C_ERR, rdbSaveInfoAuxFields (ps->rdb) == -1);

  return C_OK;
}


static int
patch_save_nondirty (patchState * ps, char *in_file, robj * xset)
{
  dumpScan scan_s, *scan;
  long long now = arc.smr_ts;
  int ret = C_ERR;

  scan = &scan_s;
  init_dumpscan (scan);

  ret = arcx_dumpscan_start (scan, in_file);
  if (ret < 0)
    {
      return C_ERR;
    }

  while (1)
    {
      ret = arcx_dumpscan_iterate (scan);
      if (ret == 0)
	{
	  break;
	}
      else if (ret < 0)
	{
	  goto werr;
	}

      if (scan->dt == ARCX_DUMP_DT_AUX)
	{
	  decrRefCount (scan->d.aux.key);
	  decrRefCount (scan->d.aux.val);
	}
      else if (scan->dt == ARCX_DUMP_DT_KV)
	{
	  long long expiretime = scan->d.kv.expiretime;
	  robj *key = scan->d.kv.key;
	  robj *val = scan->d.kv.val;
	  int expired = (expiretime != -1 && expiretime < now);

	  if (expired || setTypeIsMember (xset, key))
	    {
	      decrRefCount (key);
	      decrRefCount (val);
	    }
	  else
	    {
	      ret = rdbSaveKeyValuePair (ps->rdb, key, val, expiretime, now);
	      GOTOIF (werr, ret == -1);
	      decrRefCount (key);
	      decrRefCount (val);
	      GOTOIF (werr, arc_rdb_save_onwrite (ps->rdb, &ps->error) == -1);
	    }
	}
    }
  ret = C_OK;

werr:
  arcx_dumpscan_finish (scan, 1);
  return ret;
}

static int
patch_save_fresh (patchState * ps)
{
  int j;
  dictIterator *di = NULL;
  dictEntry *de;
  long long now = arc.smr_ts;
  rio *rdb = ps->rdb;

  for (j = 0; j < server.dbnum; j++)
    {
      redisDb *db = server.db + j;
      dict *d = db->dict;

      if (dictSize (d) == 0)
	{
	  continue;
	}
      di = dictGetSafeIterator (d);
      if (!di)
	{
	  return C_ERR;
	}

      GOTOIF (werr, rdbSaveType (rdb, RDB_OPCODE_SELECTDB) == -1);
      GOTOIF (werr, rdbSaveLen (rdb, j) == -1);

      while ((de = dictNext (di)) != NULL)
	{
	  sds keystr = dictGetKey (de);
	  robj key, *o = dictGetVal (de);
	  long long expire;

	  initStaticStringObject (key, keystr);
	  expire = getExpire (db, &key);
	  GOTOIF (werr,
		  rdbSaveKeyValuePair (rdb, &key, o, expire, now) == -1);
	  GOTOIF (werr, arc_rdb_save_onwrite (rdb, &ps->error) == -1);
	}
      dictReleaseIterator (di);
    }

  return C_OK;

werr:
  if (di)
    {
      dictReleaseIterator (di);
    }

  return C_ERR;
}

static int
patch_write_tail (patchState * ps)
{
  uint64_t cksum;
  rio *rdb = ps->rdb;

  RETURNIF (C_ERR, rdbSaveType (rdb, RDB_OPCODE_EOF) == -1);
  cksum = rdb->cksum;
  memrev64ifbe (&cksum);
  RETURNIF (C_ERR, rioWrite (rdb, &cksum, 8) == 0);

  return C_OK;
}

static int
patch_finish (patchState * ps, char *out_file)
{
  RETURNIF (C_ERR, fflush (ps->fp) == EOF);
  RETURNIF (C_ERR, fsync (fileno (ps->fp)) == -1);
  RETURNIF (C_ERR, fclose (ps->fp) == EOF);
  ps->fp = NULL;

  if (rename (ps->tempfile, out_file) == -1)
    {
      serverLog (LL_WARNING,
		 "Error moving temp DB file on the final destination: %s",
		 strerror (errno));
      unlink (ps->tempfile);
      return C_ERR;
    }
  return C_OK;
}

static int
make_patch_dump (char *in_file, char *out_file, robj * keyset)
{
  patchState ps;
  int ret = C_OK;

  init_patchstate (&ps);

  GOTOIF (done, (ret = patch_prepare (&ps)) != C_OK);
  GOTOIF (done, (ret = patch_write_head (&ps)) != C_OK);
  GOTOIF (done, (ret = patch_save_nondirty (&ps, in_file, keyset)) != C_OK);
  GOTOIF (done, (ret = patch_save_fresh (&ps)) != C_OK);
  GOTOIF (done, (ret = patch_write_tail (&ps)) != C_OK);
  GOTOIF (done, (ret = patch_finish (&ps, out_file)) != C_OK);

done:
  clear_patchstate (&ps);
  return ret;
}

static int
dirty_key_scanner (void *arg, long long seq, long long timestamp, int hash,
		   unsigned char *buf, int size)
{
  struct keyScan *ksarg = (struct keyScan *) arg;
  long long target_ts = ksarg->target_ts;
  robj *set = ksarg->keyset;
  client *c;
  int cont = SMR_SCAN_CONTINUE;
  UNUSED (seq);
  UNUSED (hash);

  if (timestamp > target_ts)
    {
      return SMR_SCAN_QUIT;
    }
  ksarg->count++;

  /* Skip Non-write commands */
  if (size == 1 && (*buf == 'C' || *buf == 'D' || *buf == 'R'))
    {
      return SMR_SCAN_CONTINUE;
    }

  /* Set fake client */
  c = arc.smrlog_client;
  sdsclear (c->querybuf);
  c->querybuf = sdsMakeRoomFor (c->querybuf, size);
  memcpy (c->querybuf, buf, size);
  sdsIncrLen (c->querybuf, size);

  /* Parse query */
  if (c->querybuf[0] == '*')
    {
      if (processMultibulkBuffer (c) != C_OK)
	{
	  cont = SMR_SCAN_ERROR;
	  goto done;
	}
    }
  else
    {
      if (processInlineBuffer (c) != C_OK)
	{
	  cont = SMR_SCAN_ERROR;
	  goto done;
	}
    }

  /* Add dirty keys */
  if (c->argc > 0)
    {
      struct redisCommand *cmd;
      int i;

      cmd = lookupCommand (c->argv[0]->ptr);
      if (cmd == NULL || cmd->firstkey == 0)
	{
	  goto done;
	}

      i = cmd->firstkey;
      while (1)
	{
	  setTypeAdd (set, c->argv[i]);

	  if (i == cmd->lastkey)
	    {
	      break;
	    }
	  if (cmd->lastkey > 0 && i > cmd->lastkey)
	    {
	      serverLog (LL_WARNING,
			 "Incompatible command specification:%s", cmd->name);
	      break;
	    }
	  i += cmd->keystep;
	  if (cmd->lastkey < 0 && i >= c->argc)
	    {
	      break;
	    }
	}
    }

done:
  resetClient (c);
  zfree (c->argv);
  c->argv = NULL;

  return cont;
}

static int
data_scanner (void *arg, long long seq, long long timestamp, int hash,
	      unsigned char *buf, int size)
{
  long long target_ts = *((long long *) arg);
  client *c;
  UNUSED (hash);

  if (timestamp > target_ts)
    return SMR_SCAN_QUIT;
  arc.smr_ts = timestamp;

  if (size == 1 && (*buf == 'C' || *buf == 'D' || *buf == 'R'))
    {
      goto smr_continue;
    }

  c = arc.smrlog_client;
  sdsclear (c->querybuf);
  c->querybuf = sdsMakeRoomFor (c->querybuf, size);
  memcpy (c->querybuf, buf, size);
  sdsIncrLen (c->querybuf, size);
  processInputBuffer (c);

  resetClient (c);
  zfree (c->argv);
  c->argv = NULL;

smr_continue:
  arc.smr_seqnum = seq + size;
  return SMR_SCAN_CONTINUE;
}

static int
dump_command (int argc, sds * argv)
{
  smrLog *smrlog;
  long long target_ts = atoll (argv[1]) * 1000;
  sds log_dir = argv[2];
  sds rdb_dir = argv[3];
  sds outfile = argv[4];
  sds rdbname;
  struct keyScan ksarg;
  int ret;
  UNUSED (argc);

  serverLog (LL_NOTICE, "target time:%lld, "
	     "smrlog dir:%s, "
	     "rdbfile dir:%s, "
	     "output file:%s", target_ts, log_dir, rdb_dir, outfile);

  /* Select rdb file */
  rdbname = get_rdb_filename_for (rdb_dir, target_ts);
  if (rdbname == NULL)
    {
      serverLog (LL_NOTICE, "Failed to initialize from rdb_dir:%s\n",
		 rdb_dir);
      exit (1);
    }

  /* Find dirty keys */
  if (load_meta_info (rdbname, 1) == C_ERR)
    {
      serverLog (LL_WARNING, "Fatal error loading the DB(%s).:%s", rdbname,
		 strerror (errno));
      return C_ERR;
    }

  serverLog (LL_NOTICE,
	     "Select rdb file:%s, smr_seqnum:%lld, smr_mstime:%lld\n",
	     rdbname, arc.smr_seqnum, arc.smr_ts);

  smrlog = smrlog_init (log_dir);
  if (smrlog == NULL)
    {
      serverLog (LL_NOTICE, "Failed to initialize from log_dir:%s\n",
		 log_dir);
      return C_ERR;
    }

  ksarg.target_ts = target_ts;
  ksarg.keyset = createSetObject ();
  ksarg.count = 0;
  ret = smrlog_scan (smrlog, arc.smr_seqnum, -1, dirty_key_scanner, &ksarg);
  if (ret < 0)
    {
      serverLog (LL_NOTICE, "SMR log scan failed:%d", ret);
      smrlog_destroy (smrlog);
    }

  serverLog (LL_NOTICE, "Total SMR Log scanned:%lld", ksarg.count);
  smrlog_destroy (smrlog);

  /* Load dirty objects */
  ret = load_objects_in_keyset (rdbname, ksarg.keyset);
  if (ret == C_ERR)
    {
      serverLog (LL_NOTICE, "Loading dirty objects failed");
      return C_ERR;
    }

  /* Play smr log to make dirty objects up to date */
  smrlog = smrlog_init (log_dir);

  if (smrlog == NULL)
    {
      serverLog (LL_NOTICE, "Failed to initialize from log_dir:%s\n",
		 log_dir);
      return C_ERR;
    }

  ret = smrlog_scan (smrlog, arc.smr_seqnum, -1, data_scanner, &target_ts);
  if (ret < 0)
    {
      serverLog (LL_NOTICE, "SMR log scan failed:%d", ret);
      smrlog_destroy (smrlog);
    }
  smrlog_destroy (smrlog);

  /* Save up to date objects */
  ret = make_patch_dump (rdbname, outfile, ksarg.keyset);
  if (ret == C_ERR)
    {
      serverLog (LL_NOTICE, "Replacing dirty objects failed");
      return C_ERR;
    }

  decrRefCount (ksarg.keyset);
  sdsfree (rdbname);

  return C_OK;
}

static void
bulk_object_peek (robj * o, char *buf, int bufsz, char **rp, int *rsz)
{
  char *p = NULL;
  int sz = 0;

  if (o->encoding == OBJ_ENCODING_INT)
    {
      snprintf (buf, bufsz, "%lld", (long long) o->ptr);
      p = buf;
      sz = strlen (buf);
    }
  else if (sdsEncodedObject (o))
    {
      p = o->ptr;
      sz = sdslen(o->ptr);
    }
  else
    {
      serverPanic ("Unknown string encoding");
    }
  *rp = p;
  *rsz = sz;
}

static int
bulk_object_callback (robj * o, int (*callback) (void *, char *, int),
		      void *ctx)
{
  char buf[64];
  char *val = NULL;
  int len = 0;

  bulk_object_peek (o, buf, sizeof (buf), &val, &len);
  if (callback (ctx, val, len) < 0)
    {
      return C_ERR;
    }
  return C_OK;
}

static int
string_iterator (robj * o, pluginCb * cb, void *ctx)
{
  return bulk_object_callback (o, cb->string_val, ctx);
}

/* isomorphic to rewriteListObject */
static int
list_iterator (robj * o, pluginCb * cb, void *ctx)
{
  quicklist *list;
  quicklistIter *li;
  quicklistEntry entry;

  if (o->encoding != OBJ_ENCODING_QUICKLIST)
    {
      serverPanic ("Unknown list encoding");
    }

  list = o->ptr;
  li = quicklistGetIterator (list, AL_START_HEAD);
  while (quicklistNext (li, &entry))
    {
      char buf[32];
      char *val;
      int vallen;

      if (entry.value)
	{
	  val = (char *) entry.value;
	  vallen = entry.sz;
	}
      else
	{
	  snprintf (buf, 32, "%lld", entry.longval);
	  val = buf;
	  vallen = strlen (buf);
	}
      if (cb->list_val (ctx, val, vallen) < 0)
	{
	  goto error;
	}
    }
  quicklistReleaseIterator (li);
  return C_OK;

error:
  quicklistReleaseIterator (li);
  return C_ERR;
}

/* isomorphic to rewriteSetObject */
static int
set_iterator (robj * o, pluginCb * cb, void *ctx)
{
  if (o->encoding == OBJ_ENCODING_INTSET)
    {
      int ii = 0;
      int64_t llval;
      while (intsetGet (o->ptr, ii++, &llval))
	{
	  char buf[32];

	  snprintf (buf, 32, "%" PRId64, llval);
	  if (cb->set_val (ctx, buf, strlen (buf)) < 0)
	    {
	      return -1;
	    }
	}
    }
  else if (o->encoding == OBJ_ENCODING_HT)
    {
      dictIterator *di = dictGetIterator (o->ptr);
      dictEntry *de;

      while ((de = dictNext (di)) != NULL)
	{
	  robj *eleobj = dictGetKey (de);
	  if (bulk_object_callback (eleobj, cb->set_val, ctx) != C_OK)
	    {
	      return C_ERR;
	    }
	}
      dictReleaseIterator (di);
    }
  else
    {
      serverPanic ("Unknown set encoding");
    }

  return C_OK;
}

/* isomorphic to rewriteSortedSetObject */
static int
zset_iterator (robj * o, pluginCb * cb, void *ctx)
{
  char dbuf[128];

  if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
      unsigned char *zl = o->ptr;
      unsigned char *eptr, *sptr;
      unsigned char *vstr;
      unsigned int vlen;
      long long vll;
      double score;

      eptr = ziplistIndex (zl, 0);
      serverAssert (eptr != NULL);
      sptr = ziplistNext (zl, eptr);
      serverAssert (sptr != NULL);

      while (eptr != NULL)
	{
	  char *val = NULL;
	  int vallen = 0;

	  serverAssert (ziplistGet (eptr, &vstr, &vlen, &vll));
	  score = zzlGetScore (sptr);
	  if (vstr != NULL)

	    {
	      val = (char *) vstr;
	      vallen = (int) vlen;
	    }
	  else
	    {
	      snprintf (dbuf, sizeof (dbuf), "%lld", vll);
	      val = dbuf;
	      vallen = strlen (dbuf);
	    }
	  if (cb->zset_val (ctx, val, vallen, score) < 0)
	    {
	      return C_ERR;
	    }
	  zzlNext (zl, &eptr, &sptr);
	}
    }
  else if (o->encoding == OBJ_ENCODING_SKIPLIST)
    {
      zset *zs = o->ptr;
      dictIterator *di = dictGetIterator (zs->dict);
      dictEntry *de;

      while ((de = dictNext (di)) != NULL)
	{
	  robj *eleobj = dictGetKey (de);
	  double *score = dictGetVal (de);
	  char *val = NULL;
	  int vallen = 0;

	  bulk_object_peek (eleobj, dbuf, sizeof (dbuf), &val, &vallen);
	  if (cb->zset_val (ctx, val, vallen, *score) < 0)
	    {
	      return C_ERR;
	    }
	}
      dictReleaseIterator (di);
    }
  else
    {
      serverPanic ("Unknown sorted zset encoding");
    }

  return C_OK;
}

/* isomorphic to rewriteHashObject */
static int
hash_iterator (robj * o, pluginCb * cb, void *ctx)
{
  hashTypeIterator *hi;

  hi = hashTypeInitIterator (o);
  while (hashTypeNext (hi) != C_ERR)
    {
      char *key = NULL, *val = NULL;
      int keylen = 0, vallen = 0;
      char kbuf[64], vbuf[64];

      /* isomorphic to rioWriteHashIteratorCursor */
      if (hi->encoding == OBJ_ENCODING_ZIPLIST)
	{
	  unsigned char *vstr = NULL;
	  unsigned int vlen = UINT_MAX;
	  long long vll = LLONG_MAX;

	  // get key
	  hashTypeCurrentFromZiplist (hi, OBJ_HASH_KEY, &vstr, &vlen, &vll);
	  if (vstr)
	    {
	      key = (char *) vstr;
	      keylen = (int) vlen;
	    }
	  else
	    {
	      snprintf (kbuf, sizeof (kbuf), "%lld", vll);
	      key = kbuf;
	      keylen = strlen (kbuf);
	    }
	  // get val
	  vstr = NULL;
	  hashTypeCurrentFromZiplist (hi, OBJ_HASH_VALUE, &vstr, &vlen, &vll);
	  if (vstr)
	    {
	      val = (char *) vstr;
	      vallen = (int) vlen;
	    }
	  else
	    {
	      snprintf (vbuf, sizeof (vbuf), "%lld", vll);
	      val = vbuf;
	      vallen = strlen (vbuf);
	    }
	}
      else if (hi->encoding == OBJ_ENCODING_HT)
	{
	  robj *rk = NULL, *rv = NULL;

	  hashTypeCurrentFromHashTable (hi, OBJ_HASH_KEY, &rk);
	  bulk_object_peek (rk, kbuf, sizeof (kbuf), &key, &keylen);
	  hashTypeCurrentFromHashTable (hi, OBJ_HASH_VALUE, &rv);
	  bulk_object_peek (rv, vbuf, sizeof (vbuf), &val, &vallen);
	}
      else
	{
	  serverPanic ("Unknown hash encoding");
	}

      if (cb->hash_val (ctx, key, keylen, val, vallen) < 0)
	{
	  hashTypeReleaseIterator (hi);
	  return C_ERR;
	}
    }
  hashTypeReleaseIterator (hi);

  return C_OK;
}

static int
sss_iterator (robj * o, pluginCb * cb, void *ctx)
{
  long long count = 0;
  sssTypeIterator *si;
  sssEntry *se;
  int ret = C_OK;
  robj *prev_ks = NULL, *prev_svc = NULL, *prev_key = NULL;
  int kv_mode, mode, prev_mode = '?';

  si = arcx_sss_type_init_iterator (o);
  while ((se = arcx_sss_iter_next (si)) != NULL)
    {
      robj *ks, *svc, *key, *val;
      long long idx, vll;
      int is_begin = 0, is_end = 0;

      // peek a item
      ret =
	arcx_sss_iter_peek (se, &ks, &svc, &key, &idx, &val, &vll, &kv_mode);
      if (ret < 0)
	{
	  ret = C_ERR;
	  goto done;
	}

      if (kv_mode == SSS_KV_LIST)
	{
	  mode = PLUGIN_SSS_KV_LIST;
	}
      else if (kv_mode == SSS_KV_SET)
	{
	  mode = PLUGIN_SSS_KV_SET;
	}
      else
	{
	  serverPanic ("Invalid sss kv_mode");
	}
      count++;

      if (prev_ks == NULL)
	{
	  is_begin = 1;
	}
      else if (sdscmp (ks->ptr, prev_ks->ptr)
	       || sdscmp (svc->ptr, prev_svc->ptr)
	       || sdscmp (key->ptr, prev_key->ptr))
	{
	  is_end = 1;
	  is_begin = 1;
	}

      if (is_end)
	{
	  if (cb->end_sss_collection (ctx, prev_mode) < 0)
	    {
	      ret = C_ERR;
	      goto done;
	    }
	}
      if (is_begin)
	{
	  if (cb->begin_sss_collection
	      (ctx, ks->ptr, sdslen (ks->ptr), svc->ptr, sdslen (svc->ptr),
	       key->ptr, sdslen (key->ptr), mode) < 0)
	    {
	      ret = C_ERR;
	      goto done;
	    }
	}

      prev_mode = mode;
      prev_ks = ks;
      prev_svc = svc;
      prev_key = key;
      if (cb->sss_val (ctx, val->ptr, sdslen (val->ptr),
		       vll == LLONG_MAX ? -1 : vll) < 0)
	{
	  ret = C_ERR;
	  goto done;
	}
    }

  if (count > 0)
    {
      if (cb->end_sss_collection (ctx, prev_mode) < 0)
	{
	  ret = C_ERR;
	  goto done;
	}
    }
  ret = C_OK;

done:
  arcx_sss_type_release_iterator (si);
  return ret;
}

static int
iterate_kv (robj * key, robj * o,
	    long long expiretime, pluginCb * cb, void *ctx)
{
  type_iterator it = NULL;
  int type;

  switch (o->type)
    {
    case OBJ_STRING:
      it = string_iterator;
      type = PLUGIN_RDB_TYPE_STRING;
      break;
    case OBJ_LIST:
      it = list_iterator;
      type = PLUGIN_RDB_TYPE_LIST;
      break;
    case OBJ_SET:
      it = set_iterator;
      type = PLUGIN_RDB_TYPE_SET;
      break;
    case OBJ_ZSET:
      it = zset_iterator;
      type = PLUGIN_RDB_TYPE_ZSET;
      break;
    case OBJ_HASH:
      it = hash_iterator;
      type = PLUGIN_RDB_TYPE_HASH;
      break;
    case OBJ_SSS:
      it = sss_iterator;
      type = PLUGIN_RDB_TYPE_SSS;
      break;
    default:
      serverPanic ("Bad rdb type");
      break;
    }

  RETURNIF (C_ERR,
	    cb->begin_key (ctx, type, key->ptr,
			   sdslen (key->ptr), expiretime) < 0);
  RETURNIF (C_ERR, it (o, cb, ctx) == C_ERR);
  RETURNIF (C_ERR, cb->end_key (ctx, type) < 0);
  return C_OK;
}

static int
dump_iterator_command (int argc, sds * argv)
{
  sds dump = argv[1];
  sds plugin = argv[2];
  int plugin_argc = 0;
  sds *plugin_argv;
  void *handle = NULL, *ctx = NULL;
  pluginCb *cb = NULL;
  dumpScan *scan = NULL, scan_s;
  int ret;
  UNUSED (argc);

  /* setup plugin */
  plugin_argv = sdssplitargs (argv[3], &plugin_argc);
  handle = dlopen (plugin, RTLD_NOW);
  if (!handle)
    {
      serverLog (LL_WARNING, "plugin open error, %s\n", dlerror ());
      return C_ERR;
    }

  cb = (pluginCb *) dlsym (handle, "callback");
  if (!cb)
    {
      serverLog (LL_WARNING,
		 "plugin has no 'callback' function, %s\n", dlerror ());
      dlclose (handle);
      return C_ERR;
    }

  if ((ret = cb->initialize (plugin_argc, plugin_argv, &ctx)) < 0)
    {
      serverLog (LL_WARNING, "failed to initize plugin: %d", ret);
      dlclose (handle);
      return C_ERR;
    }

  /* do scan with callback */
  scan = &scan_s;
  init_dumpscan (scan);
  ret = arcx_dumpscan_start (scan, dump);
  if (ret < 0)
    {
      return C_ERR;
    }

  // arc.smr_xxx fields are alreay set before
  GOTOIF (werr,
	  cb->dumpinfo (ctx, scan->rdbver, arc.smr_seqnum, arc.smr_ts) < 0);
  while (1)
    {
      ret = arcx_dumpscan_iterate (scan);
      if (ret == 0)
	{
	  break;
	}
      else if (ret < 0)
	{
	  goto werr;
	}

      if (scan->dt == ARCX_DUMP_DT_AUX)
	{
	  decrRefCount (scan->d.aux.key);
	  decrRefCount (scan->d.aux.val);
	}
      else if (scan->dt == ARCX_DUMP_DT_KV)
	{
	  long long expiretime = scan->d.kv.expiretime;
	  robj *key = scan->d.kv.key;
	  robj *val = scan->d.kv.val;
	  int keyhash, skip = 0;
	  keyhash = crc16 (key->ptr, sdslen (key->ptr));
	  // skip keys in {arc.migrate_slot | arc.migclear_slot}
	  skip = skip || (arc.migrate_slot
			  && bitmapTestBit ((unsigned char *)
					    arc.migrate_slot,
					    keyhash % ARC_KS_SIZE));
	  skip = skip || (arc.migclear_slot
			  && bitmapTestBit ((unsigned char *)
					    arc.migclear_slot,
					    keyhash % ARC_KS_SIZE));
	  if (!skip)
	    {
	      ret = iterate_kv (key, val, expiretime, cb, ctx);
	    }
	  decrRefCount (key);
	  decrRefCount (val);
	  GOTOIF (werr, ret < 0);
	}
    }

  cb->finalize (ctx);
  ret = C_OK;
werr:
  dlclose (handle);
  arcx_dumpscan_finish (scan, 0);
  return ret;
}

static int
rdb_info_command (int argc, sds * argv)
{
  DIR *dir;
  struct dirent *entry;
  sds rdb_dir = argv[1];
  sds rdbname;
  UNUSED (argc);
  dir = opendir (rdb_dir);
  if (dir == NULL)
    {
      serverLog (LL_WARNING, "Can't not open directory during rdbSave.");
      return C_ERR;
    }

  rdbname = sdsempty ();
  while ((entry = readdir (dir)) != NULL)
    {
      if (!strncmp (entry->d_name + strlen (entry->d_name) - 4, ".rdb", 4))
	{
	  struct stat filestat;
	  sdsclear (rdbname);
	  rdbname = sdscatprintf (rdbname, "%s/%s", rdb_dir, entry->d_name);
	  stat (rdbname, &filestat);
	  if (!S_ISREG (filestat.st_mode))
	    continue;
	  if (load_meta_info (rdbname, 0) == C_ERR)
	    {
	      serverLog (LL_NOTICE, "- Incompatible file(%s)", rdbname);
	    }
	  else
	    {
	      serverLog (LL_NOTICE, "smr_seqnum:%14lld, "
			 "smr_mstime:%14lld, "
			 "filename:%s", arc.smr_seqnum, arc.smr_ts, rdbname);
	    }
	}
    }
  sdsfree (rdbname);
  closedir (dir);
  return C_OK;
}

static void
init_dump_util (void)
{
  arcx_init_server_pre ();
}

static void
exit_with_usage (void)
{
  fprintf (stderr, "%s", _usage);
  exit (1);
}

static int
do_sub_commands (sds options)
{
  sds *argv;
  int argc;
  int ret;
  options = sdstrim (options, " \t\r\n");
  argv = sdssplitargs (options, &argc);
  sdstolower (argv[0]);
  init_dump_util ();
  if (!strcasecmp (argv[0], "dump") && argc == 5)
    {
      ret = dump_command (argc, argv);
    }
  else if (!strcasecmp (argv[0], "dump-iterator") && argc == 4)
    {
      ret = dump_iterator_command (argc, argv);
    }
  else if (!strcasecmp (argv[0], "rdb-info") && argc == 2)
    {
      ret = rdb_info_command (argc, argv);
    }
  else
    {
      exit_with_usage ();
      ret = C_ERR;
    }
  if (ret == C_OK)
    {
      serverLog (LL_NOTICE, "%s success\n", argv[0]);
    }
  sdsfreesplitres (argv, argc);
  return ret;
}

/* --------------------- */
/* Exported arc function */
/* --------------------- */
int
arcx_dump_util_main (int argc, char **argv)
{
  /* parse and execute */
  if (argc >= 2)
    {
      int j = 1;
      sds options = sdsempty ();
      int ret;
      if (argv[j][0] != '-' || argv[j][1] != '-')
	{
	  exit_with_usage ();
	}

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
	exit (1);
    }
  else
    {
      exit_with_usage ();
    }
  exit (0);
}
#endif

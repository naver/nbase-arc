#ifdef NBASE_ARC
#include "arc_internal.h"

#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// embedded in rio_ structure
struct arcRio
{
  int fd;
  long long w_count;
  off_t last_off;
};
#define init_arc_rio(r) do  {  \
  (r)->fd = -1;                \
  (r)->w_count = 0LL;          \
  (r)->last_off = 0;           \
} while(0)


/* ---------------- */
/* Local declations */
/* ---------------- */
static sds get_bitmap_from_argv (int argc, robj ** argv);
static void send_bulk_to_target (aeEventLoop * el, int fd, void *privdata,
				 int mask);
static void mig_start (client * c, int argc, robj ** argv);
static void mig_end (client * c);

/* ----------------- */
/* Local definitions */
/* ----------------- */
static sds
get_bitmap_from_argv (int argc, robj ** argv)
{
  sds bitmap;
  int i;

  bitmap = sdsgrowzero (sdsempty (), ARC_KS_SIZE);

  for (i = 0; i < argc; i++)
    {
      int ok, n, j;
      long long ll, from, to;
      sds *tokens;
      sds from_to = (sds) argv[i]->ptr;

      /* There are two kinds of argument type.
       * 1. <slotNo>                     ex) 1024
       * 2. <slotNo from>-<slotNo to>    ex) 0-2047 */
      tokens = sdssplitlen (from_to, sdslen (from_to), "-", 1, &n);
      if (tokens == NULL)
	{
	  return NULL;
	}

      if (n == 1)
	{
	  /* Type 1 <slotNo> */
	  ok = string2ll (tokens[0], sdslen (tokens[0]), &ll);
	  if (!ok)
	    {
	      sdsfreesplitres (tokens, n);
	      return NULL;
	    }
	  from = ll;
	  to = ll;
	}
      else if (n == 2)
	{
	  /* Type 2 <slotNo from>-<slotNo to> */
	  ok = string2ll (tokens[0], sdslen (tokens[0]), &ll);
	  if (!ok)
	    {
	      sdsfreesplitres (tokens, n);
	      return NULL;
	    }
	  from = ll;

	  ok = string2ll (tokens[1], sdslen (tokens[1]), &ll);
	  if (!ok)
	    {
	      sdsfreesplitres (tokens, n);
	      return NULL;
	    }
	  to = ll;
	}
      else
	{
	  /* not belong to Type 1 or Type 2 */
	  sdsfreesplitres (tokens, n);
	  return NULL;
	}

      sdsfreesplitres (tokens, n);

      /* range check */
      if (from < 0 || to >= ARC_KS_SIZE || from > to)
	{
	  return NULL;
	}

      /* set bit */
      for (j = from; j <= to; j++)
	{
	  bitmapSetBit ((unsigned char *) bitmap, j);
	}
    }
  return bitmap;
}

static void
send_bulk_to_target (aeEventLoop * el, int fd, void *privdata, int mask)
{
  client *c = privdata;
  char buf[PROTO_IOBUF_LEN];
  ssize_t nwritten, buflen;

  UNUSED (el);
  UNUSED (mask);

  if (c->smr->ckptdboff == 0)
    {
      /* Write the bulk write count before to transfer the DB. In theory here
       * we don't know how much room there is in the output buffer of the
       * socket, but in pratice SO_SNDLOWAT (the minimum count for output
       * operations) will never be smaller than the few bytes we need. */
      sds bulkcount;

      bulkcount = sdscatprintf (sdsempty (), ":%lld\r\n$%lld\r\n",
				arc.checkpoint_seqnum,
				(unsigned long long) c->smr->ckptdbsize);
      if (write (fd, bulkcount, sdslen (bulkcount)) !=
	  (signed) sdslen (bulkcount))
	{
	  sdsfree (bulkcount);
	  freeClient (c);
	  return;
	}
      sdsfree (bulkcount);
    }

  lseek (c->smr->ckptdbfd, c->smr->ckptdboff, SEEK_SET);
  buflen = read (c->smr->ckptdbfd, buf, PROTO_IOBUF_LEN);
  if (buflen <= 0)
    {
      serverLog (LL_WARNING, "Read error sending DB to target: %s",
		 (buflen == 0) ? "premature EOF" : strerror (errno));
      freeClient (c);
      return;
    }
  if ((nwritten = write (fd, buf, buflen)) == -1)
    {
      serverLog (LL_VERBOSE, "Write error sending DB to target: %s",
		 strerror (errno));
      freeClient (c);
      return;
    }
  c->smr->ckptdboff += nwritten;
  if (c->smr->ckptdboff == c->smr->ckptdbsize)
    {
      aeDeleteFileEvent (server.el, c->fd, AE_WRITABLE);
      c->replstate = REPL_STATE_NONE;
      if (aeCreateFileEvent (server.el, c->fd, AE_WRITABLE,
			     sendReplyToClient, c) == AE_ERR)
	{
	  freeClient (c);
	  return;
	}
      addReply (c, shared.ok);
      serverLog (LL_NOTICE, "Synchronization with target succeeded");
    }
}

/* argv includes command itself */
static void
mig_start (client * c, int argc, robj ** argv)
{
  serverLog (LL_NOTICE, "Client ask for migrate start");
  if (arc.migrate_slot != NULL)
    {
      addReplyError (c,
		     "Another migration is in progress. Unable to perform MIGSTART.");
      return;
    }

  arc.migrate_slot = get_bitmap_from_argv (argc - 1, argv + 1);
  if (!arc.migrate_slot)
    {
      addReplyError (c, "Invalid argument format.");
      return;
    }

  serverLog (LL_NOTICE, "Starting migration");
  addReply (c, shared.ok);
  return;
}

static void
mig_end (client * c)
{
  serverLog (LL_NOTICE, "Client ask for migrate end");
  if (arc.migrate_slot == NULL)
    {
      addReplyError (c,
		     "Migration is not in progress. Unable to perform MIGEND.");
      return;
    }

  serverLog (LL_NOTICE, "Finishing migration");
  sdsfree (arc.migrate_slot);
  arc.migrate_slot = NULL;
  addReply (c, shared.ok);
  return;
}

/* --------------------- */
/* Exported arc function */
/* --------------------- */

// called when a background save (including checkpoint) has finished with result.
void
arc_bgsave_done_handler (int ok)
{
  client *c;
  struct redis_stat buf;

  // -----------------------------------------------
  // handle "save", "bgsave", "cronsave", "seqsave"
  //  -----------------------------------------------
  if (arc.cluster_mode && arc.checkpoint_slots == NULL)
    {
      if (!ok)
	{
	  // already logged
	}
      else if (smr_seq_ckpted (arc.smr_conn, arc.seqnum_before_bgsave) != 0)
	{
	  serverLog (LL_WARNING,
		     "Failed to notify checkpointed sequence to smr");
	  server.lastbgsave_status = C_ERR;
	}
      else
	{
	  serverLog (LL_NOTICE,
		     "Checkpointed sequence is sent to SMR, seqnum:%lld",
		     arc.seqnum_before_bgsave);
	  arc.last_bgsave_seqnum = arc.seqnum_before_bgsave;
	}
    }

  // --------------------
  // handle "checkpoint"
  // --------------------
  if (arc.checkpoint_slots == NULL)
    {
      return;
    }
  arc.checkpoint_seqnum = arc.seqnum_before_bgsave;

  sdsfree (arc.checkpoint_slots);
  arc.checkpoint_slots = NULL;

  c = arc.checkpoint_client;
  arc.checkpoint_client = NULL;
  if (c == NULL)
    {
      return;
    }

  if (!ok)
    {
      serverLog (LL_WARNING,
		 "CHECKPOINT failed. background child returned an error");
      freeClient (c);
      return;
    }

  if ((c->smr->ckptdbfd = open (arc.checkpoint_filename, O_RDONLY)) == -1
      || redis_fstat (c->smr->ckptdbfd, &buf) == -1)
    {
      freeClient (c);
      serverLog (LL_WARNING,
		 "CHECKPOINT failed. Can't open/stat DB after CHECKPOINT: %s",
		 strerror (errno));
      return;
    }
  c->smr->ckptdboff = 0;
  c->smr->ckptdbsize = buf.st_size;
  c->replstate = SLAVE_STATE_SEND_BULK;

  aeDeleteFileEvent (server.el, c->fd, AE_WRITABLE);
  if (aeCreateFileEvent
      (server.el, c->fd, AE_WRITABLE, send_bulk_to_target, c) == AE_ERR)
    {
      freeClient (c);
    }
}

int
arc_rdb_save_rio_with_file (rio * rdb, FILE * fp, int *error)
{
  struct arcRio ario;
  int ret;

  // setup
  init_arc_rio (&ario);
  ario.fd = fileno (fp);
  if (ario.fd < 0)
    {
      *error = errno;
      return C_ERR;
    }

  // do wrapped call
  rdb->ario = &ario;
  ret = rdbSaveRio (rdb, error);
  rdb->ario = NULL;

  // teardown
  // do not: close (ario.fd);
  return ret;
}

int
arc_rdb_save_onwrite (rio * rdb, int *error)
{
  struct arcRio *ario;
  off_t curr_off;

  if ((ario = rdb->ario) == NULL || ++ario->w_count % 32 != 0)
    {
      return 0;
    }

  if ((curr_off = rioTell (rdb)) < 0)
    {
      goto werr;
    }

  if (curr_off - ario->last_off > 32 * 1024 * 1024)
    {
      if (fdatasync (ario->fd) != 0)
	{
	  goto werr;
	}
      if (posix_fadvise (ario->fd, 0, 0, POSIX_FADV_DONTNEED) != 0)
	{
	  goto werr;
	}
      ario->last_off = curr_off;
    }

  return 0;

werr:
  *error = errno;
  return -1;
}

int
arc_rdb_save_skip (sds keystr)
{
  int keyhash;

  if (arc.checkpoint_slots == NULL)
    {
      return 0;
    }

  keyhash = crc16 (keystr, sdslen (keystr));
  return bitmapTestBit ((unsigned char *) arc.checkpoint_slots,
			keyhash % ARC_KS_SIZE);
}

static int
save_aux_field_sds (rio * rdb, sds k, sds v)
{
  return rdbSaveAuxField (rdb, k, sdslen (k), v, sdslen (v));
}


// returns 0 if successful, -1 otherwise
int
arc_rdb_save_aux_fields (rio * rdb)
{
  if (rdbSaveAuxFieldStrInt (rdb, shared.db_version->ptr, arc.smr_seqnum) ==
      -1)
    {
      return -1;
    }
  if (rdbSaveAuxFieldStrInt (rdb, shared.db_smr_mstime->ptr, arc.smr_ts)
      == -1)
    {
      return -1;
    }
  if (arc.migrate_slot
      && save_aux_field_sds (rdb, (sds) shared.db_migrate_slot->ptr,
			     arc.migrate_slot) == -1)
    {
      return -1;
    }
  if (arc.migclear_slot
      && save_aux_field_sds (rdb, (sds) shared.db_migclear_slot->ptr,
			     arc.migclear_slot) == -1)
    {
      return -1;
    }
  return 0;
}


// returns 1 if hooked, 0 otherwise.
// if hooked, reference counts for key and value are decreased
int
arc_rdb_load_aux_fields_hook (robj * auxkey, robj * auxval)
{
  if (!compareStringObjects (auxkey, shared.db_version))
    {
      getLongLongFromObject (auxval, &arc.smr_seqnum);
    }
  else if (!compareStringObjects (auxkey, shared.db_smr_mstime))
    {
      getLongLongFromObject (auxval, &arc.smr_ts);
    }
  else if (!compareStringObjects (auxkey, shared.db_migrate_slot))
    {
      if (arc.migrate_slot)
	{
	  sdsfree (arc.migrate_slot);
	}
      arc.migrate_slot = sdsdup (auxval->ptr);
    }
  else if (!compareStringObjects (auxkey, shared.db_migclear_slot))
    {
      if (arc.migclear_slot)
	{
	  sdsfree (arc.migclear_slot);
	}
      arc.migclear_slot = sdsdup (auxval->ptr);
    }
  else
    {
      return 0;
    }

  decrRefCount (auxkey);
  decrRefCount (auxval);
  return 1;
}

/* ----------------------- */
/* Exported Redis commands */
/* ----------------------- */
void
checkpointCommand (client * c)
{
  sds bitarray;

  serverLog (LL_NOTICE, "Client ask for checkpoint");

  /* Here we need to check if there is a background saving operation in progress */
  if (server.rdb_child_pid != -1 || server.aof_child_pid != -1)
    {
      serverLog (LL_NOTICE,
		 "Another background operation is in progress. Unable to perform CHECKPOINT.");
      addReplyError (c, "Unable to perform CHECKPOINT");
      return;
    }

  /* set bitarray from argument */
  bitarray = get_bitmap_from_argv (c->argc - 1, c->argv + 1);
  if (!bitarray)
    {
      addReplyError (c, "Invalid argument format.");
      return;
    }

  serverLog (LL_NOTICE, "Starting CHECKPOINT");
  arc.checkpoint_slots = bitarray;
  if (rdbSaveBackground (arc.checkpoint_filename) != C_OK)
    {
      serverLog (LL_NOTICE, "Replication failed, can't CHECKPOINT");
      addReplyError (c, "Unable to perform CHECKPOINT");
      sdsfree (bitarray);
      arc.checkpoint_slots = NULL;
      return;
    }
  arc.checkpoint_seqnum = arc.smr_seqnum;	//TODO 이건 여기가 아님.
  serverLog (LL_NOTICE, "Partial Checkpoint sequence num:%lld",
	     arc.smr_seqnum);
  c->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
  c->repldbfd = -1;
  arc.checkpoint_client = c;
  return;
}

void
migstartCommand (client * c)
{
  mig_start (c, c->argc, c->argv);
}

void
migendCommand (client * c)
{
  mig_end (c);
}

void
migconfCommand (client * c)
{

  if (!strcasecmp (c->argv[1]->ptr, "migstart"))
    {
      if (c->argc != 3)
	{
	  addReply (c, shared.syntaxerr);
	  return;
	}
      mig_start (c, c->argc - 1, c->argv + 1);
    }
  else if (!strcasecmp (c->argv[1]->ptr, "migend"))
    {
      if (c->argc != 2)
	{
	  addReply (c, shared.syntaxerr);
	  return;
	}
      mig_end (c);
    }
  else if (!strcasecmp (c->argv[1]->ptr, "clearstart"))
    {
      if (c->argc != 3)
	{
	  addReply (c, shared.syntaxerr);
	  return;
	}
      serverLog (LL_NOTICE, "Client ask for starting migclear.");
      if (arc.migrate_slot || arc.migclear_slot)
	{
	  addReplyError (c,
			 "Another migration job is in progress. Unable to perform CLEARSTART.");
	  return;
	}
      arc.migclear_slot = get_bitmap_from_argv (c->argc - 2, c->argv + 2);
      if (!arc.migclear_slot)
	{
	  addReplyError (c,
			 "Unable to make a bitarray. Invalid argument format or Out of memory.");
	  return;
	}
      serverLog (LL_NOTICE, "Starting migclear.");
      addReply (c, shared.ok);
    }
  else if (!strcasecmp (c->argv[1]->ptr, "clearend"))
    {
      if (c->argc != 2)
	{
	  addReply (c, shared.syntaxerr);
	  return;
	}
      serverLog (LL_NOTICE, "Client ask for finishing migclear.");
      if (!arc.migclear_slot)
	{
	  addReplyError (c,
			 "Migclear job is not in progress. Unable to perform CLEAREND.");
	  return;
	}
      serverLog (LL_NOTICE, "Finishing migclear.");
      sdsfree (arc.migclear_slot);
      arc.migclear_slot = NULL;
      addReply (c, shared.ok);
    }
  else if (!strcasecmp (c->argv[1]->ptr, "status"))
    {
      if (c->argc != 2)
	{
	  addReply (c, shared.syntaxerr);
	  return;
	}
      if (arc.migrate_slot)
	{
	  addReplyStatus (c, "MIGSTART");
	}
      else if (arc.migclear_slot)
	{
	  addReplyStatus (c, "CLEARSTART");
	}
      else
	{
	  addReplyStatus (c, "NOTINPROGRESS");
	}
    }
  else
    {
      addReply (c, shared.syntaxerr);
    }
}

void
migpexpireatCommand (client * c)
{
  dictEntry *de;
  robj *key = c->argv[1], *param = c->argv[2];
  long long when;

  if (getLongLongFromObjectOrReply (c, param, &when, NULL) != C_OK)
    return;

  de = dictFind (c->db->dict, key->ptr);
  if (de == NULL)
    {
      addReply (c, shared.czero);
      return;
    }
  setExpire (c->db, key, when);
  addReply (c, shared.cone);
  signalModifiedKey (c->db, key);
  server.dirty++;
}

#else
//make compiler happy
int arc_checkpoint_is_not_used = 1;
#endif

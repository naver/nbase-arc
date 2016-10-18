#ifdef NBASE_ARC
#include "arc_internal.h"

/* ------------------ */
/* Local declarations */
/* ------------------ */

/* --------------- */
/* Local functions */
/* --------------- */

/* ------------------ */
/* Exported functions */
/* ------------------ */

int
arc_rewrite_sss_object (rio * r, robj * key, robj * o)
{
  sssTypeIterator *si;
  sssEntry *se;

  si = arcx_sss_type_init_iterator (o);
  while ((se = arcx_sss_iter_next (si)) != NULL)
    {
      robj *ks, *svc, *skey, *val;
      long long idx, vll;
      int kv_mode;
      char *cmd;
      int cmdlen;

      RETURNIF (0, arcx_sss_iter_peek
		(se, &ks, &svc, &skey, &idx, &val, &vll, &kv_mode) == C_ERR);

      if (kv_mode == SSS_KV_LIST)
	{
	  cmd = "S3LADDAT";
	  cmdlen = 8;
	}
      else
	{
	  serverAssert (kv_mode == SSS_KV_SET);
	  cmd = "S3SADDAT";
	  cmdlen = 8;
	}

      RETURNIF (0, rioWriteBulkCount (r, '*', 7) == 0);
      RETURNIF (0, rioWriteBulkString (r, cmd, cmdlen) == 0);
      RETURNIF (0, rioWriteBulkObject (r, ks) == 0);
      RETURNIF (0, rioWriteBulkObject (r, key) == 0);
      RETURNIF (0, rioWriteBulkObject (r, svc) == 0);
      RETURNIF (0, rioWriteBulkObject (r, skey) == 0);
      /* idx is not used */
      RETURNIF (0, rioWriteBulkObject (r, val) == 0);
      RETURNIF (0, rioWriteBulkLongLong (r, vll) == 0);
    }

  arcx_sss_type_release_iterator (si);
  return 1;
}

int
arc_sss_type_value_count (robj * o)
{
  return arcx_sss_type_value_count (o);
}

void
arc_sss_compute_dataset_digest (robj * o, unsigned char *digest)
{
  sssTypeIterator *si;
  sssEntry *se;
  char buf[32];

  si = arcx_sss_type_init_iterator (o);
  while ((se = arcx_sss_iter_next (si)) != NULL)
    {
      unsigned char eledigest[20];
      robj *ks, *svc, *key, *val;
      long long idx, vll;
      int mode;

      memset (eledigest, 0, 20);
      serverAssert (arcx_sss_iter_peek
		    (se, &ks, &svc, &key, &idx, &val, &vll, &mode) == C_OK);
      mixObjectDigest (eledigest, ks);
      mixObjectDigest (eledigest, svc);
      mixObjectDigest (eledigest, key);
      snprintf (buf, sizeof (buf), "%lld", idx);
      mixDigest (eledigest, buf, strlen (buf));
      mixObjectDigest (eledigest, val);
      snprintf (buf, sizeof (buf), "%lld", vll);
      mixDigest (eledigest, buf, strlen (buf));
      xorDigest (digest, eledigest, 20);
    }
  arcx_sss_type_release_iterator (si);
}

robj *
arc_create_sss_object (robj * key)
{
  sss *s = arcx_sss_new (key);
  robj *o = createObject (OBJ_SSS, s);
  o->encoding = OBJ_ENCODING_SSS;
  return o;
}

void
arc_free_sss_object (robj * o)
{
  arcx_sss_release ((sss *) o->ptr);
}

int
arc_rdb_save_sss_type (rio * rdb, robj * o)
{
  if (o->encoding != OBJ_ENCODING_SSS)
    {
      serverPanic ("Unknown sss encoding");
    }
  return rdbSaveType (rdb, RDB_TYPE_SSS);
}

int
arc_rdb_save_sss_object (rio * rdb, robj * o)
{
  sssTypeIterator *si;
  sssEntry *se;
  int n, val_cnt, emit_cnt = 0, nwritten = 0;

  if (o->encoding != OBJ_ENCODING_SSS)
    {
      serverPanic ("Unknown sss encoding");
    }

  val_cnt = arcx_sss_type_value_count (o);
  RETURNIF (-1, (n = rdbSaveLen (rdb, val_cnt)) == -1);
  nwritten += n;

  si = arcx_sss_type_init_iterator (o);
  while ((se = arcx_sss_iter_next (si)) != NULL)
    {
      robj *ks, *s, *k, *v;
      long long idx, vll;
      int mode;

      RETURNIF (-1, arcx_sss_iter_peek
		(se, &ks, &s, &k, &idx, &v, &vll, &mode) == C_ERR);
      RETURNIF (-1, (n = rdbSaveStringObject (rdb, ks)) == -1);
      nwritten += n;
      RETURNIF (-1, (n = rdbSaveStringObject (rdb, s)) == -1);
      nwritten += n;
      RETURNIF (-1, (n = rdbSaveStringObject (rdb, k)) == -1);
      nwritten += n;
      RETURNIF (-1, (n = rdbSaveLongLongAsStringObject (rdb, idx)) == -1);
      nwritten += n;
      RETURNIF (-1, (n = rdbSaveStringObject (rdb, v)) == -1);
      nwritten += n;
      RETURNIF (-1, (n = rdbSaveLongLongAsStringObject (rdb, vll)) == -1);
      nwritten += n;

      emit_cnt++;
    }
  arcx_sss_type_release_iterator (si);
  serverAssert (val_cnt == emit_cnt);
  return 0;
}

robj *
arc_rdb_load_sss_object (rio * rdb)
{
  size_t len;
  int ret;
  robj *o, *key;

  len = rdbLoadLen (rdb, NULL);
  RETURNIF (NULL, len == RDB_LENERR);

  key = rdb->peeked_key;
  serverAssert (key != NULL && key->type == OBJ_SSS);

  o = arc_create_sss_object (key);
  while (len > 0)
    {
      robj *ks, *s, *k, *v, *idx, *t;
      long long idxll = 0LL, vll = 0LL;

      len--;
      /* load encoded strings */
      RETURNIF (NULL, (ks = rdbLoadStringObject (rdb)) == NULL);
      RETURNIF (NULL, (s = rdbLoadStringObject (rdb)) == NULL);
      RETURNIF (NULL, (k = rdbLoadStringObject (rdb)) == NULL);
      RETURNIF (NULL, (idx = rdbLoadEncodedStringObject (rdb)) == NULL);
      RETURNIF (NULL, (v = rdbLoadStringObject (rdb)) == NULL);
      RETURNIF (NULL, (t = rdbLoadEncodedStringObject (rdb)) == NULL);

      ret = isObjectRepresentableAsLongLong (idx, &idxll);
      serverAssert (ret == C_OK);
      ret = isObjectRepresentableAsLongLong (t, &vll);
      serverAssert (ret == C_OK);

      /* add pair to sss */
      ret = arcx_sss_add_value ((sss *) o->ptr, ks, s, k, idxll, v, vll);
      decrRefCount (ks);
      decrRefCount (s);
      decrRefCount (k);
      decrRefCount (v);
      decrRefCount (idx);
      decrRefCount (t);
      serverAssert (ret == C_OK);
    }

  serverAssert (len == 0);
  return o;
}

/* returns 0 */
int
arc_rio_peek_key (rio * rdb, int rdbtype, robj * key)
{
  if (rdbtype == RDB_TYPE_SSS)
    {
      rdb->peeked_key = key;
    }
  return 0;
}

#else
//make compiler happy
int arc_sss_is_not_used = 1;
#endif

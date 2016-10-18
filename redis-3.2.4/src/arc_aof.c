#ifdef NBASE_ARC
#include "arc_internal.h"


/* Emit the commands needed to rebuild a S3 object.
 * This function returns 0 on error, 1 on success */
int
rewriteSssObject (rio * r, robj * key, robj * o)
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

      RETURNIF (0,
		arcx_sss_iter_peek (se, &ks, &svc, &skey, &idx, &val, &vll,
				    &kv_mode) == C_ERR);
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
#else
//make compiler happy
int arc_aof_is_not_used = 1;
#endif

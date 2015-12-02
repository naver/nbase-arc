#include "redis.h"
#include "rbtree.h"
#include "crc16.h"
#include <stddef.h>
#include <assert.h>

#ifdef NBASE_ARC

/*-----------------------------------------------------------------------------
 * Flags
 *----------------------------------------------------------------------------*/
/* Category flags for iterate_values_with_hook function */
#define SSS_ITER_CATEGORY_SIZE 4
#define CATEGORY_UNMATCHED 0
#define CATEGORY_MATCHED 1
#define CATEGORY_DELETE 2
#define CATEGORY_EXPIRED 3

struct sssEntry
{
  rb_node_t node;		/* rb tree node */
  robj *ks;			/* key space */
  robj *svc;			/* service object */
  robj *key;			/* key object */
  robj *val;			/* val object */
  long long int index;		/* index for list semantic key/value mapping */
  long long int expire;		/* abs expiration time. LLONG_MAX means forever */
  struct sssEntry *next;	/* temporary link for iteration */
};

/* when robj is peeked, reference count is NOT incremented */
typedef struct rbContext_ rbContext;
struct rbContext_
{
  sssEntry *ext;
  robj *ks_peek;
  robj *svc_peek;
  robj *key_peek;
  robj *val_peek;
};

struct sss
{
  dlisth head;			/* must be the first member (gc link header) */
  sds key;			/* key of s3 object in redis DB */
  rb_tree_t tree;		/* red-black tree that contains sssEntry */
  int val_count;		/* value count */
  long long index;		/* index for list semantic key/value mapping */
};

struct sssTypeIterator
{
  int is_first;
  int count;
  /* singly linked tail queue implementation */
  sssEntry *first;
  sssEntry **last;
  /* current position in iteration */
  sssEntry *next;
};

/* iterator of unmatched category 	iter[CATEGORY_UNMATCHED]
 * iterator of matched category 	iter[CATEGORY_MATCHED]
 * iterator of expired category		iter[CATEGORY_EXPIRED]
 * iterator of deleted category		iter[CATEGORY_DELETE] */
typedef struct sssIterCategories sssIterCategories;
struct sssIterCategories
{
  sssTypeIterator iter[4];
};

struct add_arg
{
  robj *val;
  long long expire;
};

struct madd_arg
{
  robj *key;
  robj *val;
  long long expire;
};

struct s3_keys_cb_arg
{
  robj *svc;
  robj *prev_svc;
};

struct s3_count_cb_arg
{
  robj *svc;			// set by caller
  robj *key;			// set by caller
  robj *val;			// set by caller
  robj *prev_arg;
};

struct s3_exists_cb_arg
{
  robj *key;
  robj *val;
  int count;
};

struct s3_expire_cb_arg
{
  robj *svc;
  robj *key;
  robj *val;
  long long expire;
};

struct s3_ttl_cb_arg
{
  robj *key;
  robj *val;
  long long ttl;
  long long curr_time;
};

struct s3_rem_svc_cb_arg
{
  robj *prev_key;
  int count;
};

/*-----------------------------------------------------------------------------
 * Forward declarations
 *----------------------------------------------------------------------------*/
static int check_kv_type (redisClient * c, robj * s3obj);
static robj *create_sss_object (robj * key);
static void ttl2abs (long long *ttl, long long curr_time);
static void abs2ttl (long long *ttl, long long curr_time);
static sss *sss_new (robj * key);
static int obj_compare (robj * o1, robj * o2);
static signed int entry_compare_func (void *c, const void *te,
				      const void *ke);
static sssEntry *new_entry (robj * ks, robj * svc, robj * key, long long idx,
			    robj * val, long long int expire);
static void del_entry (sssEntry * e);
static sssTypeIterator *new_iterator (void);
static int entry_in_range (sssEntry * entry, sssEntry * ke);
static sssTypeIterator *get_iterator (robj * s3obj, robj * ks, robj * svc,
				      robj * key);
static int entry_index_compar (const void *d1, const void *d2);
static int iter_is_empty (sssTypeIterator * si);
static sssEntry *iter_next (sssTypeIterator * si);
static sssTypeIterator *iter_sort_by_index (sssTypeIterator * iter);
static void release_iterator (sssTypeIterator * iter);
static void
iterate_values_with_hook (robj * s3obj, robj * ks, robj * svc, robj * key,
			  long long curr_time, sssIterCategories * ctg,
			  void (*cb) (sssEntry *,
				      int
				      *category_flags,
				      void *harg), void *arg);
static int sss_add_value (sss * s3, void *ks, void *svc, void *key,
			  long long idx, void *val, long long expire,
			  int *replaced);
static void s3_get (redisClient * c, robj * s3obj);
static void s3_mget (redisClient * c, robj * s3obj);
static void s3_keys (redisClient * c, robj * s3obj);
static void s3_vals (redisClient * c, robj * s3obj);
static int s3_add_args (sss * s3, robj * ks, robj * svc, robj * key, int narg,
			struct add_arg *args);
static void s3_add (redisClient * c, robj * s3obj, int narg,
		    struct add_arg *args);
static void s3_madd (redisClient * c, robj * s3obj, int narg,
		     struct madd_arg *args);
static void s3_rem_cb (sssEntry * e, int *flag, void *arg);
static void s3_rem (redisClient * c, robj * s3obj);
static void s3_mrem (redisClient * c, robj * s3obj);
static void s3_set (redisClient * c, robj * s3obj, int narg,
		    struct add_arg *args);
static void s3_replace_cb (sssEntry * e, int *flag, void *arg);
static void s3_replace (redisClient * c, robj * s3obj, long long curr_time,
			long long expire);
static void s3_count_cb (sssEntry * e, int *flag, void *arg);
static void s3_count (redisClient * c, robj * s3obj);
static void s3_exists_cb (sssEntry * e, int *flag, void *arg);
static void s3_exists (redisClient * c, robj * s3obj);
static void s3_expire_cb (sssEntry * e, int *flag, void *arg);
static void s3_expire (redisClient * c, robj * s3obj, long long expire);
static void s3_mexpire (redisClient * c, robj * s3obj, long long expire);
static void s3_ttl_cb (sssEntry * e, int *flag, void *arg);
static void s3_ttl (redisClient * c, robj * s3obj);
static void s3_forcedel_cb (sssEntry * e, int *flag, void *arg);

static void s3get_generic (redisClient * c);
static void s3mget_generic (redisClient * c);
static void s3keys_generic (redisClient * c);
static void s3vals_generic (redisClient * c);
static void s3add_generic (redisClient * c);
static void s3madd_generic (redisClient * c);
static void s3mrem_generic (redisClient * c);
static void s3set_generic (redisClient * c);
static void s3replace_generic (redisClient * c);
static void s3count_generic (redisClient * c);
static void s3exists_generic (redisClient * c);
static void s3expire_generic (redisClient * c);
static void s3mexpire_generic (redisClient * c);
static void s3ttl_generic (redisClient * c);

/*-----------------------------------------------------------------------------
 * Static variables
 *----------------------------------------------------------------------------*/
static rbContext s_Ctx;
#define init_rb_ctx(e) do  {    \
    s_Ctx.ext = (e);              \
    s_Ctx.ks_peek = NULL;         \
    s_Ctx.svc_peek = NULL;        \
    s_Ctx.key_peek = NULL;        \
    s_Ctx.val_peek = NULL;        \
} while(0)

static rb_tree_ops_t s_Ops = {
  entry_compare_func,
  entry_compare_func,
  offsetof (sssEntry, node),
  &s_Ctx
};

static int s_Kv_mode = 0;

/*-----------------------------------------------------------------------------
 * Local function implementations
 *----------------------------------------------------------------------------*/

/* 
 * if type conflict w.r.t (ks, uuid, svc) reply error and returns 1. returns 0 if ok 
 */
static int
check_kv_type (redisClient * c, robj * s3obj)
{
  sss *s3 = s3obj->ptr;
  sssEntry *e, ke;

  ke.ks = c->argv[1];
  ke.svc = c->argv[3];
  ke.key = NULL;
  ke.index = 0LL;
  ke.val = NULL;

  e = rb_tree_find_node_geq (&s3->tree, &ke);
  if (e != NULL && entry_in_range (e, &ke))
    {
      int list_set_conflict = (e->index > 0 && s_Kv_mode == SSS_KV_SET);
      int set_list_conflict = (e->index == 0LL && s_Kv_mode == SSS_KV_LIST);

      if (list_set_conflict || set_list_conflict)
	{
	  addReply (c, shared.wrongtypeerr);
	  return 1;
	}
    }
  return 0;
}

static robj *
create_sss_object (robj * key)
{
  return createSssObject (key);
}

static void
ttl2abs (long long *ttl, long long curr_time)
{
  /* ttl == 0 means forever */
  if (*ttl == 0)
    {
      *ttl = LLONG_MAX;
    }
  else
    {
      *ttl = *ttl + curr_time;
      if (*ttl <= 0)
	{
	  /* overflows */
	  *ttl = LLONG_MAX;
	}
    }
}

static void
abs2ttl (long long *expire, long long curr_time)
{
  if (*expire == LLONG_MAX)
    {
      *expire = 0;
    }
  else
    {
      *expire = *expire - curr_time;
      if (*expire <= 0)
	{
	  *expire = 1;
	}
    }
}

static sss *
sss_new (robj * key)
{
  sss *s3 = zmalloc (sizeof (sss));
  int idx;

  /* allocate random number and assign it to the global gc line */
  idx = random () % server.gc_num_line;
  dlisth_init (&s3->head);
  dlisth_insert_after (&s3->head, &server.gc_line[idx]);
  s3->key = sdsdup (key->ptr);
  rb_tree_init (&s3->tree, &s_Ops);
  s3->val_count = 0;
  s3->index = 0LL;
  return s3;
}

static int
obj_compare (robj * o1, robj * o2)
{
  sds s1, s2;
  int l1, l2;
  int cmp;

  /* NULL consideration: 
   * NULL is considered to be minimum value */
  if (o1 == o2)
    {
      return 0;
    }
  else if (o1 == NULL)
    {
      return -1;
    }
  else if (o2 == NULL)
    {
      return 1;
    }

  /* Assertion: o1 != NULL, o2 != NULL */
  s1 = o1->ptr;
  s2 = o2->ptr;
  if (s1 == s2)
    {
      return 0;
    }
  l1 = sdslen (s1);
  l2 = sdslen (s2);
  cmp = memcmp (s1, s2, (l1 > l2) ? l2 : l1);
  if (cmp == 0)
    {
      if (l1 > l2)
	{
	  return 1;
	}
      else if (l1 == l2)
	{
	  return 0;
	}
      else
	{
	  return -1;
	}
    }
  return cmp;
}

/* 
 * rb_tree operation 
 * Note: e2 is guest object (see rbtree.c)
 */
static int
entry_compare_func (void *c, const void *te, const void *ke)
{
  rbContext *ctx = (rbContext *) c;
  sssEntry *e1 = (sssEntry *) te;
  sssEntry *e2 = (sssEntry *) ke;
  int can_peek = (ctx->ext == e2);
  int cmp, cmp2;

  cmp = obj_compare (e1->ks, e2->ks);
  if (cmp == 0)
    {
      if (can_peek)
	{
	  ctx->ks_peek = e1->ks;
	}

      cmp = obj_compare (e1->svc, e2->svc);
      if (cmp == 0)
	{
	  if (can_peek)
	    {
	      ctx->svc_peek = e1->svc;
	    }

	  cmp = obj_compare (e1->key, e2->key);
	  if (cmp == 0)
	    {
	      if (can_peek)
		{
		  ctx->key_peek = e1->key;
		}

	      /* 
	       * comparision order ks -> svc -> key -> index  -> val
	       * when set semantic index is a fixed value.
	       * when list semantic index is unique.
	       */
	      cmp2 =
		(e1->index ==
		 e2->index) ? 0 : ((e1->index > e2->index) ? 1 : -1);
	      cmp = obj_compare (e1->val, e2->val);
	      if (cmp == 0)
		{
		  if (can_peek)
		    {
//                    ctx->val_peek = e1->val;
		    }
		}
	      return cmp2 != 0 ? cmp2 : cmp;
	    }
	}
    }
  return cmp;
}

static sssEntry *
new_entry (robj * ks, robj * svc, robj * key, long long idx, robj * val,
	   long long int expire)
{
  sssEntry *e = zmalloc (sizeof (sssEntry));

  e->ks = NULL;
  e->svc = NULL;
  e->key = NULL;
  e->val = NULL;
  e->index = idx;
  e->expire = expire;
  e->next = NULL;
  if (ks)
    {
      e->ks = ks;
      incrRefCount (ks);
    }
  if (svc)
    {
      e->svc = svc;
      incrRefCount (svc);
    }
  if (key)
    {
      e->key = key;
      incrRefCount (key);
    }
  if (val)
    {
      e->val = val;
      incrRefCount (val);
    }
  return e;
}

static void
del_entry (sssEntry * e)
{
  if (e == NULL)
    {
      return;
    }

  if (e->ks)
    {
      decrRefCount (e->ks);
    }
  if (e->svc)
    {
      decrRefCount (e->svc);
    }
  if (e->key)
    {
      decrRefCount (e->key);
    }
  if (e->val)
    {
      decrRefCount (e->val);
    }
  zfree (e);
}

static void
init_iterator (sssTypeIterator * iter)
{
  redisAssert (iter != NULL);
  iter->is_first = 1;
  iter->count = 0;
  iter->first = NULL;
  iter->last = &iter->first;
  iter->next = NULL;
}

static sssTypeIterator *
new_iterator (void)
{
  sssTypeIterator *iter = zmalloc (sizeof (sssTypeIterator));

  init_iterator (iter);
  return iter;
}

static int
entry_in_range (sssEntry * entry, sssEntry * ke)
{
  /* ks */
  if (ke->ks == NULL)
    {
      return 1;
    }
  if (obj_compare (entry->ks, ke->ks) != 0)
    {
      return 0;
    }
  /* svc */
  if (ke->svc == NULL)
    {
      return 1;
    }
  if (obj_compare (entry->svc, ke->svc) != 0)
    {
      return 0;
    }
  /* key */
  if (ke->key == NULL)
    {
      return 1;
    }
  if (obj_compare (entry->key, ke->key) != 0)
    {
      return 0;
    }
  return 1;
}

static sssTypeIterator *
get_iterator (robj * s3obj, robj * ks, robj * svc, robj * key)
{
  sssTypeIterator *iter = new_iterator ();
  sss *s3 = s3obj->ptr;
  sssEntry *e, ke;

  ke.ks = ks;
  ke.svc = svc;
  ke.key = key;
  ke.index = 0LL;
  ke.val = NULL;

  e = rb_tree_find_node_geq (&s3->tree, &ke);
  while (e != NULL && entry_in_range (e, &ke))
    {
      sssEntry *ne;
      ne = rb_tree_iterate (&s3->tree, e, RB_DIR_RIGHT);

      /* insert tail */
      e->next = NULL;
      *iter->last = e;
      iter->last = &e->next;
      iter->count++;

      /* advance pointer */
      e = ne;
    }
  iter->next = iter->first;
  return iter;
}

static int
entry_index_compar (const void *d1, const void *d2)
{
  sssEntry *e1 = *(sssEntry **) d1;
  sssEntry *e2 = *(sssEntry **) d2;
  return (e1->index > e2->index) ? 1 : (e1->index == e2->index ? 0 : -1);
}

static int
iter_is_empty (sssTypeIterator * si)
{
  return si->next == NULL;
}

static sssEntry *
iter_next (sssTypeIterator * si)
{
  sssEntry *curr = si->next;

  if (curr != NULL)
    {
      si->next = curr->next;
    }

  return curr;
}

static sssTypeIterator *
iter_sort_by_index (sssTypeIterator * iter)
{
  sssEntry **ary;
  sssEntry *se;
  int i = 0, count;

  if ((count = iter->count) <= 1)
    {
      return iter;
    }

  ary = zmalloc (sizeof (sssEntry *) * count);
  while ((se = sssIterNext (iter)) != NULL)
    {
      ary[i++] = se;
    }
  redisAssert (i == count);

  /* quick sort */
  qsort (ary, count, sizeof (sssEntry *), entry_index_compar);

  /* init iterator */
  init_iterator (iter);

  /* add values */
  for (i = 0; i < count; i++)
    {
      sssEntry *e = ary[i];

      e->next = NULL;
      *iter->last = e;
      iter->last = &e->next;
      iter->count++;
    }
  iter->next = iter->first;

  zfree (ary);
  return iter;
}

static void
release_iterator (sssTypeIterator * iter)
{
  /* unused links are harmless (it is set to NULL when new iterator is filled) */
  zfree (iter);
}

/* This function iterates sssEntry through the range defined by arguments and 
 * executes following jobs during iteration.
 * 1. cleanup expired values
 * 2. categorize matched and unmatched entry which is decided by callback function 
 *    and returns head entry of categorized elements.
 *    <Caution> after excuting get_iterator function, categorized lists are reset
 * 3. delete values decided by callback function */
static void
iterate_values_with_hook (robj * s3obj, robj * ks, robj * svc, robj * key,
			  long long curr_time, sssIterCategories * ctg,
			  void (*cb) (sssEntry *,
				      int
				      *category_flags, void *harg), void *arg)
{
  sssTypeIterator *iter;
  sssEntry *se;
  sss *s3 = s3obj->ptr;
  int i;

  for (i = 0; i < SSS_ITER_CATEGORY_SIZE; i++)
    {
      init_iterator (&ctg->iter[i]);
    }

  iter = get_iterator (s3obj, ks, svc, key);
  while ((se = sssIterNext (iter)) != NULL)
    {
      int flag;
      sssTypeIterator *ctg_iter;

      flag = (se->expire > curr_time) ? CATEGORY_UNMATCHED : CATEGORY_EXPIRED;

      if (flag != CATEGORY_EXPIRED && cb != NULL)
	{
	  cb (se, &flag, arg);
	}

      ctg_iter = &ctg->iter[flag];
      se->next = NULL;
      *ctg_iter->last = se;
      ctg_iter->last = &se->next;
      ctg_iter->count++;
    }

  for (i = 0; i < SSS_ITER_CATEGORY_SIZE; i++)
    {
      ctg->iter[i].next = ctg->iter[i].first;
    }
  sssTypeReleaseIterator (iter);

  /* Actually delete expired sssEntry */
  iter = &ctg->iter[CATEGORY_EXPIRED];
  while ((se = sssIterNext (iter)) != NULL)
    {
      rb_tree_remove_node (&s3->tree, se);
      s3->val_count--;
      redisAssert (s3->val_count >= 0);
      del_entry (se);
    }

  /* Actually delete deleted sssEntry */
  iter = &ctg->iter[CATEGORY_DELETE];
  while ((se = sssIterNext (iter)) != NULL)
    {
      rb_tree_remove_node (&s3->tree, se);
      s3->val_count--;
      redisAssert (s3->val_count >= 0);
      del_entry (se);
    }
}

static int
sss_add_value (sss * s3, void *ks, void *svc, void *key, long long idx,
	       void *val, long long expire, int *replaced)
{
  robj *ksobj = (robj *) ks, *sobj = (robj *) svc, *kobj =
    (robj *) key, *vobj = (robj *) val;
  sssEntry *e, *te;

  /* update s3->index: needed to recover index from rdb load */
  if (idx > s3->index)
    {
      s3->index = idx;
    }

  e = new_entry (ksobj, sobj, kobj, idx, vobj, expire);
  init_rb_ctx (e);
  te = rb_tree_insert_node (&s3->tree, e);
  if (te != e)
    {
      /* just update expire */
      te->expire = expire;
      del_entry (e);
      init_rb_ctx (NULL);
      *replaced = 1;
      return REDIS_OK;
    }
  else
    {
      s3->val_count++;
      /* properly added. we can make use of peeked item for memory saving */
      if (s_Ctx.ks_peek != NULL)
	{
	  decrRefCount (e->ks);
	  e->ks = s_Ctx.ks_peek;
	  incrRefCount (e->ks);
	}
      if (s_Ctx.svc_peek != NULL)
	{
	  decrRefCount (e->svc);
	  e->svc = s_Ctx.svc_peek;
	  incrRefCount (e->svc);
	}
      if (s_Ctx.key_peek != NULL)
	{
	  decrRefCount (e->key);
	  e->key = s_Ctx.key_peek;
	  incrRefCount (e->key);
	}
      if (s_Ctx.val_peek != NULL)
	{
	  decrRefCount (e->val);
	  e->val = s_Ctx.val_peek;
	  incrRefCount (e->val);
	}
      init_rb_ctx (NULL);
      *replaced = 0;
      return REDIS_OK;
    }
}

/*-----------------------------------------------------------------------------
 * Local function generic for list and set key-value semantic
 *----------------------------------------------------------------------------*/
static void
s3_get (redisClient * c, robj * s3obj)
{
  robj *ks, *svc, *key;
  sssIterCategories ctg;
  sssTypeIterator *iter;
  long long curr_time = smr_mstime ();

  ks = c->argv[1];
  //uuid = c->argv[2];
  svc = c->argv[3];
  key = c->argv[4];

  iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg, NULL, NULL);

  iter = &ctg.iter[CATEGORY_UNMATCHED];
  if (iter->count > 0)
    {
      sssEntry *se;

      addReplyMultiBulkLen (c, iter->count);
      while ((se = sssIterNext (iter)) != NULL)
	{
	  addReplyBulk (c, se->val);
	  iter->count--;
	}
    }
  else
    {
      addReply (c, shared.emptymultibulk);
    }
  redisAssert (iter->count == 0);
}

static void
s3_mget (redisClient * c, robj * s3obj)
{
  robj *ks, *svc;
  int key_count;
  int i;
  int tot_count = 0;
  long long curr_time = smr_mstime ();
  sssTypeIterator *iter;

  ks = c->argv[1];
  //uuid = c->argv[2];
  svc = c->argv[3];

  /* get multi bulk len to return */
  key_count = c->argc - 4;
  if (key_count == 0)
    {
      sssIterCategories ctg;
      sssEntry *se;

      iterate_values_with_hook (s3obj, ks, svc, NULL, curr_time,
				&ctg, NULL, NULL);
      iter = &ctg.iter[CATEGORY_UNMATCHED];
      tot_count += iter->count;
      addReplyMultiBulkLen (c, tot_count * 2);
      if (tot_count > 0)
	{
	  while ((se = sssIterNext (iter)) != NULL)
	    {
	      addReplyBulk (c, se->key);
	      addReplyBulk (c, se->val);
	      tot_count--;
	    }
	}
    }
  else
    {
      sssIterCategories ctg[key_count];
      sssTypeIterator *iter;
      robj *key;

      for (i = 0; i < key_count; i++)
	{
	  key = c->argv[i + 4];
	  iterate_values_with_hook (s3obj, ks, svc, key, curr_time,
				    &ctg[i], NULL, NULL);
	  iter = &ctg[i].iter[CATEGORY_UNMATCHED];
	  tot_count += iter->count;
	  if (iter->count == 0)
	    {
	      //key nill
	      tot_count++;
	    }
	}

      addReplyMultiBulkLen (c, tot_count * 2);

      for (i = 0; i < key_count; i++)
	{
	  key = c->argv[i + 4];
	  iter = &ctg[i].iter[CATEGORY_UNMATCHED];
	  if (iter_is_empty (iter))
	    {
	      addReplyBulk (c, key);
	      addReply (c, shared.nullbulk);
	      tot_count--;
	    }
	  else
	    {
	      sssEntry *se;
	      while ((se = sssIterNext (iter)) != NULL)
		{
		  addReplyBulk (c, key);
		  addReplyBulk (c, se->val);
		  tot_count--;
		}
	    }
	}
    }
  redisAssert (tot_count == 0);
}

static void
s3_keys_cb (sssEntry * e, int *flag, void *arg)
{
  struct s3_keys_cb_arg *cbarg = (struct s3_keys_cb_arg *) arg;

  if (cbarg->svc == NULL)
    {
      /* Categorize svc entries, if svc field is omitted in s3keys command */
      if (obj_compare (cbarg->prev_svc, e->svc) != 0)
	{
	  /* check if this svc is same kv_mode */
	  if ((s_Kv_mode & SSS_KV_SET && e->index == 0LL) ||
	      (s_Kv_mode & SSS_KV_LIST && e->index > 0))
	    {
	      cbarg->prev_svc = e->svc;
	      *flag = CATEGORY_MATCHED;
	    }
	}
    }
  else
    {
      /* Categorize key entries, if svc field is filled in s3keys command */
      *flag = CATEGORY_MATCHED;
    }
}

static void
s3_keys (redisClient * c, robj * s3obj)
{
  robj *ks, *svc = NULL;
  sssIterCategories ctg;
  sssTypeIterator *iter;
  long long curr_time = smr_mstime ();
  struct s3_keys_cb_arg arg;

  ks = c->argv[1];
  //uuid = c->argv[2];

  if (c->argc == 4)
    {
      svc = c->argv[3];
    }

  arg.svc = svc;
  arg.prev_svc = NULL;
  iterate_values_with_hook (s3obj, ks, svc, NULL, curr_time, &ctg,
			    s3_keys_cb, &arg);
  iter = &ctg.iter[CATEGORY_MATCHED];
  if (iter->count > 0)
    {
      sssEntry *se;

      addReplyMultiBulkLen (c, iter->count);
      if (c->argc == 4 && s_Kv_mode == SSS_KV_LIST)
	{
	  iter = iter_sort_by_index (iter);
	}
      while ((se = sssIterNext (iter)) != NULL)
	{
	  if (c->argc == 4)
	    {
	      addReplyBulk (c, se->key);
	    }
	  else
	    {
	      addReplyBulk (c, se->svc);
	    }
	  iter->count--;
	}
    }
  else
    {
      addReply (c, shared.emptymultibulk);
    }
  redisAssert (iter->count == 0);
}

static void
s3_vals (redisClient * c, robj * s3obj)
{
  robj *ks, *svc;
  sssIterCategories ctg;
  sssTypeIterator *iter;
  long long curr_time = smr_mstime ();

  ks = c->argv[1];
  //uuid = c->argv[2];
  svc = c->argv[3];

  iterate_values_with_hook (s3obj, ks, svc, NULL, curr_time, &ctg,
			    NULL, NULL);
  iter = &ctg.iter[CATEGORY_UNMATCHED];
  if (iter->count > 0)
    {
      sssEntry *se;

      addReplyMultiBulkLen (c, iter->count);
      if (s_Kv_mode == SSS_KV_LIST)
	{
	  iter = iter_sort_by_index (iter);
	}
      while ((se = sssIterNext (iter)) != NULL)
	{
	  addReplyBulk (c, se->val);
	  iter->count--;
	}
    }
  else
    {
      addReply (c, shared.emptymultibulk);
    }
  redisAssert (iter->count == 0);
}

/* returns number of keys actually added to the target list/set */
static int
s3_add_args (sss * s3, robj * ks, robj * svc, robj * key, int narg,
	     struct add_arg *args)
{
  int i;
  int nreplaced = 0;

  for (i = 0; i < narg; i++)
    {
      long long idx;
      int ret;
      int replaced = 0;

      if (s_Kv_mode == SSS_KV_LIST)
	{
	  idx = ++s3->index;
	}
      else
	{
	  idx = 0;
	}
      ret =
	sss_add_value (s3, ks, svc, key, idx, args[i].val, args[i].expire,
		       &replaced);
      redisAssert (ret == REDIS_OK);
      nreplaced += replaced;
    }
  return narg - nreplaced;
}

static void
s3_add (redisClient * c, robj * s3obj, int narg, struct add_arg *args)
{
  int actually_added = 0;
  robj *ks, *uuid, *svc, *key;
  sss *s3 = (sss *) s3obj->ptr;

  ks = c->argv[1];
  uuid = c->argv[2];
  svc = c->argv[3];
  key = c->argv[4];

  actually_added = s3_add_args (s3, ks, svc, key, narg, args);
  if (narg > 0)
    {
      server.dirty += narg;
      signalModifiedKey (c->db, uuid);
    }
  addReplyLongLong (c, actually_added);
}

static void
s3_madd (redisClient * c, robj * s3obj, int narg, struct madd_arg *args)
{
  int i;
  robj *ks, *uuid, *svc;
  sss *s3 = (sss *) s3obj->ptr;
  int tot_replaced = 0;
  int replaced = 0;

  ks = c->argv[1];
  uuid = c->argv[2];
  svc = c->argv[3];

  for (i = 0; i < narg; i++)
    {
      long long idx;
      int ret;

      if (s_Kv_mode == SSS_KV_LIST)
	{
	  idx = ++s3->index;
	}
      else
	{
	  idx = 0;
	}

      ret =
	sss_add_value ((sss *) s3obj->ptr, ks, svc, args[i].key, idx,
		       args[i].val, args[i].expire, &replaced);
      redisAssert (ret == REDIS_OK);
      tot_replaced += replaced;
    }

  if (narg > 0)
    {
      server.dirty += narg;
      signalModifiedKey (c->db, uuid);
    }
  addReplyLongLong (c, narg - tot_replaced);
}

static void
s3_rem_svc_cb (sssEntry * e, int *flag, void *arg)
{
  struct s3_rem_svc_cb_arg *cbarg = (struct s3_rem_svc_cb_arg *) arg;
  if (obj_compare (cbarg->prev_key, e->key) != 0)
    {
      cbarg->count++;
      cbarg->prev_key = e->key;
    }
  *flag = CATEGORY_DELETE;
}

static void
s3_rem_cb (sssEntry * e, int *flag, void *arg)
{
  int i;
  redisClient *c = (redisClient *) arg;

  for (i = 5; i < c->argc; i++)
    {
      if (obj_compare (e->val, c->argv[i]) == 0)
	{
	  *flag = CATEGORY_DELETE;
	}
    }
}

static void
s3_rem (redisClient * c, robj * s3obj)
{
  robj *ks, *uuid, *svc = NULL, *key = NULL;
  sssIterCategories ctg;
  int ndelete;
  long long curr_time = smr_mstime ();

  ks = c->argv[1];
  uuid = c->argv[2];

  if (c->argc >= 4)
    {
      svc = c->argv[3];
    }

  if (c->argc >= 5)
    {
      key = c->argv[4];
    }

  if (c->argc == 4)
    {
      struct s3_rem_svc_cb_arg arg;
      arg.prev_key = NULL;
      arg.count = 0;

      iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
				s3_rem_svc_cb, &arg);
      ndelete = arg.count;
    }
  else if (c->argc == 5)
    {
      iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
				s3_forcedel_cb, NULL);
      ndelete = ctg.iter[CATEGORY_DELETE].count;
    }
  else
    {
      iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
				s3_rem_cb, c);
      ndelete = ctg.iter[CATEGORY_DELETE].count;
    }

  if (ndelete)
    {
      server.dirty += ndelete;
      signalModifiedKey (c->db, uuid);
    }
  addReplyLongLong (c, ndelete);
  return;
}

static void
s3_mrem (redisClient * c, robj * s3obj)
{
  robj *ks, *uuid, *svc = NULL;
  int i, tot_deleted = 0;
  sssIterCategories ctg;
  long long curr_time = smr_mstime ();

  ks = c->argv[1];
  uuid = c->argv[2];
  svc = c->argv[3];

  for (i = 4; i < c->argc; i++)
    {
      robj *key = c->argv[i];

      iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
				s3_forcedel_cb, NULL);
      tot_deleted += ctg.iter[CATEGORY_DELETE].count;
    }

  if (tot_deleted)
    {
      server.dirty += tot_deleted;
      signalModifiedKey (c->db, uuid);
    }
  addReplyLongLong (c, tot_deleted);
  return;
}

static void
s3_forcedel_cb (sssEntry * e, int *flag, void *arg)
{
  *flag = CATEGORY_DELETE;
}

static void
s3_set (redisClient * c, robj * s3obj, int narg, struct add_arg *args)
{
  int added = 0, nexpired, ndeleted;
  sssIterCategories ctg;
  robj *ks, *uuid, *svc, *key;
  long long curr_time = smr_mstime ();
  sss *s3 = (sss *) s3obj->ptr;

  ks = c->argv[1];
  uuid = c->argv[2];
  svc = c->argv[3];
  key = c->argv[4];

  iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
			    s3_forcedel_cb, NULL);
  nexpired = ctg.iter[CATEGORY_EXPIRED].count;
  ndeleted = ctg.iter[CATEGORY_DELETE].count;
  added = s3_add_args (s3, ks, svc, key, narg, args);
  if (added + ndeleted + nexpired > 0)
    {
      server.dirty += added + ndeleted + nexpired;
      signalModifiedKey (c->db, uuid);
    }

  // returns 1 if new key is created, 0 otherwise 
  addReplyLongLong (c, ndeleted > 0 ? 0 : 1);
}

static void
s3_replace_cb (sssEntry * e, int *flag, void *arg)
{
  robj *oldval = (robj *) arg;
  if (obj_compare (e->val, oldval) == 0)
    {
      *flag = CATEGORY_DELETE;
    }
}

static void
s3_replace (redisClient * c, robj * s3obj, long long curr_time,
	    long long expire)
{
  robj *ks, *uuid, *svc, *key, *oldval, *newval;
  sss *s3 = (sss *) s3obj->ptr;
  sssIterCategories ctg;
  int nmodified;

  ks = c->argv[1];
  uuid = c->argv[2];
  svc = c->argv[3];
  key = c->argv[4];
  oldval = c->argv[5];
  newval = c->argv[6];

  iterate_values_with_hook (s3obj, ks, svc, key, curr_time,
			    &ctg, s3_replace_cb, oldval);
  nmodified = ctg.iter[CATEGORY_DELETE].count;
  if (nmodified > 0)
    {
      long long idx;

      if (s_Kv_mode == SSS_KV_LIST)
	{
	  idx = ++s3->index;
	}
      else
	{
	  idx = 0LL;
	}
      sssAddValue (s3, ks, svc, key, idx, newval, expire);
      addReply (c, shared.cone);
    }
  else
    {
      addReply (c, shared.czero);
    }

  if (nmodified > 0)
    {
      server.dirty += nmodified;
      signalModifiedKey (c->db, uuid);
    }
}

static void
s3_count_cb (sssEntry * e, int *flag, void *arg)
{
  struct s3_count_cb_arg *cbarg = (struct s3_count_cb_arg *) arg;

  if (cbarg->svc == NULL)
    {
      /* svc is omitted */
      if (obj_compare (cbarg->prev_arg, e->svc) != 0)
	{
	  /* check if this svc is same kv_mode */
	  if ((s_Kv_mode & SSS_KV_SET && e->index == 0LL) ||
	      (s_Kv_mode & SSS_KV_LIST && e->index > 0))
	    {
	      cbarg->prev_arg = e->svc;
	      *flag = CATEGORY_MATCHED;
	    }
	}
    }
  else if (cbarg->key == NULL)
    {
      /* key is omitted */
      if (obj_compare (cbarg->prev_arg, e->key) != 0)
	{
	  cbarg->prev_arg = e->key;
	  *flag = CATEGORY_MATCHED;
	}
    }
  else if (cbarg->val == NULL)
    {
      /* value is omitted */
      *flag = CATEGORY_MATCHED;
    }
  else
    {
      if (obj_compare (cbarg->val, e->val) == 0)
	{
	  *flag = CATEGORY_MATCHED;
	}
    }
}

static void
s3_count (redisClient * c, robj * s3obj)
{
  robj *ks, *svc = NULL, *key = NULL, *val = NULL;
  long long curr_time = smr_mstime ();
  sssIterCategories ctg;
  struct s3_count_cb_arg arg;

  ks = c->argv[1];
  //uuid = c->argv[2];
  if (c->argc >= 4)
    {
      svc = c->argv[3];
    }
  if (c->argc >= 5)
    {
      key = c->argv[4];
    }
  if (c->argc == 6)
    {
      val = c->argv[5];
    }

  arg.svc = svc;
  arg.key = key;
  arg.val = val;
  arg.prev_arg = NULL;
  iterate_values_with_hook (s3obj, ks, svc, key, curr_time,
			    &ctg, s3_count_cb, &arg);
  addReplyLongLong (c, ctg.iter[CATEGORY_MATCHED].count);
}

static void
s3_exists_cb (sssEntry * e, int *flag, void *arg)
{
  struct s3_exists_cb_arg *cbarg = (struct s3_exists_cb_arg *) arg;

  if (cbarg->count > 0)
    {
      /* already checked */
      return;
    }

  if (obj_compare (cbarg->key, e->key) == 0)
    {
      if (cbarg->val == NULL)
	{
	  cbarg->count = 1;
	  return;
	}
      if (obj_compare (cbarg->val, e->val) == 0)
	{
	  cbarg->count = 1;
	  return;
	}
    }
}

static void
s3_exists (redisClient * c, robj * s3obj)
{
  robj *ks, *svc, *key, *val = NULL;
  long long curr_time = smr_mstime ();
  sssIterCategories ctg;
  struct s3_exists_cb_arg arg;

  ks = c->argv[1];
  //uuid = c->argv[2];
  svc = c->argv[3];
  key = c->argv[4];
  if (c->argc == 6)
    {
      val = c->argv[5];
    }

  arg.key = key;
  arg.val = val;
  arg.count = 0;
  iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
			    s3_exists_cb, &arg);
  addReplyLongLong (c, arg.count > 0 ? 1 : 0);
}

static void
s3_expire_cb (sssEntry * e, int *flag, void *arg)
{
  struct s3_expire_cb_arg *cbarg = (struct s3_expire_cb_arg *) arg;

  if (cbarg->svc == NULL)
    {
      /* check if this svc is same kv_mode */
      if ((s_Kv_mode & SSS_KV_SET && e->index == 0LL) ||
	  (s_Kv_mode & SSS_KV_LIST && e->index > 0))
	{
	  e->expire = cbarg->expire;
	  *flag = CATEGORY_MATCHED;
	}
    }
  else if (cbarg->key == NULL)
    {
      if (obj_compare (cbarg->svc, e->svc) == 0)
	{
	  e->expire = cbarg->expire;
	  *flag = CATEGORY_MATCHED;
	}
    }
  else if (cbarg->val == NULL)
    {
      if (obj_compare (cbarg->key, e->key) == 0)
	{
	  e->expire = cbarg->expire;
	  *flag = CATEGORY_MATCHED;
	}
    }
  else
    {
      if (obj_compare (cbarg->key, e->key) == 0
	  && obj_compare (cbarg->val, e->val) == 0)
	{
	  e->expire = cbarg->expire;
	  *flag = CATEGORY_MATCHED;
	}
    }
}

static void
s3_expire (redisClient * c, robj * s3obj, long long expire)
{
  robj *ks, *uuid, *svc = NULL, *key = NULL, *val = NULL;
  long long curr_time = smr_mstime ();
  sssIterCategories ctg;
  int count;
  struct s3_expire_cb_arg arg;

  ks = c->argv[1];
  uuid = c->argv[2];
  //ttl = c->argv[3];
  if (c->argc >= 5)
    {
      svc = c->argv[4];
    }
  if (c->argc >= 6)
    {
      key = c->argv[5];
    }
  if (c->argc >= 7)
    {
      val = c->argv[6];
    }

  arg.svc = svc;
  arg.key = key;
  arg.val = val;
  arg.expire = expire;
  iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
			    s3_expire_cb, &arg);
  count = ctg.iter[CATEGORY_MATCHED].count;
  addReplyLongLong (c, count > 0 ? 1 : 0);
  if (count > 0)
    {
      server.dirty += count;
      signalModifiedKey (c->db, uuid);
    }
}

static void
s3_mexpire (redisClient * c, robj * s3obj, long long expire)
{
  robj *ks, *uuid, *svc, *key;
  long long curr_time = smr_mstime ();
  int key_count, nreplaced = 0;
  int i;
  sssIterCategories ctg;
  struct s3_expire_cb_arg arg;

  ks = c->argv[1];
  uuid = c->argv[2];
  svc = c->argv[3];
  //ttl = c->argv[4];

  key_count = c->argc - 5;
  for (i = 0; i < key_count; i++)
    {
      key = c->argv[i + 5];
      arg.svc = svc;
      arg.key = key;
      arg.val = NULL;
      arg.expire = expire;
      iterate_values_with_hook (s3obj, ks, svc, key, curr_time,
				&ctg, s3_expire_cb, &arg);
      nreplaced += ctg.iter[CATEGORY_MATCHED].count;
    }

  addReplyLongLong (c, nreplaced > 0 ? 1 : 0);
  if (nreplaced > 0)
    {
      server.dirty += nreplaced;
      signalModifiedKey (c->db, uuid);
    }
}

static void
s3_ttl_cb (sssEntry * e, int *flag, void *arg)
{
  struct s3_ttl_cb_arg *cbarg = (struct s3_ttl_cb_arg *) arg;

  if (cbarg->ttl != -1)
    {
      return;
    }

  if (cbarg->val == NULL)
    {
      if (obj_compare (cbarg->key, e->key) == 0)
	{
	  cbarg->ttl = e->expire;
	  abs2ttl (&cbarg->ttl, cbarg->curr_time);
	}
    }
  else
    {
      if (obj_compare (cbarg->key, e->key) == 0
	  && obj_compare (cbarg->val, e->val) == 0)
	{
	  cbarg->ttl = e->expire;
	  abs2ttl (&cbarg->ttl, cbarg->curr_time);
	}
    }
}

static void
s3_ttl (redisClient * c, robj * s3obj)
{
  robj *ks, *svc, *key = NULL, *val = NULL;
  sssIterCategories ctg;
  long long curr_time = smr_mstime ();
  struct s3_ttl_cb_arg arg;

  ks = c->argv[1];
  //uuid = c->argv[2];
  svc = c->argv[3];
  key = c->argv[4];
  if (c->argc >= 6)
    {
      val = c->argv[5];
    }

  arg.key = key;
  arg.val = val;
  arg.ttl = -1LL;
  arg.curr_time = curr_time;
  iterate_values_with_hook (s3obj, ks, svc, key, curr_time, &ctg,
			    s3_ttl_cb, &arg);
  addReplyLongLong (c, arg.ttl);
}

/*
 * S3LGET ks uuid svc key
 * returns value [value ...]
 */
static void
s3get_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.emptymultibulk);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }
  s3_get (c, s3obj);
}

/*
 * S3LMGET ks uuid svc [key [key...]]
 * returns [key value] [key nill] ...
 */
static void
s3mget_generic (redisClient * c)
{
  robj *s3obj, *uuid;
  int i;

  uuid = c->argv[2];
  s3obj = lookupKeyRead (c->db, uuid);
  if (s3obj == NULL)
    {
      //return [key nill]...
      addReplyMultiBulkLen (c, (c->argc - 4) * 2);
      for (i = 4; i < c->argc; i++)
	{
	  addReplyBulk (c, c->argv[i]);
	  addReply (c, shared.nullbulk);
	}
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_mget (c, s3obj);
}

/*
 * S3LKEYS ks uuid svc
 */
static void
s3keys_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  if (c->argc > 4)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.emptymultibulk);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS)
      || (c->argc > 3 && check_kv_type (c, s3obj)))
    {
      return;
    }

  s3_keys (c, s3obj);
}

/*
 * S3LVALS ks uuid svc
 */
static void
s3vals_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.emptymultibulk);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_vals (c, s3obj);
}

/*
 * S3LADD ks uuid svc key value ttl [value ttl...]
 * returns number of values added
 */
static void
s3add_generic (redisClient * c)
{
  robj *s3obj, *uuid;
  struct add_arg *args;
  int narg, i, ret;
  long long curr_time = smr_mstime ();

  if ((c->argc - 7) % 2 != 0)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  narg = (c->argc - 5) / 2;
  args = zmalloc (narg * sizeof (struct add_arg));
  for (i = 0; i < narg; i++)
    {
      args[i].val = c->argv[5 + i * 2];
      ret = getLongLongFromObject (c->argv[5 + i * 2 + 1], &args[i].expire);
      if (ret != REDIS_OK)
	{
	  addReplyError (c, "ttl value is not a integer or out of range");
	  zfree (args);
	  return;
	}
      ttl2abs (&args[i].expire, curr_time);
    }

  uuid = c->argv[2];
  s3obj = lookupKeyWrite (c->db, uuid);
  if (s3obj == NULL)
    {
      s3obj = create_sss_object (uuid);
      dbAdd (c->db, uuid, s3obj);
    }
  else if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      zfree (args);
      return;
    }

  s3_add (c, s3obj, narg, args);
  zfree (args);
}

/*
 * S3LADDAT ks uuid svc key value timestamp [value timestamp...] 
 * (timestamp in milliseconds)
 *
 * returns number of values added
 */
static void
s3addat_generic (redisClient * c)
{
  robj *s3obj, *uuid;
  struct add_arg *args;
  int narg, i, ret;

  if ((c->argc - 7) % 2 != 0)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  narg = (c->argc - 5) / 2;
  args = zmalloc (narg * sizeof (struct add_arg));
  for (i = 0; i < narg; i++)
    {
      args[i].val = c->argv[5 + i * 2];
      ret = getLongLongFromObject (c->argv[5 + i * 2 + 1], &args[i].expire);
      if (ret != REDIS_OK)
	{
	  addReplyError (c, "expire value is not a integer or out of range");
	  zfree (args);
	  return;
	}
    }

  uuid = c->argv[2];
  s3obj = lookupKeyWrite (c->db, uuid);
  if (s3obj == NULL)
    {
      s3obj = create_sss_object (uuid);
      dbAdd (c->db, uuid, s3obj);
    }
  else if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      zfree (args);
      return;
    }

  s3_add (c, s3obj, narg, args);
  zfree (args);
}

/*
 * S3LMADD ks uuid svc key value ttl [key value ttl...]
 * returns number of values added
 */
static void
s3madd_generic (redisClient * c)
{
  robj *s3obj, *uuid;
  struct madd_arg *args;
  int narg, i, ret;
  long long curr_time = smr_mstime ();

  if ((c->argc - 7) % 3 != 0)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  narg = (c->argc - 4) / 3;
  args = zmalloc (narg * sizeof (struct madd_arg));
  for (i = 0; i < narg; i++)
    {
      args[i].key = c->argv[4 + i * 3];
      args[i].val = c->argv[4 + i * 3 + 1];
      ret = getLongLongFromObject (c->argv[4 + i * 3 + 2], &args[i].expire);
      if (ret != REDIS_OK)
	{
	  addReplyError (c, "ttl value is not a integer or out of range");
	  zfree (args);
	  return;
	}
      ttl2abs (&args[i].expire, curr_time);
    }

  uuid = c->argv[2];
  s3obj = lookupKeyWrite (c->db, uuid);
  if (s3obj == NULL)
    {
      s3obj = create_sss_object (uuid);
      dbAdd (c->db, uuid, s3obj);
    }
  else if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      zfree (args);
      return;
    }

  s3_madd (c, s3obj, narg, args);
  zfree (args);
}

/*
 * S3LREM ks uuid [svc [key [value...]]]
 * returns number of values removed
 */
static void
s3rem_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_rem (c, s3obj);
}

/*
 * S3LMREM ks uuid svc key [key...]
 * returns number of values removed
 */
static void
s3mrem_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_mrem (c, s3obj);
}

/*
 * S3LSET ks uuid svc key value ttl [value ttl...]
 * returns 1 if new key was added, 0 otherwise
 */
static void
s3set_generic (redisClient * c)
{
  robj *s3obj, *uuid;
  struct add_arg *args;
  int narg, i, ret;
  long long curr_time = smr_mstime ();

  if ((c->argc - 7) % 2 != 0)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  narg = (c->argc - 5) / 2;
  args = zmalloc (narg * sizeof (struct add_arg));
  for (i = 0; i < narg; i++)
    {
      args[i].val = c->argv[5 + i * 2];
      ret = getLongLongFromObject (c->argv[5 + i * 2 + 1], &args[i].expire);
      if (ret != REDIS_OK)
	{
	  addReplyError (c, "ttl value is not a integer or out of range");
	  zfree (args);
	  return;
	}
      ttl2abs (&args[i].expire, curr_time);
    }

  uuid = c->argv[2];
  s3obj = lookupKeyWrite (c->db, uuid);
  if (s3obj == NULL)
    {
      s3obj = create_sss_object (uuid);
      dbAdd (c->db, uuid, s3obj);
    }
  else if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      zfree (args);
      return;
    }

  s3_set (c, s3obj, narg, args);
  zfree (args);
}

/*
 * S3LREPLACE ks uuid svc key oldvalue newvalue ttl
 * returns 0 if there is no such value, 1 if there is value and replaced
 */
static void
s3replace_generic (redisClient * c)
{
  robj *s3obj, *uuid, *ttl;
  long long expire;
  long long curr_time = smr_mstime ();
  int ret;

  ttl = c->argv[7];
  ret = getLongLongFromObject (ttl, &expire);
  if (ret != REDIS_OK)
    {
      addReplyError (c, "ttl value is not a integer or out of range");
      return;
    }
  ttl2abs (&expire, curr_time);

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  else if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_replace (c, s3obj, curr_time, expire);
}

/* 
 * S3LCOUNT ks uuid svc [key]
 * returns the number of entity (keys, values) each of (svc, svc key) respectively
 */
static void
s3count_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  if (c->argc > 6)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS)
      || (c->argc > 3 && check_kv_type (c, s3obj)))
    {
      return;
    }

  s3_count (c, s3obj);
}

/*
 * S3LEXISTS ks uuid svc key [value]
 * returns 1 if there is entry specified (key, key value) respectively, 0 otherewise
 */
static void
s3exists_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  if (c->argc > 6)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_exists (c, s3obj);
}

/*
 * S3LEXPIRE ks uuid svc ttl [key [value]]
 * set expiration time of entries
 * returns 1 if at least one entry is modified, 0 otherwise
 */
static void
s3expire_generic (redisClient * c)
{
  robj *s3obj, *uuid, *ttl;
  long long expire;
  long long curr_time = smr_mstime ();
  int ret;

  if (c->argc < 4 || c->argc > 7)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }
  ttl = c->argv[3];
  ret = getLongLongFromObject (ttl, &expire);
  if (ret != REDIS_OK)
    {
      addReplyError (c, "ttl value is not a integer or out of range");
      return;
    }
  ttl2abs (&expire, curr_time);

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS)
      || (c->argc > 4 && check_kv_type (c, s3obj)))
    {
      return;
    }

  s3_expire (c, s3obj, expire);
}


/*
 * S3LMEXPIRE ks uuid svc ttl key [key ...]
 * set expiration time of entries
 * returns 1 if at least one entry is modified, 0 otherwise
 */
static void
s3mexpire_generic (redisClient * c)
{
  robj *s3obj, *uuid, *ttl;
  long long expire;
  long long curr_time = smr_mstime ();
  int ret;

  ttl = c->argv[4];
  ret = getLongLongFromObject (ttl, &expire);
  if (ret != REDIS_OK)
    {
      addReplyError (c, "ttl value is not a integer or out of range");
      return;
    }
  ttl2abs (&expire, curr_time);

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_mexpire (c, s3obj, expire);
}

/* 
 * S3LTTL ks uuid svc key [value]
 * returns TTL of first entry that matches, -l otherwise
 */
static void
s3ttl_generic (redisClient * c)
{
  robj *s3obj, *uuid;

  if (c->argc < 5 || c->argc > 6)
    {
      addReplyErrorFormat (c, "wrong number of arguments for '%s' command",
			   c->cmd->name);
      return;
    }

  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.cnegone);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS) || check_kv_type (c, s3obj))
    {
      return;
    }

  s3_ttl (c, s3obj);
}

/*-----------------------------------------------------------------------------
 * Exported part (inter module)
 *----------------------------------------------------------------------------*/
sss *
sssNew (robj * key)
{
  return sss_new (key);
}

void
sssRelease (sss * s)
{
  sssEntry *e;

  dlisth_delete (&s->head);

  sdsfree (s->key);

  /* For each (left -> right) */
  e = rb_tree_iterate (&s->tree, NULL, RB_DIR_LEFT);
  while (e != NULL)
    {
      sssEntry *ne;

      ne = rb_tree_iterate (&s->tree, e, RB_DIR_RIGHT);
      rb_tree_remove_node (&s->tree, e);
      del_entry (e);
      e = ne;
    }
  zfree (s);
}

int
sssAddValue (sss * s3, void *ks, void *svc, void *key, long long idx,
	     void *val, long long expire)
{
  int dummy = 0;
  return sss_add_value (s3, ks, svc, key, idx, val, expire, &dummy);
}

int
sssTypeValueCount (robj * o)
{
  sss *s3 = o->ptr;
  return s3->val_count;
}

sssTypeIterator *
sssTypeInitIterator (robj * subject)
{
  return get_iterator (subject, NULL, NULL, NULL);
}


/* returns 1 if next item exists, 0 if no more item available */
sssEntry *
sssIterNext (sssTypeIterator * si)
{
  return iter_next (si);
}

int
sssIterPeek (sssEntry * e, robj ** ks, robj ** svc, robj ** key,
	     long long *idx, robj ** val, long long int *expire, int *kv_mode)
{
  *ks = e->ks;
  *svc = e->svc;
  *key = e->key;
  *idx = e->index;
  *val = e->val;
  *expire = e->expire;
  *kv_mode = e->index > 0 ? SSS_KV_LIST : SSS_KV_SET;
  return REDIS_OK;
}

void
sssTypeReleaseIterator (sssTypeIterator * si)
{
  release_iterator (si);
}

long long
sssGarbageCollect (long long timeout)
{
  int idx;
  long long curr_time;
  long long curr_server_time;
  long long tot_dead = 0;
  robj s3obj, key;

  curr_time = smr_mstime ();
  curr_server_time = mstime ();
  idx = 0;
  do
    {
      dlisth *h, *head;

      head = &server.gc_line[(server.gc_idx + idx) % server.gc_num_line];
      for (h = head->next; h != head; h = h->next)
	{
	  sss *s3 = (sss *) h;
	  sssIterCategories ctg;

	  if (server.migrate_slot)
	    {
	      int hashsize = NBASE_ARC_KS_SIZE;
	      int keyhash = crc16 (s3->key, sdslen (s3->key), 0) % hashsize;
	      if (rdbGetBit (server.migrate_slot, keyhash))
		{
                  /* Skipping keys on migration */
		  continue;
		}
	    }
	  /* below line is a hack to use cleanup_values_with_hook */
	  s3obj.ptr = s3;
	  iterate_values_with_hook (&s3obj, NULL, NULL, NULL, curr_time,
				    &ctg, NULL, NULL);
	  tot_dead += ctg.iter[CATEGORY_EXPIRED].count;

	  if (s3->val_count == 0)
	    {
	      h = h->prev;
	      initStaticStringObject (key, s3->key);
	      dbDelete (&server.db[REDIS_CLUSTER_DB], &key);
	    }
	}
    }
  while (++idx < server.gc_num_line
	 && (curr_server_time + timeout) > mstime ());

  server.gc_idx = (server.gc_idx + idx) % server.gc_num_line;
  return tot_dead;
}

/*-----------------------------------------------------------------------------
 * List S3 commands
 *----------------------------------------------------------------------------*/
void
s3lgetCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3get_generic (c);
}

void
s3lmgetCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3mget_generic (c);
}

void
s3lkeysCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3keys_generic (c);
}

void
s3lvalsCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3vals_generic (c);
}

void
s3laddCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3add_generic (c);
}

void
s3laddatCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3addat_generic (c);
}

void
s3lmaddCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3madd_generic (c);
}

void
s3lremCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3rem_generic (c);
}

void
s3lmremCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3mrem_generic (c);
}

void
s3lsetCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3set_generic (c);
}

void
s3lreplaceCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3replace_generic (c);
}

void
s3lcountCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3count_generic (c);
}

void
s3lexistsCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3exists_generic (c);
}

void
s3lexpireCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3expire_generic (c);
}

void
s3lmexpireCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3mexpire_generic (c);
}

void
s3lttlCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST;
  s3ttl_generic (c);
}

void
s3sgetCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3get_generic (c);
}

void
s3smgetCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3mget_generic (c);
}

void
s3skeysCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3keys_generic (c);
}

void
s3svalsCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3vals_generic (c);
}

void
s3saddCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3add_generic (c);
}

void
s3saddatCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3addat_generic (c);
}

void
s3smaddCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3madd_generic (c);
}

void
s3sremCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3rem_generic (c);
}

void
s3smremCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3mrem_generic (c);
}

void
s3ssetCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3set_generic (c);
}

void
s3sreplaceCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3replace_generic (c);
}

void
s3scountCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3count_generic (c);
}

void
s3sexistsCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3exists_generic (c);
}

void
s3sexpireCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3expire_generic (c);
}

void
s3smexpireCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3mexpire_generic (c);
}

void
s3sttlCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_SET;
  s3ttl_generic (c);
}

void
s3keysCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST | SSS_KV_SET;
  s3keys_generic (c);
}

void
s3countCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST | SSS_KV_SET;
  s3count_generic (c);
}

void
s3expireCommand (redisClient * c)
{
  s_Kv_mode = SSS_KV_LIST | SSS_KV_SET;
  s3expire_generic (c);
}

void
s3remCommand (redisClient * c)
{
  robj *s3obj, *ks;
  int uuid_count, tot_deleted = 0;
  long long curr_time = smr_mstime ();
  robj **s3objs = NULL;
  int i;

  ks = c->argv[1];
  uuid_count = c->argc - 2;
  s3objs = zmalloc (uuid_count * sizeof (robj *));

  //check uuid
  for (i = 0; i < uuid_count; i++)
    {
      robj *uuid = c->argv[i + 2];

      s3obj = lookupKeyRead (c->db, uuid);
      s3objs[i] = s3obj;
      if (s3obj == NULL)
	{
	  continue;
	}
      if (checkType (c, s3obj, REDIS_SSS))
	{
	  zfree (s3objs);
	  return;
	}
    }

  for (i = 0; i < uuid_count; i++)
    {
      sssIterCategories ctg;

      if (s3objs[i] == NULL)
	{
	  continue;
	}
      iterate_values_with_hook (s3objs[i], ks, NULL, NULL, curr_time,
				&ctg, s3_forcedel_cb, NULL);
      tot_deleted += (ctg.iter[CATEGORY_DELETE].count > 0 ? 1 : 0);
    }
  zfree (s3objs);
  addReplyLongLong (c, tot_deleted);
}

void
s3mremCommand (redisClient * c)
{
  robj *s3obj, *ks, *uuid;
  int svc_count, tot_deleted = 0;
  long long curr_time = smr_mstime ();
  int i;

  ks = c->argv[1];
  uuid = c->argv[2];
  s3obj = lookupKeyReadOrReply (c, uuid, shared.czero);
  if (s3obj == NULL)
    {
      return;
    }
  if (checkType (c, s3obj, REDIS_SSS))
    {
      return;
    }

  svc_count = c->argc - 3;
  for (i = 0; i < svc_count; i++)
    {
      robj *svc;
      sssIterCategories ctg;

      svc = c->argv[i + 3];
      iterate_values_with_hook (s3obj, ks, svc, NULL, curr_time,
				&ctg, s3_forcedel_cb, NULL);
      tot_deleted += (ctg.iter[CATEGORY_DELETE].count > 0 ? 1 : 0);
    }
  addReplyLongLong (c, tot_deleted);
}

void
s3gcCommand (redisClient * c)
{
  long long timeout;
  long long tot_dead;

  if (getLongLongFromObjectOrReply (c, c->argv[1], &timeout, NULL) !=
      REDIS_OK)
    {
      return;
    }

  tot_dead = sssGarbageCollect (timeout);
  addReplyLongLong (c, tot_dead);
  return;
}
#endif /* NBASE_ARC */

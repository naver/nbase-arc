
#ifdef NBASE_ARC

#include "arc_internal.h"
#include "bio.h"
#include <pthread.h>

/* ------------------ */
/* Local declarations */
/* ------------------ */
static int is_bgdel_obj (robj * o);
static void free_redis_object (robj * o);

/* -------------- */
/* Local variable */
/* -------------- */
static volatile int bgdel_bio_mode = 0;
static pthread_spinlock_t bgdel_spin;
static pthread_key_t bgdel_thr_key;

/* --------------- */
/* Local functions */
/* --------------- */
static int
is_bgdel_obj (robj * o)
{
  long long items = 0;
  if (arc.object_bio_delete_min_elems > 0)
    {
      switch (o->type)
	{
	case OBJ_STRING:
	  break;
	case OBJ_LIST:
	  items = listTypeLength (o);
	  break;
	case OBJ_SET:
	  items = setTypeSize (o);
	  break;
	case OBJ_ZSET:
	  items = zsetLength (o);
	  break;
	case OBJ_HASH:
	  items = hashTypeLength (o);
	  break;
	case OBJ_SSS:
	  items = arc_sss_type_value_count (o);
	  break;
	default:
	  serverPanic ("Unknown object type");
	  break;
	}
      return items >= arc.object_bio_delete_min_elems;
    }
  return 0;
}

static void
free_redis_object (robj * o)
{
  switch (o->type)
    {
    case OBJ_STRING:
      freeStringObject (o);
      break;
    case OBJ_LIST:
      freeListObject (o);
      break;
    case OBJ_SET:
      freeSetObject (o);
      break;
    case OBJ_ZSET:
      freeZsetObject (o);
      break;
    case OBJ_HASH:
      freeHashObject (o);
      break;
    case OBJ_SSS:
      arc_free_sss_object (o);
      break;
    default:
      serverPanic ("Unknown object type");
      break;
    }
  zfree (o);
}

/* ----------------------- */
/* Exported arcx functions */
/* ----------------------- */
int
arcx_can_bgdel (void)
{
  // getspecific == NULL means that it is not background thread!
  return bgdel_bio_mode && pthread_getspecific (bgdel_thr_key) == NULL;
}

/* ---------------------- */
/* Exported arc functions */
/* ---------------------- */
void
arc_disable_bgdel (void)
{
  bgdel_bio_mode = 0;
}

void
arc_enable_bgdel (void)
{
  bgdel_bio_mode = 1;
  pthread_setspecific (bgdel_thr_key, (void *) (long) 1);
}

void
arc_init_bgdel (void)
{
  char *errmsg = NULL;

  if (pthread_spin_init (&bgdel_spin, PTHREAD_PROCESS_PRIVATE) != 0)
    {
      errmsg = "Fatal: Can't Initialize spinlock.";
      goto error;
    }
  if (pthread_key_create (&bgdel_thr_key, NULL) != 0)
    {
      errmsg = "Fatal: Can't Initialize TSD.";
      goto error;
    }
  return;

error:
  serverLog (LL_WARNING, "%s", errmsg);
  exit (1);
}

void
arc_incr_ref_count (robj * o)
{
  if (bgdel_bio_mode)
    {
      pthread_spin_lock (&bgdel_spin);
      o->refcount++;
      pthread_spin_unlock (&bgdel_spin);
    }
  else
    {
      o->refcount++;
    }
}

void
arc_decr_ref_count (robj * o)
{
  int refcount;

  if (bgdel_bio_mode)
    {
      pthread_spin_lock (&bgdel_spin);
      if (o->refcount <= 0)
	{
	  serverPanic ("decrRefCount against refcount <= 0");
	}
      refcount = --o->refcount;
      pthread_spin_unlock (&bgdel_spin);
    }
  else
    {
      refcount = --o->refcount;
    }

  if (refcount == 0)
    {
      if (o->encoding == OBJ_ENCODING_SSS)
	{
	  arcx_sss_unlink_gc ((sss *) o->ptr);
	}

      if (arcx_can_bgdel () && is_bgdel_obj (o))
	{
	  arc_create_bgdel_job (BIO_BGDEL, (void *) o, BIO_BGDEL_ROBJ);
	  return;
	}
      else
	{
	  free_redis_object (o);
	}
    }
}

void
arc_bgdel (void *arg1, void *arg2)
{
  long deltype = (long) arg2;

  if (deltype == BIO_BGDEL_ROBJ)
    {
      free_redis_object ((robj *) arg1);
      arc.stat_bgdel_keys++;
    }
  else if (deltype == BIO_BGDEL_S3ENTRY)
    {
      arcx_sss_del_entries (arg1);
    }
  else
    {
      serverPanic ("Wrong deltype in arc_bgdel().");
    }
}

#endif

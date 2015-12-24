/*
 * Copyright 2015 Naver Corp.
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

#ifdef SFI_ENABLED
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>

#include "rbtree.h"
#include "sfi.h"

typedef struct
{
  rb_node_t node;		/* rb tree node */
  char *probe_name;		/* probe name */
  sfi_callback callback;
  sfi_dtor dtor;
  void *arg;
} sfiEntry;

/* ------------------- */
/* FORWARD DECLARATION */
/* ------------------- */
static int entry_compare_func (void *c, const void *te, const void *ke);
static void del_entry (sfiEntry * e);

/* --------------- */
/* STATIC VARIABLE */
/* --------------- */
static rb_tree_t *s_tree_p = NULL;
static rb_tree_t s_tree_s;
static rb_tree_ops_t s_tree_ops = {
  entry_compare_func,
  entry_compare_func,
  offsetof (sfiEntry, node),
  NULL
};

static pthread_rwlock_t s_tree_rwlock = PTHREAD_RWLOCK_INITIALIZER;
#define sfi_rlock() do { pthread_rwlock_rdlock(&s_tree_rwlock); } while (0)
#define sfi_runlock() do { pthread_rwlock_unlock(&s_tree_rwlock); } while (0)
#define sfi_wlock() do { pthread_rwlock_wrlock(&s_tree_rwlock); } while (0)
#define sfi_wunlock() do { pthread_rwlock_unlock(&s_tree_rwlock); } while (0)

/* ------------------------- */
/* LOCAL FUNCTION DEFINITION */
/* ------------------------- */
static int
entry_compare_func (void *c, const void *te, const void *ke)
{
  sfiEntry *e1 = (sfiEntry *) te;
  sfiEntry *e2 = (sfiEntry *) ke;

  return strcasecmp (e1->probe_name, e2->probe_name);
}

static void
del_entry (sfiEntry * e)
{
  if (e)
    {
      if (e->probe_name)
	{
	  free (e->probe_name);
	}
      free (e);
    }
}

/* ------------------ */
/* EXPORTED FUNCTIONS */
/* ------------------ */
int
sfi_enabled (void)
{
  return 1;
}

#define CHECK_INIT() if(s_tree_p == NULL) return
void
sfi_init (void)
{
  sfi_wlock ();
  if (s_tree_p == NULL)
    {
      rb_tree_init (&s_tree_s, &s_tree_ops);
      s_tree_p = &s_tree_s;
    }
  sfi_wunlock ();
}

void
sfi_term (void)
{
  CHECK_INIT ();
  sfi_disable_all ();
}

void
sfi_enable (const char *name, sfi_callback callback, sfi_dtor dtor, void *arg)
{
  sfiEntry *e;
  sfiEntry ke;
  char *probe_name;

  CHECK_INIT ();
  ke.probe_name = (char *) name;

  sfi_wlock ();
  e = rb_tree_find_node (s_tree_p, &ke);
  if (e != NULL)
    {
      /* inplace update */
      if (e->dtor != NULL)
	{
	  e->dtor (e->arg);
	}
      e->callback = callback;
      e->dtor = dtor;
      e->arg = arg;
      goto done;
    }
  else
    {
      e = malloc (sizeof (sfiEntry));
      assert (e != NULL);
      if (e == NULL)
	{
	  goto done;
	}
      probe_name = strdup (name);
      assert (probe_name != NULL);
      if (probe_name == NULL)
	{
	  free (e);
	  goto done;
	}
      e->probe_name = probe_name;
      e->callback = callback;
      e->dtor = dtor;
      e->arg = arg;
      rb_tree_insert_node (s_tree_p, e);
      goto done;
    }
done:
  sfi_wunlock ();
}

void
sfi_disable (const char *name)
{
  sfiEntry *e;
  sfiEntry ke;

  CHECK_INIT ();
  ke.probe_name = (char *) name;

  sfi_wlock ();
  e = rb_tree_find_node (s_tree_p, &ke);
  if (e != NULL)
    {
      if (e->dtor != NULL)
	{
	  e->dtor (e->arg);
	}
      rb_tree_remove_node (s_tree_p, e);
      del_entry (e);
    }
  sfi_wunlock ();
}

void
sfi_disable_all (void)
{
  sfiEntry *e;

  CHECK_INIT ();
  sfi_wlock ();
  e = rb_tree_iterate (s_tree_p, NULL, RB_DIR_LEFT);
  while (e != NULL)
    {
      sfiEntry *ne;

      ne = rb_tree_iterate (s_tree_p, e, RB_DIR_RIGHT);
      if (e->dtor != NULL)
	{
	  e->dtor (e->arg);
	}
      rb_tree_remove_node (s_tree_p, e);
      del_entry (e);
      e = ne;
    }
  sfi_wunlock ();
}

void
sfi_probe (const char *name, sfi_probe_arg * arg)
{
  sfiEntry *e;
  sfiEntry ke;

  CHECK_INIT ();
  ke.probe_name = (char *) name;

  sfi_rlock ();
  e = rb_tree_find_node (s_tree_p, &ke);
  if (e != NULL && e->callback != NULL)
    {
      SFI_CB_RET cb_ret;
      int rm_probe = 0;
      cb_ret = e->callback (name, arg, e->arg);

      switch (cb_ret)
	{
	case CB_ERROR:
	case CB_OK_DONE:
	  rm_probe = 1;
	  break;
	case CB_OK_HAS_MORE:
	  break;
	default:
	  assert (0);
	}

      if (rm_probe)
	{
	  if (e->dtor != NULL)
	    {
	      e->dtor (e->arg);
	    }
	  rb_tree_remove_node (s_tree_p, e);
	  del_entry (e);
	}
    }
  sfi_runlock ();
}

int
sfi_probe_count (void)
{
  sfiEntry *e = NULL;
  int count = 0;

  if (s_tree_p == NULL)
    {
      return 0;
    }

  sfi_rlock ();
  e = rb_tree_iterate (s_tree_p, NULL, RB_DIR_LEFT);
  while (e != NULL)
    {
      count++;
      e = rb_tree_iterate (s_tree_p, e, RB_DIR_RIGHT);
    }
  sfi_runlock ();

  return count;
}
#else /* SFI_ENABLED */
int
sfi_enabled (void)
{
  return 0;
}
#endif

#include <stddef.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>

#include "rbtree.h"
#include "ipacl.h"

typedef struct
{
  ipacl_color_t def;
  rb_tree_t rb;
} aclImpl;

typedef struct
{
  rb_node_t node;
  char *ip;
  ipacl_color_t color;
  long long expire;
} aclNode;


static int node_cmp (void *c, const void *te, const void *ke);

/* Static variable */
static rb_tree_ops_t s_tree_ops = {
  node_cmp,
  node_cmp,
  offsetof (aclNode, node),
};

/* --------------- */
/* Local functions */
/* --------------- */
static int
node_cmp (void *c, const void *te, const void *ke)
{
  aclNode *n1 = (aclNode *) te, *n2 = (aclNode *) ke;
  return strcmp (n1->ip, n2->ip);
}

/* -------- */
/* Exported */
/* -------- */
ipacl_t *
ipacl_new (ipacl_color_t def)
{
  aclImpl *ipacl;

  ipacl = calloc (1, sizeof (*ipacl));
  if (ipacl == NULL)
    {
      return NULL;
    }
  ipacl->def = def;
  rb_tree_init (&ipacl->rb, &s_tree_ops);
  return ipacl;
}

void
ipacl_destroy (ipacl_t * acl)
{
  ipacl_reap (acl, LLONG_MAX);
  free (acl);
}

// returns 1 (added), 0 (updated), -1 (error)
int
ipacl_set (ipacl_t * acl, char *ip, ipacl_color_t color, long long expire)
{
  aclImpl *impl = (aclImpl *) acl;
  aclNode *node, ke;
  char *ipdup;

  ke.ip = ip;
  node = rb_tree_find_node (&impl->rb, &ke);
  if (node != NULL)
    {
      node->color = color;
      node->expire = expire;
      return 0;
    }

  node = malloc (sizeof (*node));
  ipdup = strdup (ip);
  if (node == NULL || ipdup == NULL)
    {
      free (node);
      free (ipdup);
      return -1;
    }
  node->ip = ipdup;
  node->color = color;
  node->expire = expire;
  rb_tree_insert_node (&impl->rb, node);
  return 1;
}

ipacl_color_t
ipacl_check (ipacl_t * acl, char *ip, long long curr)
{
  aclImpl *impl = (aclImpl *) acl;
  aclNode *node, ke;

  ke.ip = ip;
  node = rb_tree_find_node (&impl->rb, &ke);
  if (node != NULL && node->expire > curr)
    {
      return node->color;
    }
  return impl->def;
}

// returns the number of nodes reaped
int
ipacl_reap (ipacl_t * acl, long long curr)
{
  aclImpl *impl = (aclImpl *) acl;
  aclNode *node;
  int cnt = 0;

  node = rb_tree_iterate (&impl->rb, NULL, RB_DIR_LEFT);
  while (node != NULL)
    {
      aclNode *ne = rb_tree_iterate (&impl->rb, node, RB_DIR_RIGHT);
      if (node->expire <= curr)
	{
	  rb_tree_remove_node (&impl->rb, node);
	  free (node->ip);
	  free (node);
	  cnt++;
	}
      node = ne;
    }
  return cnt;
}

// returns number of node iterated
int
ipacl_iterate (ipacl_t * acl, ipacl_itorf itorf, void *ctx)
{
  aclImpl *impl = (aclImpl *) acl;
  aclNode *node;
  int cnt = 0;

  RB_TREE_FOREACH (node, &impl->rb)
  {
    if (itorf != NULL)
      {
	int cont = itorf (ctx, node->ip, node->color, node->expire);
	if (cont == 0)
	  {
	    return cnt;
	  }
      }
    cnt++;
  }
  return cnt;
}

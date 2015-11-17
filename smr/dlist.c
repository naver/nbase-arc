#include <stdlib.h>
#include <assert.h>

#include "dlist.h"

int
dlisth_map (dlisth * h, dlist_map_func func, void *arg)
{
  dlisth *tmp;
  int cont;
  assert (h != NULL);
  assert (func != NULL);
  cont = 0;
  for (tmp = h->next; tmp != h; tmp = tmp->next)
    {
      int r = func (tmp, arg, &cont);
      if (r != 0)
	return r;
      else if (cont == 0)
	break;
    }
  return 0;
}

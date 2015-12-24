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

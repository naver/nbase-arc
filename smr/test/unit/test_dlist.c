#include <assert.h>
#include "dlist.h"

typedef struct
{
  dlisth head;
  int ival;
} testStruct;

static int
add_func (dlisth * h, void *arg, int *cont)
{
  int *sum = (int *) arg;
  testStruct *st = (testStruct *) h;

  *sum += st->ival;
  if (st->ival == 0)
    {
      *cont = 0;
    }
  else
    {
      *cont = 1;
    }

  return 0;
}

int
main (int argc, char *argv[])
{
  testStruct a, b, c;
  dlisth head;
  int ret;
  int sum = 0;

  dlisth_init (&head);

  dlisth_init (&a.head);
  dlisth_init (&b.head);
  dlisth_init (&c.head);
  a.ival = 10;
  b.ival = 20;
  c.ival = 0;

  dlisth_insert_before (&a.head, &head);
  dlisth_insert_before (&b.head, &head);
  dlisth_insert_before (&c.head, &head);

  ret = dlisth_map (&head, add_func, &sum);
  assert (ret == 0);
  assert (sum == 30);

  return 0;
}

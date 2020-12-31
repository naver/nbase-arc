#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "ipacl.h"


static int
count_itorf (void *ctx, char *ip, ipacl_color_t color, long long expire)
{
  int *count = (int *) ctx;
  *count = *count + 1;
  return 1;
}

static int
countdown_itorf (void *ctx, char *ip, ipacl_color_t color, long long expire)
{
  int *count = (int *) ctx;
  *count = *count - 1;
  if (*count <= 0)
    {
      return 0;
    }
  return 1;
}

int
main (int argc, char *argv[])
{
  ipacl_t *acl;
  int i, cnt;
  ipacl_color_t color;

  // new
  acl = ipacl_new (IPACL_WHITE);

  // set
  for (i = 0; i < 1000; i++)
    {
      char ip[64];
      sprintf (ip, "10.10.%d.%d", i / 256, i % 256);
      cnt = ipacl_set (acl, ip, IPACL_BLACK, (long long) i);
      assert (cnt == 1);
      cnt = ipacl_set (acl, ip, IPACL_BLACK, (long long) i);
      assert (cnt == 0);
    }

  // check
  // - exist
  color = ipacl_check (acl, "10.10.1.1", 0);
  assert (color == IPACL_BLACK);
  // - exist but expired
  color = ipacl_check (acl, "10.10.1.1", 1000);
  assert (color == IPACL_WHITE);
  // - non exist
  color = ipacl_check (acl, "1.1.1.1", 0);
  assert (color == IPACL_WHITE);

  // reap
  cnt = ipacl_reap (acl, 500);
  assert (cnt == 501); //0..500

  // iterate
  do
    {
      int count = 0;
      cnt = ipacl_iterate (acl, count_itorf, &count);
      assert (cnt == count);
      count = 100;
      cnt = ipacl_iterate (acl, countdown_itorf, &count);
      assert (cnt == 99 && count == 0); // decrement and then check
    }
  while (0);

  // destroy
  ipacl_destroy (acl);

  return 0;
}

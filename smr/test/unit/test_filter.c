#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "part_filter.h"

int
main (int argc, char *argv[])
{
  int i;
  int ret;
  partFilter *filter;
  char buf[8192];
  char small_buf[4];
  char *rle;

  filter = create_part_filter (0);
  assert (filter == NULL);

  filter = create_part_filter (-1);
  assert (filter == NULL);

  filter = create_part_filter (8192);
  assert (filter != NULL);
  assert (part_filter_get_num_part (filter) == 8192);
  assert (part_filter_get_num_part (NULL) == -1);

  destroy_part_filter (filter);

  filter = create_part_filter (8192);
  assert (filter != NULL);

  /* basic get/set */
  for (i = 0; i < 8192; i++)
    {
      ret = part_filter_get (filter, i);
      assert (ret == 0);
      ret = part_filter_set (filter, i, 1);
      assert (ret == 0);
      ret = part_filter_get (filter, i);
      assert (ret == 1);
      ret = part_filter_set (filter, i, 0);
      assert (ret == 0);
      ret = part_filter_get (filter, i);
      assert (ret == 0);
    }
  ret = part_filter_get (filter, -1);
  assert (ret == -1);
  ret = part_filter_get (filter, 8192);
  assert (ret == -1);
  ret = part_filter_set (filter, -1, 0);
  assert (ret == -1);
  ret = part_filter_set (filter, 8192, 0);
  assert (ret == -1);

  /* rle tests */
  ret = part_filter_set_rle (filter, "0 8192");
  assert (ret == 0);
  for (i = 0; i < 8192; i++)
    {
      ret = part_filter_get (filter, i);
      assert (ret == 0);
    }
  ret = part_filter_format_rle (filter, buf, sizeof (buf));
  assert (ret == 0);
  assert (strcmp (buf, "0 8192") == 0);

  /* rle test: permits count larger than part number */
  ret = part_filter_set_rle (filter, "1 9000");
  assert (ret == 0);
  for (i = 0; i < 8192; i++)
    {
      ret = part_filter_get (filter, i);
      assert (ret == 1);
    }
  ret = part_filter_format_rle (filter, buf, sizeof (buf));
  assert (ret == 0);
  assert (strcmp (buf, "1 8192") == 0);

  /* rle test: complex test */
  rle =
    "0 1 1 1 0 2 1 4 0 8 1 16 0 32 1 64 0 128 1 256 0 512 1 1024 0 2048 1 4096";
  ret = part_filter_set_rle (filter, rle);
  assert (ret == 0);
  ret = part_filter_format_rle (filter, small_buf, sizeof (small_buf));
  assert (ret == -1);
  ret = part_filter_format_rle (filter, buf, sizeof (buf));
  assert (ret == 0);
  assert (strcmp (buf, rle) == 0);


  /* rle test: token version */
  do
    {
      char *tokens[] =
	{ "0", "1", "1", "1", "0", "2", "1", "4", "0", "8", "1", "16", "0",
	"32", "1", "64", "0", "128", "1", "256", "0", "512", "1", "1024", "0",
	"2048", "1", "4096"
      };
      ret =
	part_filter_set_rle_tokens (filter, tokens,
				    sizeof (tokens) / sizeof (char *));
      assert (ret == 0);
    }
  while (0);

  destroy_part_filter (filter);
  return 0;
}

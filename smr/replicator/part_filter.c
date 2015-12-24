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
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include "part_filter.h"

struct partFilter
{
  int num_part;
  unsigned long *bits;
};

partFilter *
create_part_filter (int num_part)
{
  partFilter *filter;
  int num_words;

  if (num_part <= 0)
    {
      return NULL;
    }

  filter = malloc (sizeof (partFilter));
  if (filter == NULL)
    {
      return NULL;
    }
  filter->num_part = num_part;
  num_words = num_part / sizeof (unsigned long) + 1;	// +1: I'm rich
  filter->bits = calloc (1, sizeof (unsigned long) * num_words);
  if (filter->bits == NULL)
    {
      free (filter);
      return NULL;
    }

  return filter;
}

void
destroy_part_filter (partFilter * filter)
{
  if (filter != NULL)
    {
      if (filter->bits != NULL)
	{
	  free (filter->bits);
	}
      free (filter);
    }
}

int
part_filter_get_num_part (partFilter * filter)
{
  if (filter == NULL)
    {
      return -1;
    }
  return filter->num_part;
}

int
part_filter_get (partFilter * filter, int part)
{
  int idx;
  int off;

  if (filter == NULL)
    {
      return -1;
    }
  else if (part < 0 || part >= filter->num_part)
    {
      return -1;
    }

  idx = part / sizeof (unsigned long);
  off = part - idx * sizeof (unsigned long);
  return (filter->bits[idx] & (1 << off)) > 0;
}

int
part_filter_set (partFilter * filter, int part, int on)
{
  int idx;
  int off;

  if (filter == NULL)
    {
      return -1;
    }
  else if (part < 0 || part >= filter->num_part)
    {
      return -1;
    }

  on = (on & ~1) ? 1 : on;
  idx = part / sizeof (unsigned long);
  off = part - idx * sizeof (unsigned long);
  if (on)
    {
      filter->bits[idx] |= (1 << off);
    }
  else
    {
      filter->bits[idx] &= ~(1 << off);
    }

  return 0;
}

int
part_filter_set_rle (partFilter * filter, const char *spec)
{
  int offset;
  char *sp;
  char *endptr;

  if (filter == NULL)
    {
      return -1;
    }

  offset = 0;
  sp = (char *) spec;
  endptr = NULL;
  while (1)
    {
      long val;
      long count;

      errno = 0;
      val = strtol (sp, &endptr, 10);
      if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
	  || (errno != 0 && val == 0))
	{
	  return -1;
	}
      else if (val & ~1)
	{
	  return -1;
	}
      sp = endptr;

      errno = 0;
      count = strtol (sp, &endptr, 10);
      if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
	  || (errno != 0 && val == 0))
	{
	  return -1;
	}
      else if (count < 1)
	{
	  return -1;
	}
      sp = endptr;

      while (count-- > 0)
	{
	  if (part_filter_set (filter, offset++, val) != 0)
	    {
	      return -1;
	    }
	  if (offset >= filter->num_part)
	    {
	      goto done;
	    }
	}
    }

done:
  return 0;
}

int
part_filter_set_rle_tokens (partFilter * filter, char **tokens, int num_tok)
{
  int offset;
  int ti;

  if (filter == NULL)
    {
      return -1;
    }

  offset = 0;
  ti = 0;
  while (1)
    {
      long val;
      long count;
      char *endptr;

      errno = 0;
      val = strtol (tokens[ti++], &endptr, 10);
      if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
	  || (errno != 0 && val == 0))
	{
	  return -1;
	}
      else if (val & ~1)
	{
	  return -1;
	}

      errno = 0;
      count = strtol (tokens[ti++], &endptr, 10);
      if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
	  || (errno != 0 && val == 0))
	{
	  return -1;
	}
      else if (count < 1)
	{
	  return -1;
	}

      while (count-- > 0)
	{
	  if (part_filter_set (filter, offset++, val) != 0)
	    {
	      return -1;
	    }
	  if (offset >= filter->num_part)
	    {
	      goto done;
	    }
	}
    }

done:
  return 0;
}


int
part_filter_format_rle (partFilter * filter, char *buf, int buf_sz)
{
  int remain;
  char *bp;
  int offset;
  int val;
  int old;
  int count;
  int ret;
  int first;

  if (filter == NULL)
    {
      return -1;
    }

  offset = 0;
  bp = buf;
  remain = buf_sz;

  if ((val = part_filter_get (filter, offset++)) == -1)
    {
      return -1;
    }
  old = val;
  count = 1;

  first = 1;
  while (offset < filter->num_part)
    {
      if ((val = part_filter_get (filter, offset)) == -1)
	{
	  return -1;
	}

      if (val == old)
	{
	  count++;
	}
      else
	{
	  ret =
	    snprintf (bp, remain, "%s%d %d", first ? "" : " ", old, count);
	  first = 0;
	  if (ret < 0 || ret > remain)
	    {
	      return -1;
	    }
	  remain -= ret;
	  bp += ret;

	  old = val;
	  count = 1;
	}
      offset++;
    }

  ret = snprintf (bp, remain, "%s%d %d", first ? "" : " ", old, count);
  if (ret < 0 || ret > remain)
    {
      return -1;
    }
  remain -= ret;
  bp += ret;
  return 0;
}

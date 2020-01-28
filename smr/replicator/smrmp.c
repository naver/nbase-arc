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
#include <string.h>
#include <errno.h>
#include <assert.h>
#include "smrmp.h"

#define DELIM " \t\r\n"
int
smrmp_parse_msg (char *buf, int buf_sz, char ***rtok)
{
  char **tokens = NULL;
  int tsz = 0;
  int idx;
  char *token;
  char *save_ptr = NULL;

  if (buf == NULL || buf_sz < 0 || rtok == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  /* make default token array */
  tsz = 32;
  tokens = malloc ((tsz + 1) * sizeof (char *));
  if (tokens == NULL)
    {
      return -1;
    }
  tokens[0] = NULL;
  idx = 0;

  token = strtok_r (buf, DELIM, &save_ptr);
  if (token == NULL)
    {
      errno = EINVAL;
      goto error;
    }

  if ((tokens[idx++] = strdup (token)) == NULL)
    {
      goto error;
    }

  while ((token = strtok_r (NULL, DELIM, &save_ptr)) != NULL)
    {
      if (idx >= tsz)
	{
	  char **new_tokens;

	  new_tokens = realloc (tokens, (tsz * 2 + 1) * sizeof (char *));
	  if (new_tokens == NULL)
	    {
	      goto error;
	    }
	  tokens = new_tokens;
	  tsz = tsz * 2;
	}

      if ((tokens[idx++] = strdup (token)) == NULL)
	{
	  goto error;
	}
    }
  tokens[idx] = NULL;
  *rtok = tokens;
  return 0;

error:
  if (tokens != NULL)
    {
      smrmp_free_msg (tokens);
      return -1;
    }
  return -1;
}


void
smrmp_free_msg (char **rtok)
{
  int i = 0;

  if (rtok == NULL)
    {
      return;
    }

  while (rtok[i] != NULL)
    {
      free (rtok[i++]);
    }
  free (rtok);
}

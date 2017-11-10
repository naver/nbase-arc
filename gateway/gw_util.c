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

#include "gw_util.h"

/* TODO Implement assert function more friendly */

/* --- Index helper --- */
struct index_helper
{
  int size;
  int total_idx;
  int *idx_to_number;
  int *number_to_idx;
  int *idx_to_count;
};

static void
index_helper_realloc (index_helper * helper, int new_size)
{
  int i;

  helper->idx_to_number =
    zrealloc (helper->idx_to_number, sizeof (int) * new_size);
  helper->number_to_idx =
    zrealloc (helper->number_to_idx, sizeof (int) * new_size);
  helper->idx_to_count =
    zrealloc (helper->idx_to_number, sizeof (int) * new_size);
  for (i = helper->size; i < new_size; i++)
    {
      helper->idx_to_number[i] = -1;
      helper->number_to_idx[i] = -1;
      helper->idx_to_count[i] = 0;
    }
  helper->size = new_size;
}

index_helper *
index_helper_create (int init_size)
{
  index_helper *helper;
  int i;

  helper = zmalloc (sizeof (index_helper));
  helper->size = init_size;
  helper->total_idx = 0;
  helper->idx_to_number = zmalloc (sizeof (int) * init_size);
  helper->number_to_idx = zmalloc (sizeof (int) * init_size);
  helper->idx_to_count = zmalloc (sizeof (int) * init_size);
  for (i = 0; i < init_size; i++)
    {
      helper->idx_to_number[i] = -1;
      helper->number_to_idx[i] = -1;
      helper->idx_to_count[i] = 0;
    }
  return helper;
}

void
index_helper_set (index_helper * helper, int number, int *ret_idx,
		  int *ret_count)
{
  int idx;

  if (number >= helper->size)
    {
      index_helper_realloc (helper, number * 2);
    }
  if (helper->number_to_idx[number] == -1)
    {
      idx = helper->total_idx++;
      helper->number_to_idx[number] = idx;
      helper->idx_to_number[idx] = number;
    }
  else
    {
      idx = helper->number_to_idx[number];
    }
  helper->idx_to_count[idx]++;
  *ret_idx = idx;
  *ret_count = helper->idx_to_count[idx];
}

void
index_helper_get_by_idx (index_helper * helper, int idx, int *ret_number,
			 int *ret_count)
{

  *ret_number = helper->idx_to_number[idx];
  *ret_count = helper->idx_to_count[idx];
}

void
index_helper_get_by_number (index_helper * helper, int number, int *ret_idx,
			    int *ret_count)
{
  int idx;

  *ret_idx = idx = helper->number_to_idx[number];
  *ret_count = helper->idx_to_count[idx];
}

int
index_helper_total (index_helper * helper)
{
  return helper->total_idx;
}

void
index_helper_clear (index_helper * helper)
{
  int i;

  for (i = 0; i < helper->total_idx; i++)
    {
      int number;

      number = helper->idx_to_number[i];
      helper->idx_to_number[i] = -1;
      helper->number_to_idx[number] = -1;
      helper->idx_to_count[i] = 0;
    }
  helper->total_idx = 0;
}

void
index_helper_destroy (index_helper * helper)
{
  zfree (helper->idx_to_number);
  zfree (helper->number_to_idx);
  zfree (helper->idx_to_count);
  zfree (helper);
}

/* --- Time util --- */
/* Return the UNIX time in microseconds */
long long
ustime (void)
{
  struct timeval tv;
  long long ust;

  gettimeofday (&tv, NULL);
  ust = ((long long) tv.tv_sec) * 1000000;
  ust += tv.tv_usec;
  return ust;
}

/* Return the UNIX time in milliseconds */
long long
mstime (void)
{
  return ustime () / 1000;
}

/* --- Dictionary setup --- */
// str
unsigned int
dictStrHash (const void *key)
{
  return dictGenHashFunction ((unsigned char *) key, strlen ((char *) key));
}

unsigned int
dictStrCaseHash (const void *key)
{
  return dictGenCaseHashFunction ((unsigned char *) key,
				  strlen ((char *) key));
}

void *
dictStrDup (void *privdata, const void *val)
{
  DICT_NOTUSED (privdata);
  return zstrdup ((char *) val);
}

int
dictStrKeyCompare (void *privdata, const void *key1, const void *key2)
{
  int l1, l2;
  DICT_NOTUSED (privdata);
  l1 = strlen ((char *) key1);
  l2 = strlen ((char *) key2);
  if (l1 != l2)
    return 0;
  return memcmp (key1, key2, l1) == 0;
}

int
dictStrKeyCaseCompare (void *privdata, const void *key1, const void *key2)
{
  DICT_NOTUSED (privdata);
  return strcasecmp ((char *) key1, (char *) key2) == 0;
}

void
dictStrDestructor (void *privdata, void *val)
{
  DICT_NOTUSED (privdata);
  zfree (val);
}

// sds
unsigned int
dictSdsHash (const void *key)
{
  return dictGenHashFunction ((unsigned char *) key, sdslen ((char *) key));
}

unsigned int
dictSdsCaseHash (const void *key)
{
  return dictGenCaseHashFunction ((unsigned char *) key,
				  sdslen ((char *) key));
}

void *
dictSdsDup (void *privdata, const void *val)
{
  DICT_NOTUSED (privdata);
  return sdsdup ((sds) val);
}

int
dictSdsKeyCompare (void *privdata, const void *key1, const void *key2)
{
  int l1, l2;
  DICT_NOTUSED (privdata);
  l1 = sdslen ((sds) key1);
  l2 = sdslen ((sds) key2);
  if (l1 != l2)
    return 0;
  return memcmp (key1, key2, l1) == 0;
}

int
dictSdsKeyCaseCompare (void *privdata, const void *key1, const void *key2)
{
  DICT_NOTUSED (privdata);
  return strcasecmp ((char *) key1, (char *) key2) == 0;
}

void
dictSdsDestructor (void *privdata, void *val)
{
  DICT_NOTUSED (privdata);
  sdsfree (val);
}

/* Resizing aeCreateFileEvent */
int
aexCreateFileEvent (aeEventLoop * eventLoop, int fd, int mask,
		    aeFileProc * proc, void *clientData)
{
  int ret;

  errno = 0;
  ret = aeCreateFileEvent (eventLoop, fd, mask, proc, clientData);
  if (ret == AE_ERR && errno == ERANGE)
    {
      int curr = aeGetSetSize (eventLoop);
      int ssz = curr * 2 > fd ? curr * 2 : fd + 1024;

      ret = aeResizeSetSize (eventLoop, ssz);
      if (ret != AE_OK)
	{
	  return ret;
	}
      ret = aeCreateFileEvent (eventLoop, fd, mask, proc, clientData);
    }
  return ret;
}

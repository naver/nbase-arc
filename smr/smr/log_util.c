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

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef SFI_ENABLED
#include <pthread.h>
#endif

#include "smr.h"
#include "log_internal.h"

#define ERRNO_FILE_ID UTIL_FILE_ID

long long
currtime_usec (void)
{
  struct timeval tv;
  long long usec;

  gettimeofday (&tv, NULL);
  usec = tv.tv_sec * 1000000;
  usec += tv.tv_usec;
  return usec;
}

int
ll_cmpr (const void *v1, const void *v2)
{
  long long ll1 = *(long long *) v1;
  long long ll2 = *(long long *) v2;

  return (ll1 > ll2) ? 1 : ((ll1 == ll2) ? 0 : -1);
}

int
init_log_file (int fd)
{
  int ret;

  ret = ftruncate (fd, SMR_LOG_FILE_ACTUAL_SIZE);
  if (ret < 0)
    {
      ERRNO_POINT ();
      return -1;
    }
  // extended parts reads null bytes
  return 0;
}


#ifdef SFI_ENABLED
static pthread_once_t sfi_once = PTHREAD_ONCE_INIT;
static pthread_key_t sfi_key;
// ISO C forbids conversion of function pointer to object pointer type
struct funcWrap
{
  void (*callback) (char *, int);
};

static void
initialize_key (void)
{
  pthread_key_create (&sfi_key, free);
}

void
sfi_mshmcs_probe (char *file, int line)
{
  struct funcWrap *wrap;

  (void) pthread_once (&sfi_once, initialize_key);
  wrap = pthread_getspecific (sfi_key);
  if (wrap)
    {
      wrap->callback (file, line);
    }
}

void
sfi_mshmcs_register (void (*callback) (char *, int))
{
  struct funcWrap *wrap = NULL;

  (void) pthread_once (&sfi_once, initialize_key);
  wrap = malloc (sizeof (struct funcWrap));
  if (wrap)
    {
      wrap->callback = callback;
    }
  pthread_setspecific (sfi_key, wrap);
}
#endif

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

#include "slowlog.h"

struct testArg
{
  long long ms;
  int count;
  long min_ms;
  long max_ms;
};
#define init_test_arg(a) do { \
  (a)->ms = -1;               \
  (a)->count = 0;             \
  (a)->min_ms = 0;            \
  (a)->max_ms = 0;            \
} while(0)

static int
test_mapf (slowLogEntry * e, void *arg)
{
  struct testArg *a = (struct testArg *) arg;

  assert (a->ms <= e->start_ms);
  a->ms = e->start_ms;

  assert (e->start_ms >= a->min_ms && e->start_ms <= a->max_ms);
  a->count++;
  return 1;
}

static void
test_cascade3 (void)
{
  int i, ret;
  slowLog *s1 = NULL;
  slowLog *s2 = NULL;
  slowLog *s3 = NULL;
  int count = 10000;
  int bar1 = 1000;
  int bar2 = 100;
  int bar3 = 10;
  struct testArg arg;
  slowLogStat stat;

  // make cascade of slow log
  s1 = new_slowlog (count / 3, bar1, NULL);
  assert (s1 != NULL);

  s2 = new_slowlog (count / 3, bar2, s1);
  assert (s2 != NULL);

  s3 = new_slowlog (count / 3, bar3, s2);
  assert (s3 != NULL);

  for (i = 1; i <= count; i++)
    {
      ret = slowlog_add (s3, i, i, 't', 't');
      assert (ret == 0);
    }

  ret = slowlog_stat (s1, &stat);
  assert (ret == 0);
  assert (stat.count == count / 3);
  assert (stat.tot_count == count - bar1 + 1);

  ret = slowlog_stat (s2, &stat);
  assert (ret == 0);
  assert (stat.count == count / 3);
  assert (stat.tot_count == count - bar2 + 1);

  ret = slowlog_stat (s3, &stat);
  assert (ret == 0);
  assert (stat.count == count / 3);
  assert (stat.tot_count == count - bar3 + 1);

  // test
  init_test_arg (&arg);
  arg.min_ms = count - count / 3 + 1;
  arg.max_ms = count;
  ret = slowlog_map (s1, test_mapf, &arg, 1);
  assert (ret == 1);

  init_test_arg (&arg);
  arg.min_ms = count - count / 3 + 1;
  arg.max_ms = count;
  ret = slowlog_map (s2, test_mapf, &arg, 1);
  assert (ret == 1);

  init_test_arg (&arg);
  arg.min_ms = count - count / 3 + 1;
  arg.max_ms = count;
  ret = slowlog_map (s3, test_mapf, &arg, 1);
  assert (ret == 1);

  // terminate
  delete_slowlog (s1);
  delete_slowlog (s2);
  delete_slowlog (s3);
}

int
main (int argc, char *argv[])
{
  test_cascade3 ();
}

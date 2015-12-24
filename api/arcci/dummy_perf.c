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

#include "fmacros.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <signal.h>

#include "arcci.h"

struct worker_arg
{
  arc_t *arc;
  int tid;
  pthread_t thr;
};

/* -------------- */
/* LOCAL VARIABLE */
/* -------------- */
static volatile int ok_to_run = 0;
static volatile int global_tick = 0;
static char *buf_1M = NULL;

/* --------------- */
/* LOCAL FUNCTIONS */
/* --------------- */
static void *
worker_thread (void *data)
{
  int ret;
  struct worker_arg *arg = (struct worker_arg *) data;
  long long count, saved_count;
  long long error, saved_error;
  int local_tick;

  assert (arg != NULL);

  local_tick = global_tick;
  saved_count = count = 0;
  saved_error = error = 0;

  while (ok_to_run)
    {
      arc_request_t *rqst;
      arc_reply_t *reply;
      int rand_val = rand ();
      int next_rqst = 0;
      int be_errno;

    peek_rqst:
      rqst = arc_create_request ();
      assert (rqst != NULL);
      switch (next_rqst)
	{
	case 0:
	  ret =
	    arc_append_command (rqst, "SET %s%d %s", "key", rand_val, "val");
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 1:
	  ret = arc_append_command (rqst, "GET %s%d", "key", rand_val);
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 2:
	  ret = arc_append_command (rqst, "DEL %s%d", "key", rand_val);
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 3:
	  ret =
	    arc_append_command (rqst, "SET %s%d %s", "key", rand_val + 1,
				"val");
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 4:
	  ret =
	    arc_append_command (rqst, "MGET %s%d %s%d", "key", rand_val,
				"key", rand_val + 1);
	  assert (ret == 0);
	  next_rqst++;
	  /* fall thourgh */
	default:
	  next_rqst = 0;
	  break;
	}

      ret = arc_do_request (arg->arc, rqst, 1000, &be_errno);
      if (ret == 0)
	{
	  count++;
	  while ((ret = arc_get_reply (rqst, &reply, &be_errno)) == 0
		 && reply != NULL)
	    {
	      if (reply == NULL)
		{
		  break;
		}
	      arc_free_reply (reply);
	      reply = NULL;
	    }
	  assert (ret == 0);
	}
      else
	{
	  reply = NULL;
	  ret = arc_get_reply (rqst, &reply, &be_errno);
	  assert (ret == -1);
	  assert (reply == NULL);
	  error++;
	}
      arc_free_request (rqst);

      /* print stats */
      if (local_tick != global_tick)
	{
	  printf ("[%d][%d] %lld %lld\n", global_tick, arg->tid,
		  count - saved_count, error - saved_error);
	  local_tick = global_tick;
	  saved_count = count;
	  saved_error = error;
	}

      if (next_rqst != 0)
	{
	  goto peek_rqst;
	}
    }
  return NULL;
}


static char *usage_ =
  "dummy-perf                                                                 \n"
  "Usage: %s <options>                                                        \n"
  "option:                                                                    \n"
  "    -z zookeeper hosts (multiple host can be specified separated by comma) \n"
  "       e.g.) my.zk.host:2100                                               \n"
  "    -c <cluster name>                                                      \n"
  "    -g <gateway hosts> (when set, no -z and -c option can be set)          \n"
  "    -n <number of threads>                                                 \n"
  "    -l <log file prefix>                                                   \n"
  "    -v                 (when set log level to DEBUG)                       \n"
  "    -s <run time in seconds>                                               \n";


static void
print_usage (char *prog)
{
  printf (usage_, prog);
}

static void
arg_error (char *msg)
{
  printf ("%s\n", msg);
  exit (1);
}

/* ---- */
/* MAIN */
/* ---- */
int
main (int argc, char *argv[])
{
  arc_t *arc;
  char *zk_addr = NULL;
  char *cluster_name = NULL;
  char *hosts = NULL;
  int num_thr = 1;
  int run_sec = 10;
  int remain_sec;
  struct worker_arg *args = NULL;
  int i, ret;
  arc_conf_t conf;
  int use_zk = 0;
  int log_level = ARC_LOG_LEVEL_INFO;
  char *log_file_prefix = NULL;

  while ((ret = getopt (argc, argv, "z:c:g:n:s:l:v")) > 0)
    {
      switch (ret)
	{
	case 'z':
	  zk_addr = optarg;
	  break;
	case 'c':
	  cluster_name = optarg;
	  break;
	case 'g':
	  hosts = optarg;
	  break;
	case 'n':
	  num_thr = atoi (optarg);
	  if (num_thr < 0)
	    {
	      arg_error ("Invalid number of thread");
	    }
	  break;
	case 's':
	  run_sec = atoi (optarg);
	  if (run_sec < 0)
	    {
	      arg_error ("Invalid runtime in second");
	    }
	  break;
	case 'l':
	  log_file_prefix = optarg;
	  break;
	case 'v':
	  log_level = ARC_LOG_LEVEL_DEBUG;
	  break;
	default:
	  exit (1);
	  break;
	}
    }
  if (hosts != NULL)
    {
      if (zk_addr || cluster_name)
	{
	  arg_error
	    ("If 'g' option is specified, 'z' and 'c' options can not be specified");
	}
    }
  else if (zk_addr != NULL && cluster_name != NULL)
    {
      if (hosts)
	{
	  arg_error
	    ("If 'z' and 'c' options are set, 'g' option can not be specified");
	}
      use_zk = 1;
    }
  else
    {
      print_usage (argv[0]);
      exit (0);
    }

  signal (SIGHUP, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);

  buf_1M = malloc (1024 * 1024);
  assert (buf_1M != NULL);
  for (i = 0; i < 1024 * 1024; i++)
    {
      buf_1M[i] = "0123456789"[i % 10];
    }

  arc_init_conf (&conf);
  conf.log_level = log_level;
  conf.log_file_prefix = log_file_prefix;

  if (use_zk)
    {
      arc = arc_new_zk (zk_addr, cluster_name, &conf);
    }
  else
    {
      arc = arc_new_gw (hosts, &conf);
    }
  assert (arc != NULL);

  args = malloc (sizeof (struct worker_arg) * num_thr);
  assert (args != NULL);

  ok_to_run = 1;

  /* launch workers */
  for (i = 0; i < num_thr; i++)
    {
      args[i].arc = arc;
      args[i].tid = i;
      pthread_create (&args[i].thr, NULL, worker_thread, &args[i]);
    }

  /* tick as specified */
  remain_sec = run_sec;
  while (remain_sec > 0)
    {
      sleep (1);
      global_tick++;
      remain_sec--;
    }

  /* wait for the worker thread to finish */
  ok_to_run = 0;
  for (i = 0; i < num_thr; i++)
    {
      args[i].arc = arc;
      args[i].tid = i;
      pthread_join (args[i].thr, NULL);
    }

  free (args);
  arc_destroy (arc);
  free (buf_1M);
  buf_1M = NULL;
  return 0;
}

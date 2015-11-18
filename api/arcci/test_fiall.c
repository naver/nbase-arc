#include "fmacros.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>

#include "arcci.h"
#include "arcci_common.h"

struct worker_arg
{
  arc_t *arc;
  int tid;
  pthread_t thr;
};

#ifdef FI_ENABLED
#define FI_CALL_NOT_NULL(stmt) while((stmt) == NULL);
#define FI_CALL_NO_ERR(stmt) while((stmt) != 0);
#else
#define FI_CALL_NOT_NULL(stmt) stmt
#define FI_CALL_NO_ERR(stmt) stmt
#endif
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
      FI_CALL_NOT_NULL (rqst = arc_create_request ());
      assert (rqst != NULL);
      switch (next_rqst)
	{
	case 0:
	  FI_CALL_NO_ERR (ret =
			  arc_append_command (rqst, "SET %s%d %s", "key",
					      rand_val, "val"));
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 1:
	  FI_CALL_NO_ERR (ret =
			  arc_append_command (rqst, "GET %s%d", "key",
					      rand_val));
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 2:
	  FI_CALL_NO_ERR (ret =
			  arc_append_command (rqst, "DEL %s%d", "key",
					      rand_val));
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 3:
	  FI_CALL_NO_ERR (ret =
			  arc_append_command (rqst, "SET %s%d %s", "key",
					      rand_val + 1, "val"));
	  assert (ret == 0);
	  next_rqst++;
	  break;
	case 4:
	  FI_CALL_NO_ERR (ret =
			  arc_append_command (rqst, "MGET %s%d %s%d", "key",
					      rand_val, "key", rand_val + 1));
	  assert (ret == 0);
	  next_rqst = 0;
	  break;
	  /* fall thourgh */
	default:
	  assert (0);
	  break;
	}

#if FI_ENABLED
      ret = arc_do_request (arg->arc, rqst, 1000, &be_errno);
      if (ret == -1)
	{
	  arc_free_request (rqst);
	  rqst = NULL;
	  goto peek_rqst;
	}
#else
      ret = arc_do_request (arg->arc, rqst, 1000, &be_errno);
#endif
      if (ret == 0)
	{
	  count++;
	  FI_CALL_NO_ERR (ret = arc_get_reply (rqst, &reply, &be_errno));
	  while (ret == 0 && reply != NULL)
	    {
	      if (reply == NULL)
		{
		  break;
		}
	      arc_free_reply (reply);
	      reply = NULL;
	      FI_CALL_NO_ERR (ret = arc_get_reply (rqst, &reply, &be_errno));
	    }
	  assert (ret == 0);
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
  "test-fiall                                                                 \n"
  "Usage: %s <options>                                                        \n"
  "option:                                                                    \n"
  "    -z zookeeper hosts (multiple host can be specified separated by comma) \n"
  "       e.g.) my.zk.host:2100                                               \n"
  "    -c cluster name                                                        \n"
  "    -g gateway hosts (when set, no -z and -c option can be set)            \n"
  "    -s run time in seconds                                                 \n";


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
#ifdef FI_ENABLED
  fi_t *fe_fi;
  fi_t *be_fi;
#endif

#ifdef FI_ENABLED
  fi_Enabled = 1;
  fe_fi = arc_fi_new ();
  assert (fe_fi != NULL);
  be_fi = arc_fi_new ();
  assert (be_fi != NULL);
  fe_Fi = fe_fi;
  be_Fi = be_fi;
#endif

  while ((ret = getopt (argc, argv, "z:c:g:s:")) > 0)
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
	case 's':
	  run_sec = atoi (optarg);
	  if (run_sec < 0)
	    {
	      arg_error ("Invalid runtime in second");
	    }
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
  conf.log_level = ARC_LOG_LEVEL_DEBUG;

  if (use_zk)
    {
      FI_CALL_NOT_NULL (arc = arc_new_zk (zk_addr, cluster_name, &conf));
    }
  else
    {
      FI_CALL_NOT_NULL (arc = arc_new_gw (hosts, &conf));
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

#ifdef FI_ENABLED
  fi_Enabled = 0;
  arc_fi_destroy (fe_fi);
  arc_fi_destroy (be_fi);
  fe_Fi = NULL;
  be_Fi = NULL;
#endif
  free (args);
  arc_destroy (arc);
  free (buf_1M);
  buf_1M = NULL;
  return 0;
}

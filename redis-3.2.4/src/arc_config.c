#ifdef NBASE_ARC
#include "arc_internal.h"

/* ------------------ */
/* Local declarations */
/* ------------------ */
static void append_server_cronsave_params (int minute, int hour);
static void reset_server_cronsave_params ();
static void set_memory_limit_values (void);

/* --------------- */
/* Local functions */
/* --------------- */
static void
append_server_cronsave_params (int minute, int hour)
{
  arc.cronsave_params =
    zrealloc (arc.cronsave_params,
	      sizeof (struct cronsaveParam) * (arc.cronsave_paramslen + 1));
  arc.cronsave_params[arc.cronsave_paramslen].minute = minute;
  arc.cronsave_params[arc.cronsave_paramslen].hour = hour;
  arc.cronsave_paramslen++;
}

static void
reset_server_cronsave_params ()
{
  zfree (arc.cronsave_params);
  arc.cronsave_params = NULL;
  arc.cronsave_paramslen = 0;
}

static void
set_memory_limit_values (void)
{
  unsigned long total_kb, free_kb, cached_kb;
  if (arcx_get_memory_usage (&total_kb, &free_kb, &cached_kb) == C_ERR)
    {
      serverLog (LL_WARNING, "Can't update memory usage from meminfo file");
      exit (1);
    }

  arc.mem_limit_activated = 0;
  arc.mem_max_allowed_exceeded = 0;
  arc.mem_hard_limit_exceeded = 0;

  arc.mem_limit_active_kb = total_kb * arc.mem_limit_active_perc / 100;
  arc.mem_hard_limit_kb = total_kb * arc.mem_hard_limit_perc / 100;
  arc.mem_max_allowed_byte = 0;
}

static char *blocked_conf_items[] = {
  "save",
  "maxmemory",
  //TODO more
  NULL
};

static int
is_blocked_conf_item (char *item)
{
  int i = 0;
  while (blocked_conf_items[i] != NULL)
    {
      if (strcasecmp (item, blocked_conf_items[i]) == 0)
	{
	  return 1;
	}
      i++;
    }
  return 0;
}


/* -------- */
/* Exported */
/* -------- */
/* returns 0 if not matched, 1 if reply is set */
int
arc_config_set (client * c)
{
  long long ll;
  robj *o = c->argv[3];

  if (arc.cluster_mode && is_blocked_conf_item (c->argv[2]->ptr))
    {
      goto blocked;
    }

  if (!strcasecmp (c->argv[2]->ptr, "number-of-rdb-backups"))
    {
      if (getLongLongFromObject (o, &ll) == C_ERR ||
	  ll < 0 || ll > ARC_MAX_RDB_BACKUPS)
	goto badfmt;
      arc.num_rdb_backups = ll;
    }
  else
    if (!strcasecmp (c->argv[2]->ptr, "memory-limit-activation-percentage"))
    {
      if (getLongLongFromObject (o, &ll) == C_ERR || ll <= 0 || ll > 100)
	goto badfmt;
      arc.mem_limit_active_perc = ll;
      set_memory_limit_values ();
    }
  else if (!strcasecmp (c->argv[2]->ptr, "memory-max-allowed-percentage"))
    {
      if (getLongLongFromObject (o, &ll) == C_ERR || ll <= 0 || ll > 100)
	goto badfmt;
      arc.mem_max_allowed_perc = ll;
      set_memory_limit_values ();
    }
  else if (!strcasecmp (c->argv[2]->ptr, "memory-hard-limit-percentage"))
    {
      if (getLongLongFromObject (o, &ll) == C_ERR || ll <= 0 || ll > 100)
	goto badfmt;
      arc.mem_hard_limit_perc = ll;
      set_memory_limit_values ();
    }
  else if (!strcasecmp (c->argv[2]->ptr, "object-bio-delete-min-elems"))
    {
      if (getLongLongFromObject (o, &ll) == C_ERR || ll < 0)
	goto badfmt;
      arc.object_bio_delete_min_elems = ll;
    }
  else if (!strcasecmp (c->argv[2]->ptr, "sss-gc-interval"))
    {
      if (getLongLongFromObject (o, &ll) == C_ERR || ll < 100 || ll > INT_MAX)
	goto badfmt;
      arc.gc_interval = ll;
    }
  else
    {
      return 0;
    }

  addReply (c, shared.ok);
  return 1;

blocked:
  addReplyErrorFormat (c, "Blocked conf item for CONFIG SET '%s'",
		       (char *) c->argv[2]->ptr);
  return 1;
badfmt:
  addReplyErrorFormat (c, "Invalid argument '%s' for CONFIG SET '%s'",
		       (char *) o->ptr, (char *) c->argv[2]->ptr);
  return 1;
}

/**
 * returns 0 (with proper err_ret) if match any config. 1 otherwise
 */
int
arc_config_cmp_load (int argc, sds * argv, char **err_ret)
{
  char *err = NULL;

  if (arc.cluster_mode && is_blocked_conf_item (argv[0]))
    {
      err = "Not permitted in nbase-arc mode";
      goto loaderr;
    }

  if (!strcasecmp (argv[0], "cronsave"))
    {
      if (argc == 3)
	{
	  int minute = atoi (argv[1]);
	  int hour = atoi (argv[2]);
	  if (minute < 0 || minute > 60 || hour < 0 || hour > 23)
	    {
	      err = "Invalid cronsave parameter";
	      goto loaderr;
	    }
	  append_server_cronsave_params (minute, hour);
	}
      else if (argc == 2 && !strcasecmp (argv[1], ""))
	{
	  reset_server_cronsave_params ();
	}
    }
  else if (!strcasecmp (argv[0], "seqsave") && argc == 2)
    {
      arc.seqsave_gap = atoll (argv[1]);
      arc.seqsave_gap *= 1024 * 1024 * 1024;	/* convert giga byte to byte */
      if (arc.seqsave_gap <= 0)
	{
	  err = "Invalid seqsave parameter";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "number-of-rdb-backups") && argc == 2)
    {
      arc.num_rdb_backups = atoi (argv[1]);
      if (arc.num_rdb_backups < 0
	  || arc.num_rdb_backups > ARC_MAX_RDB_BACKUPS)
	{
	  err = "Invalid number-of-rdb-backups parameter";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "sss-gc-lines") && argc == 2)
    {
      arc.gc_num_line = atoi (argv[1]);
      if (arc.gc_num_line < 1)
	{
	  err = "Invalid number of sss-gc-lines";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "sss-gc-interval") && argc == 2)
    {
      arc.gc_interval = atoi (argv[1]);
      if (arc.gc_interval < 100)
	{
	  err =
	    "Invalid number of sss-gc-interval. Minimum interval is 100ms.";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "cluster") && argc == 1)
    {
    }
  else if (!strcasecmp (argv[0], "smr-local-port") && argc == 2)
    {
      arc.smr_lport = atoi (argv[1]);
      if (arc.smr_lport < 0 || arc.smr_lport > 65535)
	{
	  err = "Invalid smr local port";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "smr-seqnum-reset") && argc == 1)
    {
      arc.smr_seqnum_reset = 1;
    }
  else if (!strcasecmp (argv[0], "memory-limit-activation-percentage")
	   && argc == 2)
    {
      arc.mem_limit_active_perc = atoi (argv[1]);
      if (arc.mem_limit_active_perc <= 0 || arc.mem_limit_active_perc > 100)
	{
	  err = "Invalid percentage of memory limiting activation";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "memory-max-allowed-percentage")
	   && argc == 2)
    {
      arc.mem_max_allowed_perc = atoi (argv[1]);
      if (arc.mem_max_allowed_perc <= 0 || arc.mem_max_allowed_perc > 100)
	{
	  err = "Invalid percentage of maximum allowed memory";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "memory-hard-limit-percentage") && argc == 2)
    {
      arc.mem_hard_limit_perc = atoi (argv[1]);
      if (arc.mem_hard_limit_perc <= 0 || arc.mem_hard_limit_perc > 100)
	{
	  err = "Invalid percentage of memory hard limit";
	  goto loaderr;
	}
    }
  else if (!strcasecmp (argv[0], "object-bio-delete-min-elems") && argc == 2)
    {
      arc.object_bio_delete_min_elems = atoi (argv[1]);
      if (arc.object_bio_delete_min_elems < 0)
	{
	  err =
	    "Invalid number of minimum elements of a structure for background deletion";
	  goto loaderr;
	}
    }
  else
    {
      /* no match */
      return 1;
    }

  *err_ret = NULL;
  return 0;

loaderr:
  *err_ret = err;
  return 0;
}

void
arcx_set_memory_limit_values (void)
{
  set_memory_limit_values ();
}
#endif

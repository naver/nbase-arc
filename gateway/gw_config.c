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

#include "gw_config.h"

static dictType confDictType = {
  dictStrHash,
  dictStrDup,
  NULL,
  dictStrKeyCompare,
  dictStrDestructor,
  NULL
};

static void
free_cluster_conf (cluster_conf * conf)
{
  block_range *range, *tvar;
  dictIterator *di;
  dictEntry *de;

  if (conf->cluster_name)
    {
      sdsfree (conf->cluster_name);
    }
  if (conf->nbase_arc_home)
    {
      sdsfree (conf->nbase_arc_home);
    }
  if (conf->slots)
    {
      zfree (conf->slots);
    }
  if (conf->slot_state)
    {
      zfree (conf->slot_state);
    }
  if (conf->partition)
    {
      di = dictGetIterator (conf->partition);
      while ((de = dictNext (di)) != NULL)
	{
	  partition_conf *pg = dictGetVal (de);

	  sdsfree (pg->pg_id);
	  dictRelease (pg->redis);
	  zfree (pg);
	}
      dictReleaseIterator (di);
      dictRelease (conf->partition);
    }
  if (conf->redis)
    {
      di = dictGetIterator (conf->redis);
      while ((de = dictNext (di)) != NULL)
	{
	  redis_conf *redis = dictGetVal (de);

	  sdsfree (redis->redis_id);
	  sdsfree (redis->pg_id);
	  sdsfree (redis->addr);
	  sdsfree (redis->domain_socket_path);
	  zfree (redis);
	}
      dictReleaseIterator (di);
      dictRelease (conf->redis);
    }
  TAILQ_FOREACH_SAFE (range, &conf->block_range_q, block_range_tqe, tvar)
  {
    TAILQ_REMOVE (&conf->block_range_q, range, block_range_tqe);
    zfree (range);
  }
  freeLocalIpAddrs (conf->local_ip_addrs);
  zfree (conf);
}

#define REDIS_BASEPORT_OFFSET 9
sds
default_domain_path (sds nbase_arc_home, int port)
{
  sds path;

  if (!nbase_arc_home)
    {
      return NULL;
    }

  path = sdsMakeRoomFor (sdsempty (), PATH_MAX + 1);
  path =
    sdscatprintf (path, "%s/pgs/%d/redis/redis.sock", nbase_arc_home,
		  port - REDIS_BASEPORT_OFFSET);
  return path;
}

/* (Example)
 * 8192                     // slot count
 * 0 4096 1 4096            // RLE, node_id to slot mapping
 * 0 0 50                   // node_id to redis_id mapping
 * 1 1 51                   // ...
 * 0:127.0.0.1:8200:8209 1:127.0.0.1:9200:9209 50:127.0.0.1:8210:8219 51:127.0.0.1:9210:9219    
 * // redis_id, ip, port mapping (redis_id:ip:smr_port,redis_port) */

cluster_conf *
create_conf_from_cm_data (const char *cluster_name,
			  const char *nbase_arc_home,
			  int prefer_domain_socket, sds data)
{
  cluster_conf *conf;
  int nline, narg, nitem;
  long nslot;
  sds *lines = NULL, *args = NULL, *items = NULL;
  int ret, i, j, slot_idx;

  conf = zcalloc (sizeof (cluster_conf));

  conf->cluster_name = sdsnew (cluster_name);
  conf->nbase_arc_home = nbase_arc_home ? sdsnew (nbase_arc_home) : NULL;
  conf->prefer_domain_socket = prefer_domain_socket;

  TAILQ_INIT (&conf->block_range_q);

  conf->local_ip_addrs = getLocalIpAddrs ();
  if (!conf->local_ip_addrs)
    {
      goto release_return;
    }

  lines = sdssplitlen (data, sdslen (data), "\r\n", 2, &nline);
  if (!lines || nline < 6)
    {
      goto release_return;
    }
  // Initialize slot array
  ret = string2l (lines[0], sdslen (lines[0]), &nslot);
  if (!ret || nslot <= 0)
    {
      goto release_return;
    }
  conf->nslot = nslot;
  conf->slots = zcalloc (sizeof (partition_conf *) * nslot);
  conf->slot_state = zcalloc (sizeof (int) * nslot);

  // Init partition and slot mapping from RLE
  conf->partition = dictCreate (&confDictType, NULL);
  args = sdssplitlen (lines[1], sdslen (lines[1]), " ", 1, &narg);
  if (!args || narg % 2 == 1)
    {
      goto release_return;
    }
  slot_idx = 0;
  for (i = 0; i < narg; i += 2)
    {
      partition_conf *pg;
      sds pg_id;
      long len;

      pg_id = args[i];
      ret = string2l (args[i + 1], sdslen (args[i + 1]), &len);
      if (!ret)
	{
	  goto release_return;
	}

      pg = dictFetchValue (conf->partition, pg_id);
      if (!pg)
	{
	  pg = zmalloc (sizeof (partition_conf));
	  pg->pg_id = sdsdup (pg_id);
	  pg->redis = dictCreate (&confDictType, NULL);
	  ret = dictAdd (conf->partition, pg->pg_id, pg);
	  assert (ret == DICT_OK);
	}
      for (j = 0; j < len; j++)
	{
	  conf->slots[slot_idx++] = pg;
	}
    }
  sdsfreesplitres (args, narg);
  args = NULL;

  // Init redis server
  conf->redis = dictCreate (&confDictType, NULL);
  for (i = 2; i < nline - 3; i++)
    {
      sds pg_id;
      partition_conf *pg;

      args = sdssplitlen (lines[i], sdslen (lines[i]), " ", 1, &narg);
      if (!args || narg < 1)
	{
	  goto release_return;
	}
      pg_id = args[0];
      pg = dictFetchValue (conf->partition, pg_id);
      if (!pg)
	{
	  pg = zmalloc (sizeof (partition_conf));
	  pg->pg_id = sdsdup (pg_id);
	  pg->redis = dictCreate (&confDictType, NULL);
	  ret = dictAdd (conf->partition, pg->pg_id, pg);
	  assert (ret == DICT_OK);
	}
      for (j = 1; j < narg; j++)
	{
	  sds redis_id;
	  redis_conf *redis;

	  redis_id = args[j];
	  redis = dictFetchValue (conf->redis, redis_id);
	  if (redis)
	    {
	      // duplicate
	      goto release_return;
	    }
	  redis = zcalloc (sizeof (redis_conf));
	  redis->redis_id = sdsdup (redis_id);
	  redis->pg_id = sdsdup (pg_id);
	  ret = dictAdd (conf->redis, redis->redis_id, redis);
	  assert (ret == DICT_OK);
	  ret = dictAdd (pg->redis, redis->redis_id, redis);
	  assert (ret == DICT_OK);
	}
      sdsfreesplitres (args, narg);
      args = NULL;
    }

  // Init address and port of server
  args =
    sdssplitlen (lines[nline - 3], sdslen (lines[nline - 3]), " ", 1, &narg);
  if (!args || narg != (signed) dictSize (conf->redis))
    {
      goto release_return;
    }
  for (i = 0; i < narg; i++)
    {
      sds redis_id;
      redis_conf *redis;
      long port, smr_port;

      items = sdssplitlen (args[i], sdslen (args[i]), ":", 1, &nitem);
      if (!items || nitem != 4)
	{
	  goto release_return;
	}
      redis_id = items[0];
      redis = dictFetchValue (conf->redis, redis_id);
      if (!redis)
	{
	  goto release_return;
	}
      // SMR_PORT not used
      redis->addr = sdsdup (items[1]);
      ret = string2l (items[2], sdslen (items[2]), &smr_port);
      if (!ret)
	{
	  goto release_return;
	}
      ret = string2l (items[3], sdslen (items[3]), &port);
      if (!ret)
	{
	  goto release_return;
	}
      redis->port = port;
      redis->is_local = isLocalIpAddr (conf->local_ip_addrs, redis->addr);
      redis->domain_socket_path =
	redis->is_local ? default_domain_path (conf->nbase_arc_home,
					       redis->port) : NULL;
      redis->deleting = 0;
      sdsfreesplitres (items, nitem);
      items = NULL;
    }
  sdsfreesplitres (args, narg);
  args = NULL;

  sdsfreesplitres (lines, nline);
  lines = NULL;

  return conf;

release_return:
  if (lines)
    {
      sdsfreesplitres (lines, nline);
    }
  if (args)
    {
      sdsfreesplitres (args, narg);
    }
  if (items)
    {
      sdsfreesplitres (items, nitem);
    }
  if (conf)
    {
      free_cluster_conf (conf);
    }
  return NULL;
}

static int
sync_write (int fd, sds buf)
{
  ssize_t nwritten, pos;

  pos = 0;
  while (1)
    {
      nwritten = write (fd, buf + pos, sdslen (buf) - pos);
      if (nwritten == -1)
	{
	  return ERR;
	}

      pos += nwritten;
      if (pos == (ssize_t) sdslen (buf))
	{
	  return OK;
	}
    }
}

static int
compare_last_str (sds buf, const char *last, size_t len)
{
  if (!memcmp (buf + sdslen (buf) - len, last, len))
    {
      return OK;
    }
  else
    {
      return ERR;
    }
}

#define SYNC_IOBUF_LEN (1024*16)
static sds
sync_read_until (int fd, const char *until_str, size_t until_len)
{
  sds buf;
  ssize_t nread;

  buf = sdsMakeRoomFor (sdsempty (), SYNC_IOBUF_LEN);
  while (1)
    {
      if (sdsavail (buf) == 0)
	{
	  buf = sdsMakeRoomFor (buf, 2 * sdslen (buf));
	}

      nread = read (fd, buf + sdslen (buf), 1);
      if (nread == -1 || nread == 0)
	{
	  sdsfree (buf);
	  return NULL;
	}

      sdsIncrLen (buf, nread);
      if (compare_last_str (buf, until_str, until_len) == OK)
	{
	  return buf;
	}
    }
}

static sds
sync_request (int fd, sds rqst, const char *until_str, size_t until_len)
{
  sds resp;
  int ret;

  ret = sync_write (fd, rqst);
  if (ret == ERR)
    {
      return NULL;
    }
  resp = sync_read_until (fd, until_str, until_len);
  if (!resp)
    {
      return NULL;
    }
  return resp;
}

static int
parse_redirect_addr (sds buf, sds * addr, int *port)
{
  char *start, *end;
  int ret;
  long value;

  // parse addr
  start = strstr (buf, "\"ip\":\"");
  if (!start)
    {
      return ERR;
    }
  start += 6;

  end = strchr (start, '"');
  if (!end)
    {
      return ERR;
    }

  *addr = sdscpylen (sdsempty (), start, end - start);

  // parse port
  start = strstr (buf, "\"port\":\"");
  if (!start)
    {
      sdsfree (*addr);
      *addr = NULL;
      return ERR;
    }
  start += 8;

  end = strchr (start, '"');
  if (!end)
    {
      sdsfree (*addr);
      *addr = NULL;
      return ERR;
    }

  ret = string2l (start, end - start, &value);
  if (!ret)
    {
      sdsfree (*addr);
      *addr = NULL;
      return ERR;
    }

  *port = value;
  return OK;
}

static int
is_leader_confmaster (int fd, sds * redir_addr, int *redir_port)
{
  sds rqst, resp;
  sds addr;
  int port, ret;

  *redir_addr = NULL;
  *redir_port = 0;

  rqst = sdsnew ("cluster_ls\r\n");
  resp = sync_request (fd, rqst, "\r\n", 2);
  sdsfree (rqst);
  if (!resp)
    {
      return ERR;
    }
  if (!strncmp ("{\"state\":\"redirect\",", resp, 20))
    {
      ret = parse_redirect_addr (resp, &addr, &port);
      if (ret == OK)
	{
	  *redir_addr = addr;
	  *redir_port = port;
	}
      sdsfree (resp);
      return ERR;
    }
  sdsfree (resp);
  return OK;
}

static int
connect_to_confmaster (const char *addr, int port)
{
  sds redir_addr;
  int fd, redir_port, ret;
  char ebuf[ANET_ERR_LEN];

  fd = anetTcpConnect (ebuf, (char *) addr, port);
  if (fd == ANET_ERR)
    {
      gwlog (GW_WARNING,
	     "Unable to connect to Configuration Master, addr:%s, port:%d, err:%s",
	     addr, port, ebuf);
      return ERR;
    }
  // leader configuration master
  ret = is_leader_confmaster (fd, &redir_addr, &redir_port);
  if (ret == OK)
    {
      return fd;
    }

  // follower configuration master
  close (fd);

  // unable to get redirect address
  if (!redir_addr)
    {
      gwlog (GW_WARNING,
	     "Unable to get redirect address of Configuration Master, addr:%s, port:%d, err:%s",
	     addr, port, strerror (errno));
      return ERR;
    }
  // redirect to leader configuration master
  fd = anetTcpConnect (ebuf, redir_addr, redir_port);
  if (fd == ANET_ERR)
    {
      gwlog (GW_WARNING,
	     "Unable to connect to redirected Configuration Master, addr:%s, port:%d, err:%s",
	     redir_addr, redir_port, ebuf);
      sdsfree (redir_addr);
      return ERR;
    }
  sdsfree (redir_addr);

  return fd;
}

cluster_conf *
create_conf_from_confmaster (const char *cm_addr, int cm_port,
			     const char *cluster_name,
			     const char *nbase_arc_home,
			     int prefer_domain_socket)
{
  cluster_conf *conf;
  sds rqst, resp, conf_str;
  int fd;

  fd = connect_to_confmaster (cm_addr, cm_port);
  if (fd == ERR)
    {
      return NULL;
    }

  // Configuration master returns a error message when it receives 'get_cluster_info'
  // command with non-existing cluster name as a argument. The error message starts
  // with '{"state":"error","msg......' and ends with '\r\n'. So we should read until
  // '\r\n' and verify the message. If the response is a success and cluster exists,
  // we need to read once more until '\r\n\r\n', because the successful response is
  // in the form of multiline strings with trailing empty line.
  rqst = sdscatprintf (sdsempty (), "get_cluster_info %s\r\n", cluster_name);
  resp = sync_request (fd, rqst, "\r\n", 2);
  if (!resp)
    {
      gwlog (GW_WARNING,
	     "Unable to request to Configuration Master, request:%s, err:%s",
	     rqst, strerror (errno));
      sdsfree (rqst);
      close (fd);
      return NULL;
    }
  if (!strncmp ("{\"state\":\"error\"", resp, 16))
    {
      gwlog (GW_WARNING, "Cluster \"%s\" dose not exist.", cluster_name);
      sdsfree (rqst);
      sdsfree (resp);
      close (fd);
      return NULL;
    }
  conf_str = sdsdup (resp);
  sdsfree (resp);
  sdsfree (rqst);

  resp = sync_read_until (fd, "\r\n\r\n", 4);
  conf_str = sdscatsds (conf_str, resp);
  sdsfree (resp);

  close (fd);

  gwlog (GW_DEBUG,
	 "Initialize gateway with the configuration from ConfMaster.\n%s",
	 conf_str);
  conf =
    create_conf_from_cm_data (cluster_name, nbase_arc_home,
			      prefer_domain_socket, conf_str);
  sdsfree (conf_str);

  return conf;
}

cluster_conf *
duplicate_conf (cluster_conf * conf)
{
  cluster_conf *new;
  dictIterator *di;
  dictEntry *de;
  int i, ret;

  new = zcalloc (sizeof (cluster_conf));

  new->cluster_name = sdsdup (conf->cluster_name);
  new->nbase_arc_home =
    conf->nbase_arc_home ? sdsdup (conf->nbase_arc_home) : NULL;
  new->prefer_domain_socket = conf->prefer_domain_socket;

  TAILQ_INIT (&new->block_range_q);

  new->local_ip_addrs = dupLocalIpAddrs (conf->local_ip_addrs);

  new->nslot = conf->nslot;
  new->slots = zcalloc (sizeof (partition_conf *) * conf->nslot);
  new->slot_state = zcalloc (sizeof (int) * conf->nslot);

  new->partition = dictCreate (&confDictType, NULL);
  di = dictGetIterator (conf->partition);
  while ((de = dictNext (di)) != NULL)
    {
      partition_conf *pg, *new_pg;

      pg = dictGetVal (de);
      new_pg = zmalloc (sizeof (partition_conf));
      new_pg->pg_id = sdsdup (pg->pg_id);
      new_pg->redis = dictCreate (&confDictType, NULL);
      ret = dictAdd (new->partition, new_pg->pg_id, new_pg);
      assert (ret == DICT_OK);
    }
  dictReleaseIterator (di);

  new->redis = dictCreate (&confDictType, NULL);
  di = dictGetIterator (conf->redis);
  while ((de = dictNext (di)) != NULL)
    {
      redis_conf *redis, *new_redis;
      partition_conf *new_pg;

      redis = dictGetVal (de);
      new_redis = zcalloc (sizeof (redis_conf));
      new_redis->redis_id = sdsdup (redis->redis_id);
      new_redis->pg_id = sdsdup (redis->pg_id);
      new_redis->addr = sdsdup (redis->addr);
      new_redis->port = redis->port;
      new_redis->domain_socket_path =
	redis->domain_socket_path ? sdsdup (redis->domain_socket_path) : NULL;
      new_redis->is_local = redis->is_local;
      new_redis->deleting = redis->deleting;
      ret = dictAdd (new->redis, new_redis->redis_id, new_redis);
      assert (ret == DICT_OK);
      new_pg = dictFetchValue (new->partition, new_redis->pg_id);
      assert (new_pg);
      ret = dictAdd (new_pg->redis, new_redis->redis_id, new_redis);
      assert (ret == DICT_OK);
    }
  dictReleaseIterator (di);

  for (i = 0; i < conf->nslot; i++)
    {
      sds pg_id;
      partition_conf *new_pg;

      if (conf->slots[i])
	{
	  pg_id = conf->slots[i]->pg_id;
	  new_pg = dictFetchValue (new->partition, pg_id);
	  new->slots[i] = new_pg;
	}
      else
	{
	  new->slots[i] = NULL;
	}
      new->slot_state[i] = conf->slot_state[i];
    }

  return new;
}

void
destroy_conf (cluster_conf * conf)
{
  free_cluster_conf (conf);
}

#define CLUSTER_INFO_SDS_INIT_SIZE  4096
sds
get_cluster_info_sds (cluster_conf * conf)
{
  dictIterator *di, *di_redis;
  dictEntry *de, *de_redis;
  sds buf;
  block_range *range;
  partition_conf *last_pg;
  int i, idx_start;

  buf = sdsMakeRoomFor (sdsempty (), CLUSTER_INFO_SDS_INIT_SIZE);

  buf = sdscatprintf (buf,
		      "=========== Redis Cluster ===========\r\n"
		      "Slot Size       : %d\r\n"
		      "Partition Count : %ld\r\n\r\n", conf->nslot,
		      dictSize (conf->partition));

  buf = sdscatprintf (buf, "[Partition Groups]\r\n");
  di = dictGetIterator (conf->partition);
  while ((de = dictNext (di)) != NULL)
    {
      partition_conf *pg;

      pg = dictGetVal (de);
      buf = sdscatprintf (buf, "  - PG %s\r\n", pg->pg_id);

      di_redis = dictGetIterator (pg->redis);
      while ((de_redis = dictNext (di_redis)) != NULL)
	{
	  redis_conf *redis;

	  redis = dictGetVal (de_redis);
	  buf =
	    sdscatprintf (buf, "    - Redis %-4s %s:%d%s\r\n",
			  redis->redis_id, redis->addr, redis->port,
			  redis->deleting ? "  (DELETING)" : "");
	}
      dictReleaseIterator (di_redis);
    }
  dictReleaseIterator (di);

  buf = sdscatprintf (buf, "\r\n[Hash Slot Mapping]\r\n");
  idx_start = 0;
  last_pg = conf->slots[idx_start];
  for (i = 1; i < conf->nslot; i++)
    {
      if (last_pg != conf->slots[i])
	{
	  if (last_pg)
	    {
	      buf =
		sdscatprintf (buf, "  %4d - %4d -> PG %-8s (%d)\r\n",
			      idx_start, i - 1, last_pg->pg_id,
			      i - idx_start);
	    }
	  else
	    {
	      buf =
		sdscatprintf (buf, "  %4d - %4d -> UNALLOCATED (%d)\r\n",
			      idx_start, i - 1, i - idx_start);
	    }
	  idx_start = i;
	  last_pg = conf->slots[i];
	}
    }
  if (last_pg)
    {
      buf =
	sdscatprintf (buf, "  %4d - %4d -> PG %-8s (%d)\r\n", idx_start,
		      i - 1, last_pg->pg_id, i - idx_start);
    }
  else
    {
      buf =
	sdscatprintf (buf, "  %4d - %4d -> UNALLOCATED (%d)\r\n", idx_start,
		      i - 1, i - idx_start);
    }

  if (!TAILQ_EMPTY (&conf->block_range_q))
    {
      buf = sdscatprintf (buf, "\r\n[Hash Slot Blocked]\r\n");
      TAILQ_FOREACH (range, &conf->block_range_q, block_range_tqe)
      {
	if (conf->slot_state[range->from] == SLOT_STATE_BLOCKED)
	  {
	    buf =
	      sdscatprintf (buf, "  %4d - %4d   BLOCKED  (%d)\r\n",
			    range->from, range->to,
			    range->to - range->from + 1);
	  }
	else
	  {
	    buf =
	      sdscatprintf (buf, "  %4d - %4d   BLOCKING (%d)\r\n",
			    range->from, range->to,
			    range->to - range->from + 1);
	  }
      }
    }

  return buf;
}

sds
get_pg_map_rle (cluster_conf * conf)
{
  sds rle;
  partition_conf *last_pg;
  int i, idx_start;

  rle = sdsMakeRoomFor (sdsempty (), 4096);

  idx_start = 0;
  last_pg = conf->slots[idx_start];
  for (i = 1; i < conf->nslot; i++)
    {
      if (last_pg != conf->slots[i])
	{
	  if (idx_start != 0)
	    {
	      rle = sdscat (rle, " ");
	    }
	  rle =
	    sdscatprintf (rle, "%s %d",
			  (last_pg) ? last_pg->pg_id : "UNALLOCATED",
			  i - idx_start);
	  idx_start = i;
	  last_pg = conf->slots[i];
	}
    }
  if (idx_start != 0)
    {
      rle = sdscat (rle, " ");
    }
  rle =
    sdscatprintf (rle, "%s %d", (last_pg) ? last_pg->pg_id : "UNALLOCATED",
		  i - idx_start);

  return rle;
}

sds
get_redis_id_list (cluster_conf * conf)
{
  dictIterator *di;
  dictEntry *de;
  sds str;
  int first;

  str = sdsMakeRoomFor (sdsempty (), 4096);

  first = 1;
  di = dictGetIterator (conf->redis);
  while ((de = dictNext (di)) != NULL)
    {
      redis_conf *redis = dictGetVal (de);

      if (!first)
	{
	  str = sdscat (str, " ");
	}
      else
	{
	  first = 0;
	}
      str = sdscatprintf (str, "%s", redis->redis_id);
    }
  dictReleaseIterator (di);

  return str;
}

sds
get_delay_filter (cluster_conf * conf)
{
  sds delay;
  int i, last_state, idx_start;

  delay = sdsMakeRoomFor (sdsempty (), 4096);

  idx_start = 0;
  last_state = conf->slot_state[idx_start];
  for (i = 1; i < conf->nslot; i++)
    {
      if (last_state == SLOT_STATE_NORMAL
	  && conf->slot_state[i] != SLOT_STATE_NORMAL)
	{
	  if (idx_start != 0)
	    {
	      delay = sdscat (delay, " ");
	    }
	  delay = sdscatprintf (delay, "0 %d", i - idx_start);
	  idx_start = i;
	  last_state = conf->slot_state[i];
	}
      else if (last_state != SLOT_STATE_NORMAL
	       && conf->slot_state[i] == SLOT_STATE_NORMAL)
	{
	  if (idx_start != 0)
	    {
	      delay = sdscat (delay, " ");
	    }
	  delay = sdscatprintf (delay, "1 %d", i - idx_start);
	  idx_start = i;
	  last_state = conf->slot_state[i];
	}
    }
  if (idx_start != 0)
    {
      delay = sdscat (delay, " ");
    }
  if (last_state == SLOT_STATE_NORMAL)
    {
      delay = sdscatprintf (delay, "0 %d", i - idx_start);
    }
  else
    {
      delay = sdscatprintf (delay, "1 %d", i - idx_start);
    }

  return delay;
}

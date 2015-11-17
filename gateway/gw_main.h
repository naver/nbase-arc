#ifndef _GW_MAIN_H_
#define _GW_MAIN_H_

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <errno.h>
#include <assert.h>

#include "gw_config.h"
#include "gw_async_chan.h"
#include "gw_worker.h"
#include "gw_log.h"
#include "anet.h"
#include "ae.h"

typedef struct master_t
{
  aeEventLoop *el;
  long long cron_teid;
  int hz;

  int fd;
  int admin_fd;

  client_pool *admin_cli;
  command_manager *admin_cmd_mgr;

  async_chan *master_async;
  array_async_chans worker_asyncs;

  array_worker workers;

  cluster_conf *conf;
  sbuf_hdr *shared_stream;
  sbuf_mempool *mp_sbuf;
} master_t;
#endif

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

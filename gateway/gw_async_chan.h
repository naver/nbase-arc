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

#ifndef _GW_ASYNC_CHAN_H_
#define _GW_ASYNC_CHAN_H_

#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>

#include "gw_config.h"
#include "gw_array.h"
#include "queue.h"
#include "zmalloc.h"
#include "anet.h"
#include "ae.h"

#define ASYNC_IOBUF_SIZE 1024
#define ASYNC_PIPE_READ 0
#define ASYNC_PIPE_WRITE 1
#define ARRAY_INIT_SIZE_ASYNC_CHAN 64

typedef struct async_event async_event;
typedef struct async_chan async_chan;
typedef struct array_async_chans array_async_chans;
typedef struct async_event_tqh async_event_tqh;
typedef void async_exec_proc (void *arg, void *chan_data);

TAILQ_HEAD (async_event_tqh, async_event);
ARRAY_HEAD (array_async_chans, async_chan *, ARRAY_INIT_SIZE_ASYNC_CHAN);

struct async_event
{
  TAILQ_ENTRY (async_event) async_event_tqe;

  async_exec_proc *exec;
  void *arg;
};

struct async_chan
{
  aeEventLoop *el;
  int pipefd[2];

  pthread_mutex_t q_lock;
  async_event_tqh event_q;

  void *chan_data;
};

async_chan *async_create (aeEventLoop * el, void *chan_data);
void async_destroy (async_chan * chan);
void async_send_event (aeEventLoop * my_el, async_chan * target,
		       async_exec_proc * exec, void *arg);
#endif

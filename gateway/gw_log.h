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

#ifndef _GW_LOG_H_
#define _GW_LOG_H_

#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <limits.h>
#include <syslog.h>
#include <time.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

/* Log Levels */
#define GW_DEBUG 0
#define GW_VERBOSE 1
#define GW_NOTICE 2
#define GW_WARNING 3

/* Default Settings */
#define GWLOG_FILE_PREFIX           "gateway"
#define GWLOG_FILE_ROLLING_SIZE     (64*1024*1024)
#define GWLOG_SYSLOG_IDENT          "gateway"
#define GWLOG_SYSLOG_FACILITY       LOG_LOCAL0
#define GWLOG_MAX_MSG_LEN           2048

struct logger
{
  int verbosity;

  /* File */
  pthread_mutex_t logfile_lock;
  const char *logfile_prefix;
  FILE *logfile_fp;
  int logfile_rolling_size;
  int logfile_size;

  /* Syslog */
  const char *syslog_ident;
  int syslog_facility;

  /* Flags */
  unsigned console_enabled:1;
  unsigned logfile_enabled:1;
  unsigned syslog_enabled:1;
};

void gwlog_init (char *prefix, int verbosity, int using_console,
		 int using_logfile, int using_syslog);
void gwlog_finalize (void);
void gwlog (int level, const char *fmt, ...);
#endif

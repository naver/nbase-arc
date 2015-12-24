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

#ifndef _OS_H_
#define _OS_H_

#ifndef _WIN32	// Linux
#include <pthread.h>
#include <unistd.h>
#define INVALID_PIPE -1
#define EVENTLOOP_MAXFD_INITIAL_VALUE -1
typedef int pipe_t;
typedef int socket_t;
#else	// Windows
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#define INVALID_PIPE INVALID_HANDLE_VALUE
#define EVENTLOOP_MAXFD_INITIAL_VALUE 0
typedef HANDLE pipe_t;
typedef SOCKET socket_t;
#endif

int close_pipe(pipe_t pipe_fd);
int close_socket(socket_t socket_fd);

#endif

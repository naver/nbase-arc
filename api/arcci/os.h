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

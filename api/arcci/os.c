#include "os.h"
#ifdef _WIN32
#include "win32fixes.h"
#endif

int close_pipe(pipe_t fd)
{
#ifndef _WIN32
  return close(fd);
#else
  return aeWinCloseComm((socket_t)fd);
#endif
}

int close_socket(socket_t fd)
{
#ifndef _WIN32
	return close(fd);
#else
	return aeWinCloseComm(fd);
#endif
}

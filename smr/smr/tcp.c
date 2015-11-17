#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <assert.h>

#include "tcp.h"

static void
set_error (char *ebuf, int ebuf_sz, char *format, ...)
{
  va_list args;

  assert (ebuf != NULL ? (ebuf_sz > 0) : 1);

  if (ebuf != NULL)
    {
      va_start (args, format);
      vsnprintf (ebuf, ebuf_sz, format, args);
      va_end (args);
    }
}

static int
tcp_socket (char *ebuf, int ebuf_sz)
{
  int socket_fd;
  int one = 1;

  if ((socket_fd = socket (AF_INET, SOCK_STREAM, 0)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to create a socket");
      return -1;
    }

  if (setsockopt (socket_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof (one)) ==
      -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to setsockopt: SO_REUSEADDR: %s",
		 strerror (errno));
      return -1;
    }

  return socket_fd;
}


static int
tcp_set_option_internal (int fd, int opt)
{
  assert (fd > 0);

  if (opt & TCP_OPT_NODELAY)
    {
      int ival = 1;
      if (setsockopt
	  (fd, IPPROTO_TCP, TCP_NODELAY, &ival, sizeof (int)) == -1)
	{
	  return -1;
	}
    }

  if (opt & TCP_OPT_KEEPALIVE)
    {
      int ival = 1;
      if (setsockopt
	  (fd, IPPROTO_TCP, SO_KEEPALIVE, &ival, sizeof (int)) == -1)
	{
	  return -1;
	}
    }

  if (opt & TCP_OPT_NONBLOCK)
    {
      int flags;
      if ((flags = fcntl (fd, F_GETFL)) == -1)
	{
	  return -1;
	}

      flags |= O_NONBLOCK;
      if (fcntl (fd, F_SETFL, flags) == -1)
	{
	  return -1;
	}
    }
  return 0;
}

int
tcp_connect (char *addr, int port, int opt, char *ebuf, int ebuf_sz)
{
  int socket_fd;
  struct sockaddr_in sa;

  assert (addr != NULL);
  assert (port > 0);

  /* create a socket */
  if ((socket_fd = tcp_socket (ebuf, ebuf_sz)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to create a socket");
      return -1;
    }

  sa.sin_family = AF_INET;
  sa.sin_port = htons (port);

  /* try to convert IP host address from dot notation */
  if (inet_aton (addr, &sa.sin_addr) == 0)
    {
      struct hostent *he;

      /* try to convert host address from name notation */
      he = gethostbyname (addr);
      if (he == NULL)
	{
	  set_error (ebuf, ebuf_sz, "Failed to resolve address: %s", addr);
	  close (socket_fd);
	  return -1;
	}
      memcpy (&sa.sin_addr, he->h_addr, sizeof (struct in_addr));
    }

  /* connect */
  if (connect (socket_fd, (struct sockaddr *) &sa, sizeof (sa)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to connect: %s", strerror (errno));
      close (socket_fd);
      return -1;
    }

  /* set options */
  if (opt && tcp_set_option_internal (socket_fd, opt) == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to set connection option: %s",
		 strerror (errno));
      close (socket_fd);
      return -1;
    }

  return socket_fd;
}

int
tcp_set_option (int fd, int opt)
{
  return tcp_set_option_internal (fd, opt);
}

int
tcp_read_fully (int fd, void *buf_, int count)
{
  int tr = 0;
  char *buf = buf_;

  assert (fd > 0);
  assert (buf != NULL);
  assert (count >= 0);

  while (tr < count)
    {
      int nr = read (fd, buf + tr, count - tr);
      if (nr > 0)
	{
	  tr += nr;
	}
      else if (nr == 0)
	{
	  /* connection closed (means failed to read fully) */
	  return -1;
	}
      else
	{
	  return -1;
	}
    }
  return tr;
}

int
tcp_write_fully (int fd, void *buf_, int count)
{
  int tw = 0;
  char *buf = buf_;

  assert (fd > 0);
  assert (buf != NULL);
  assert (count >= 0);

  while (tw < count)
    {
      int nw = write (fd, buf + tw, count - tw);
      if (nw > 0)
	{
	  tw += nw;
	}
      else if (nw == 0)
	{
	  /* connection closed (means failed to write fully) */
	  return -1;
	}
      else
	{
	  return -1;
	}
    }
  return tw;
}

int
tcp_server (char *addr, int port, char *ebuf, int ebuf_sz)
{
  int server_fd;
  struct sockaddr_in sa;

  /* create server socket */
  server_fd = tcp_socket (ebuf, ebuf_sz);
  if (server_fd == -1)
    {
      set_error (ebuf, ebuf_sz, "Failed to create serversocket %s %d",
		 addr ? addr : "<Null>", port);
      return -1;
    }

  memset (&sa, 0, sizeof (sa));
  sa.sin_family = AF_INET;
  sa.sin_port = htons (port);
  sa.sin_addr.s_addr = htonl (INADDR_ANY);

  if (addr != NULL && inet_aton (addr, &sa.sin_addr) == 0)
    {
      set_error (ebuf, ebuf_sz, "Invalid bind address %s", addr);
      close (server_fd);
      return -1;
    }

  /* bind */
  if (bind (server_fd, &sa, sizeof (sa)) == -1)
    {
      set_error (ebuf, ebuf_sz, "Bind error: %s", strerror (errno));
      close (server_fd);
      return -1;
    }

  /* listen */
  if (listen (server_fd, 1024) == -1)
    {
      set_error (ebuf, ebuf_sz, "Listen error: %s", strerror (errno));
      close (server_fd);
      return -1;
    }

  return server_fd;
}


int
tcp_accept (int server_fd, char *ip, int *port, char *ebuf, int ebuf_sz)
{
  int fd = -1;
  struct sockaddr_in sa;
  socklen_t salen = sizeof (sa);

  while (1)
    {
      fd = accept (server_fd, &sa, &salen);
      if (fd == -1)
	{
	  if (errno == EINTR)
	    {
	      continue;
	    }
	  else
	    {
	      set_error (ebuf, ebuf_sz, "Accept error: %s", strerror (errno));
	      return -1;
	    }
	}
      break;
    }

  if (ip != NULL)
    {
      strcpy (ip, inet_ntoa (sa.sin_addr));
    }

  if (port != NULL)
    {
      *port = ntohs (sa.sin_port);
    }

  return fd;
}

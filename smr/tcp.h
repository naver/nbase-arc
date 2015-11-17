/*
 * Simple tcp connection related functions 
 */

#ifndef _TCP_H_
#define _TCP_H_


/* TCP connection options */
#define TCP_OPT_NODELAY         0x01
#define TCP_OPT_NONBLOCK        0x02
#define TCP_OPT_KEEPALIVE       0x04

extern int tcp_connect (char *addr, int port, int opt, char *ebuf,
			int ebuf_sz);
extern int tcp_set_option (int fd, int opt);
extern int tcp_read_fully (int fd, void *buf, int count);
extern int tcp_write_fully (int fd, void *buf, int count);
extern int tcp_server (char *addr, int port, char *ebuf, int ebuf_sz);
extern int tcp_accept (int server_fd, char *ip, int *port, char *ebuf,
		       int ebuf_sz);

#endif

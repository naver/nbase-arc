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
extern int tcp_read_fully_deadline (int fd, void *buf, int count,
				    long long deadline_msec);
extern int tcp_write_fully (int fd, void *buf, int count);
extern int tcp_write_fully_deadline (int fd, void *buf, int count,
				     long long deadline_msec);
extern int tcp_server (char *addr, int port, char *ebuf, int ebuf_sz);
extern int tcp_accept (int server_fd, char *ip, int *port, char *ebuf,
		       int ebuf_sz);

#endif

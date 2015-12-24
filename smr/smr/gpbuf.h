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

#ifndef GPGUF_H_
#define GPBUF_H_

typedef struct gpbuf_st gpbuf_t;

struct gpbuf_st
{
  char *ub;			// buffer given by user 
  int ubsz;			// buffer size given by user
  int sz;			// buffer size
  char *bp;			// buffer begin
  char *cp;			// buffer current
  char *ep;			// buffer end
};
#define gpbuf_buf(gp)   ((gp)->cp)
#define gpbuf_avail(gp) ((gp)->ep - (gp)->cp)
#define gpbuf_used(gp)  ((gp)->cp - (gp)->bp)

extern void gpbuf_init (gpbuf_t * gp, char *buf, int sz);
extern int gpbuf_reserve (gpbuf_t * gp, int sz);
extern int gpbuf_consume (gpbuf_t * gp, int sz);
extern void gpbuf_cleanup (gpbuf_t * gp);
extern int gpbuf_gut (gpbuf_t * gp, char **retdata);
extern int gpbuf_write (gpbuf_t * gp, char *buf, int count);
extern int gpbuf_printf (gpbuf_t * gp, char *format, ...);

#endif

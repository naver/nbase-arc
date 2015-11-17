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

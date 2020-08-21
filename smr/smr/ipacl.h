#ifndef _IPACL_H
#define _IPACL_H

typedef enum
{
  IPACL_WHITE = 0,
  IPACL_BLACK = 1
} ipacl_color_t;

typedef void ipacl_t;
// returns 0 to quit iteration, 1 to continue
typedef int (*ipacl_itorf) (void *ctx, char *ip, ipacl_color_t color,
			    long long expire);

extern ipacl_t *ipacl_new (ipacl_color_t def);
extern void ipacl_destroy (ipacl_t * acl);
extern int ipacl_set (ipacl_t * acl, char *ip, ipacl_color_t color,
		      long long expire);
extern ipacl_color_t ipacl_check (ipacl_t * acl, char *ip, long long curr);
extern int ipacl_reap (ipacl_t * acl, long long curr);
extern int ipacl_iterate (ipacl_t * acl, ipacl_itorf itorf, void *ctx);

#endif /* _IPACL_H */

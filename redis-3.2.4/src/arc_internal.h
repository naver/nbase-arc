#ifndef _ARC_INTERNAL_H_
#define _ARC_INTERNAL_H_

#include "server.h"

/* ----------- */
/* Redis scope */
/* ----------- */
/* server.c */
extern dictType keyptrDictType;
extern dictType keylistDictType;

extern void createSharedObjects (void);
extern struct evictionPoolEntry *evictionPoolAlloc (void);
extern void createPidFile (void);
#ifdef __linux__
extern int linuxMemoryWarnings (void);
#endif
extern void loadDataFromDisk (void);
extern int serverCron (struct aeEventLoop *eventLoop, long long id,
		       void *clientData);
extern void beforeSleep (struct aeEventLoop *eventLoop);
/* debug.c */
extern void xorDigest(unsigned char *digest, void *ptr, size_t len);
extern void mixObjectDigest(unsigned char *digest, robj *o);
extern void mixDigest(unsigned char *digest, void *ptr, size_t len);

/* --------------- */
/* nbase-arc scope */
/* --------------- */
/* arc_util.c */
extern char **arcx_get_local_ip_addrs ();
extern int arcx_is_local_ip_addr (char **local_ip_list, char *cmp_ip);
extern int arcx_get_memory_usage (unsigned long *total_kb,
				  unsigned long *free_kb,
				  unsigned long *cached_kb);
extern int arcx_get_dump (char *source_addr, int source_port, char *filename,
			  char *range, int net_limit);

/* arc_config.c */
extern void arcx_set_memory_limit_values (void);

#endif

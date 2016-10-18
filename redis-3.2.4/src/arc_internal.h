#ifndef _ARC_INTERNAL_H_
#define _ARC_INTERNAL_H_

#include "server.h"

#define smr_mstime mstime
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
extern void xorDigest (unsigned char *digest, void *ptr, size_t len);
extern void mixObjectDigest (unsigned char *digest, robj * o);
extern void mixDigest (unsigned char *digest, void *ptr, size_t len);
/* rdb.c */
extern int rdbSaveRio (rio * rdb, int *error);
extern int rdbSaveStringObject (rio * rdb, robj * obj);
extern ssize_t rdbSaveLongLongAsStringObject (rio * rdb, long long value);
extern int rdbSaveAuxFieldStrStr (rio * rdb, char *key, char *val);
extern int rdbSaveAuxFieldStrInt (rio * rdb, char *key, long long val);
extern int rdbSaveAuxField (rio * rdb, void *key, size_t keylen, void *val,
			    size_t vallen);
extern robj *rdbLoadEncodedStringObject (rio * rdb);
/* rio.c */
extern int rioWriteBulkObject (rio * r, robj * obj);
/* cluster.c */
extern void bitmapSetBit (unsigned char *bitmap, int pos);
extern int bitmapTestBit (unsigned char *bitmap, int pos);

/* --------------- */
/* nbase-arc scope */
/* --------------- */
// syntax sugar
#define RETURNIF(ret,expr) if(expr) return ret

/* arc_t_sss.c */
#define SSS_KV_LIST 1
#define SSS_KV_SET  2
typedef struct sss sss;
typedef struct sssEntry sssEntry;
typedef struct sssTypeIterator sssTypeIterator;

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

/* arc_t_sss.c */
extern sss *arcx_sss_new (robj * key);
extern void arcx_sss_release (sss * s);
extern void arcx_sss_unlink_gc (sss * s);
extern int arcx_sss_add_value (sss * s, void *ks, void *svc, void *key,
			       long long idx, void *val, long long exp);
extern int arcx_sss_type_value_count (robj * o);
extern sssTypeIterator *arcx_sss_type_init_iterator (robj * subject);
extern sssEntry *arcx_sss_iter_next (sssTypeIterator * si);
extern int arcx_sss_iter_peek (sssEntry * e, robj ** ks, robj ** svc,
			       robj ** key, long long *idx, robj ** val,
			       long long int *expire, int *kv_mode);
extern void arcx_sss_type_release_iterator (sssTypeIterator * si);
extern long long arcx_sss_garbage_collect (long long timeout);
extern void *arcx_sss_obc_new (void);
extern int arcx_sss_gc_cron (void);
extern void arcx_sss_del_entries (void *);


#endif

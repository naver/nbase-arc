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
extern int rdbSaveInfoAuxFields (rio * rdb);
extern long long rdbLoadMillisecondTime (rio * rdb);
extern robj *rdbLoadEncodedStringObject (rio * rdb);
/* rio.c */
extern int rioWriteBulkObject (rio * r, robj * obj);
/* cluster.c */
extern void bitmapSetBit (unsigned char *bitmap, int pos);
extern int bitmapTestBit (unsigned char *bitmap, int pos);
/* aof.c */
extern int rewriteListObject (rio * r, robj * key, robj * o);
extern int rewriteSetObject (rio * r, robj * key, robj * o);
extern int rewriteSortedSetObject (rio * r, robj * key, robj * o);
extern int rewriteHashObject (rio * r, robj * key, robj * o);
/* networking.c */
extern int processMultibulkBuffer (client * c);
extern int processInlineBuffer (client * c);

/* --------------- */
/* nbase-arc scope */
/* --------------- */
// syntax sugar
#define RETURNIF(ret,expr) if(expr) return ret
#define GOTOIF(l, expr) if(expr) goto l

/* arc_t_sss.c */
#define SSS_KV_LIST 1
#define SSS_KV_SET  2
typedef struct sss sss;
typedef struct sssEntry sssEntry;
typedef struct sssTypeIterator sssTypeIterator;

typedef struct
{
  FILE *fp;
  off_t filesz;
  rio rdb;
  int rdbver;
  // per iteration fields
  int dt;
  union
  {
    struct
    {
      int dbid;
    } selectdb;
    struct
    {
      robj *key;		// receiver owns reference count
      robj *val;		// receiver owns reference count
    } aux;
    struct
    {
      long long expiretime;
      int type;
      robj *key;		// receiver owns reference count
      robj *val;		// receiver owns reference count
    } kv;
  } d;
} dumpScan;

#define init_dumpscan(s) do {       \
    memset(s, 0, sizeof(dumpScan)); \
}  while(0)

#define ARCX_DUMP_DT_NONE     0
#define ARCX_DUMP_DT_SELECTDB 1
#define ARCX_DUMP_DT_AUX      2
#define ARCX_DUMP_DT_KV       3

/* arc_util.c */
extern char **arcx_get_local_ip_addrs ();
extern int arcx_is_local_ip_addr (char **local_ip_list, char *cmp_ip);
extern int arcx_get_memory_usage (unsigned long *total_kb,
				  unsigned long *free_kb,
				  unsigned long *cached_kb);
extern int arcx_get_dump (char *source_addr, int source_port, char *filename,
			  char *range, int net_limit);

extern int arcx_dumpscan_start (dumpScan * ds, char *file);
/* returns 1 if ds has iteritem, 0 if no more, -1 error */
extern int arcx_dumpscan_iterate (dumpScan * ds);
extern int arcx_dumpscan_finish (dumpScan * ds, int will_need);

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

/* arc_checkpoint.c */

// embedded in rio_ structure
struct arcRio
{
  int fd;
  long long w_count;
  off_t last_off;
};
#define init_arc_rio(r) do  {  \
  (r)->fd = -1;                \
  (r)->w_count = 0LL;          \
  (r)->last_off = 0;           \
} while(0)


extern int arcx_is_auxkey (robj * key);

/* arc_cluster_util.c */
extern int arcx_cluster_util_main (int argc, char **argv);

/* arc_dump_util.c */
extern int arcx_dump_util_main (int argc, char **argv);

/* arc_server.c */
extern void arcx_init_server_pre (void);

#endif

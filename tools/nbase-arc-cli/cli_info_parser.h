#ifndef _CLI_INFO_PARSER_H_
#define _CLI_INFO_PARSER_H_

#include "sds.h"
#include "dict.h"

dict *getDictFromInfoStr (sds str);
void releaseDict (dict * d);
long long fetchLongLong (dict * d, const void *key, int *ok);
sds fetchSds (dict * d, const void *key, int *ok);
void getDbstat (dict * d, int *ok, long long *keys, long long *expires,
		long long *avg_ttl);
void getRedisInfo (dict * d, const void *key, int *ok, sds * addr, int *port,
		   int *pg_id, int *max_latency, sds * max_cmd);
int getPgMapSize (dict * d, int *ok);
int getSlotNoOfKey (dict * d, const char *key, int *ok);
int *getPgMapArray (dict * d, int *ok);
int getPgIdOfKey (dict * d, sds key, int *ok);
sds *getRedisIdsOfPg (dict * d, int *ok, int pg_id, int *count);
void freeRedisIdsOfPg (sds * tokens, int count);
#endif

#ifndef _GW_UTIL_H_
#define _GW_UTIL_H_

#include <sys/time.h>
#include <string.h>

#include "gw_config.h"
#include "zmalloc.h"
#include "dict.h"
#include "sds.h"

#define run_with_period(_ms_) if ((_ms_ <= 1000/(*hz)) || !((*cronloops)%((_ms_)/(1000/(*hz)))))

/* Index helper */
typedef struct index_helper index_helper;

#define INDEX_HELPER_INIT_SIZE 8192
index_helper *index_helper_create (int init_size);
void index_helper_set (index_helper * helper, int number, int *ret_idx,
		       int *ret_count);
void index_helper_get_by_idx (index_helper * helper, int idx, int *ret_number,
			      int *ret_count);
void index_helper_get_by_number (index_helper * helper, int number,
				 int *ret_idx, int *ret_count);
int index_helper_total (index_helper * helper);
void index_helper_clear (index_helper * helper);
void index_helper_destroy (index_helper * helper);

/* Time util */
long long ustime (void);
long long mstime (void);

/* Dictionary setup */
unsigned int dictStrHash (const void *key);
unsigned int dictStrCaseHash (const void *key);
void *dictStrDup (void *privdata, const void *val);
int dictStrKeyCompare (void *privdata, const void *key1, const void *key2);
int dictStrKeyCaseCompare (void *privdata, const void *key1,
			   const void *key2);
void dictStrDestructor (void *privdata, void *val);
unsigned int dictSdsHash (const void *key);
unsigned int dictSdsCaseHash (const void *key);
void *dictSdsDup (void *privdata, const void *val);
int dictSdsKeyCompare (void *privdata, const void *key1, const void *key2);
int dictSdsKeyCaseCompare (void *privdata, const void *key1,
			   const void *key2);
void dictSdsDestructor (void *privdata, void *val);
#endif

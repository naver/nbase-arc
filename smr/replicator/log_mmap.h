#ifndef _LOG_MMAP_H_
#define _LOG_MMAP_H_

#include "common.h"

typedef enum
{
  GET_CREATE = 0,
  GET_EXIST = 1
} entryGetOpt;

extern logMmapEntry *logmmap_entry_get (smrReplicator * rep, long long seq,
					entryGetOpt opt, int is_write);
extern void logmmap_entry_release (smrReplicator * rep, logMmapEntry * entry);
extern void logmmap_entry_release_raw (smrReplicator * rep,
				       logMmapEntry * entry);

extern logMmapEntry *logmmap_entry_addref (smrReplicator * rep,
					   logMmapEntry * entry);
#endif

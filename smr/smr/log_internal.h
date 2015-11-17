#ifndef _LOG_INTERNAL_H_
#define _LOG_INTERNAL_H_

#include "smr_log.h"

/* check helper */
#define CHECK_SEQ(seq) (seq >= 0)

/* map_advise values */
typedef enum
{
  ADV_WRITE = 0,
  ADV_FLUSH = 1,
  ADV_READ = 2
} logAdvise;

/* help macro (must define page_size within lexical scope) */
#define _pg_mask (~(page_size -1))
#define syspg_off(off) ((off) & _pg_mask)
#define cksum_off(off) (SMR_LOG_FILE_DATA_SIZE+sizeof(logChecksum)*(1+(off)/SMR_LOG_PAGE_SIZE))

/* generic log sub-system */
typedef struct logDev_ logDev;
struct logDev_
{
  int (*get_seqs) (logDev * dev, long long **seqs, int *seqs_size);
  int (*decache) (logDev * dev, long long seq);
  int (*synced) (logDev * dev, long long seq);
  int (*purge) (logDev * dev, long long seq);
  int (*purge_after) (logDev * dev, long long seq_inc);
  smrLogAddr *(*get_mmap) (logDev * dev, long long seq, int for_read,
			   int create);
  int (*munmap) (logDev * dev, smrLogAddr * addr);
  int (*remove_one) (logDev * dev, long long upper, int gap_in_sec,
		     int *removed, long long *removed_seq, int *has_more);
  void (*close) (logDev * dev);
};
#define init_log_dev(d) do {   \
  (d)->get_seqs = NULL;        \
  (d)->decache = NULL;         \
  (d)->synced = NULL;          \
  (d)->purge = NULL;           \
  (d)->purge_after = NULL;     \
  (d)->get_mmap = NULL;        \
  (d)->munmap = NULL;          \
  (d)->remove_one = NULL;      \
  (d)->close = NULL;           \
} while(0)


/* smrLog handle definition (smr_log.h) */
struct smrLog_s
{
  int page_size;
  logDev *mem;
  logDev *disk;
};
#define init_smrlog(s) do {                \
  (s)->page_size = 4096;                   \
  (s)->mem = NULL;                         \
  (s)->disk = NULL;                        \
} while(0)

/* log_disk.c */
extern logDev *open_disk_dev (char *basedir);
extern int create_disk_dev (char *basedir);
extern int unlink_disk_dev (char *basedir);
/* log_mem.c */
extern logDev *open_mem_dev (char *basedir);
extern int create_mem_dev (char *basedir);
extern int info_mem_dev (char *basedir, char **buf);
extern int unlink_mem_dev (char *basedir);
extern int sync_mem_dev (char *basedir);
extern int mem_handle_machine_restart (logDev * mem, logDev * disk);
/* log_util.c */
extern long long currtime_usec (void);
extern int ll_cmpr (const void *v1, const void *v2);
extern int init_log_file (int fd);
#ifdef SFI_ENABLED
extern void sfi_mshmcs_probe (char *file, int line);
extern void sfi_mshmcs_register (void (*)(char *, int));
#endif

/* log_addr.c */
extern int addr_truncate (smrLogAddr * addr, int offset);
extern int addr_is_finalized (smrLogAddr * addr);
extern int addr_finalize (smrLogAddr * addr);
extern int addr_offset (smrLogAddr * addr);
extern int dev_truncate (logDev * dev, long long base_seq, long long seq);

#endif

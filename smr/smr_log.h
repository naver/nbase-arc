#ifndef _SMR_LOG_H_
#define _SMR_LOG_H_
/*
 * smr_log.h
 *
 * Note on: log file layout and checksum
 *
 * log file
 * +------------------+
 * |                  | ----+
 * | data             |     |
 * | ....             |     | SMR_LOG_FILE_DATA_SIZE
 * |                  | ----+
 * +------------------+
 * | logCheckSum      | master checksum  --+
 * +------------------+                    | SMR_LOG_FILE_CHECKSUM_SIZE
 * | ....             | data checksum -----+
 * +------------------+
 *
 * master checksum:
 *   - off: offset of data checksum record
 *   - checksum: crc16 checksum of data-checksum records upto 'off' (exclusive)
 *
 * data checksum:
 * represend a page whose size  is 1<<MSR_LOG_PAGE_BITS
 *   - off: offset of within a page (0 start)
 *   - checksum: crc16 checksum of data page upto 'off' (exclusive)
 */

typedef struct logChecksum
{
  short off;			// offset
  unsigned short checksum;	// use crc16
} logChecksum;

/* constants */
#define SMR_LOG_PAGE_BITS           12
#define SMR_LOG_PAGE_SIZE           (1<<SMR_LOG_PAGE_BITS)
#define SMR_LOG_FILE_DATA_BITS      26
#define SMR_LOG_FILE_DATA_SIZE      (1<<SMR_LOG_FILE_DATA_BITS)
//+1 for master checksum
#define SMR_LOG_NUM_CHECKSUM        (1+(1<<(SMR_LOG_FILE_DATA_BITS-SMR_LOG_PAGE_BITS)))
#define SMR_LOG_FILE_CHECKSUM_SIZE  (sizeof(logChecksum) * SMR_LOG_NUM_CHECKSUM)
#define SMR_LOG_FILE_ACTUAL_SIZE    (SMR_LOG_FILE_DATA_SIZE + SMR_LOG_FILE_CHECKSUM_SIZE)

/* helper macro */
#define seq_round_down(seq)      ((seq) & ~((long long)SMR_LOG_FILE_DATA_SIZE - 1))
#define seq_round_up(seq)        seq_round_down((seq) + SMR_LOG_FILE_DATA_SIZE)

/* 
 * somewhat hacky error notifiation
 * - embed file/line/errno. for error related with log
 */
#define _make_errno(f,l,e) (-1*((((f) * 10000 + (l)))*1000 + (e)))

#define ERRNO_POINT()  do {                                                                 \
  errno = errno < 0 ?  errno : _make_errno(ERRNO_FILE_ID, __LINE__, errno);                 \
} while (0)
#define ADDR_FILE_ID    1
#define DISK_FILE_ID    2
#define LOG_FILE_ID     3
#define MEM_FILE_ID     4
#define RECOVER_FILE_ID 5
#define SCAN_FILE_ID    6
#define UTIL_FILE_ID    7
#define BE_FILE_ID      8

//special errno (use 99 as ERRNO_FILE_ID)
#define ERRNO_NO_MASTER_FILE _make_errno(99, 0, 0)
#define ERRNO_NO_SPACE _make_errno(99, 1, 0)
#define ERRNO_NO_ENTRY _make_errno(99, 2, 0)
#define ERRNO_OFFSET_EXCEED _make_errno(99, 3, 0)

/* handle definition */
typedef struct smrLog_s smrLog;

/* address */
typedef enum
{
  IN_NONE = 0,
  IN_BUF = 1,			/* in master shared memory buffer */
  IN_SHM = 2,			/* in system shared memory i.e. /dev/shm */
  IN_DISK = 3			/* in disk */
} addrLoc;

typedef struct smrLogAddr_ smrLogAddr;
struct smrLogAddr_
{
  addrLoc loc;
  long long seq;
  void *addr;
  void *ref;
};
#define init_smr_log_addr(a) do {  \
  (a)->loc = IN_NONE;              \
  (a)->seq = 0LL;                  \
  (a)->addr = NULL;                \
  (a)->ref = NULL;                 \
} while(0)

/* log_recover.c */
extern int smrlog_recover (smrLog * handle, long long *min_seq,
			   long long *max_seq, long long *msg_min_seq,
			   long long *msg_max_seq, long long *max_commit_seq);
/* log_log.c */
extern int smrlog_open_master (char *basedir);
extern int smrlog_unlink_master (char *basedir);
extern int smrlog_sync_master (char *basedir);
extern int smrlog_info_master (char *basedir, char **buf);
extern smrLog *smrlog_init (char *basedir);
extern void smrlog_destroy (smrLog * handle);
extern int smrlog_remove_one (smrLog * handle, long long upper,
			      int gap_in_sec, int *removed,
			      long long *removed_seq, int *has_more);
extern int smrlog_purge_all (smrLog * handle);
extern int smrlog_purge_after (smrLog * handle, long long seq_inclusive);
extern int smrlog_os_decache (smrLog * handle, long long seq);
extern smrLogAddr *smrlog_read_mmap (smrLog * handle, long long seq);
extern smrLogAddr *smrlog_write_mmap (smrLog * handle, long long seq,
				      int create);
extern smrLogAddr *smrlog_get_disk_mmap (smrLog * handle, long long seq);
extern int smrlog_sync_upto (smrLog * handle, long long upto);

/* log_addr.c */
extern void smrlog_munmap (smrLog * handle, smrLogAddr * addr);
extern int smrlog_append (smrLog * handle, smrLogAddr * addr, char *buf,
			  int sz);
extern int smrlog_sync (smrLog * handle, smrLogAddr * addr);
extern int smrlog_sync_maps (smrLog * handle, smrLogAddr * src_addr,
			     smrLogAddr * dest_addr);
extern int smrlog_sync_partial (smrLog * handle, smrLogAddr * addr, int from,
				int to, int sync);
extern int smrlog_finalize (smrLog * handle, smrLogAddr * addr);
extern int smrlog_willlog (smrLog * handle, smrLogAddr * addr, int off,
			   int size);
extern int smrlog_get_offset (smrLog * handle, smrLogAddr * addr);
extern int smrlog_get_remain (smrLog * handle, smrLogAddr * addr);
extern int smrlog_get_buf (smrLog * handle, smrLogAddr * addr, char **buf,
			   int *avail);
extern int smrlog_is_finalized (smrLog * handle, smrLogAddr * addr);
extern logChecksum *smrlog_get_master_checksum (smrLog * handle,
						smrLogAddr * addr);
extern logChecksum *smrlog_get_checksums (smrLog * handle, smrLogAddr * addr,
					  int *count);
/* log_scan.c */
// scanner returns 1 to continue, 0 to quit, < 0 for error 
typedef int (*smrlog_scanner) (void *arg, long long seq, long long timestamp,
			       int hash, unsigned char *buf, int size);
// return -1 when error, 0 or 1 if success 
extern int smrlog_scan (smrLog * smrlog, long long start, long long end,
			smrlog_scanner scanner, void *arg);
#endif

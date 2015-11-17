#include "redis.h"
#include "dump_plugin.h"
#include "lzf.h"
#include "endianconv.h"
#include "zipmap.h"
#include "crc16.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>

#define SMR_SCAN_CONTINUE 1
#define SMR_SCAN_QUIT 0
#define SMR_SCAN_ERROR -1

/*-----------------------------------------------------------------------------
 * RDB Operations *
 *----------------------------------------------------------------------------*/
static int rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

static sds rdbLoadInteger(rio *rdb, int enctype) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        redisPanic("Unknown RDB integer encoding type");
    }
    return sdsfromlonglong(val);
}

static sds rdbLoadLzfString(rio *rdb) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    if ((clen = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((c = zmalloc(clen)) == NULL) goto err;
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    if (rioRead(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) == 0) goto err;
    zfree(c);
    return val;
err:
    zfree(c);
    sdsfree(val);
    return NULL;
}

static sds rdbLoadStringRaw(rio *rdb) { 
    int isencoded;
    uint32_t len;
    sds val;

    len = rdbLoadLen(rdb, &isencoded);
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return rdbLoadInteger(rdb,len);
        case REDIS_RDB_ENC_LZF:
            return rdbLoadLzfString(rdb);
        default:
            redisPanic("Unknown RDB encoding type");
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;
    val = sdsnewlen(NULL,len);
    if (len && rioRead(rdb,val,len) == 0) {
        sdsfree(val);
        return NULL;
    }
    return val;
}

static int rdbLoadMetaInfo(char *filename)
{
    int type, rdbver;
    char buf[1024];
    FILE *fp;
    rio rdb;
    robj *key = NULL, *val = NULL;
    int fd;

    fp = fopen(filename,"r");
    if (!fp) {
        errno = ENOENT;
        return REDIS_ERR;
    }
    rioInitWithFile(&rdb,fp);
    if (rioRead(&rdb,buf,9) == 0) goto err;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return REDIS_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > REDIS_RDB_VERSION) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return REDIS_ERR;
    }

    /* smr seqnum */
    if ((type = rdbLoadType(&rdb)) == -1) goto err;
    if (type != REDIS_RDB_TYPE_STRING) goto err;
    if ((key = rdbLoadStringObject(&rdb)) == NULL) goto err;
    if ((val = rdbLoadStringObject(&rdb)) == NULL) goto err;
    if (compareStringObjects(key, shared.db_version)) goto err;
    getLongLongFromObject(val, &server.smr_seqnum);
    decrRefCount(key);
    decrRefCount(val);
    key = NULL;
    val = NULL;

    /* smr mstime */
    if ((type = rdbLoadType(&rdb)) == -1) goto err;
    if (type != REDIS_RDB_TYPE_STRING) goto err;
    if ((key = rdbLoadStringObject(&rdb)) == NULL) goto err;
    if ((val = rdbLoadStringObject(&rdb)) == NULL) goto err;
    if (compareStringObjects(key, shared.db_smr_mstime)) goto err;
    getLongLongFromObject(val, &server.smr_mstime);
    decrRefCount(key);
    decrRefCount(val);
    key = NULL;
    val = NULL;

    /* migrate slot (optional) */
    sdsfree(server.migrate_slot);
    sdsfree(server.migclear_slot);
    server.migrate_slot = NULL;
    server.migclear_slot = NULL;
    do {
        if ((type = rdbLoadType(&rdb)) == -1) break;
        if (type != REDIS_RDB_TYPE_STRING) break;
        if ((key = rdbLoadStringObject(&rdb)) == NULL) break;
        if ((val = rdbLoadStringObject(&rdb)) == NULL) break;
        if (!compareStringObjects(key, shared.db_migrate_slot)) {
            if (val->encoding == REDIS_ENCODING_INT) goto err;
            assert(val->encoding != REDIS_ENCODING_INT);
            server.migrate_slot = sdsdup(val->ptr);
        } else if (!compareStringObjects(key, shared.db_migclear_slot)) {
            if (val->encoding == REDIS_ENCODING_INT) goto err;
            assert(val->encoding != REDIS_ENCODING_INT);
            server.migclear_slot = sdsdup(val->ptr);
        }
    } while (0);
    if (key != NULL) decrRefCount(key);
    if (val != NULL) decrRefCount(val);
    key = NULL;
    val = NULL;

    /* Applying fadvise, in order to not occupying cached memory */
    fd = fileno(fp);
    if (fdatasync(fd) != 0) goto err;
    if (posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0) goto err;

    fclose(fp);
    return REDIS_OK;

err:
    if (key != NULL) decrRefCount(key);
    if (val != NULL) decrRefCount(val);

    fclose(fp);
    return REDIS_ERR;
}

static sds getProperRdbForTargetTime(sds rdb_dir, long long target_time)
{
    DIR *dir;
    struct dirent *entry;
    sds rdbname, max_possible_rdbname = NULL;
    long long max_possible_time = 0;

    dir = opendir(rdb_dir);
    if (dir == NULL) {
        redisLog(REDIS_WARNING, "Can't not open directory during rdbSave.");
        return NULL;
    }

    rdbname = sdsempty();
    while ((entry = readdir(dir)) != NULL) {
        if (!strncmp(entry->d_name, "dump", 4)
            && !strncmp(entry->d_name+strlen(entry->d_name)-4,".rdb",4))
        {
            struct stat filestat;
            
            sdsclear(rdbname);
            rdbname = sdscatprintf(rdbname, "%s/%s", rdb_dir, entry->d_name);
            stat(rdbname, &filestat);
            if (!S_ISREG(filestat.st_mode)) continue;

            if (rdbLoadMetaInfo(rdbname) == REDIS_ERR) continue;

            if (server.smr_mstime <= target_time && server.smr_mstime > max_possible_time) {
                max_possible_time = server.smr_mstime;
                if (max_possible_rdbname == NULL) max_possible_rdbname = sdsempty();
                max_possible_rdbname = sdscpy(max_possible_rdbname, rdbname);
            }
        }
    }
    sdsfree(rdbname);
    closedir(dir);
    return max_possible_rdbname;
}

#define MAX_HEADERS 16
static int rdbLoadDirtyObjects(char *filename, robj *dirty_key_set)
{
    uint32_t dbid;
    int type, rdbver;
    redisDb *db = server.db+0;
    char buf[1024];
    long long now = 0LL;
    long long hdr_count = 0LL;
    FILE *fp;
    int fd;
    uint64_t w_count = 0;
    off_t last_flush = 0;
    off_t curr_off = 0;
    rio rdb;

    server.smr_seqnum = 0;
    server.smr_mstime = 0;
    sdsfree(server.migrate_slot);
    server.migrate_slot = NULL;
    sdsfree(server.migclear_slot);
    server.migclear_slot = NULL;

    fp = fopen(filename,"r");
    if (!fp) {
        redisLog(REDIS_WARNING, "Failed opening .rdb for loading: %s",
            strerror(errno));
        return REDIS_ERR;
    }
    fd = fileno(fp);

    rioInitWithFile(&rdb, fp);
    if (server.rdb_checksum)
        rdb.update_cksum = rioGenericUpdateChecksum;
    if (rioRead(&rdb,buf,9) == 0) goto err;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return REDIS_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > REDIS_RDB_VERSION) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return REDIS_ERR;
    }

    while(1) {
        robj *key, *val;
        long long expiretime = -1;

        /* Read type. */
        if ((type = rdbLoadType(&rdb)) == -1) goto err;
        if (type == REDIS_RDB_OPCODE_EXPIRETIME) {
            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto err;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto err;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliesconds. */
            expiretime *= 1000;
        } else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
            /* Milliseconds precision expire times introduced with RDB
             * version 3. */
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto err;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto err;
        }

        if (type == REDIS_RDB_OPCODE_EOF)
            break;

        /* Handle SELECT DB opcode as a special case */
        if (type == REDIS_RDB_OPCODE_SELECTDB) {
            if ((dbid = rdbLoadLen(&rdb,NULL)) == REDIS_RDB_LENERR)
                goto err;
            if (dbid >= (unsigned)server.dbnum) {
                redisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
                exit(1);
            }
            db = server.db+dbid;
            continue;
        }
        /* Read key */
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto err;
        /* Read value */
        if ((val = rdbLoadObject(type,&rdb,key)) == NULL) goto err;

        /* Applying fadvise, in order to not occupying cached memory */
        w_count++;
        if(w_count % 32 == 0 && (curr_off = rioTell(&rdb)) - last_flush >= (32*1024*1024)) {
            if (fdatasync(fd) != 0) goto err;
            if (posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0) goto err;
            last_flush = curr_off;
        }

        /* Save special objects */
        if (hdr_count < MAX_HEADERS) {
            hdr_count++;
            if (!compareStringObjects(key, shared.db_version)) {
                getLongLongFromObject(val, &server.smr_seqnum);
                decrRefCount(key);
                decrRefCount(val);
                continue;
            }
            if (!compareStringObjects(key, shared.db_smr_mstime)) {
                getLongLongFromObject(val, &server.smr_mstime);
                now = server.smr_mstime;
                decrRefCount(key);
                decrRefCount(val);
                continue;
            }
            if (!compareStringObjects(key, shared.db_migrate_slot)) {
                assert(val->encoding != REDIS_ENCODING_INT);
                server.migrate_slot = sdsdup(val->ptr);
                decrRefCount(key);
                decrRefCount(val);
                continue;
            }
            if (!compareStringObjects(key, shared.db_migclear_slot)) {
                assert(val->encoding != REDIS_ENCODING_INT);
                server.migclear_slot = sdsdup(val->ptr);
                decrRefCount(key);
                decrRefCount(val);
                continue;
            }
        }

        if (expiretime != -1 && expiretime < now) {
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }

        if (setTypeIsMember(dirty_key_set, key)) {
            dbAdd(db,key,val);
        } else {
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }

        if (expiretime != -1) setExpire(db,key,expiretime);
        decrRefCount(key);
    }

    /* Verify the checksum if RDB version is >= 5 */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        if (rioRead(&rdb,&cksum,8) == 0) goto err;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            redisLog(REDIS_WARNING,"RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            redisLog(REDIS_WARNING,"Wrong RDB checksum. Aborting now.");
            return REDIS_ERR;
        }
    }

    fclose(fp);
    return REDIS_OK;

err: /* unexpected end of file is handled here with a fatal exit */
    fclose(fp);
    redisLog(REDIS_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    return REDIS_ERR; /* Just to avoid warning */
}

static int rdbApplyChanges(char *in_file, char *out_file, robj *dirty_key_set)
{
    uint32_t dbid;
    int type, rdbver;
    long long now = server.smr_mstime;
    long long hdr_count = 0LL;
    char buf[1024];
    char tmpfile[256];
    char magic[10];
    FILE *in_fp = NULL, *out_fp = NULL;
    rio in_rdb, out_rdb;
    dictIterator *di = NULL;
    dictEntry *de;
    int j;
    uint64_t cksum;
    int in_fd, out_fd;
    uint64_t in_w_count = 0, out_w_count = 0;
    off_t in_last_flush = 0, out_last_flush = 0;
    off_t in_curr_off = 0, out_curr_off = 0;

    /* Original RDB file */
    in_fp = fopen(in_file,"r");
    if (!in_fp) {
        redisLog(REDIS_WARNING, "Failed opening .rdb for saving: %s",
            strerror(errno));
        errno = ENOENT;
        return REDIS_ERR;
    }
    in_fd = fileno(in_fp);

    rioInitWithFile(&in_rdb,in_fp);
    if (server.rdb_checksum)
        in_rdb.update_cksum = rioGenericUpdateChecksum;
    if (rioRead(&in_rdb,buf,9) == 0) goto err;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(in_fp);
        redisLog(REDIS_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return REDIS_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > REDIS_RDB_VERSION) {
        fclose(in_fp);
        redisLog(REDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return REDIS_ERR;
    }

    /* Output RDB File */
    snprintf(tmpfile,256,"temp-dump-util-%d.rdb", (int) getpid());
    out_fp = fopen(tmpfile,"w");
    if (!out_fp) {
        fclose(in_fp);
        redisLog(REDIS_WARNING, "Failed opening .rdb for saving: %s",
            strerror(errno));
        errno = ENOENT;
        return REDIS_ERR;
    }
    out_fd = fileno(out_fp);

    rioInitWithFile(&out_rdb,out_fp);
    if (server.rdb_checksum)
        out_rdb.update_cksum = rioGenericUpdateChecksum;
    snprintf(magic,sizeof(magic),"REDIS%04d",REDIS_RDB_VERSION);
    if (rdbWriteRaw(&out_rdb,magic,9) == -1) goto err;
    if (rdbSaveType(&out_rdb, REDIS_RDB_TYPE_STRING) == -1) goto err;
    if (rdbSaveStringObject(&out_rdb, shared.db_version) == -1) goto err;
    if (rdbSaveLongLongAsStringObject(&out_rdb, server.smr_seqnum) == -1) goto err;
    if (rdbSaveType(&out_rdb, REDIS_RDB_TYPE_STRING) == -1) goto err;
    if (rdbSaveStringObject(&out_rdb, shared.db_smr_mstime) == -1) goto err;
    if (rdbSaveLongLongAsStringObject(&out_rdb, server.smr_mstime) == -1) goto err;
    if (server.migrate_slot) {
        robj val;
        if (rdbSaveType(&out_rdb, REDIS_RDB_TYPE_STRING) == -1) goto err;
        if (rdbSaveStringObject(&out_rdb, shared.db_migrate_slot) == -1) goto err;
        initStaticStringObject(val,server.migrate_slot);
        if (rdbSaveObject(&out_rdb,&val) == -1) goto err;
    }
    if (server.migclear_slot) {
        robj val;
        if (rdbSaveType(&out_rdb, REDIS_RDB_TYPE_STRING) == -1) goto err;
        if (rdbSaveStringObject(&out_rdb, shared.db_migclear_slot) == -1) goto err;
        initStaticStringObject(val,server.migclear_slot);
        if (rdbSaveObject(&out_rdb,&val) == -1) goto err;
    }

    /* Copy Non-dirty objects of Original RDB File */
    while(1) {
        robj *key, *val;
        long long expiretime = -1;

        /* Read type. */
        if ((type = rdbLoadType(&in_rdb)) == -1) goto err;
        if (type == REDIS_RDB_OPCODE_EXPIRETIME) {
            if ((expiretime = rdbLoadTime(&in_rdb)) == -1) goto err;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&in_rdb)) == -1) goto err;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliesconds. */
            expiretime *= 1000;
        } else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
            /* Milliseconds precision expire times introduced with RDB
             * version 3. */
            if ((expiretime = rdbLoadMillisecondTime(&in_rdb)) == -1) goto err;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&in_rdb)) == -1) goto err;
        }

        if (type == REDIS_RDB_OPCODE_EOF)
            break;

        /* Handle SELECT DB opcode as a special case */
        if (type == REDIS_RDB_OPCODE_SELECTDB) {
            if ((dbid = rdbLoadLen(&in_rdb,NULL)) == REDIS_RDB_LENERR)
                goto err;
            if (dbid >= (unsigned)server.dbnum) {
                redisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
                goto err;
            }
            /* Write the SELECT DB opcode */
            if (rdbSaveType(&out_rdb,REDIS_RDB_OPCODE_SELECTDB) == -1) goto err;
            if (rdbSaveLen(&out_rdb,dbid) == -1) goto err;
            continue;
        }
        /* Read key */
        if ((key = rdbLoadStringObject(&in_rdb)) == NULL) goto err;
        /* Read value */
        if ((val = rdbLoadObject(type,&in_rdb,key)) == NULL) goto err;

        /* Save special objects */
        if (hdr_count < MAX_HEADERS) {
            hdr_count++;
            if (!compareStringObjects(key, shared.db_version)
                || !compareStringObjects(key, shared.db_smr_mstime)
                || !compareStringObjects(key, shared.db_migrate_slot)
                || !compareStringObjects(key, shared.db_migclear_slot)) 
            {
                decrRefCount(key);
                decrRefCount(val);
                continue;
            }
        }

        if (expiretime != -1 && expiretime < now) {
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }

        if (!setTypeIsMember(dirty_key_set, key)) {
            if (rdbSaveKeyValuePair(&out_rdb,key,val,expiretime,now) == -1) goto err;
        }
        decrRefCount(key);
        decrRefCount(val);

        /* Applying fadvise, in order to not occupying cached memory */
        in_w_count++;
        if(in_w_count % 32 == 0 && (in_curr_off = rioTell(&in_rdb)) - in_last_flush >= (32*1024*1024)) {
            if (fdatasync(in_fd) != 0) goto err;
            if (posix_fadvise(in_fd, 0, 0, POSIX_FADV_DONTNEED) != 0) goto err;
            in_last_flush = in_curr_off;
        }
        out_w_count++;
        if(out_w_count % 32 == 0 && (out_curr_off = rioTell(&out_rdb)) - out_last_flush >= (32*1024*1024)) {
            if (fdatasync(out_fd) != 0) goto err;
            if (posix_fadvise(out_fd, 0, 0, POSIX_FADV_DONTNEED) != 0) goto err;
            out_last_flush = out_curr_off;
        }
    }

    /* Verify the checksum of Original RDB File */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = in_rdb.cksum;

        if (rioRead(&in_rdb,&cksum,8) == 0) goto err;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            redisLog(REDIS_WARNING,"RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            redisLog(REDIS_WARNING,"Wrong RDB checksum. Aborting now.");
            goto err;
        }
    }
    
    fclose(in_fp);
    in_fp = NULL;

    /* Append Dirty objects */
    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);
        if (!di) {
            fclose(out_fp);
            return REDIS_ERR;
        }

        /* Write the SELECT DB opcode */
        if (rdbSaveType(&out_rdb,REDIS_RDB_OPCODE_SELECTDB) == -1) goto err;
        if (rdbSaveLen(&out_rdb,j) == -1) goto err;

        /* Iterate this DB writing every entry */
        while((de = dictNext(di)) != NULL) {
            sds keystr = dictGetKey(de);
            robj key, *o = dictGetVal(de);
            long long expire;
            
            initStaticStringObject(key,keystr);
            expire = getExpire(db,&key);
            if (rdbSaveKeyValuePair(&out_rdb,&key,o,expire,now) == -1) goto err;

            /* Applying fadvise, in order to not occupying cached memory */
            out_w_count++;
            if(out_w_count % 32 == 0 && (out_curr_off = rioTell(&out_rdb)) - out_last_flush >= (32*1024*1024)) {
                if (fdatasync(out_fd) != 0) goto err;
                if (posix_fadvise(out_fd, 0, 0, POSIX_FADV_DONTNEED) != 0) goto err;
                out_last_flush = out_curr_off;
            }
        }
        dictReleaseIterator(di);
    }
    di = NULL; /* So that we don't release it again on error. */

    /* EOF opcode */
    if (rdbSaveType(&out_rdb,REDIS_RDB_OPCODE_EOF) == -1) goto err;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. */
    cksum = out_rdb.cksum;
    memrev64ifbe(&cksum);
    rioWrite(&out_rdb,&cksum,8);

    fflush(out_fp);
    fsync(fileno(out_fp));
    fclose(out_fp);
    out_fp = NULL;

    if (rename(tmpfile,out_file) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }

    return REDIS_OK;
err:
    if (in_fp) fclose(in_fp);
    if (out_fp) fclose(out_fp);
    unlink(tmpfile);
    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

/*-----------------------------------------------------------------------------
 * Time target dump
 *----------------------------------------------------------------------------*/
struct s_key_scan_arg
{
    long long target_time;
    robj *dirty_key_set;
    long long key_scan_count;
};

static int dirty_key_scanner(void *arg, long long seq, long long timestamp, int hash,
                        unsigned char *buf, int size)
{
    struct s_key_scan_arg *key_scan_arg = (struct s_key_scan_arg *) arg;
    long long target_time = key_scan_arg->target_time;
    robj *set = key_scan_arg->dirty_key_set;
    redisClient *c;

    if (timestamp > target_time) return SMR_SCAN_QUIT;

    key_scan_arg->key_scan_count++;

    /* Skip Non-write commands */
    if (size == 1 && (*buf == 'C' || *buf == 'D' || *buf == 'R')) {
        return SMR_SCAN_CONTINUE;
    }


    /* Set fake client */
    c = server.smrlog_client;
    sdsclear(c->querybuf);
    c->querybuf = sdsMakeRoomFor(c->querybuf, size);
    memcpy(c->querybuf, buf, size);
    sdsIncrLen(c->querybuf, size);

    /* Parse query */
    if (c->querybuf[0] == '*') {
        if (processMultibulkBuffer(c) != REDIS_OK) goto smr_continue;
    } else {
        if (processInlineBuffer(c) != REDIS_OK) goto smr_continue;
    }

    /* Add dirty keys */
    if (c->argc > 0) {
        struct redisCommand *cmd;
        int i;

        cmd = lookupCommand(c->argv[0]->ptr);
        if (cmd == NULL || cmd->firstkey == 0) goto smr_continue;

        i = cmd->firstkey;
        while (1) {
            setTypeAdd(set, c->argv[i]);

            if (i == cmd->lastkey) break;
            if (cmd->lastkey > 0 && i > cmd->lastkey) {
                redisLog(REDIS_WARNING, "Incompatible command specification:%s", cmd->name);
                break;
            }
            i += cmd->keystep;
            if (cmd->lastkey < 0 &&  i >= c->argc) break;
        }
    }

smr_continue:
    resetClient(c);
    zfree(c->argv);
    c->argv = NULL;

    return SMR_SCAN_CONTINUE;
}

static int data_scanner(void *arg, long long seq, long long timestamp, int hash,
                        unsigned char *buf, int size)
{
    long long target_time = *((long long *) arg);
    redisClient *c;

    if (timestamp > target_time) return SMR_SCAN_QUIT;
    server.smr_mstime = timestamp;

    if (size == 1 && (*buf == 'C' || *buf == 'D' || *buf == 'R')) {
        if (*buf == 'D') {
            server.smr_oom_until = timestamp + REDIS_OOM_DURATION_MS;
        }
        goto smr_continue;
    }

    c = server.smrlog_client;
    sdsclear(c->querybuf);
    c->querybuf = sdsMakeRoomFor(c->querybuf, size);
    memcpy(c->querybuf, buf, size);
    sdsIncrLen(c->querybuf, size);
    processInputBuffer(c);

    resetClient(c);
    zfree(c->argv);
    c->argv = NULL;

smr_continue:
    server.smr_seqnum = seq + size;
    return SMR_SCAN_CONTINUE;
}

int timeTargetDump(int argc, sds *argv) {
    smrLog *smrlog;
    long long target_time = atoll(argv[1]) * 1000;
    sds log_dir = argv[2];
    sds rdb_dir = argv[3];
    sds outfile = argv[4];
    sds rdbname;
    struct s_key_scan_arg key_scan_arg;
    int ret;

    /* initialize */
    redisLog(REDIS_NOTICE, "target time:%lld, "
                           "smrlog dir:%s, "
                           "rdbfile dir:%s, "
                           "output file:%s", 
                           target_time,
                           log_dir,
                           rdb_dir,
                           outfile);
    server.cluster_mode = 1;
    initServer();

    /* Select rdb file */
    rdbname = getProperRdbForTargetTime(rdb_dir, target_time);
    if (rdbname == NULL) {
        redisLog(REDIS_NOTICE, "Failed to initialize from rdb_dir:%s\n", rdb_dir);
        exit(1);
    }

    /* Phase 1, find out dirty keys */
    if (rdbLoadMetaInfo(rdbname) == REDIS_ERR) {
        redisLog(REDIS_WARNING, "Fatal error loading the DB(%s).:%s",rdbname, strerror(errno));
        return REDIS_ERR;
    }

    redisLog(REDIS_NOTICE, "Select rdb file:%s, smr_seqnum:%lld, smr_mstime:%lld\n", 
                            rdbname, server.smr_seqnum, server.smr_mstime);


    smrlog = smrlog_init (log_dir);
    if (smrlog == NULL) {
        redisLog(REDIS_NOTICE, "Failed to initialize from log_dir:%s\n", log_dir);
        return REDIS_ERR;
    }

    key_scan_arg.target_time = target_time;
    key_scan_arg.dirty_key_set = createSetObject();
    key_scan_arg.key_scan_count = 0;
    ret = smrlog_scan(smrlog, server.smr_seqnum, -1, dirty_key_scanner, &key_scan_arg);
    if (ret < 0) {
        redisLog(REDIS_NOTICE, "SMR log scan failed:%d", ret);
        smrlog_destroy(smrlog);
    }

    redisLog(REDIS_NOTICE, "Total SMR Log scanned:%lld", key_scan_arg.key_scan_count);
    smrlog_destroy(smrlog);

    /* Phase 2, Load dirty objects and play smr log */
    ret = rdbLoadDirtyObjects(rdbname, key_scan_arg.dirty_key_set);
    if (ret == REDIS_ERR) {
        redisLog(REDIS_NOTICE, "Loading dirty objects failed");
        return REDIS_ERR;
    }

    /* Play smr log */
    smrlog = smrlog_init (log_dir);
    if (smrlog == NULL) {
        redisLog(REDIS_NOTICE, "Failed to initialize from log_dir:%s\n", log_dir);
        return REDIS_ERR;
    }

    ret = smrlog_scan(smrlog, server.smr_seqnum, -1, data_scanner, &target_time);
    if (ret < 0) {
        redisLog(REDIS_NOTICE, "SMR log scan failed:%d", ret);
        smrlog_destroy(smrlog);
    }

    smrlog_destroy(smrlog);

    ret = rdbApplyChanges(rdbname, outfile, key_scan_arg.dirty_key_set);
    if (ret == REDIS_ERR) {
        redisLog(REDIS_NOTICE, "Replacing dirty objects failed");
        return REDIS_ERR;
    }

    decrRefCount(key_scan_arg.dirty_key_set);
    sdsfree(rdbname);

    return REDIS_OK;
}

/*-----------------------------------------------------------------------------
 * Dump Iterator
 *----------------------------------------------------------------------------*/
static int getPluginType(int type) {
    switch (type) {
        case REDIS_RDB_TYPE_STRING:
            return PLUGIN_RDB_TYPE_STRING;

        case REDIS_RDB_TYPE_LIST:
        case REDIS_RDB_TYPE_LIST_ZIPLIST:
            return PLUGIN_RDB_TYPE_LIST;

        case REDIS_RDB_TYPE_SET:
        case REDIS_RDB_TYPE_SET_INTSET:
            return PLUGIN_RDB_TYPE_SET;

        case REDIS_RDB_TYPE_ZSET:
        case REDIS_RDB_TYPE_ZSET_ZIPLIST:
            return PLUGIN_RDB_TYPE_ZSET;

        case REDIS_RDB_TYPE_HASH:
        case REDIS_RDB_TYPE_HASH_ZIPMAP:
        case REDIS_RDB_TYPE_HASH_ZIPLIST:
            return PLUGIN_RDB_TYPE_HASH;
        
        case REDIS_RDB_TYPE_SSS:
            return PLUGIN_RDB_TYPE_SSS;
    }
    return type;
}

static int dumpIterator(int argc, sds *argv) {
    sds dump = argv[1];
    sds plugin = argv[2];
    sds plugin_args = argv[3];
    sds *plugin_argv;
    int plugin_argc;
    void *handle, *ctx;
    struct dump_plugin_callback *cb;
    uint32_t dbid;
    int type, rdbver;
    char buf[1024];
    FILE *fp;
    int fd;
    uint64_t w_count = 0;
    off_t last_flush = 0;
    off_t curr_off = 0;
    rio rdb;
    long long hdr_count = 0LL;
    long long smr_mstime = -1LL;
    long long smr_seqnum = -1LL;
    sds migrate_slot = NULL;
    sds migclear_slot = NULL;
    int skip = 0;
    
    /* Initialize necessary shared objects */
    createSharedObjects();

    plugin_argv = sdssplitargs(plugin_args, &plugin_argc);

    handle = dlopen(plugin, RTLD_NOW);
    if (!handle) {
        redisLog(REDIS_WARNING, "plugin open error, %s\n", dlerror());
        return REDIS_ERR;
    }
    cb = (struct dump_plugin_callback *) dlsym(handle, "callback");

    if (cb->initialize(plugin_argc, plugin_argv, &ctx) != RET_OK) return REDIS_ERR;
    
    if (!(fp = fopen(dump,"r"))) return REDIS_ERR;
    rioInitWithFile(&rdb,fp);
    fd = fileno(fp);

    if (rioRead(&rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Wrong signature trying to load DB from file");
        return REDIS_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > REDIS_RDB_VERSION) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        return REDIS_ERR;
    }

    while(1) {
        long long expiretime = -1;
        sds key;

        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        if (type == REDIS_RDB_OPCODE_EXPIRETIME) {
            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliesconds. */
            expiretime *= 1000;
        } else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
            /* Milliseconds precision expire times introduced with RDB
             * version 3. */
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        }

        if (type == REDIS_RDB_OPCODE_EOF)
            break;

        /* Handle SELECT DB opcode as a special case */
        if (type == REDIS_RDB_OPCODE_SELECTDB) {
            if ((dbid = rdbLoadLen(&rdb,NULL)) == REDIS_RDB_LENERR) goto eoferr;
            continue;
        }
        /* Read key */
        if ((key = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;

        /* Skip meta info of RDB */
        if (hdr_count < MAX_HEADERS) {
            hdr_count++;
            if (!sdscmp(key, shared.db_version->ptr)
                || !sdscmp(key, shared.db_smr_mstime->ptr)
                || !sdscmp(key, shared.db_migrate_slot->ptr)
                || !sdscmp(key, shared.db_migclear_slot->ptr))
            {
                robj key_obj;
                robj *val;

                initStaticStringObject(key_obj, key);
                if ((val = rdbLoadObject(type, &rdb, &key_obj)) == NULL) goto eoferr;

                if (!sdscmp(key, shared.db_version->ptr)) {
                    getLongLongFromObject(val, &smr_seqnum);
                } else if (!sdscmp(key, shared.db_smr_mstime->ptr)) {
                    getLongLongFromObject(val, &smr_mstime);
                } else if (!sdscmp(key, shared.db_migrate_slot->ptr)) {
                    migrate_slot = sdsdup(val->ptr);
                } else if (!sdscmp(key, shared.db_migclear_slot->ptr)) {
                    migclear_slot = sdsdup(val->ptr);
                }

                if (smr_mstime != -1 && smr_seqnum != -1) {
                    cb->dumpinfo(ctx, rdbver, smr_seqnum, smr_mstime);
                }

                sdsfree(key);
                decrRefCount(val);
                continue;
            }
        }

        skip = 0;
        if (migrate_slot || migclear_slot) {
            int hashsize = NBASE_ARC_KS_SIZE;
            int keyhash = crc16 (key, sdslen(key), 0) % hashsize;
            if ((migrate_slot && rdbGetBit (migrate_slot, keyhash))
                || (migclear_slot && rdbGetBit (migclear_slot, keyhash))) 
            {
                skip = 1;
            }
        }


        /* Read value */
        if (type == REDIS_RDB_TYPE_STRING) {
            sds val;

            if ((val = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;

            if (!skip) cb->begin_key(ctx, getPluginType(type), key, sdslen(key), expiretime);
            sdsfree(key);
            if (!skip) cb->string_val(ctx, val, sdslen(val));
            sdsfree(val);
        } else if (type == REDIS_RDB_TYPE_LIST ||
                   type == REDIS_RDB_TYPE_SET  ||
                   type == REDIS_RDB_TYPE_ZSET ||
                   type == REDIS_RDB_TYPE_HASH)
        {
            uint32_t len;

            if ((len = rdbLoadLen(&rdb, NULL)) == REDIS_RDB_LENERR) goto eoferr;

            if (!skip) cb->begin_key(ctx, getPluginType(type), key, sdslen(key), expiretime);
            sdsfree(key);

            while(len--) {
                sds ele = NULL, field = NULL, value = NULL;
                double score = 0;

                switch (type) {
                    case REDIS_RDB_TYPE_LIST:
                        if ((ele = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                        if (!skip) cb->list_val(ctx, ele, sdslen(ele));
                        break;
                    case REDIS_RDB_TYPE_SET:
                        if ((ele = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                        if (!skip) cb->set_val(ctx, ele, sdslen(ele));
                        break;
                    case REDIS_RDB_TYPE_ZSET:
                        if ((ele = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                        if (rdbLoadDoubleValue(&rdb, &score) == -1) goto eoferr;
                        if (!skip) cb->zset_val(ctx, ele, sdslen(ele), score);
                        break;
                    case REDIS_RDB_TYPE_HASH:
                        if ((field = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                        if ((value = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                        if (!skip) cb->hash_val(ctx, field, sdslen(field), value, sdslen(value));
                        break;
                    default:
                        redisPanic("Unknown type");
                        break;
                }

                sdsfree(ele);
                sdsfree(field);
                sdsfree(value);
            }
        } else if (type == REDIS_RDB_TYPE_SSS) {
            uint32_t len;
            sds ks, s, k, v, idx, t;
            sds prev_ks = NULL, prev_s = NULL, prev_k = NULL;
            int ok, mode, prev_mode = 0;
            long long expire;

            if ((len = rdbLoadLen(&rdb, NULL)) == REDIS_RDB_LENERR) goto eoferr;

            if (len == 0) {
                sdsfree(key);
                continue;
            }

            if (!skip) cb->begin_key(ctx, getPluginType(type), key, sdslen(key), expiretime);
            sdsfree(key);

            while(len--) {
                if ((ks = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                if ((s = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                if ((k = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                if ((idx = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                if ((v = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                if ((t = rdbLoadStringRaw(&rdb)) == NULL) goto eoferr;
                
                if (sdslen(idx) == 1 && !memcmp(idx, "0", 1)) {
                    mode = PLUGIN_SSS_KV_SET;
                } else {
                    redisLog(REDIS_NOTICE, "list mode");
                    mode = PLUGIN_SSS_KV_LIST;
                }

                if (!prev_ks && !prev_s && !prev_k) {
                    if (!skip) cb->begin_sss_collection(ctx, ks, sdslen(ks), s, sdslen(s), k, sdslen(k), mode);
                } else if (sdscmp(ks,prev_ks) || sdscmp(s, prev_s) || sdscmp(k, prev_k)) {
                    if (!skip) cb->end_sss_collection(ctx, prev_mode);
                    if (!skip) cb->begin_sss_collection(ctx, ks, sdslen(ks), s, sdslen(s), k, sdslen(k), mode);
                    
                    if (sdscmp(ks,prev_ks)) sdsfree(prev_ks);
                    if (sdscmp(ks,prev_s)) sdsfree(prev_s);
                    if (sdscmp(ks,prev_k)) sdsfree(prev_k);
                }
                prev_ks = ks;
                prev_s = s;
                prev_k = k;
                prev_mode = mode;

                ok = string2ll(t,sdslen(t),&expire);
                if (!ok) goto eoferr;
                if (expire == LLONG_MAX) {
                    if (!skip) cb->sss_val(ctx, v, sdslen(v), -1);
                } else {
                    if (!skip) cb->sss_val(ctx, v, sdslen(v), expire);
                }

                sdsfree(v);
                sdsfree(idx);
                sdsfree(t);
            }
            if (!skip) cb->end_sss_collection(ctx, prev_mode);

        } else if (type == REDIS_RDB_TYPE_HASH_ZIPMAP  ||
                   type == REDIS_RDB_TYPE_LIST_ZIPLIST ||
                   type == REDIS_RDB_TYPE_SET_INTSET   ||
                   type == REDIS_RDB_TYPE_ZSET_ZIPLIST ||
                   type == REDIS_RDB_TYPE_HASH_ZIPLIST)
        {
            sds aux = rdbLoadStringRaw(&rdb);
            unsigned int len;
            uint32_t is_len;
            unsigned char *zi;  /* ziplist index */
            unsigned char *zl;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;
            int64_t intele;
            sds ele = NULL, field = NULL, value = NULL;
            double score;
            uint32_t i;

            if (aux == NULL) goto eoferr;

            if (!skip) cb->begin_key(ctx, getPluginType(type), key, sdslen(key), expiretime);
            sdsfree(key);

            zl = (unsigned char *) aux;
            switch (type) {
                case REDIS_RDB_TYPE_HASH_ZIPMAP:
                    {
                        unsigned char *fstr, *vstr;
                        unsigned int flen, vlen;
                        zi = zipmapRewind(zl);
                        
                        while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                            if (!skip) cb->hash_val(ctx, (char *) fstr, flen, (char *) vstr, vlen);
                        }
                    }
                    break;
                case REDIS_RDB_TYPE_LIST_ZIPLIST:
                    len = ziplistLen(zl);
                    zi = ziplistIndex(zl, 0);
                    while(len--) {
                        ziplistGet(zi,&vstr,&vlen,&vlong);
                        ele = (vstr) ? sdsnewlen(vstr, vlen) : sdsfromlonglong(vlong);
                        if (!skip) cb->list_val(ctx, ele, sdslen(ele));
                        zi = ziplistNext(zl,zi);

                        sdsfree(ele);
                        ele = NULL;

                        if (zi == NULL) break;
                    }
                    break;
                case REDIS_RDB_TYPE_SET_INTSET:
                    is_len = intsetLen((void *) zl);
                    for (i = 0; i < is_len; i++) {
                        intsetGet((void *) zl, i,&intele);
                        ele = sdsfromlonglong(intele);
                        if (!skip) cb->set_val(ctx, ele, sdslen(ele));
                        
                        sdsfree(ele);
                        ele = NULL;
                    }
                    break;
                case REDIS_RDB_TYPE_ZSET_ZIPLIST:
                    len = ziplistLen(zl) / 2;
                    zi = ziplistIndex(zl,0);
                    while(len--) {
                        ziplistGet(zi,&vstr,&vlen,&vlong);
                        ele = (vstr) ? sdsnewlen(vstr, vlen) : sdsfromlonglong(vlong);
                        zi = ziplistNext(zl,zi);

                        ziplistGet(zi,&vstr,&vlen,&vlong);
                        if (vstr) {
                            char buf[128];

                            memcpy(buf,vstr,vlen);
                            buf[vlen] = '\0';
                            score = strtod(buf,NULL);
                        } else {
                            score = vlong;
                        }
                        zi = ziplistNext(zl,zi);

                        if (!skip) cb->zset_val(ctx, ele, sdslen(ele), score);

                        sdsfree(ele);
                        ele = NULL;

                        if (zi == NULL) break;
                    }
                    break;
                case REDIS_RDB_TYPE_HASH_ZIPLIST:
                    len = ziplistLen(zl) / 2;
                    zi = ziplistIndex(zl, 0);
                    while(len--) {
                        ziplistGet(zi,&vstr,&vlen,&vlong);
                        field = (vstr) ? sdsnewlen(vstr, vlen) : sdsfromlonglong(vlong);
                        zi = ziplistNext(zl,zi);

                        ziplistGet(zi,&vstr,&vlen,&vlong);
                        value = (vstr) ? sdsnewlen(vstr, vlen) : sdsfromlonglong(vlong);
                        zi = ziplistNext(zl,zi);

                        if (!skip) cb->hash_val(ctx, field, sdslen(field), value, sdslen(value));

                        sdsfree(field);
                        field = NULL;
                        sdsfree(value);
                        value = NULL;

                        if (zi == NULL) break;
                    }
                    break;
                default:
                    redisPanic("Unknown RDB type");
                    break;
            }
            sdsfree(aux);
        }
        if (!skip) cb->end_key(ctx, getPluginType(type));

        /* Applying fadvise, in order to not occupying cached memory */
        w_count++;
        if(w_count % 32 == 0 && (curr_off = rioTell(&rdb)) - last_flush >= (32*1024*1024)) {
            if (fdatasync(fd) != 0) goto eoferr;
            if (posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0) goto eoferr;
            last_flush = curr_off;
        }
    }

    cb->finalize(ctx);

    fclose(fp);
    dlclose(handle);
    return REDIS_OK;

eoferr:
    fclose(fp);
    dlclose(handle);
    return REDIS_ERR;
}

/*-----------------------------------------------------------------------------
 * RDB Info
 *----------------------------------------------------------------------------*/
static int rdbInfo(int argc, sds *argv) 
{
    DIR *dir;
    struct dirent *entry;
    sds rdb_dir = argv[1];
    sds rdbname;

    server.cluster_mode = 1;
    initServer();

    dir = opendir(rdb_dir);
    if (dir == NULL) {
        redisLog(REDIS_WARNING, "Can't not open directory during rdbSave.");
        return REDIS_ERR;
    }

    rdbname = sdsempty();
    while ((entry = readdir(dir)) != NULL) {
        if (!strncmp(entry->d_name+strlen(entry->d_name)-4,".rdb",4))
        {
            struct stat filestat;
            
            sdsclear(rdbname);
            rdbname = sdscatprintf(rdbname, "%s/%s", rdb_dir, entry->d_name);
            stat(rdbname, &filestat);
            if (!S_ISREG(filestat.st_mode)) continue;

            if (rdbLoadMetaInfo(rdbname) == REDIS_ERR) {
                redisLog(REDIS_NOTICE, "- Incompatible file(%s)", rdbname);
            } else {
                redisLog(REDIS_NOTICE, "smr_seqnum:%14lld, "
                                       "smr_mstime:%14lld, "
                                       "filename:%s",
                                       server.smr_seqnum,
                                       server.smr_mstime,
                                       rdbname);
            }
        }
    }
    sdsfree(rdbname);
    closedir(dir);

    return REDIS_OK;
}

/*-----------------------------------------------------------------------------
 * Dump-util Executor
 *----------------------------------------------------------------------------*/
static void dumpUtilUsage() {
    fprintf(stderr,"Usage: \n");
    fprintf(stderr,"       ./dump-util --dump <epochtime in sec> <path/to/smrlog_dir> <path/to/rdb_dir> <output filename>\n");
    fprintf(stderr,"       ./dump-util --dump-iterator <path/to/rdbfile> <path/to/plugin> \"<plugin args>\"\n");
    fprintf(stderr,"       ./dump-util --rdb-info <rdb dir>\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./dump-util --dump 1389766796 ../smr/log ../redis out.rdb\n");
    fprintf(stderr,"       ./dump-util --dump-iterator dump.rdb dump2json.so \"dump.json\"\n");
    fprintf(stderr,"       ./dump-util --rdb-info dump.rdb\n");
    exit(1);
}

static int executeCommandFromOptions(sds options) {
    sds *argv;
    int argc;
    int ret;

    options = sdstrim(options, " \t\r\n");
    argv = sdssplitargs(options,&argc);
    sdstolower(argv[0]);

    if (!strcasecmp(argv[0],"dump") && argc == 5) {
        ret = timeTargetDump(argc, argv);
    } else if (!strcasecmp(argv[0],"dump-iterator") && argc == 4) {
        ret = dumpIterator(argc, argv);
    } else if (!strcasecmp(argv[0],"rdb-info") && argc == 2) {
        ret = rdbInfo(argc, argv);
    } else {
        dumpUtilUsage();
        ret = REDIS_ERR;
    }
    if (ret == REDIS_OK) redisLog(REDIS_NOTICE,"%s success\n", argv[0]);
    sdsfreesplitres(argv, argc);
    return ret;
}

/*-----------------------------------------------------------------------------
 * Dump-util Main
 *----------------------------------------------------------------------------*/
int dumpUtilMain(int argc, char **argv) {
    /* parse and execute */
    if (argc >= 2) {
        int j = 1;
        sds options = sdsempty();
        int ret;

        if (argv[j][0] != '-' || argv[j][1] != '-') dumpUtilUsage();

        while(j != argc) {
            if (argv[j][0] == '-' && argv[j][1] == '-') {
                /* Option name */
                if (sdslen(options)) dumpUtilUsage();
                options = sdscat(options,argv[j]+2);
                options = sdscat(options," ");
            } else {
                /* Option argument */
                options = sdscatrepr(options,argv[j],strlen(argv[j]));
                options = sdscat(options," ");
            }
            j++;
        }
        ret = executeCommandFromOptions(options);
        sdsfree(options);
        if (ret != REDIS_OK) exit(1);
    } else {
        dumpUtilUsage();
    }
    exit(0);
}

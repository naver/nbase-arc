#include "fmacros.h"
#include <fcntl.h>

#include "redis.h"

#ifdef NBASE_ARC

#define PLAYDUMP_LOAD       0
#define PLAYDUMP_RANGEDEL   1
struct dumpState {
    rio aof, rdb;
    sds readbuf;
    int fd;
    int aofpos;
    int tps_hz, opcount;
    int ret, send_done;
    int cronloops;
    int flags;
    FILE *fp;
    off_t cursize, totalsize;
};

/* Usage */
static void clusterUtilUsage() {
    fprintf(stderr,"Usage: \n");
    fprintf(stderr,"       ./cluster-util --playdump <source rdb filename> <target address> <port> <tps>\n");
    fprintf(stderr,"       ./cluster-util --getdump <source address> <port> <target rdb filename> <partition range(inclusive)>\n");
    fprintf(stderr,"       ./cluster-util --getandplay <source address> <port> <target address> <port> <partition range(inclusive)> <tps>\n");
    fprintf(stderr,"       ./cluster-util --rangedel <address> <port> <partition range(inclusive)> <tps>\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./cluster-util --playdump dump.rdb localhost 6379 30000\n");
    fprintf(stderr,"       ./cluster-util --getdump localhost 6379 dump.rdb 0-8191\n");
    fprintf(stderr,"       ./cluster-util --getandplay localhost 6379 localhost 7379 0-8191 30000\n");
    fprintf(stderr,"       ./cluster-util --rangedel localhost 6379 0-1024 30000\n");
    exit(1);
}

/* utility functions */
static int getLoadCommandFromRdb(rio *rdb, rio *aof, int *count) {
    int type;
    long long expiretime = -1;
    robj *key, *val;

    /* Read type. */
    if ((type = rdbLoadType(rdb)) == -1) return REDIS_ERR;
    if (type == REDIS_RDB_OPCODE_EXPIRETIME) {
        if ((expiretime = rdbLoadTime(rdb)) == -1) return REDIS_ERR;
        /* We read the time so we need to read the object type again. */
        if ((type = rdbLoadType(rdb)) == -1) return REDIS_ERR;
        /* the EXPIRETIME opcode specifies time in seconds, so convert
         * into milliesconds. */
        expiretime *= 1000;
    } else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
        /* Milliseconds precision expire times introduced with RDB
         * version 3. */
        if ((expiretime = rdbLoadMillisecondTime(rdb)) == -1) return REDIS_ERR;
        /* We read the time so we need to read the object type again. */
        if ((type = rdbLoadType(rdb)) == -1) return REDIS_ERR;
    }

    if (type == REDIS_RDB_OPCODE_EOF)
        return REDIS_RDB_OPCODE_EOF;

    /* Handle SELECT DB opcode as a special case */
    if (type == REDIS_RDB_OPCODE_SELECTDB) {
        int dbid;
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";

        if ((dbid = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR)
            return REDIS_ERR;
        if (dbid >= (unsigned)server.dbnum) {
            redisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
            return REDIS_ERR;
        }
        if (rioWrite(aof,selectcmd,sizeof(selectcmd)-1) == 0) return REDIS_ERR;
        if (rioWriteBulkLongLong(aof,dbid) == 0) return REDIS_ERR;
        *count = 1;
        return REDIS_RDB_OPCODE_SELECTDB;
    }
    /* Read key */
    if ((key = rdbLoadStringObject(rdb)) == NULL) return REDIS_ERR;
    /* Read value */
    if ((val = rdbLoadObject(type,rdb,key)) == NULL) return REDIS_ERR;

    if (!compareStringObjects(key, shared.db_version)) {
        long long ver;
        getLongLongFromObject(val, &ver);
        decrRefCount(key);
        decrRefCount(val);
        return REDIS_RDB_OPCODE_VERSION;
    }

    if (!compareStringObjects(key, shared.db_smr_mstime)) {
        long long smr_mstime;
        getLongLongFromObject(val, &smr_mstime);
        decrRefCount(key);
        decrRefCount(val);
        return REDIS_RDB_DB_SMR_MSTIME;
    }

    if (!compareStringObjects(key, shared.db_migrate_slot)
        || !compareStringObjects(key, shared.db_migclear_slot)) {
        decrRefCount(key);
        decrRefCount(val);
        return REDIS_RDB_DB_MIGRATE_SLOT;
    }

    /* Save the key and associated value */
    if (val->type == REDIS_STRING) {
        /* Emit a SET command */
        char cmd[]="*3\r\n$3\r\nSET\r\n";
        if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) return REDIS_ERR;
        /* Key and value */
        if (rioWriteBulkObject(aof,key) == 0) return REDIS_ERR;
        if (rioWriteBulkObject(aof,val) == 0) return REDIS_ERR;
        *count = 1;
    } else if (val->type == REDIS_LIST) {
        if (rewriteListObject(aof,key,val) == 0) return REDIS_ERR;
        *count = 1;
    } else if (val->type == REDIS_SET) {
        if (rewriteSetObject(aof,key,val) == 0) return REDIS_ERR;
        *count = 1;
    } else if (val->type == REDIS_ZSET) {
        if (rewriteSortedSetObject(aof,key,val) == 0) return REDIS_ERR;
        *count = 1;
    } else if (val->type == REDIS_HASH) {
        if (rewriteHashObject(aof,key,val) == 0) return REDIS_ERR;
        *count = 1;
    } else if (val->type == REDIS_SSS) {
        if (rewriteSssObject(aof,key,val) == 0) return REDIS_ERR;
        *count = sssTypeValueCount(val);
    } else {
        redisLog(REDIS_WARNING, "Unknown object type in rdb file");
        return REDIS_ERR;
    }
    /* Save the expire time */
    if (expiretime != -1) {
        char cmd[]="*3\r\n$12\r\nMIGPEXPIREAT\r\n";
        if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) return REDIS_ERR;
        if (rioWriteBulkObject(aof,key) == 0) return REDIS_ERR;
        if (rioWriteBulkLongLong(aof,expiretime) == 0) return REDIS_ERR;
        *count += 1;
    }

    /* delete robjs */
    decrRefCount(key);
    decrRefCount(val);
    return REDIS_OK;
}

static int getDelCommandFromRdb(rio *rdb, rio *aof, int *count) {
    int type;
    long long expiretime = -1;
    robj *key, *val;

    /* Read type. */
    if ((type = rdbLoadType(rdb)) == -1) return REDIS_ERR;
    if (type == REDIS_RDB_OPCODE_EXPIRETIME) {
        if ((expiretime = rdbLoadTime(rdb)) == -1) return REDIS_ERR;
        /* We read the time so we need to read the object type again. */
        if ((type = rdbLoadType(rdb)) == -1) return REDIS_ERR;
        /* the EXPIRETIME opcode specifies time in seconds, so convert
         * into milliesconds. */
        expiretime *= 1000;
    } else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
        /* Milliseconds precision expire times introduced with RDB
         * version 3. */
        if ((expiretime = rdbLoadMillisecondTime(rdb)) == -1) return REDIS_ERR;
        /* We read the time so we need to read the object type again. */
        if ((type = rdbLoadType(rdb)) == -1) return REDIS_ERR;
    }

    if (type == REDIS_RDB_OPCODE_EOF)
        return REDIS_RDB_OPCODE_EOF;

    /* Handle SELECT DB opcode as a special case */
    if (type == REDIS_RDB_OPCODE_SELECTDB) {
        int dbid;

        if ((dbid = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR)
            return REDIS_ERR;
        if (dbid >= (unsigned)server.dbnum) {
            redisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
            return REDIS_ERR;
        }
        return REDIS_RDB_OPCODE_SELECTDB;
    }
    /* Read key */
    if ((key = rdbLoadStringObject(rdb)) == NULL) return REDIS_ERR;
    /* Read value */
    if ((val = rdbLoadObject(type,rdb,key)) == NULL) return REDIS_ERR;

    if (!compareStringObjects(key, shared.db_version)) {
        long long ver;
        getLongLongFromObject(val, &ver);
        decrRefCount(key);
        decrRefCount(val);
        return REDIS_RDB_OPCODE_VERSION;
    }

    if (!compareStringObjects(key, shared.db_smr_mstime)) {
        long long smr_mstime;
        getLongLongFromObject(val, &smr_mstime);
        decrRefCount(key);
        decrRefCount(val);
        return REDIS_RDB_DB_SMR_MSTIME;
    }
    
    if (!compareStringObjects(key, shared.db_migrate_slot)
        || !compareStringObjects(key, shared.db_migclear_slot)) {
        decrRefCount(key);
        decrRefCount(val);
        return REDIS_RDB_DB_MIGRATE_SLOT;
    }

    if (key->type == REDIS_STRING) {
        char cmd[] = "*2\r\n$3\r\nDEL\r\n";
        if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) return REDIS_ERR;
        if (rioWriteBulkObject(aof,key) == 0) return REDIS_ERR;
        *count = 1;
    } else {
        redisLog(REDIS_WARNING, "DUMP file is invalid format");
    }

    decrRefCount(key);
    decrRefCount(val);
    return REDIS_OK;
}

static void playdump_rqst(aeEventLoop *el, int fd, void *data, int mask)
{
    struct dumpState *ds = (struct dumpState *) data;
    int ret = 0, nw;

    if (ds->aof.io.buffer.pos == 0) {
        /* fill buffer */
        do {
            int count = 0;
            if (ds->flags == PLAYDUMP_LOAD) {
                ret = getLoadCommandFromRdb(&ds->rdb, &ds->aof, &count);
            } else if (ds->flags == PLAYDUMP_RANGEDEL) {
                ret = getDelCommandFromRdb(&ds->rdb, &ds->aof, &count);
            } else {
                goto err;
            }
            ds->opcount += count;
            if (ds->opcount >= ds->tps_hz) break;
        } while (ret != REDIS_ERR && ret != REDIS_RDB_OPCODE_EOF && ds->aof.io.buffer.pos < NBASE_ARC_KS_SIZE);

        if (ret == REDIS_ERR) {
            goto err;
        }
        if (ret == REDIS_RDB_OPCODE_EOF) {
            char pingcmd[] = "*1\r\n$4\r\nPING\r\n";
            if (rioWrite(&ds->aof, pingcmd, sizeof(pingcmd)-1) == 0) {
                goto err;
            }
            ds->send_done = 1;
        }
    }

    nw = write(fd, ds->aof.io.buffer.ptr + ds->aofpos, ds->aof.io.buffer.pos - ds->aofpos);

    if (nw == -1 && errno == EAGAIN) {
        return;
    } else if (nw <= 0) {
        redisLog(REDIS_WARNING,"I/O error writing to target, ret:%d, err:%s",
                nw, strerror(errno));
        goto err;
    }

    ds->aofpos += nw;

    if (ds->aofpos == ds->aof.io.buffer.pos) {
        sdsclear(ds->aof.io.buffer.ptr);
        ds->aof.io.buffer.pos = 0;
        ds->aofpos = 0;
        if (ds->opcount >= ds->tps_hz || ret == REDIS_RDB_OPCODE_EOF) {
            aeDeleteFileEvent(el, fd, AE_WRITABLE);
        }
    }

    return;

err:
    ds->ret = REDIS_ERR;
    aeDeleteFileEvent(el, fd, AE_READABLE);
    aeDeleteFileEvent(el, fd, AE_WRITABLE);
    aeStop(el);
}

static void playdump_resp(aeEventLoop *el, int fd, void *data, int mask) {
    struct dumpState *ds = (struct dumpState *) data;
    int nr;

    nr = read(fd, ds->readbuf + sdslen(ds->readbuf), sdsavail(ds->readbuf));
    if (nr == -1 && errno == EAGAIN) {
        return;
    } else if (nr <= 0) {
        ds->ret = REDIS_ERR;
        aeDeleteFileEvent(el, fd, AE_READABLE);
        aeDeleteFileEvent(el, fd, AE_WRITABLE);
        aeStop(el);
        return;
    }

    sdsIncrLen(ds->readbuf, nr);
    if (ds->send_done) {
        if (strstr(ds->readbuf, "+PONG\r\n")) {
            aeDeleteFileEvent(el, fd, AE_WRITABLE);
            aeDeleteFileEvent(el, fd, AE_READABLE);
            aeStop(el);
        }
        sdsrange(ds->readbuf, -10, -1);
    } else {
        sdsclear(ds->readbuf);
    }
}

static int playdump_cron(struct aeEventLoop *el, long long id, void *data) {
    struct dumpState *ds = (struct dumpState *) data;
    if (ds->opcount >= ds->tps_hz && !ds->send_done) {
        if (aeCreateFileEvent(el, ds->fd, AE_WRITABLE, playdump_rqst, ds) == AE_ERR) {
            ds->ret = REDIS_ERR;
            aeDeleteFileEvent(el, ds->fd, AE_READABLE);
            aeDeleteFileEvent(el, ds->fd, AE_WRITABLE);
            aeStop(el);
        }
    }

    if (!(ds->cronloops%server.hz)) {
        ds->cursize = ftello(ds->fp);
        if (ds->flags == PLAYDUMP_LOAD) {
            redisLog(REDIS_NOTICE, "load %lld%% done", (long long)ds->cursize*100/ds->totalsize);
        } else if (ds->flags == PLAYDUMP_RANGEDEL) {
            redisLog(REDIS_NOTICE, "delete %lld%% done", (long long)ds->cursize*100/ds->totalsize);
        }

    }
    ds->opcount = 0;
    ds->cronloops++;
    return 1000/server.hz;
}

static int playdump(char *filename, char *target_addr, int target_port, int tps, int flags) {
    int rdbver;
    char magic[10];
    FILE *fp;
    sds aofbuf;
    int fd;
    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;
    struct dumpState ds;
    long long teid;

    /* open rdb file */
    fp = fopen(filename, "r");
    if (!fp) {
        redisLog(REDIS_WARNING, "Can not open rdb dumpfile(%s)", filename);
        return REDIS_ERR;
    }
    fseeko(fp, 0L, SEEK_END);
    ds.totalsize = ftello(fp);
    fseeko(fp, 0L, SEEK_SET);

    rioInitWithFile(&ds.rdb, fp);

    /* check dumpfile */
    if (rioRead(&ds.rdb,magic,9) == 0) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
        return REDIS_ERR;
    }
    magic[9] = '\0';
    if (memcmp(magic,"REDIS",5) != 0) {
        fclose(fp);
        redisLog(REDIS_WARNING, "Wrong signature trying to load DB from file");
        return REDIS_ERR;
    }
    rdbver = atoi(magic+5);
    if (rdbver < 1 || rdbver > REDIS_RDB_VERSION) {
        fclose(fp);
        redisLog(REDIS_WARNING, "Can't handle RDB format version %d",rdbver);
        return REDIS_ERR;
    }

    /* connect to target redis server */
    fd = anetTcpNonBlockConnect(neterr, target_addr, target_port);
    if (fd == ANET_ERR) {
        fclose(fp);
        redisLog(REDIS_WARNING, "Unable to connect to redis server(%s:%d): %s",
            target_addr, target_port, neterr);
        return REDIS_ERR;
    }
        
    /* initialize aof */
    ds.readbuf = sdsempty();
    ds.readbuf = sdsMakeRoomFor(ds.readbuf, 8192); 
    aofbuf = sdsempty();
    aofbuf = sdsMakeRoomFor(aofbuf, 8192);
    rioInitWithBuffer(&ds.aof, aofbuf);
    ds.fp = fp;
    ds.fd = fd;
    ds.tps_hz = tps / server.hz;
    ds.opcount = 0;
    ds.aofpos = 0;
    ds.send_done = 0;
    ds.ret = REDIS_OK;
    ds.cronloops = 0;
    ds.flags = flags;

    el = aeCreateEventLoop(1024);
    if (aeCreateFileEvent(el, fd, AE_WRITABLE, playdump_rqst, &ds) == AE_ERR) {
        redisLog(REDIS_WARNING,"Unable to create file event for playdump command.");
        goto loaderr;
    }
    if (aeCreateFileEvent(el, fd, AE_READABLE, playdump_resp, &ds) == AE_ERR) {
        redisLog(REDIS_WARNING,"Unable to create file event for playdump command.");
        goto loaderr;
    }
    teid = aeCreateTimeEvent(el, 1, playdump_cron, &ds, NULL);

    aeMain(el);

    aeDeleteTimeEvent(el, teid);

    if (ds.ret == REDIS_ERR) {
        redisLog(REDIS_WARNING,"Target redis server is not responding correctly.");
        goto loaderr;
    }

    /* finalize */
    aeDeleteEventLoop(el);
    fclose(fp);
    close(fd);
    sdsfree(ds.readbuf);
    sdsfree(ds.aof.io.buffer.ptr);
    return REDIS_OK;

loaderr:
    aeDeleteEventLoop(el);
    fclose(fp);
    close(fd);
    sdsfree(ds.readbuf);
    sdsfree(ds.aof.io.buffer.ptr);
    return REDIS_ERR;
}

#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
int getdump(char *source_addr, int source_port, char *filename, char *range) {
    char tmpfile[256];
    int dfd, maxtries = 5;
    int fd;
    char neterr[ANET_ERR_LEN];
    char buf[4096];
    int len;
    
    /* connect to source redis server */
    fd = anetTcpConnect(neterr, source_addr, source_port);
    if (fd == ANET_ERR) {
        redisLog(REDIS_WARNING, "Unable to connect to redis server(%s:%d): %s",
            source_addr, source_port, neterr);
        return REDIS_ERR;
    }

    /* Send the PING */
    syncWrite(fd,"PING\r\n",6,100);
    /* Recieve the PONG */
    if (syncReadLine(fd,buf,sizeof(buf),
                server.repl_syncio_timeout*1000) == -1)
    {
        redisLog(REDIS_WARNING,
                "I/O error reading PING reply from source: %s",
                strerror(errno));
        close(fd);
        return REDIS_ERR;
    }
    if (buf[0] != '-' && buf[0] != '+') {
        redisLog(REDIS_WARNING,"Unexpected reply to PING from source.");
        close(fd);
        return REDIS_ERR;
    }
    redisLog(REDIS_NOTICE, "Source replied to PING, getdump can continue...");

    /* Issue the CHECKPOINT command */
    len = sprintf(buf, "*2\r\n$10\r\nCHECKPOINT\r\n$%ld\r\n%s\r\n", strlen(range), range);
    if (syncWrite(fd,buf,len,server.repl_syncio_timeout*1000) == -1) {
        redisLog(REDIS_WARNING,"I/O error writing to source: %s",
                strerror(errno));
        close(fd);
        return REDIS_ERR;
    }
    if (syncReadLine(fd, buf, 1024, server.repl_syncio_timeout*1000) == -1) {
        redisLog(REDIS_WARNING,
                "I/O error reading smr sequence number from source: %s",
                strerror(errno));
        close(fd);
        return REDIS_ERR;
    }
    if (buf[0] != ':') {
        redisLog(REDIS_WARNING,"Bad protocol from source, the first byte is not ':', are you sure the host and port are right?");
        redisLog(REDIS_WARNING,"Reply from source:%s", buf);
        close(fd);
        return REDIS_ERR;
    }
    redisLog(REDIS_NOTICE, "Checkpoint Sequence Number%s", buf);

    /* Prepare a suitable temp file for bulk transfer */
    while(maxtries--) {
        snprintf(tmpfile,256,
            "temp-%d.%ld.rdb",(int)time(NULL),(long int)getpid());
        dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
        if (dfd != -1) break;
        sleep(1);
    }
    if (dfd == -1) {
        redisLog(REDIS_WARNING,"Opening the temp file needed for SOURCE <-> TARGET synchronization: %s",strerror(errno));
        close(fd);
        return REDIS_ERR;
    }

    /* Read the buf length from the source reply */
    if (syncReadLine(fd,buf,1024,server.repl_syncio_timeout*1000) == -1) {
        redisLog(REDIS_WARNING,
                "I/O error reading bulk count from source: %s",
                strerror(errno));
        close(fd);
        close(dfd);
        return REDIS_ERR;
    }
    if (buf[0] == '-') {
        redisLog(REDIS_WARNING, "Source aborted get dump with an error: %s",
                buf+1);
        close(fd);
        close(dfd);
        return REDIS_ERR;
    } else if (buf[0] != '$') {
        redisLog(REDIS_WARNING,"Bad protocol from SOURCE, the first byte is not '$', are you sure the host and port are right?");
        close(fd);
        close(dfd);
        return REDIS_ERR;
    }
    server.repl_transfer_size = strtol(buf+1,NULL,10);
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    redisLog(REDIS_NOTICE,
        "SOURCE <-> TARGET sync: receiving %ld bytes from source",
        server.repl_transfer_size);

    /* Read bulk data */
    do {
        off_t left = server.repl_transfer_size - server.repl_transfer_read;
        ssize_t nread, readlen;
        readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
        nread = read(fd,buf,readlen);
        if (nread <= 0) {
            redisLog(REDIS_WARNING,"I/O error trying to getdump with source: %s",
                (nread == -1) ? strerror(errno) : "connection lost");
            close(fd);
            close(dfd);
            return REDIS_ERR;
        }
        if (write(dfd,buf,nread) != nread) {
            redisLog(REDIS_WARNING,"Write error or short write writing to the DB dump file needed for TARGET <-> SOURCE synchronization: %s", strerror(errno));
            close(fd);
            close(dfd);
            return REDIS_ERR;
        }
        server.repl_transfer_read += nread;

        /* Sync data on disk from time to time, otherwise at the end of the transfer
         * we may suffer a big delay as the memory buffers are copied into the
         * actual disk. */
        if (server.repl_transfer_read >=
            server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
        {
            off_t sync_size = server.repl_transfer_read -
                              server.repl_transfer_last_fsync_off;
            rdb_fsync_range(dfd,
                server.repl_transfer_last_fsync_off, sync_size);
            server.repl_transfer_last_fsync_off += sync_size;
        }
    } while (server.repl_transfer_read != server.repl_transfer_size);

    if (syncReadLine(fd,buf,1024,server.repl_syncio_timeout*1000) == -1) {
        redisLog(REDIS_WARNING,
                "I/O error reading +OK from source: %s",
                strerror(errno));
        close(fd);
        close(dfd);
        return REDIS_ERR;
    }

    if (buf[0] != '+' || buf[1] != 'O' || buf[2] != 'K') {
        redisLog(REDIS_WARNING,"Unexpected reply from source.");
        close(fd);
        close(dfd);
        return REDIS_ERR;
    }

    if (rename(tmpfile, filename) == -1) {
        redisLog(REDIS_WARNING,"Failed trying to rename the temp DB into dump.rdb in MASTER <-> SLAVE synchronization: %s", strerror(errno));
        close(fd);
        close(dfd);
        return REDIS_ERR;
    }
    return REDIS_OK;
}

/*-----------------------------------------------------------------------------
 * Cluster Util Commands
 *----------------------------------------------------------------------------*/

/* Play Dump Command
 * --playdump <rdb filename> <target address> <target port> */
static int playdumpCommand(int argc, sds *argv) {
    char *filename;
    char *target_addr;
    int target_port;
    int tps;

    /* parameter */
    filename = (char *) argv[1];
    target_addr = (char *) argv[2];
    target_port = atoi(argv[3]);
    tps = atoi(argv[4]);

    if (target_port <= 0) {
        redisLog(REDIS_WARNING, "Negative number of target port in playdump command");
        return REDIS_ERR;
    }

    if (tps <= 0) {
        redisLog(REDIS_WARNING, "Negative number of tps in playdump command");
        return REDIS_ERR;
    }

    return playdump(filename, target_addr, target_port, tps, PLAYDUMP_LOAD);
}

/* get dump Command
 * --getdump <source address> <port> <target rdb filename> <partition range(inclusive)> */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
static int getdumpCommand(int argc, char **argv) {
    char *filename;
    char *source_addr;
    int source_port;
    char *range;
    
    /* parameter */
    source_addr = (char *) argv[1];
    source_port = atoi(argv[2]);
    filename = (char *) argv[3];
    range = (char *) argv[4];
    if (source_port <= 0) {
        redisLog(REDIS_WARNING, "Negative number of source port in getdump command");
        return REDIS_ERR;
    }

    if (!strchr(range, '-')) {
        redisLog(REDIS_WARNING, "Invalid range format");
        return REDIS_ERR;
    }

    return getdump(source_addr, source_port, filename, range);
}

/* get and play Command
 * --getandplay <source address> <port> <target address> <port> <partition range(inclusive)> */
static int getandplayCommand(int argc, char **argv) {
    char filename[256];
    char *source_addr, *target_addr;
    int source_port, target_port;
    int tps;
    char *range;
    
    /* parameter */
    source_addr = (char *) argv[1];
    source_port = atoi(argv[2]);
    target_addr = (char *) argv[3];
    target_port = atoi(argv[4]);
    range = (char *) argv[5];
    tps = atoi(argv[6]);

    if (source_port <= 0 || target_port <= 0) {
        redisLog(REDIS_WARNING, "Negative number of port in getandplay command");
        return REDIS_ERR;
    }
    
    if (tps <= 0) {
        redisLog(REDIS_WARNING, "Negative number of tps in getandplay command");
        return REDIS_ERR;
    }

    if (!strchr(range, '-')) {
        redisLog(REDIS_WARNING, "Invalid range format");
        return REDIS_ERR;
    }

    /* tempfile */
    snprintf(filename,256,"temp-getandplay-%d.rdb", (int) getpid());
    if (getdump(source_addr, source_port, filename, range) != REDIS_OK) {
        unlink(filename);
        return REDIS_ERR;
    }

    if (playdump(filename, target_addr, target_port, tps, PLAYDUMP_LOAD) != REDIS_OK) {
        unlink(filename);
        return REDIS_ERR;
    }

    unlink(filename);
    return REDIS_OK;
}

/* range del Command
 * --rangedel <address> <port> <partition range(inclusive)> <tps> */
static int rangedelCommand(int argc, char **argv) {
    char filename[256];
    char *addr;
    int port, tps;
    char *range;

    /* parameter */
    addr = (char *) argv[1];
    port = atoi(argv[2]);
    range = (char *) argv[3];
    tps = atoi(argv[4]);

    if (port <= 0) {
        redisLog(REDIS_WARNING, "Negative number of port in rangedel command");
        return REDIS_ERR;
    }

    if (tps <= 0) {
        redisLog(REDIS_WARNING, "Negative number of tps in rangedel command");
        return REDIS_ERR;
    }

    if (!strchr(range, '-')) {
        redisLog(REDIS_WARNING, "Invalid range format");
        return REDIS_ERR;
    }

    /* tempfile */
    snprintf(filename,256,"temp-rangedel-%d.rdb", (int) getpid());
    if (getdump(addr, port, filename, range) != REDIS_OK) {
        unlink(filename);
        return REDIS_ERR;
    }

    if (playdump(filename, addr, port, tps, PLAYDUMP_RANGEDEL) != REDIS_OK) {
        unlink(filename);
        return REDIS_ERR;
    }

    unlink(filename);
    return REDIS_OK;
}

/* Command Executor */
static int executeCommandFromOptions(sds options) {
    sds *argv;
    int argc;
    int ret;

    options = sdstrim(options, " \t\r\n");
    argv = sdssplitargs(options,&argc);
    sdstolower(argv[0]);

    if (!strcasecmp(argv[0],"playdump") && argc == 5) {
        ret = playdumpCommand(argc, argv);
    } else if (!strcasecmp(argv[0],"getdump") && argc == 5) {
        ret = getdumpCommand(argc, argv);
    } else if (!strcasecmp(argv[0],"getandplay") && argc == 7) {
        ret = getandplayCommand(argc, argv);
    } else if (!strcasecmp(argv[0],"rangedel") && argc == 5) {
        ret = rangedelCommand(argc, argv);
    } else {
        clusterUtilUsage();
        ret = REDIS_ERR;
    }
    if (ret == REDIS_OK) redisLog(REDIS_NOTICE, "%s success\n", argv[0]);
    sdsfreesplitres(argv, argc);
    return ret;
}

/* initialize */
void initClusterUtil() {
    createSharedObjects();
    server.gc_line = zmalloc(sizeof(dlisth)*server.gc_num_line);
    for(int j = 0; j < server.gc_num_line; j++) {
        dlisth_init(&server.gc_line[j]);
    }
}

/* Main */
int clusterUtilMain(int argc, char **argv) {
    /* init */
    initClusterUtil();

    /* parse and execute */
    if (argc >= 2) {
        int j = 1;
        sds options = sdsempty();
        int ret;

        if (argv[j][0] != '-' || argv[j][1] != '-') clusterUtilUsage();

        while(j != argc) {
            if (argv[j][0] == '-' && argv[j][1] == '-') {
                /* Option name */
                if (sdslen(options)) clusterUtilUsage();
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
        clusterUtilUsage();
    }
    exit(0);
}
#endif /* NBASE_ARC */

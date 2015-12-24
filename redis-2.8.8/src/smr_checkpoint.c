/*
 * Copyright 2015 Naver Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fmacros.h"
#include <sys/stat.h>
#include <assert.h>
#include <fcntl.h>

#include "redis.h"
#include "endianconv.h"
#include "crc16.h"

#ifdef NBASE_ARC
static sds rdbSetBit (sds bitarray, int bitoffset, int value) {
    int byte, byteval, bit;
    byte = bitoffset >> 3;
    bitarray = sdsgrowzero (bitarray, byte + 1);
    
    bit = 7 - (bitoffset & 0x7);
    byteval = bitarray[byte];
    byteval &= ~(1 << bit);
    byteval |= ((value & 0x1) << bit);
    bitarray[byte] = byteval;
    return bitarray;
}

int rdbGetBit (sds bitarray, int bitoffset) {
    int byte, bit;
    byte = bitoffset >> 3;
    assert (byte <= sdslen (bitarray));
    bit = 7 - (bitoffset & 0x7);
    return bitarray[byte] & (1 << bit);
}

static int rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

/* Save the Checkpoint on disk. Return REDIS_ERR on error, REDIS_OK on success */
static int rdbCheckpoint(char *filename, sds bitarray, int hashsize) {
    dictIterator *di = NULL;
    dictEntry *de;
    char tmpfile[256];
    char magic[10];
    int j;
    long long now = smr_mstime();
    FILE *fp;
    rio rdb;
    uint64_t cksum;
    int fd;
    uint64_t w_count = 0;

    assert (sdslen (bitarray) * 8 >= hashsize);

    snprintf(tmpfile,256,"temp-checkpoint-%d.rdb", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        redisLog(REDIS_WARNING, "Failed opening .rdb for saving: %s",
            strerror(errno));
        return REDIS_ERR;
    }

    rioInitWithFile(&rdb,fp);
    if (server.rdb_checksum)
        rdb.update_cksum = rioGenericUpdateChecksum;
    snprintf(magic,sizeof(magic),"REDIS%04d",REDIS_RDB_VERSION);
    if (rdbWriteRaw(&rdb,magic,9) == -1) goto werr;

    fd = fileno(fp);
    if (rdbSaveType (&rdb, REDIS_RDB_TYPE_STRING) == -1) goto werr;
    if (rdbSaveStringObject (&rdb, shared.db_version) == -1) goto werr;
    if (rdbSaveLongLongAsStringObject (&rdb, server.smr_seqnum) == -1) goto werr;
    if (rdbSaveType (&rdb, REDIS_RDB_TYPE_STRING) == -1) goto werr;
    if (rdbSaveStringObject(&rdb, shared.db_smr_mstime) == -1) goto werr;
    if (rdbSaveLongLongAsStringObject(&rdb, server.smr_mstime) == -1) goto werr;

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db + j;
        dict *d = db->dict;
        if (dictSize (d) == 0) continue;
        di = dictGetSafeIterator (d);
        if (!di) {
            fclose (fp);
            return REDIS_ERR;
        }
    
        /* Write the SELECT DB opcode */
        if (rdbSaveType (&rdb, REDIS_RDB_OPCODE_SELECTDB) == -1) goto werr;
        if (rdbSaveLen (&rdb, j) == -1) goto werr;
    
        /* Iterate this DB writing every entry */
        while ((de = dictNext (di)) != NULL) {
            sds keystr = dictGetKey (de);
            robj key, *o = dictGetVal (de);
            long long expire;
            int keyhash = crc16 (keystr, sdslen (keystr), 0) % hashsize;
            if (rdbGetBit (bitarray, keyhash)) {
                initStaticStringObject (key, keystr);
                expire = getExpire (db, &key);
                if (rdbSaveKeyValuePair (&rdb, &key, o, expire, now) == -1) goto werr;
                w_count++;
                if (w_count > 0 && (w_count % 1000) == 0) {
                    if (fdatasync(fd) != 0) goto werr;
                    if (posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0) goto werr;
                }
            }
        }
        dictReleaseIterator (di);
    }
    di = NULL; /* So that we don't release it again on error. */

    /* EOF opcode */
    if (rdbSaveType(&rdb,REDIS_RDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. */
    cksum = rdb.cksum;
    memrev64ifbe(&cksum);
    rioWrite(&rdb,&cksum,8);

    /* Make sure data will not remain on the OS's output buffers */
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }
    redisLog(REDIS_NOTICE, "Checkpoint saved on disk");
    return REDIS_OK;

werr:
    fclose(fp);
    unlink(tmpfile);
    redisLog(REDIS_WARNING,"Write error checkpoint on disk: %s", strerror(errno));
    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

static int rdbCheckpointBackground (char *filename, sds bitarray, int hashsize)
{
    pid_t childpid;
    long long start;

    if (server.rdb_child_pid != -1 || server.checkpoint_pid != -1) return REDIS_ERR;

    start = ustime();
    if ((childpid = fork()) == 0) {
        int retval;

        /* Child */
	closeListeningSockets(0);
        retval = rdbCheckpoint(filename, bitarray, hashsize);
        exitFromChild((retval == REDIS_OK) ? 0 : 1);
    } else {
        /* Parent */
        server.stat_fork_time = ustime()-start;
        if (childpid == -1) {
            redisLog(REDIS_WARNING,"Can't save in background: fork: %s",
                    strerror(errno));
            return REDIS_ERR;
        }
        redisLog(REDIS_NOTICE,"Partial checkpoint started by pid %d",childpid);
        server.checkpoint_pid = childpid;
        updateDictResizePolicy();
        return REDIS_OK;
    }
    return REDIS_OK;
}

static sds getBitarrayFromArgv(int argc, robj **argv, int hashsize) {
    sds bitarray;
    int i;

    bitarray = sdsgrowzero(sdsempty(), (hashsize >> 3) + 1);
    for (i = 0; i < argc; i++) {
        int ok, n, j;
        long long ll, from, to;
        sds *tokens;
        sds from_to = (sds) argv[i]->ptr;

        /* There are two kinds of argument type.
         * 1. <slotNo>                     ex) 1024
         * 2. <slotNo from>-<slotNo to>    ex) 0-2047 */
        tokens = sdssplitlen(from_to, sdslen(from_to), "-", 1, &n);
        if (tokens == NULL) {
            sdsfree(bitarray);
            return NULL;
        }

        if (n == 1) {
            /* Type 1 <slotNo> */
            ok = string2ll(tokens[0], sdslen(tokens[0]), &ll);
            if (!ok) {
                sdsfreesplitres(tokens, n);
                sdsfree(bitarray);
                return NULL;
            }
            from = ll;
            to = ll;
        } else if (n == 2) {
            /* Type 2 <slotNo from>-<slotNo to> */
            ok = string2ll(tokens[0], sdslen(tokens[0]), &ll);
            if (!ok) {
                sdsfreesplitres(tokens, n);
                sdsfree(bitarray);
                return NULL;
            }
            from = ll;

            ok = string2ll(tokens[1], sdslen(tokens[1]), &ll);
            if (!ok) {
                sdsfreesplitres(tokens, n);
                sdsfree(bitarray);
                return NULL;
            }
            to = ll;
        } else {
            /* not belong to Type 1 or Type 2 */
            sdsfreesplitres(tokens, n);
            sdsfree(bitarray);
            return NULL;
        }

        sdsfreesplitres(tokens, n);

        /* range check */
        if (from < 0 || to >= hashsize || from > to) {
            sdsfree(bitarray);
            return NULL;
        }
            
        /* set bit */
        for (j = from; j <= to; j++) {
            bitarray = rdbSetBit(bitarray, j, 1);
        }
    }
    return bitarray;
}

static void rdbRemoveCheckpointTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-checkpoint-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

static void sendBulkToTarget(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    char buf[REDIS_IOBUF_LEN];
    ssize_t nwritten, buflen;

    if (c->repldboff == 0) {
        /* Write the bulk write count before to transfer the DB. In theory here
         * we don't know how much room there is in the output buffer of the
         * socket, but in pratice SO_SNDLOWAT (the minimum count for output
         * operations) will never be smaller than the few bytes we need. */
        sds bulkcount;

        bulkcount = sdscatprintf(sdsempty(),":%lld\r\n$%lld\r\n",
            server.checkpoint_seqnum, (unsigned long long) c->repldbsize);
        if (write(fd,bulkcount,sdslen(bulkcount)) != (signed)sdslen(bulkcount))
        {
            sdsfree(bulkcount);
            close(c->repldbfd);
            freeClient(c);
            return;
        }
        sdsfree(bulkcount);
    }
    lseek(c->repldbfd,c->repldboff,SEEK_SET);
    buflen = read(c->repldbfd,buf,REDIS_IOBUF_LEN);
    if (buflen <= 0) {
        redisLog(REDIS_WARNING,"Read error sending DB to target: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        close(c->repldbfd);
        freeClient(c);
        return;
    }
    if ((nwritten = write(fd,buf,buflen)) == -1) {
        redisLog(REDIS_VERBOSE,"Write error sending DB to target: %s",
            strerror(errno));
        close(c->repldbfd);
        freeClient(c);
        return;
    }
    c->repldboff += nwritten;
    if (c->repldboff == c->repldbsize) {
        close(c->repldbfd);
        c->repldbfd = -1;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
        c->replstate = REDIS_REPL_NONE;
        if (aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
            sendReplyToClient, c) == AE_ERR) {
            freeClient(c);
            return;
        }
        addReply(c, shared.ok);
        redisLog(REDIS_NOTICE,"Synchronization with target succeeded");
    }
}

/* A background checkpointing child (CHECKPOINT) terminated its work. Handle this. */
void backgroundCheckpointDoneHandler(int exitcode, int bysignal) {
    redisClient *c = server.checkpoint_client;
    
    if (!bysignal && exitcode == 0) {
        redisLog(REDIS_NOTICE,
            "Background checkpointing terminated with success");
    } else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background checkpointing error");
    } else {
        redisLog(REDIS_WARNING,
            "Background checkpointing terminated by signal %d", bysignal);
        rdbRemoveCheckpointTempFile(server.checkpoint_pid);
    }
    server.checkpoint_pid = -1;
    server.checkpoint_client = NULL;

    if (c != NULL && c->replstate == SMR_REPL_WAIT_CHECKPOINT_END) {
        struct redis_stat buf;

        if (exitcode != 0) {
            freeClient(c);
            redisLog(REDIS_WARNING,"CHECKPOINT failed. Background child returned an error");
            return;
        }
        if ((c->repldbfd = open(server.checkpoint_filename,O_RDONLY)) == -1 ||
            redis_fstat(c->repldbfd,&buf) == -1) 
        {
            if (c->repldbfd != -1) close(c->repldbfd);
            freeClient(c);
            redisLog(REDIS_WARNING,"CHECKPOINT failed. Can't open/stat DB after CHECKPOINT: %s", 
                strerror(errno));
            return;
        }
        c->repldboff = 0;
        c->repldbsize = buf.st_size;
        c->replstate = REDIS_REPL_SEND_BULK;
        aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
        if (aeCreateFileEvent(server.el, c->fd, AE_WRITABLE, sendBulkToTarget, c) == AE_ERR) {
            close(c->repldbfd);
            freeClient(c);
        }
    }
}

void checkpointCommand(redisClient *c) {
    redisLog(REDIS_NOTICE,"Client ask for checkpoint");
    /* Here we need to check if there is a background saving operation
     * in progress */
    if (server.rdb_child_pid != -1 || server.checkpoint_pid != -1) {
	redisLog(REDIS_NOTICE,"Another background operation is in progress. Unable to perform CHECKPOINT.");
        addReplyError(c,"Unable to perform CHECKPOINT");
        return;
    } else {
        /* Ok we don't have a background operation in progress, let's start one */

	/* set bitarray from argument */
        int hashsize = NBASE_ARC_KS_SIZE; 
        sds bitarray = getBitarrayFromArgv(c->argc-1, c->argv+1, hashsize);
        if (!bitarray) {
            addReplyError(c, "Unable to make a bitarray. Invalid argument format or Out of memory.");
            return;
        }

        redisLog(REDIS_NOTICE,"Starting CHECKPOINT");
        if (rdbCheckpointBackground(server.checkpoint_filename,
		bitarray, hashsize) != REDIS_OK) {
            redisLog(REDIS_NOTICE,"Replication failed, can't CHECKPOINT");
            addReplyError(c,"Unable to perform CHECKPOINT");
            sdsfree(bitarray);
            return;
        }
        server.checkpoint_seqnum = server.smr_seqnum;
	redisLog(REDIS_NOTICE, "Partial Checkpoint sequence num:%lld", server.smr_seqnum);
        c->replstate = SMR_REPL_WAIT_CHECKPOINT_END;
        sdsfree(bitarray);
    }
    c->repldbfd = -1;
    server.checkpoint_client = c;
    return;
}

void migstartCommand(redisClient *c) {
    int hashsize = NBASE_ARC_KS_SIZE;

    redisLog(REDIS_NOTICE,"Client ask for migrate start");
    if (server.migrate_slot != NULL) {
        addReplyError(c, "Another migration start operation is in progress. Unable to perform MIGSTART.");
        return;
    }

    server.migrate_slot = getBitarrayFromArgv(c->argc-1, c->argv+1, hashsize);
    if (!server.migrate_slot) {
        addReplyError(c, "Unable to make a bitarray. Invalid argument format or Out of memory.");
        return;
    }

    redisLog(REDIS_NOTICE,"Starting migration");
    addReply(c,shared.ok);
    return;
}

void migendCommand(redisClient *c) {
    redisLog(REDIS_NOTICE,"Client ask for migrate end");
    if (server.migrate_slot == NULL) {
        addReplyError(c, "Migration start operation is not in progress. Unable to perform MIGEND.");
        return;
    }

    redisLog(REDIS_NOTICE,"Finishing migration");
    sdsfree(server.migrate_slot);
    server.migrate_slot = NULL;
    addReply(c,shared.ok);
    return;
}

void migconfCommand(redisClient *c) {
    int hashsize = NBASE_ARC_KS_SIZE;

    if (!strcasecmp(c->argv[1]->ptr,"migstart")) {
        if (c->argc != 3)  {
            addReply(c, shared.syntaxerr);
            return;
        }
        redisLog(REDIS_NOTICE, "Client ask for starting migration.");
        if (server.migrate_slot || server.migclear_slot) {
            addReplyError(c, "Another migration job is in progress. Unable to perform MIGSTART.");
            return;
        }
        server.migrate_slot = getBitarrayFromArgv(c->argc-2, c->argv+2, hashsize);
        if (!server.migrate_slot) {
            addReplyError(c, "Unable to make a bitarray. Invalid argument format or Out of memory.");
            return;
        }
        redisLog(REDIS_NOTICE, "Starting migration.");
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"migend")) {
        if (c->argc != 2) {
            addReply(c, shared.syntaxerr);
            return;
        }
        redisLog(REDIS_NOTICE, "Client ask for finishing migration.");
        if (!server.migrate_slot) {
            addReplyError(c, "Migration job is not in progress. Unable to perform MIGEND.");
            return;
        }
        redisLog(REDIS_NOTICE,"Finishing migration");
        sdsfree(server.migrate_slot);
        server.migrate_slot = NULL;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"clearstart")) {
        if (c->argc != 3) {
            addReply(c, shared.syntaxerr);
            return;
        }
        redisLog(REDIS_NOTICE, "Client ask for starting migclear.");
        if (server.migrate_slot || server.migclear_slot) {
            addReplyError(c, "Another migration job is in progress. Unable to perform CLEARSTART.");
            return;
        }
        server.migclear_slot = getBitarrayFromArgv(c->argc-2, c->argv+2, hashsize);
        if (!server.migclear_slot) {
            addReplyError(c, "Unable to make a bitarray. Invalid argument format or Out of memory.");
            return;
        }
        redisLog(REDIS_NOTICE, "Starting migclear.");
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"clearend")) {
        if (c->argc != 2) {
            addReply(c, shared.syntaxerr);
            return;
        }
        redisLog(REDIS_NOTICE, "Client ask for finishing migclear.");
        if (!server.migclear_slot) {
            addReplyError(c, "Migclear job is not in progress. Unable to perform CLEAREND.");
            return;
        }
        redisLog(REDIS_NOTICE,"Finishing migclear.");
        sdsfree(server.migclear_slot);
        server.migclear_slot = NULL;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"status")) {
        if (c->argc != 2) {
            addReply(c, shared.syntaxerr);
            return;
        }
        if (server.migrate_slot) {
            addReplyStatus(c, "MIGSTART");
        } else if (server.migclear_slot) {
            addReplyStatus(c, "CLEARSTART");
        } else {
            addReplyStatus(c, "NOTINPROGRESS");
        }
    } else {
        addReply(c, shared.syntaxerr);
    }
}

void migpexpireatCommand(redisClient *c) {
    dictEntry *de;
    robj *key = c->argv[1], *param = c->argv[2];
    long long when;

    if (getLongLongFromObjectOrReply(c, param, &when, NULL) != REDIS_OK)
        return;

    de = dictFind(c->db->dict,key->ptr);
    if (de == NULL) {
        addReply(c,shared.czero);
        return;
    }
    setExpire(c->db,key,when);
    addReply(c,shared.cone);
    signalModifiedKey(c->db,key);
    server.dirty++;
}
#endif /* NBASE_ARC */

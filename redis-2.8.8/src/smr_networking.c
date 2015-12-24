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

#include "redis.h"
#include "crc16.h"
#include <assert.h>

#ifdef NBASE_ARC

/* forward declaration */
int smrProcessCommand(redisClient *c);

static void smrSetErrorReason(redisClient *c, char *err) {
    sdsfree(c->smr.protocol_error_reply);
    c->smr.protocol_error_reply = sdsnew(err);
}

static void smrSetErrorReasonFormat(redisClient *c, const char *fmt, ...) {
    size_t l, j;
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Make sure there are no newlines in the string, otherwise invalid protocol
     * is emitted. */
    l = sdslen(s);
    for (j = 0; j < l; j++) {
        if (s[j] == '\r' || s[j] == '\n') s[j] = ' ';
    }
    smrSetErrorReason(c,s);
    sdsfree(s);
}

void freeClientSmrArgv(redisClient *c) {
    int j;
    for (j = 0; j < c->smr.argc; j++)
        decrRefCount(c->smr.argv[j]);
    c->smr.argc = 0;
    c->smr.cmd = NULL;
}

void smrInitClient(redisClient *c) {
    /* If this is a fake client(fd == -1) for executing smr log or lua script
     * , smr data structure is not necessary.*/
    if (c->fd == -1) {
        return;
    }
    redisAssert(c->fd > 0);
    c->smr.querybuf = sdsempty();
    c->smr.querybuf_peak = 0;
    c->smr.querylen = 0;
    c->smr.argc = 0;
    c->smr.argv = NULL;
    dlisth_init(&c->smr.client_callbacks);
    c->smr.cmd = NULL;
    c->smr.reqtype = 0;
    c->smr.multibulklen = 0;
    c->smr.bulklen = -1;
    c->smr.flags = 0;
    c->smr.protocol_error_reply = NULL;
}

static void smrSetProtocolError(redisClient *c, int pos) {
    callbackInfo *cb;

    if (server.verbosity >= REDIS_VERBOSE) {
        sds client = getClientInfoString(c);
        redisLog(REDIS_VERBOSE,
            "Protocol error from client: %s", client);
        sdsfree(client);
    }
    c->smr.flags |= REDIS_SSS_CLIENT_CLOSING;
    sdsrange(c->smr.querybuf,pos,-1);
    smr_session_close(server.smr_conn, c->fd);

    if (c->smr.argv) {
        freeClientSmrArgv(c);
        zfree(c->smr.argv);
        c->smr.argv = NULL;
    }

    cb = zmalloc(sizeof(callbackInfo));
    dlisth_insert_before(&cb->global_head, &server.global_callbacks);
    dlisth_insert_before(&cb->client_head, &c->smr.client_callbacks);
    cb->client = c;
    cb->argc = 0;
    cb->argv = NULL;
}

/* resetClient prepare the client to process the next command */
void smrResetClient(redisClient *c) {
    if (c->smr.argv) {
        freeClientSmrArgv(c);
        zfree(c->smr.argv);
        c->smr.argv = NULL;
    }

    sdsrange(c->smr.querybuf,c->smr.querylen,-1);
    c->smr.querylen = 0;
    c->smr.reqtype = 0;
    c->smr.multibulklen = 0;
    c->smr.bulklen = -1;
}

/* modified from original source from networking.c
 * 1. defer applying sdsrange() to querybuf 
 * 2. modify protocol error handling */
int smrProcessInlineBuffer(redisClient *c) {
    char *newline;
    int argc, j, has_cr;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(c->smr.querybuf,'\n');
    has_cr = 0;

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (sdslen(c->smr.querybuf) > REDIS_INLINE_MAX_SIZE) {
            smrSetErrorReason(c,"Protocol error: too big inline request");
            smrSetProtocolError(c,0);
        }
        return REDIS_ERR;
    }

    /* Handle the \r\n case. */
    if (newline && newline != c->smr.querybuf && *(newline-1) == '\r') {
        newline--;
        has_cr = 1;
    }

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->smr.querybuf);
    aux = sdsnewlen(c->smr.querybuf,querylen);
    argv = sdssplitargs(aux, &argc);
    sdsfree(aux);
    if (argv == NULL) {
        smrSetErrorReason(c,"Protocol error: unbalanced quotes in request");
        smrSetProtocolError(c,0);
        return REDIS_ERR;
    }

    /* Leave data after the first line of the query in the buffer */
    /* NBASE_ARC bug fix: Check carriage return */
    c->smr.querylen = querylen+1+has_cr;

    /* Setup argv array on client structure */
    assert(!c->smr.argv);
    c->smr.argv = zmalloc(sizeof(robj *)*argc);

    /* Create redis objects for all arguments. */
    for (c->smr.argc = 0, j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            c->smr.argv[c->smr.argc] = createObject(REDIS_STRING,argv[j]);
            c->smr.argc++;
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return REDIS_OK;
}

/* modified from original source from networking.c
 * 1. defer applying sdsrange() to querybuf 
 * 2. modify protocol error handling 
 * 3. remove optimization */
int smrProcessMultibulkBuffer(redisClient *c) {
    char *newline = NULL;
    int pos = c->smr.querylen, ok;
    long long ll;

    if (c->smr.multibulklen == 0) {
        /* The client should have been reset */
        redisAssertWithInfo(c,NULL,c->smr.argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->smr.querybuf,'\r');
        if (newline == NULL) {
            if (sdslen(c->smr.querybuf) > REDIS_INLINE_MAX_SIZE) {
                smrSetErrorReason(c,"Protocol error: too big mbulk count string");
                smrSetProtocolError(c,0);
            }
            return REDIS_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->smr.querybuf) > ((signed)sdslen(c->smr.querybuf)-2))
            return REDIS_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        redisAssertWithInfo(c,NULL,c->smr.querybuf[0] == '*');
        ok = string2ll(c->smr.querybuf+1,newline-(c->smr.querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            smrSetErrorReason(c,"Protocol error: invalid multibulk length");
            smrSetProtocolError(c,pos);
            return REDIS_ERR;
        }

        pos = (newline-c->smr.querybuf)+2;
        if (ll <= 0) {
            /* argc == 0, reset in processInputBuffer */
            c->smr.querylen = pos;
            return REDIS_OK;
        }

        c->smr.multibulklen = ll;

        /* Setup argv array on client structure */
        assert(!c->smr.argv);
        c->smr.argv = zmalloc(sizeof(robj *)*c->smr.multibulklen);
    }

    redisAssertWithInfo(c,NULL,c->smr.multibulklen > 0);
    while(c->smr.multibulklen) {
        /* Read bulk length if unknown */
        if (c->smr.bulklen == -1) {
            newline = strchr(c->smr.querybuf+pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->smr.querybuf)-pos > REDIS_INLINE_MAX_SIZE) {
                    smrSetErrorReason(c,"Protocol error: too big bulk count string");
                    smrSetProtocolError(c,0);
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->smr.querybuf) > ((signed)sdslen(c->smr.querybuf)-2))
                break;

            if (c->smr.querybuf[pos] != '$') {
                smrSetErrorReasonFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->smr.querybuf[pos]);
                smrSetProtocolError(c,pos);
                return REDIS_ERR;
            }

            ok = string2ll(c->smr.querybuf+pos+1,newline-(c->smr.querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                smrSetErrorReason(c,"Protocol error: invalid bulk length");
                smrSetProtocolError(c,pos);
                return REDIS_ERR;
            }

            pos += newline-(c->smr.querybuf+pos)+2;
            c->smr.bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->smr.querybuf)-pos < (unsigned)(c->smr.bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            c->smr.argv[c->smr.argc++] =
                createStringObject(c->smr.querybuf+pos,c->smr.bulklen);
            pos += c->smr.bulklen+2;
            c->smr.bulklen = -1;
            c->smr.multibulklen--;
        }
    }

    /* Save pos */
    c->smr.querylen = pos;

    /* We're done when c->multibulk == 0 */
    if (c->smr.multibulklen == 0) return REDIS_OK;

    /* Still not read to process the command */
    return REDIS_ERR;
}

void smrProcessInputBuffer(redisClient *c) {
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->smr.querybuf)) {
        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & REDIS_BLOCKED) return;

        /* REDIS_SSS_CLIENT_CLOSING closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands). */
        if (c->smr.flags & REDIS_SSS_CLIENT_CLOSING) return;

        /* Determine request type when unknown. */
        if (!c->smr.reqtype) {
            if (c->smr.querybuf[0] == '*') {
                c->smr.reqtype = REDIS_REQ_MULTIBULK;
            } else {
                c->smr.reqtype = REDIS_REQ_INLINE;
            }
        }

        if (c->smr.reqtype == REDIS_REQ_INLINE) {
            if (smrProcessInlineBuffer(c) != REDIS_OK) break;
        } else if (c->smr.reqtype == REDIS_REQ_MULTIBULK) {
            if (smrProcessMultibulkBuffer(c) != REDIS_OK) break;
        } else {
            redisPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->smr.argc > 0) smrProcessCommand(c);
        smrResetClient(c);
    }
}

void smrReadQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = (redisClient*) privdata;
    int nread, readlen;
    size_t qblen;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    server.current_client = c;
    readlen = REDIS_IOBUF_LEN;

    qblen = sdslen(c->smr.querybuf);
    if (c->smr.querybuf_peak < qblen) c->smr.querybuf_peak = qblen;
    c->smr.querybuf = sdsMakeRoomFor(c->smr.querybuf, readlen);
    nread = read(fd, c->smr.querybuf+qblen, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            redisLog(REDIS_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        redisLog(REDIS_VERBOSE, "Client closed connection");
        freeClient(c);
        return;
    }
    if (nread) {
        sdsIncrLen(c->smr.querybuf,nread);
        c->lastinteraction = server.unixtime;
    } else {
        server.current_client = NULL;
        return;
    }
    if (sdslen(c->smr.querybuf) > server.client_max_querybuf_len) {
        sds ci = getClientInfoString(c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->smr.querybuf,64);
        redisLog(REDIS_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClient(c);
        return;
    }
    smrProcessInputBuffer(c);
    server.current_client = NULL;
}

static int smrGetKeyhash(redisClient *c) {
    /* If the query is readonly type or invalid, replicating log isn't necessary.
     * So, keyhash is not requried */
    if (!c->smr.cmd || c->smr.cmd->flags & REDIS_CMD_READONLY) {
        return SMR_SESSION_DATA_HASH_NONE;
    } else if ((c->smr.cmd->arity > 0 && c->smr.cmd->arity != c->smr.argc) ||
            (c->smr.argc < -c->smr.cmd->arity)) {
        return SMR_SESSION_DATA_HASH_NONE;
    }

    if (c->smr.cmd->firstkey == c->smr.cmd->lastkey) {
        /* If the query is single key type, keyhash of the key is required for migration */
        sds key = c->smr.argv[c->smr.cmd->firstkey]->ptr;
        return crc16(key, sdslen(key), 0);
    } else {
        /* If the query is multi key type, this query should always be copied for migration */
        return SMR_SESSION_DATA_HASH_ALL;
    }
}

/* Backend Ping command which is not replicated in smr */
void bpingCommand(redisClient *c) {
    addReply(c,shared.pong);
}

/* To block input after this command */
void quitCommand(redisClient *c) {
    c->smr.flags |= REDIS_SSS_CLIENT_CLOSING;
}

static void replaceParsedArgs(redisClient *c, robj *o) {
    freeClientSmrArgv(c);
    zfree(c->smr.argv);
    c->smr.argv = NULL;

    c->smr.argv = zmalloc(sizeof(robj *) * 2);
    incrRefCount(shared.addreply_through_smr);
    c->smr.argv[0] = shared.addreply_through_smr;
    c->smr.argv[1] = o;
    c->smr.argc = 2;
    c->smr.cmd = NULL;
}

int smrProcessCommand(redisClient *c) {
    int keyhash = 0, cmdflags = 0;
    int sid;
    callbackInfo *cb;

    c->smr.cmd = lookupCommand(c->smr.argv[0]->ptr);

    /* c->smr.cmd == NULL is permitted. because responses for the pipelined
     * requests must be sent in order. (NO SUCH COMMAND should be included) */
    if (c->smr.cmd != NULL) {
        if (c->smr.cmd->proc == bpingCommand) {
            bpingCommand(c);
            return REDIS_OK;
        } else if (c->smr.cmd->proc == quitCommand) {
            quitCommand(c);
        }
        cmdflags = c->smr.cmd->flags;

#ifndef COVERAGE_TEST
        /* if the command is not permitted in cluster environment,
         * act as if this command does not exist */
        if (c->smr.cmd->flags & REDIS_CMD_NOCLUSTER) {
            replaceParsedArgs(c, createStringObject("-ERR Unsupported Command\r\n", 26));
            cmdflags = 0;
        }
#endif
    }

    /* If this partition group is in OOM state, reply oom error message. 
     * The message has to be in order through smr log stream.
     *
     * we can use mstime of local machine for checking oom, 
     * because message is not inserted to smr log, yet. */
    if (mstime() < server.smr_oom_until && (cmdflags & REDIS_CMD_DENYOOM)) {
        incrRefCount(shared.oomerr);
        replaceParsedArgs(c, shared.oomerr);
        cmdflags = 0;
    }
        
    keyhash = smrGetKeyhash(c);
    assert(c->fd < 65535);
    sid = (0xFFFF0000 & (cmdflags << 16)) | (0x0000FFFF & c->fd);

    /* save parsed argv to reuse after smr callback is invoked */
    cb = zmalloc(sizeof(callbackInfo));
    dlisth_insert_before(&cb->global_head, &server.global_callbacks);
    dlisth_insert_before(&cb->client_head, &c->smr.client_callbacks);
    cb->client = c;
    cb->argc = c->smr.argc;
    cb->argv = c->smr.argv;
    cb->hash = keyhash;
    c->smr.argc = 0;
    c->smr.argv = NULL;
    
    if (cmdflags & REDIS_CMD_WRITE) {
        smr_session_data(server.smr_conn, sid, keyhash, c->smr.querybuf, c->smr.querylen);
    } else {
        smr_session_data(server.smr_conn, sid, keyhash, "R", 1);
    }

    return REDIS_OK;
}
#endif /* NBASE_ARC */

/**
 * Copyright (c) 2011 Jonathan Leibiusky
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.navercorp.redis.cluster.connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.redis.cluster.util.RedisInputStream;
import com.navercorp.redis.cluster.util.RedisOutputStream;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

/**
 * Redis Cluster Protocol. (jedis 2.1.0)
 *
 * @author jaehong.kim
 */
public final class RedisProtocol {
    private final static Logger log = LoggerFactory
            .getLogger(RedisProtocol.class);

    /**
     * The Constant DEFAULT_PORT.
     */
    public static final int DEFAULT_PORT = 6379;

    /**
     * The Constant DEFAULT_TIMEOUT.
     */
    public static final int DEFAULT_TIMEOUT = 2000;

    /**
     * The Constant DEFAULT_DATABASE.
     */
    public static final int DEFAULT_DATABASE = 0;

    /**
     * The Constant CHARSET.
     */
    public static final String CHARSET = "UTF-8";

    /**
     * The Constant DOLLAR_BYTE.
     */
    public static final byte DOLLAR_BYTE = '$';

    /**
     * The Constant ASTERISK_BYTE.
     */
    public static final byte ASTERISK_BYTE = '*';

    /**
     * The Constant PLUS_BYTE.
     */
    public static final byte PLUS_BYTE = '+';

    /**
     * The Constant MINUS_BYTE.
     */
    public static final byte MINUS_BYTE = '-';

    /**
     * The Constant COLON_BYTE.
     */
    public static final byte COLON_BYTE = ':';

    /**
     * Instantiates a new redis protocol.
     */
    private RedisProtocol() {
        // this prevent the class from instantiation
    }

    /**
     * Send command.
     *
     * @param os      the os
     * @param command the command
     * @param args    the args
     */
    public static void sendCommand(final RedisOutputStream os,
                                   final Command command, final byte[]... args) {
        sendCommand(os, command.raw, args);
    }

    /**
     * Send command.
     *
     * @param os      the os
     * @param command the command
     * @param args    the args
     */
    private static void sendCommand(final RedisOutputStream os,
                                    final byte[] command, final byte[]... args) {
        try {
            os.write(ASTERISK_BYTE);
            os.writeIntCrLf(args.length + 1);
            os.write(DOLLAR_BYTE);
            os.writeIntCrLf(command.length);
            os.write(command);
            os.writeCrLf();

            for (final byte[] arg : args) {
                os.write(DOLLAR_BYTE);
                os.writeIntCrLf(arg.length);
                os.write(arg);
                os.writeCrLf();
            }
        } catch (IOException e) {
            throw new JedisConnectionException(e);
        }
    }

    /**
     * Process error.
     *
     * @param is the is
     */
    private static void processError(final RedisInputStream is) {
        String message = is.readLine();
        log.trace("[RedisProtocol] Reply error. -{}", message);
        throw new JedisDataException(message);
    }

    /**
     * Process.
     *
     * @param is the is
     * @return the object
     */
    private static Object process(final RedisInputStream is) {
        byte b = is.readByte();
        if (b == MINUS_BYTE) {
            processError(is);
            return null;
        } else if (b == ASTERISK_BYTE) {
            return processMultiBulkReply(is);
        } else if (b == COLON_BYTE) {
            return processInteger(is);
        } else if (b == DOLLAR_BYTE) {
            return processBulkReply(is);
        } else if (b == PLUS_BYTE) {
            return processStatusCodeReply(is);
        } else {
            throw new JedisConnectionException("Unknown reply: " + (char) b);
        }
    }

    /**
     * Process status code reply.
     *
     * @param is the is
     * @return the byte[]
     */
    private static byte[] processStatusCodeReply(final RedisInputStream is) {
        final String statusCode = is.readLine();
        log.trace("[RedisProtocol] Reply status-code. +{}", statusCode);
        return SafeEncoder.encode(statusCode);
    }

    /**
     * Process bulk reply.
     *
     * @param is the is
     * @return the byte[]
     */
    private static byte[] processBulkReply(final RedisInputStream is) {
        int len = Integer.parseInt(is.readLine());
        log.trace("[RedisProtocol] Reply bulk-reply. ${}", len);
        if (len == -1) {
            return null;
        }
        byte[] read = new byte[len];
        int offset = 0;
        while (offset < len) {
            offset += is.read(read, offset, (len - offset));
        }
        // read 2 more bytes for the command delimiter
        is.readByte();
        is.readByte();

        return read;
    }

    /**
     * Process integer.
     *
     * @param is the is
     * @return the long
     */
    private static Long processInteger(final RedisInputStream is) {
        final String num = is.readLine();
        log.trace("[RedisProtocol] Reply integer. :{}", num);
        return Long.valueOf(num);
    }

    /**
     * Process multi bulk reply.
     *
     * @param is the is
     * @return the list
     */
    private static List<Object> processMultiBulkReply(final RedisInputStream is) {
        int num = Integer.parseInt(is.readLine());
        log.trace("[RedisProtocol] Reply multi-bulk-reply. *{}", num);
        if (num == -1) {
            return null;
        }
        List<Object> ret = new ArrayList<Object>(num);
        for (int i = 0; i < num; i++) {
            try {
                ret.add(process(is));
            } catch (JedisDataException e) {
                ret.add(e);
            }
        }
        return ret;
    }

    /**
     * Read.
     *
     * @param is the is
     * @return the object
     */
    public static Object read(final RedisInputStream is) {
        return process(is);
    }

    /**
     * To byte array.
     *
     * @param value the value
     * @return the byte[]
     */
    public static final byte[] toByteArray(final boolean value) {
        return toByteArray(value ? 1 : 0);
    }

    /**
     * To byte array.
     *
     * @param value the value
     * @return the byte[]
     */
    public static final byte[] toByteArray(final int value) {
        return SafeEncoder.encode(String.valueOf(value));
    }

    /**
     * To byte array.
     *
     * @param value the value
     * @return the byte[]
     */
    public static final byte[] toByteArray(final long value) {
        return SafeEncoder.encode(String.valueOf(value));
    }

    /**
     * To byte array.
     *
     * @param value the value
     * @return the byte[]
     */
    public static final byte[] toByteArray(final double value) {
        return SafeEncoder.encode(String.valueOf(value));
    }

    /**
     * The Enum Command.
     */
    public static enum Command {

        /**
         * The PING.
         */
        PING,

        /**
         * The SET.
         */
        SET,

        /**
         * The GET.
         */
        GET,
        
        GEOADD,
        
        GEODIST,
        
        GEOHASH,
        
        GEOPOS,
        
        GEORADIUS,
        
        GEORADIUSBYMEMBER,
        
        PFADD,
        
        PFCOUNT,
        
        PFDEBUG,

        /**
         * The QUIT.
         */
        QUIT,

        /**
         * The EXISTS.
         */
        EXISTS,

        /**
         * The DEL.
         */
        DEL,

        /**
         * The TYPE.
         */
        TYPE,

        /**
         * The FLUSHDB.
         */
        FLUSHDB,

        /**
         * The KEYS.
         */
        KEYS,

        /**
         * The RANDOMKEY.
         */
        RANDOMKEY,

        /**
         * The RENAME.
         */
        RENAME,

        /**
         * The RENAMENX.
         */
        RENAMENX,

        /**
         * The RENAMEX.
         */
        RENAMEX,

        /**
         * The EXPIRE.
         */
        EXPIRE,

        /**
         * The EXPIREAT.
         */
        EXPIREAT,

        PEXPIRE,

        PEXPIREAT,

        /**
         * The TTL.
         */
        TTL,

        PTTL,

        /**
         * The SELECT.
         */
        SELECT,

        /**
         * The MOVE.
         */
        MOVE,

        /**
         * The FLUSHALL.
         */
        FLUSHALL,

        /**
         * The GETSET.
         */
        GETSET,

        /**
         * The MGET.
         */
        MGET,

        /**
         * The SETNX.
         */
        SETNX,

        /**
         * The SETEX.
         */
        SETEX,

        /**
         * The MSET.
         */
        MSET,

        /**
         * The MSETNX.
         */
        MSETNX,

        /**
         * The DECRBY.
         */
        DECRBY,

        /**
         * The DECR.
         */
        DECR,

        /**
         * The INCRBY.
         */
        INCRBY,

        INCRBYFLOAT,

        /**
         * The INCR.
         */
        INCR,

        /**
         * The APPEND.
         */
        APPEND,

        /**
         * The SUBSTR.
         */
        SUBSTR,

        PSETEX,

        BITCOUNT,
        
        BITFIELD,
        
        BITPOS,

        /**
         * The HSET.
         */
        HSET,

        /**
         * The HGET.
         */
        HGET,

        /**
         * The HSETNX.
         */
        HSETNX,

        /**
         * The HMSET.
         */
        HMSET,

        /**
         * The HMGET.
         */
        HMGET,

        /**
         * The HINCRBY.
         */
        HINCRBY,

        /**
         * The HEXISTS.
         */
        HEXISTS,

        /**
         * The HDEL.
         */
        HDEL,

        /**
         * The HLEN.
         */
        HLEN,

        /**
         * The HKEYS.
         */
        HKEYS,

        /**
         * The HVALS.
         */
        HVALS,

        /**
         * The HGETALL.
         */
        HGETALL,

        HSCAN,
        
        HSTRLEN,

        HINCRBYFLOAT,

        /**
         * The RPUSH.
         */
        RPUSH,

        /**
         * The LPUSH.
         */
        LPUSH,

        /**
         * The LLEN.
         */
        LLEN,

        /**
         * The LRANGE.
         */
        LRANGE,

        /**
         * The LTRIM.
         */
        LTRIM,

        /**
         * The LINDEX.
         */
        LINDEX,

        /**
         * The LSET.
         */
        LSET,

        /**
         * The LREM.
         */
        LREM,

        /**
         * The LPOP.
         */
        LPOP,

        /**
         * The RPOP.
         */
        RPOP,

        /**
         * The RPOPLPUSH.
         */
        RPOPLPUSH,

        /**
         * The SADD.
         */
        SADD,

        /**
         * The SMEMBERS.
         */
        SMEMBERS,

        /**
         * The SREM.
         */
        SREM,

        /**
         * The SPOP.
         */
        SPOP,

        /**
         * The SMOVE.
         */
        SMOVE,

        /**
         * The SCARD.
         */
        SCARD,

        /**
         * The SISMEMBER.
         */
        SISMEMBER,

        /**
         * The SINTER.
         */
        SINTER,

        /**
         * The SINTERSTORE.
         */
        SINTERSTORE,

        /**
         * The SUNION.
         */
        SUNION,

        /**
         * The SUNIONSTORE.
         */
        SUNIONSTORE,

        /**
         * The SDIFF.
         */
        SDIFF,

        /**
         * The SDIFFSTORE.
         */
        SDIFFSTORE,

        /**
         * The SRANDMEMBER.
         */
        SRANDMEMBER,

        SSCAN,

        /**
         * The ZADD.
         */
        ZADD,

        /**
         * The ZRANGE.
         */
        ZRANGE,

        /**
         * The ZREM.
         */
        ZREM,

        /**
         * The ZINCRBY.
         */
        ZINCRBY,
        
        ZLEXCOUNT,

        /**
         * The ZRANK.
         */
        ZRANK,
        
        ZRANGEBYLEX,
        
        ZREVRANGEBYLEX,
        
        ZREMRANGEBYLEX,

        /**
         * The ZREVRANK.
         */
        ZREVRANK,

        /**
         * The ZREVRANGE.
         */
        ZREVRANGE,

        /**
         * The ZCARD.
         */
        ZCARD,

        /**
         * The ZSCORE.
         */
        ZSCORE,

        /**
         * The MULTI.
         */
        MULTI,

        /**
         * The DISCARD.
         */
        DISCARD,

        /**
         * The EXEC.
         */
        EXEC,

        /**
         * The WATCH.
         */
        WATCH,

        /**
         * The UNWATCH.
         */
        UNWATCH,

        /**
         * The SORT.
         */
        SORT,

        /**
         * The BLPOP.
         */
        BLPOP,

        /**
         * The BRPOP.
         */
        BRPOP,

        /**
         * The AUTH.
         */
        AUTH,

        /**
         * The SUBSCRIBE.
         */
        SUBSCRIBE,

        /**
         * The PUBLISH.
         */
        PUBLISH,

        /**
         * The UNSUBSCRIBE.
         */
        UNSUBSCRIBE,

        /**
         * The PSUBSCRIBE.
         */
        PSUBSCRIBE,

        /**
         * The PUNSUBSCRIBE.
         */
        PUNSUBSCRIBE,

        /**
         * The ZCOUNT.
         */
        ZCOUNT,

        /**
         * The ZRANGEBYSCORE.
         */
        ZRANGEBYSCORE,

        /**
         * The ZREVRANGEBYSCORE.
         */
        ZREVRANGEBYSCORE,

        /**
         * The ZREMRANGEBYRANK.
         */
        ZREMRANGEBYRANK,

        /**
         * The ZREMRANGEBYSCORE.
         */
        ZREMRANGEBYSCORE,

        /**
         * The ZUNIONSTORE.
         */
        ZUNIONSTORE,

        /**
         * The ZINTERSTORE.
         */
        ZINTERSTORE,

        ZSCAN,

        /**
         * The SAVE.
         */
        SAVE,

        /**
         * The BGSAVE.
         */
        BGSAVE,

        /**
         * The BGREWRITEAOF.
         */
        BGREWRITEAOF,

        /**
         * The LASTSAVE.
         */
        LASTSAVE,

        /**
         * The SHUTDOWN.
         */
        SHUTDOWN,

        /**
         * The INFO.
         */
        INFO,

        /**
         * The MONITOR.
         */
        MONITOR,

        /**
         * The SLAVEOF.
         */
        SLAVEOF,

        /**
         * The CONFIG.
         */
        CONFIG,

        /**
         * The STRLEN.
         */
        STRLEN,

        /**
         * The SYNC.
         */
        SYNC,

        /**
         * The LPUSHX.
         */
        LPUSHX,

        /**
         * The PERSIST.
         */
        PERSIST,

        /**
         * The RPUSHX.
         */
        RPUSHX,

        /**
         * The ECHO.
         */
        ECHO,

        /**
         * The LINSERT.
         */
        LINSERT,

        /**
         * The DEBUG.
         */
        DEBUG,

        /**
         * The BRPOPLPUSH.
         */
        BRPOPLPUSH,

        /**
         * The SETBIT.
         */
        SETBIT,

        /**
         * The GETBIT.
         */
        GETBIT,

        /**
         * The SETRANGE.
         */
        SETRANGE,

        /**
         * The GETRANGE.
         */
        GETRANGE,

        /**
         * The EVAL.
         */
        EVAL,

        /**
         * The EVALSHA.
         */
        EVALSHA,

        /**
         * The SCRIPT.
         */
        SCRIPT,

        /**
         * The SLOWLOG.
         */
        SLOWLOG,

        /**
         * The OBJECT.
         */
        OBJECT,

        /**
         * The DBSIZE.
         */
        DBSIZE,

        DUMP,

        RESTORE,
        
        SCAN,
        
        CSCAN,
        
        CSCANDIGEST,
        
        CSCANLEN,

        /**
         * The S3 rem.
         */
        S3REM,

        /**
         * The S3 keys.
         */
        S3KEYS,

        /**
         * The S3 count.
         */
        S3COUNT,

        /**
         * The S3 expire.
         */
        S3EXPIRE,

        /**
         * The S3 lget.
         */
        S3LGET,

        /**
         * The S3 lmget.
         */
        S3LMGET,

        /**
         * The S3 lkeys.
         */
        S3LKEYS,

        /**
         * The S3 lvals.
         */
        S3LVALS,

        /**
         * The S3 ladd.
         */
        S3LADD,

        S3LADDAT,

        /**
         * The S3 lrem.
         */
        S3LREM,

        /**
         * The S3 lmrem.
         */
        S3LMREM,

        /**
         * The S3 lset.
         */
        S3LSET,

        /**
         * The S3 lreplace.
         */
        S3LREPLACE,

        /**
         * The S3 lcount.
         */
        S3LCOUNT,

        /**
         * The S3 lexists.
         */
        S3LEXISTS,

        /**
         * The S3 lexpire.
         */
        S3LEXPIRE,

        /**
         * The S3 lmexpire.
         */
        S3LMEXPIRE,

        /**
         * The S3 lttl.
         */
        S3LTTL,

        /**
         * The S3 lmadd.
         */
        S3LMADD,

        /**
         * The S3 sget.
         */
        S3SGET,

        /**
         * The S3 smget.
         */
        S3SMGET,

        /**
         * The S3 skeys.
         */
        S3SKEYS,

        /**
         * The S3 sadd.
         */
        S3SADD,

        S3SADDAT,

        /**
         * The S3 sset.
         */
        S3SSET,

        /**
         * The S3 srem.
         */
        S3SREM,

        /**
         * The S3 smrem.
         */
        S3SMREM,

        /**
         * The S3 scount.
         */
        S3SCOUNT,

        /**
         * The S3 sexists.
         */
        S3SEXISTS,

        /**
         * The S3 sexpire.
         */
        S3SEXPIRE,

        /**
         * The S3 sttl.
         */
        S3STTL,

        /**
         * The S3 svals.
         */
        S3SVALS,

        /**
         * The S3 smadd.
         */
        S3SMADD,
        
        TOUCH;

        /**
         * The raw.
         */
        public final byte[] raw;

        /**
         * Instantiates a new command.
         */
        Command() {
            raw = SafeEncoder.encode(this.name());
        }
    }

    /**
     * The Enum Keyword.
     */
    public static enum Keyword {

        /**
         * The AGGREGATE.
         */
        AGGREGATE,

        /**
         * The ALPHA.
         */
        ALPHA,

        /**
         * The ASC.
         */
        ASC,

        /**
         * The BY.
         */
        BY,

        /**
         * The DESC.
         */
        DESC,

        /**
         * The GET.
         */
        GET,

        /**
         * The LIMIT.
         */
        LIMIT,

        /**
         * The MESSAGE.
         */
        MESSAGE,

        /**
         * The NO.
         */
        NO,

        /**
         * The NOSORT.
         */
        NOSORT,

        /**
         * The PMESSAGE.
         */
        PMESSAGE,

        /**
         * The PSUBSCRIBE.
         */
        PSUBSCRIBE,

        /**
         * The PUNSUBSCRIBE.
         */
        PUNSUBSCRIBE,

        /**
         * The OK.
         */
        OK,

        /**
         * The ONE.
         */
        ONE,

        /**
         * The QUEUED.
         */
        QUEUED,

        /**
         * The SET.
         */
        SET,

        /**
         * The STORE.
         */
        STORE,

        /**
         * The SUBSCRIBE.
         */
        SUBSCRIBE,

        /**
         * The UNSUBSCRIBE.
         */
        UNSUBSCRIBE,

        /**
         * The WEIGHTS.
         */
        WEIGHTS,

        /**
         * The WITHSCORES.
         */
        WITHSCORES,

        /**
         * The RESETSTAT.
         */
        RESETSTAT,

        /**
         * The RESET.
         */
        RESET,

        /**
         * The FLUSH.
         */
        FLUSH,

        /**
         * The EXISTS.
         */
        EXISTS,

        /**
         * The LOAD.
         */
        LOAD,

        /**
         * The KILL.
         */
        KILL,

        /**
         * The LEN.
         */
        LEN,

        /**
         * The REFCOUNT.
         */
        REFCOUNT,

        /**
         * The ENCODING.
         */
        ENCODING,

        /**
         * The IDLETIME.
         */
        IDLETIME,
        
        INCRBY,
        
        SAT,
        
        OVERFLOW,
        
        PX, 
        
        XX;

        /**
         * The raw.
         */
        public final byte[] raw;

        /**
         * Instantiates a new keyword.
         */
        Keyword() {
            raw = SafeEncoder.encode(this.name().toLowerCase());
        }
    }
}
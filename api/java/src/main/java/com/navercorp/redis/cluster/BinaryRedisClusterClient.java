/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.navercorp.redis.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.navercorp.redis.cluster.connection.RedisProtocol;
import com.navercorp.redis.cluster.triples.TriplesRedisClusterClient;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.ScanParams;

import com.navercorp.redis.cluster.connection.RedisProtocol.Command;
import com.navercorp.redis.cluster.connection.RedisProtocol.Keyword;

/**
 * The Class BinaryRedisClusterClient.
 *
 * @author jaehong.kim
 */
public class BinaryRedisClusterClient extends TriplesRedisClusterClient {

    /**
     * Instantiates a new binary redis cluster client.
     *
     * @param host the host
     */
    public BinaryRedisClusterClient(final String host) {
        super(host);
    }

    /**
     * Instantiates a new binary redis cluster client.
     *
     * @param host the host
     * @param port the port
     */
    public BinaryRedisClusterClient(final String host, final int port) {
        super(host, port);
    }

    /**
     * Join parameters.
     *
     * @param first the first
     * @param rest  the rest
     * @return the byte[][]
     */
    private byte[][] joinParameters(byte[] first, byte[][] rest) {
        byte[][] result = new byte[rest.length + 1][];
        result[0] = first;
        for (int i = 0; i < rest.length; i++) {
            result[i + 1] = rest[i];
        }
        return result;
    }

    /*
     * @see RedisConnection#connect()
     */
    @Override
    public void connect() {
        if (!isConnected()) {
            super.connect();
        }
    }

    /**
     * Ping.
     */
    public void ping() {
        sendCommand(Command.PING);
    }

    /**
     * Sets the.
     *
     * @param key   the key
     * @param value the value
     */
    public void set(final byte[] key, final byte[] value) {
        sendCommand(Command.SET, key, value);
    }

    /**
     * Gets the.
     *
     * @param key the key
     */
    public void get(final byte[] key) {
        sendCommand(Command.GET, key);
    }

    /**
     * Quit.
     */
    public void quit() {
        sendCommand(Command.QUIT);
    }

    /**
     * Exists.
     *
     * @param key the key
     */
    public void exists(final byte[] key) {
        sendCommand(Command.EXISTS, key);
    }

    /**
     * Del.
     *
     * @param keys the keys
     */
    public void del(final byte[]... keys) {
        sendCommand(Command.DEL, keys);
    }

    /**
     * Type.
     *
     * @param key the key
     */
    public void type(final byte[] key) {
        sendCommand(Command.TYPE, key);
    }

    public void persist(final byte[] key) {
        sendCommand(Command.PERSIST, key);
    }

    /**
     * Expire.
     *
     * @param key     the key
     * @param seconds the seconds
     */
    public void expire(final byte[] key, final int seconds) {
        sendCommand(Command.EXPIRE, key, RedisProtocol.toByteArray(seconds));
    }

    /**
     * Expire at.
     *
     * @param key                   the key
     * @param millisecondsTimestamp the unix time
     */
    public void expireAt(final byte[] key, final long millisecondsTimestamp) {
        sendCommand(Command.EXPIREAT, key, RedisProtocol.toByteArray(millisecondsTimestamp));
    }

    public void pexpire(final byte[] key, final long milliseconds) {
        sendCommand(Command.PEXPIRE, key, RedisProtocol.toByteArray(milliseconds));
    }

    public void pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        sendCommand(Command.PEXPIREAT, key, RedisProtocol.toByteArray(millisecondsTimestamp));
    }

    public void ttl(final byte[] key) {
        sendCommand(Command.TTL, key);
    }

    public void pttl(final byte[] key) {
        sendCommand(Command.PTTL, key);
    }

    public void flushAll() {
        sendCommand(Command.FLUSHALL);
    }

    public void getSet(final byte[] key, final byte[] value) {
        sendCommand(Command.GETSET, key, value);
    }

    /**
     * Setnx.
     *
     * @param key   the key
     * @param value the value
     */
    public void setnx(final byte[] key, final byte[] value) {
        sendCommand(Command.SETNX, key, value);
    }

    /**
     * Setex.
     *
     * @param key     the key
     * @param seconds the seconds
     * @param value   the value
     */
    public void setex(final byte[] key, final int seconds, final byte[] value) {
        sendCommand(Command.SETEX, key, RedisProtocol.toByteArray(seconds), value);
    }

    /**
     * Msetnx.
     *
     * @param keysvalues the keysvalues
     */
    public void msetnx(final byte[]... keysvalues) {
        sendCommand(Command.MSETNX, keysvalues);
    }

    /**
     * Decr by.
     *
     * @param key     the key
     * @param integer the integer
     */
    public void decrBy(final byte[] key, final long integer) {
        sendCommand(Command.DECRBY, key, RedisProtocol.toByteArray(integer));
    }

    /**
     * Decr.
     *
     * @param key the key
     */
    public void decr(final byte[] key) {
        sendCommand(Command.DECR, key);
    }

    /**
     * Incr by.
     *
     * @param key     the key
     * @param integer the integer
     */
    public void incrBy(final byte[] key, final long integer) {
        sendCommand(Command.INCRBY, key, RedisProtocol.toByteArray(integer));
    }

    public void incrByFloat(final byte[] key, final double increment) {
        sendCommand(Command.INCRBYFLOAT, key, RedisProtocol.toByteArray(increment));
    }

    /**
     * Incr.
     *
     * @param key the key
     */
    public void incr(final byte[] key) {
        sendCommand(Command.INCR, key);
    }

    /**
     * Append.
     *
     * @param key   the key
     * @param value the value
     */
    public void append(final byte[] key, final byte[] value) {
        sendCommand(Command.APPEND, key, value);
    }

    /**
     * Substr.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void substr(final byte[] key, final int start, final int end) {
        sendCommand(Command.SUBSTR, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    public void psetex(final byte[] key, final long milliseconds, final byte[] value) {
        sendCommand(Command.PSETEX, key, RedisProtocol.toByteArray(milliseconds), value);
    }

    public void bitcount(final byte[] key) {
        sendCommand(Command.BITCOUNT, key);
    }

    public void bitcount(final byte[] key, long start, long end) {
        sendCommand(Command.BITCOUNT, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    /**
     * Hset.
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     */
    public void hset(final byte[] key, final byte[] field, final byte[] value) {
        sendCommand(Command.HSET, key, field, value);
    }

    /**
     * Hget.
     *
     * @param key   the key
     * @param field the field
     */
    public void hget(final byte[] key, final byte[] field) {
        sendCommand(Command.HGET, key, field);
    }

    /**
     * Hsetnx.
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     */
    public void hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        sendCommand(Command.HSETNX, key, field, value);
    }

    /**
     * Hmset.
     *
     * @param key  the key
     * @param hash the hash
     */
    public void hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        final List<byte[]> params = new ArrayList<byte[]>();
        params.add(key);

        for (final Entry<byte[], byte[]> entry : hash.entrySet()) {
            params.add(entry.getKey());
            params.add(entry.getValue());
        }
        sendCommand(Command.HMSET, params.toArray(new byte[params.size()][]));
    }

    /**
     * Hmget.
     *
     * @param key    the key
     * @param fields the fields
     */
    public void hmget(final byte[] key, final byte[]... fields) {
        final byte[][] params = new byte[fields.length + 1][];
        params[0] = key;
        System.arraycopy(fields, 0, params, 1, fields.length);
        sendCommand(Command.HMGET, params);
    }

    /**
     * Hincr by.
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     */
    public void hincrBy(final byte[] key, final byte[] field, final long value) {
        sendCommand(Command.HINCRBY, key, field, RedisProtocol.toByteArray(value));
    }

    /**
     * Hexists.
     *
     * @param key   the key
     * @param field the field
     */
    public void hexists(final byte[] key, final byte[] field) {
        sendCommand(Command.HEXISTS, key, field);
    }

    /**
     * Hdel.
     *
     * @param key    the key
     * @param fields the fields
     */
    public void hdel(final byte[] key, final byte[]... fields) {
        sendCommand(Command.HDEL, joinParameters(key, fields));
    }

    /**
     * Hlen.
     *
     * @param key the key
     */
    public void hlen(final byte[] key) {
        sendCommand(Command.HLEN, key);
    }

    /**
     * Hkeys.
     *
     * @param key the key
     */
    public void hkeys(final byte[] key) {
        sendCommand(Command.HKEYS, key);
    }

    /**
     * Hvals.
     *
     * @param key the key
     */
    public void hvals(final byte[] key) {
        sendCommand(Command.HVALS, key);
    }

    /**
     * Hget all.
     *
     * @param key the key
     */
    public void hgetAll(final byte[] key) {
        sendCommand(Command.HGETALL, key);
    }

    public void hscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(cursor);
        args.addAll(params.getParams());
        sendCommand(Command.HSCAN, args.toArray(new byte[args.size()][]));
    }

    public void hincrByFloat(final byte[] key, final byte[] field, double increment) {
        sendCommand(Command.HINCRBYFLOAT, key, field, RedisProtocol.toByteArray(increment));
    }

    /**
     * Rpush.
     *
     * @param key     the key
     * @param strings the strings
     */
    public void rpush(final byte[] key, final byte[]... strings) {
        sendCommand(Command.RPUSH, joinParameters(key, strings));
    }

    /**
     * Lpush.
     *
     * @param key     the key
     * @param strings the strings
     */
    public void lpush(final byte[] key, final byte[]... strings) {
        sendCommand(Command.LPUSH, joinParameters(key, strings));
    }

    /**
     * Llen.
     *
     * @param key the key
     */
    public void llen(final byte[] key) {
        sendCommand(Command.LLEN, key);
    }

    /**
     * Lrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void lrange(final byte[] key, final long start, final long end) {
        sendCommand(Command.LRANGE, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    /**
     * Ltrim.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void ltrim(final byte[] key, final long start, final long end) {
        sendCommand(Command.LTRIM, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    /**
     * Lindex.
     *
     * @param key   the key
     * @param index the index
     */
    public void lindex(final byte[] key, final long index) {
        sendCommand(Command.LINDEX, key, RedisProtocol.toByteArray(index));
    }

    /**
     * Lset.
     *
     * @param key   the key
     * @param index the index
     * @param value the value
     */
    public void lset(final byte[] key, final long index, final byte[] value) {
        sendCommand(Command.LSET, key, RedisProtocol.toByteArray(index), value);
    }

    /**
     * Lrem.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     */
    public void lrem(final byte[] key, long count, final byte[] value) {
        sendCommand(Command.LREM, key, RedisProtocol.toByteArray(count), value);
    }

    /**
     * Lpop.
     *
     * @param key the key
     */
    public void lpop(final byte[] key) {
        sendCommand(Command.LPOP, key);
    }

    /**
     * Rpop.
     *
     * @param key the key
     */
    public void rpop(final byte[] key) {
        sendCommand(Command.RPOP, key);
    }

    /**
     * Sadd.
     *
     * @param key     the key
     * @param members the members
     */
    public void sadd(final byte[] key, final byte[]... members) {
        sendCommand(Command.SADD, joinParameters(key, members));
    }

    /**
     * Smembers.
     *
     * @param key the key
     */
    public void smembers(final byte[] key) {
        sendCommand(Command.SMEMBERS, key);
    }

    /**
     * Srem.
     *
     * @param key     the key
     * @param members the members
     */
    public void srem(final byte[] key, final byte[]... members) {
        sendCommand(Command.SREM, joinParameters(key, members));
    }

    /**
     * Scard.
     *
     * @param key the key
     */
    public void scard(final byte[] key) {
        sendCommand(Command.SCARD, key);
    }

    /**
     * Sismember.
     *
     * @param key    the key
     * @param member the member
     */
    public void sismember(final byte[] key, final byte[] member) {
        sendCommand(Command.SISMEMBER, key, member);
    }

    /**
     * Srandmember.
     *
     * @param key the key
     */
    public void srandmember(final byte[] key) {
        sendCommand(Command.SRANDMEMBER, key);
    }

    public void srandmember(final byte[] key, final int count) {
        sendCommand(Command.SRANDMEMBER, key, RedisProtocol.toByteArray(count));
    }

    public void sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(cursor);
        args.addAll(params.getParams());
        sendCommand(Command.SSCAN, args.toArray(new byte[args.size()][]));
    }

    /**
     * Zadd.
     *
     * @param key    the key
     * @param score  the score
     * @param member the member
     */
    public void zadd(final byte[] key, final double score, final byte[] member) {
        sendCommand(Command.ZADD, key, RedisProtocol.toByteArray(score), member);
    }

    /**
     * Zadd binary.
     *
     * @param key          the key
     * @param scoreMembers the score members
     */
    public void zaddBinary(final byte[] key, Map<Double, byte[]> scoreMembers) {
        ArrayList<byte[]> args = new ArrayList<byte[]>(scoreMembers.size() * 2 + 1);

        args.add(key);

        for (Map.Entry<Double, byte[]> entry : scoreMembers.entrySet()) {
            args.add(RedisProtocol.toByteArray(entry.getKey()));
            args.add(entry.getValue());
        }

        byte[][] argsArray = new byte[args.size()][];
        args.toArray(argsArray);

        sendCommand(Command.ZADD, argsArray);
    }

    public void zaddBinary2(final byte[] key, final Map<byte[], Double> scoreMembers) {
        ArrayList<byte[]> args = new ArrayList<byte[]>(scoreMembers.size() * 2 + 1);
        args.add(key);

        for (Map.Entry<byte[], Double> entry : scoreMembers.entrySet()) {
            args.add(RedisProtocol.toByteArray(entry.getValue()));
            args.add(entry.getKey());
        }

        byte[][] argsArray = new byte[args.size()][];
        args.toArray(argsArray);

        sendCommand(Command.ZADD, argsArray);
    }

    /**
     * Zrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrange(final byte[] key, final long start, final long end) {
        sendCommand(Command.ZRANGE, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    /**
     * Zrem.
     *
     * @param key     the key
     * @param members the members
     */
    public void zrem(final byte[] key, final byte[]... members) {
        sendCommand(Command.ZREM, joinParameters(key, members));
    }

    /**
     * Zincrby.
     *
     * @param key    the key
     * @param score  the score
     * @param member the member
     */
    public void zincrby(final byte[] key, final double score, final byte[] member) {
        sendCommand(Command.ZINCRBY, key, RedisProtocol.toByteArray(score), member);
    }

    /**
     * Zrank.
     *
     * @param key    the key
     * @param member the member
     */
    public void zrank(final byte[] key, final byte[] member) {
        sendCommand(Command.ZRANK, key, member);
    }

    /**
     * Zrevrank.
     *
     * @param key    the key
     * @param member the member
     */
    public void zrevrank(final byte[] key, final byte[] member) {
        sendCommand(Command.ZREVRANK, key, member);
    }

    /**
     * Zrevrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrevrange(final byte[] key, final long start, final long end) {
        sendCommand(Command.ZREVRANGE, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    /**
     * Zrange with scores.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrangeWithScores(final byte[] key, final long start, final long end) {
        sendCommand(Command.ZRANGE, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end), Keyword.WITHSCORES.raw);
    }

    /**
     * Zrevrange with scores.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrevrangeWithScores(final byte[] key, final long start, final long end) {
        sendCommand(Command.ZREVRANGE, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end), Keyword.WITHSCORES.raw);
    }

    /**
     * Zcard.
     *
     * @param key the key
     */
    public void zcard(final byte[] key) {
        sendCommand(Command.ZCARD, key);
    }

    /**
     * Zscore.
     *
     * @param key    the key
     * @param member the member
     */
    public void zscore(final byte[] key, final byte[] member) {
        sendCommand(Command.ZSCORE, key, member);
    }

    /**
     * Zcount.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zcount(final byte[] key, final byte[] min, final byte[] max) {
        sendCommand(Command.ZCOUNT, key, min, max);
    }

    /**
     * Zrange by score.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        sendCommand(Command.ZRANGEBYSCORE, key, min, max);
    }

    /**
     * Zrevrange by score.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     */
    public void zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        sendCommand(Command.ZREVRANGEBYSCORE, key, max, min);
    }

    /**
     * Zrange by score.
     *
     * @param key    the key
     * @param min    the min
     * @param max    the max
     * @param offset the offset
     * @param count  the count
     */
    public void zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset, int count) {
        sendCommand(Command.ZRANGEBYSCORE, key, min, max, Keyword.LIMIT.raw, RedisProtocol.toByteArray(offset), RedisProtocol.toByteArray(count));
    }

    /**
     * Zrevrange by score.
     *
     * @param key    the key
     * @param max    the max
     * @param min    the min
     * @param offset the offset
     * @param count  the count
     */
    public void zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset, int count) {
        sendCommand(Command.ZREVRANGEBYSCORE, key, max, min, Keyword.LIMIT.raw, RedisProtocol.toByteArray(offset), RedisProtocol.toByteArray(count));
    }

    /**
     * Zrange by score with scores.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        sendCommand(Command.ZRANGEBYSCORE, key, min, max, Keyword.WITHSCORES.raw);
    }

    /**
     * Zrevrange by score with scores.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     */
    public void zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        sendCommand(Command.ZREVRANGEBYSCORE, key, max, min, Keyword.WITHSCORES.raw);
    }

    /**
     * Zrange by score with scores.
     *
     * @param key    the key
     * @param min    the min
     * @param max    the max
     * @param offset the offset
     * @param count  the count
     */
    public void zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                        final int count) {
        sendCommand(Command.ZRANGEBYSCORE, key, min, max, Keyword.LIMIT.raw, RedisProtocol.toByteArray(offset), RedisProtocol.toByteArray(count), Keyword.WITHSCORES.raw);
    }

    /**
     * Zrevrange by score with scores.
     *
     * @param key    the key
     * @param max    the max
     * @param min    the min
     * @param offset the offset
     * @param count  the count
     */
    public void zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min, final int offset,
                                           final int count) {
        sendCommand(Command.ZREVRANGEBYSCORE, key, max, min, Keyword.LIMIT.raw, RedisProtocol.toByteArray(offset), RedisProtocol.toByteArray(count), Keyword.WITHSCORES.raw);
    }

    /**
     * Zremrange by rank.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zremrangeByRank(final byte[] key, final long start, final long end) {
        sendCommand(Command.ZREMRANGEBYRANK, key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    /**
     * Zremrange by score.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        sendCommand(Command.ZREMRANGEBYSCORE, key, start, end);
    }

    public void zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(cursor);
        args.addAll(params.getParams());
        sendCommand(Command.ZSCAN, args.toArray(new byte[args.size()][]));
    }

    /**
     * Slaveof no one.
     */
    public void slaveofNoOne() {
        sendCommand(Command.SLAVEOF, Keyword.NO.raw, Keyword.ONE.raw);
    }

    /**
     * Config get.
     *
     * @param pattern the pattern
     */
    public void configGet(final byte[] pattern) {
        sendCommand(Command.CONFIG, Keyword.GET.raw, pattern);
    }

    /**
     * Config set.
     *
     * @param parameter the parameter
     * @param value     the value
     */
    public void configSet(final byte[] parameter, final byte[] value) {
        sendCommand(Command.CONFIG, Keyword.SET.raw, parameter, value);
    }

    /**
     * Strlen.
     *
     * @param key the key
     */
    public void strlen(final byte[] key) {
        sendCommand(Command.STRLEN, key);
    }

    /**
     * Lpushx.
     *
     * @param key    the key
     * @param string the string
     */
    public void lpushx(final byte[] key, final byte[] string) {
        sendCommand(Command.LPUSHX, key, string);
    }

    /**
     * Rpushx.
     *
     * @param key    the key
     * @param string the string
     */
    public void rpushx(final byte[] key, final byte[] string) {
        sendCommand(Command.RPUSHX, key, string);
    }

    /**
     * Linsert.
     *
     * @param key   the key
     * @param where the where
     * @param pivot the pivot
     * @param value the value
     */
    public void linsert(final byte[] key, final BinaryClient.LIST_POSITION where, final byte[] pivot, final byte[] value) {
        sendCommand(Command.LINSERT, key, where.raw, pivot, value);
    }

    /**
     * Debug.
     *
     * @param params the params
     */
    public void debug(final DebugParams params) {
        sendCommand(Command.DEBUG, params.getCommand());
    }

    /**
     * Setbit.
     *
     * @param key    the key
     * @param offset the offset
     * @param value  the value
     */
    public void setbit(byte[] key, long offset, byte[] value) {
        sendCommand(Command.SETBIT, key, RedisProtocol.toByteArray(offset), value);
    }

    /**
     * Gets the bit.
     *
     * @param key    the key
     * @param offset the offset
     * @return the bit
     */
    public void getbit(byte[] key, long offset) {
        sendCommand(Command.GETBIT, key, RedisProtocol.toByteArray(offset));
    }

    /**
     * Setrange.
     *
     * @param key    the key
     * @param offset the offset
     * @param value  the value
     */
    public void setrange(byte[] key, long offset, byte[] value) {
        sendCommand(Command.SETRANGE, key, RedisProtocol.toByteArray(offset), value);
    }

    /**
     * Gets the range.
     *
     * @param key         the key
     * @param startOffset the start offset
     * @param endOffset   the end offset
     * @return the range
     */
    public void getrange(byte[] key, long startOffset, long endOffset) {
        sendCommand(Command.GETRANGE, key, RedisProtocol.toByteArray(startOffset), RedisProtocol.toByteArray(endOffset));
    }

    /*
     * @see RedisConnection#disconnect()
     */
    public void disconnect() {
        super.disconnect();
    }

    /**
     * Slowlog len.
     */
    public void slowlogLen() {
        sendCommand(Command.SLOWLOG, Keyword.LEN.raw);
    }

    /**
     * Object refcount.
     *
     * @param key the key
     */
    public void objectRefcount(byte[] key) {
        sendCommand(Command.OBJECT, Keyword.REFCOUNT.raw, key);
    }

    /**
     * Object idletime.
     *
     * @param key the key
     */
    public void objectIdletime(byte[] key) {
        sendCommand(Command.OBJECT, Keyword.IDLETIME.raw, key);
    }

    /**
     * Object encoding.
     *
     * @param key the key
     */
    public void objectEncoding(byte[] key) {
        sendCommand(Command.OBJECT, Keyword.ENCODING.raw, key);
    }

    public void info() {
        sendCommand(Command.INFO);
    }

    public void info(final String section) {
        sendCommand(Command.INFO, section);
    }

    public void mget(final byte[]... keys) {
        sendCommand(Command.MGET, keys);
    }

    public void mset(final byte[]... keysvalues) {
        sendCommand(Command.MSET, keysvalues);
    }

    public void dbSize() {
        sendCommand(Command.DBSIZE);
    }

    public void dump(final byte[] key) {
        sendCommand(Command.DUMP, key);
    }

    public void restore(final byte[] key, final long ttl, final byte[] serializedValue) {
        sendCommand(Command.RESTORE, key, RedisProtocol.toByteArray(ttl), serializedValue);
    }
}
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
package com.navercorp.redis.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.navercorp.redis.cluster.connection.RedisProtocol;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.ScanParams;
import redis.clients.util.SafeEncoder;

/**
 * The Class RedisClusterClient.
 *
 * @author jaehong.kim
 */
public class RedisClusterClient extends BinaryRedisClusterClient {

    /**
     * Instantiates a new redis cluster client.
     *
     * @param host the host
     */
    public RedisClusterClient(final String host) {
        super(host);
    }

    /**
     * Instantiates a new redis cluster client.
     *
     * @param host the host
     * @param port the port
     */
    public RedisClusterClient(final String host, final int port) {
        super(host, port);
    }

    /**
     * Sets the.
     *
     * @param key   the key
     * @param value the value
     */
    public void set(final String key, final String value) {
        set(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    /**
     * Gets the.
     *
     * @param key the key
     */
    public void get(final String key) {
        get(SafeEncoder.encode(key));
    }

    /**
     * Exists.
     *
     * @param key the key
     */
    public void exists(final String key) {
        exists(SafeEncoder.encode(key));
    }

    /**
     * Del.
     *
     * @param keys the keys
     */
    public void del(final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        del(bkeys);
    }

    /**
     * Type.
     *
     * @param key the key
     */
    public void type(final String key) {
        type(SafeEncoder.encode(key));
    }

    public void persist(final String key) {
        persist(SafeEncoder.encode(key));
    }

    /**
     * Expire.
     *
     * @param key     the key
     * @param seconds the seconds
     */
    public void expire(final String key, final int seconds) {
        expire(SafeEncoder.encode(key), seconds);
    }

    /**
     * Expire at.
     *
     * @param key                   the key
     * @param millisecondsTimestamp the unix time
     */
    public void expireAt(final String key, final long millisecondsTimestamp) {
        expireAt(SafeEncoder.encode(key), millisecondsTimestamp);
    }

    public void pexpire(final String key, final long milliseconds) {
        pexpire(SafeEncoder.encode(key), milliseconds);
    }

    public void pexpireAt(final String key, final long millisecondsTimestamp) {
        pexpireAt(SafeEncoder.encode(key), millisecondsTimestamp);
    }

    public void ttl(final String key) {
        ttl(SafeEncoder.encode(key));
    }

    public void pttl(final String key) {
        pttl(SafeEncoder.encode(key));
    }

    public void getSet(final String key, final String value) {
        getSet(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public void setnx(final String key, final String value) {
        setnx(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public void setex(final String key, final int seconds, final String value) {
        setex(SafeEncoder.encode(key), seconds, SafeEncoder.encode(value));
    }

    public void msetnx(final String... keysvalues) {
        final byte[][] bkeysvalues = new byte[keysvalues.length][];
        for (int i = 0; i < keysvalues.length; i++) {
            bkeysvalues[i] = SafeEncoder.encode(keysvalues[i]);
        }
        msetnx(bkeysvalues);
    }

    /**
     * Decr by.
     *
     * @param key     the key
     * @param integer the integer
     */
    public void decrBy(final String key, final long integer) {
        decrBy(SafeEncoder.encode(key), integer);
    }

    public void decr(final String key) {
        decr(SafeEncoder.encode(key));
    }

    public void incrBy(final String key, final long integer) {
        incrBy(SafeEncoder.encode(key), integer);
    }

    public void incrByFloat(final String key, final double increment) {
        incrByFloat(SafeEncoder.encode(key), increment);
    }

    public void incr(final String key) {
        incr(SafeEncoder.encode(key));
    }

    public void append(final String key, final String value) {
        append(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public void substr(final String key, final int start, final int end) {
        substr(SafeEncoder.encode(key), start, end);
    }

    public void psetex(final String key, final long milliseconds, final String value) {
        psetex(SafeEncoder.encode(key), milliseconds, SafeEncoder.encode(value));
    }

    public void bitcount(final String key) {
        bitcount(SafeEncoder.encode(key));
    }

    public void bitcount(final String key, long start, long end) {
        bitcount(SafeEncoder.encode(key), start, end);
    }

    /**
     * Hset.
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     */
    public void hset(final String key, final String field, final String value) {
        hset(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value));
    }

    /**
     * Hget.
     *
     * @param key   the key
     * @param field the field
     */
    public void hget(final String key, final String field) {
        hget(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    /**
     * Hsetnx.
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     */
    public void hsetnx(final String key, final String field, final String value) {
        hsetnx(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value));
    }

    /**
     * Hmset.
     *
     * @param key  the key
     * @param hash the hash
     */
    public void hmset(final String key, final Map<String, String> hash) {
        final Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>(hash.size());
        for (final Entry<String, String> entry : hash.entrySet()) {
            bhash.put(SafeEncoder.encode(entry.getKey()), SafeEncoder.encode(entry.getValue()));
        }
        hmset(SafeEncoder.encode(key), bhash);
    }

    /**
     * Hmget.
     *
     * @param key    the key
     * @param fields the fields
     */
    public void hmget(final String key, final String... fields) {
        final byte[][] bfields = new byte[fields.length][];
        for (int i = 0; i < bfields.length; i++) {
            bfields[i] = SafeEncoder.encode(fields[i]);
        }
        hmget(SafeEncoder.encode(key), bfields);
    }

    /**
     * Hincr by.
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     */
    public void hincrBy(final String key, final String field, final long value) {
        hincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    /**
     * Hexists.
     *
     * @param key   the key
     * @param field the field
     */
    public void hexists(final String key, final String field) {
        hexists(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    /**
     * Hdel.
     *
     * @param key    the key
     * @param fields the fields
     */
    public void hdel(final String key, final String... fields) {
        hdel(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields));
    }

    /**
     * Hlen.
     *
     * @param key the key
     */
    public void hlen(final String key) {
        hlen(SafeEncoder.encode(key));
    }

    /**
     * Hkeys.
     *
     * @param key the key
     */
    public void hkeys(final String key) {
        hkeys(SafeEncoder.encode(key));
    }

    /**
     * Hvals.
     *
     * @param key the key
     */
    public void hvals(final String key) {
        hvals(SafeEncoder.encode(key));
    }

    /**
     * Hget all.
     *
     * @param key the key
     */
    public void hgetAll(final String key) {
        hgetAll(SafeEncoder.encode(key));
    }

    public void hscan(final String key, final String cursor, final ScanParams params) {
        hscan(SafeEncoder.encode(key), SafeEncoder.encode(cursor), params);
    }

    public void hincrByFloat(final String key, final String field, double increment) {
        hincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), increment);
    }

    /**
     * Rpush.
     *
     * @param key    the key
     * @param string the string
     */
    public void rpush(final String key, final String... string) {
        rpush(SafeEncoder.encode(key), SafeEncoder.encodeMany(string));
    }

    /**
     * Lpush.
     *
     * @param key    the key
     * @param string the string
     */
    public void lpush(final String key, final String... string) {
        lpush(SafeEncoder.encode(key), SafeEncoder.encodeMany(string));
    }

    /**
     * Llen.
     *
     * @param key the key
     */
    public void llen(final String key) {
        llen(SafeEncoder.encode(key));
    }

    /**
     * Lrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void lrange(final String key, final long start, final long end) {
        lrange(SafeEncoder.encode(key), start, end);
    }

    /**
     * Ltrim.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void ltrim(final String key, final long start, final long end) {
        ltrim(SafeEncoder.encode(key), start, end);
    }

    /**
     * Lindex.
     *
     * @param key   the key
     * @param index the index
     */
    public void lindex(final String key, final long index) {
        lindex(SafeEncoder.encode(key), index);
    }

    /**
     * Lset.
     *
     * @param key   the key
     * @param index the index
     * @param value the value
     */
    public void lset(final String key, final long index, final String value) {
        lset(SafeEncoder.encode(key), index, SafeEncoder.encode(value));
    }

    /**
     * Lrem.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     */
    public void lrem(final String key, long count, final String value) {
        lrem(SafeEncoder.encode(key), count, SafeEncoder.encode(value));
    }

    /**
     * Lpop.
     *
     * @param key the key
     */
    public void lpop(final String key) {
        lpop(SafeEncoder.encode(key));
    }

    /**
     * Rpop.
     *
     * @param key the key
     */
    public void rpop(final String key) {
        rpop(SafeEncoder.encode(key));
    }

    /**
     * Sadd.
     *
     * @param key     the key
     * @param members the members
     */
    public void sadd(final String key, final String... members) {
        sadd(SafeEncoder.encode(key), SafeEncoder.encodeMany(members));
    }

    /**
     * Smembers.
     *
     * @param key the key
     */
    public void smembers(final String key) {
        smembers(SafeEncoder.encode(key));
    }

    /**
     * Srem.
     *
     * @param key     the key
     * @param members the members
     */
    public void srem(final String key, final String... members) {
        srem(SafeEncoder.encode(key), SafeEncoder.encodeMany(members));
    }

    /**
     * Scard.
     *
     * @param key the key
     */
    public void scard(final String key) {
        scard(SafeEncoder.encode(key));
    }

    /**
     * Sismember.
     *
     * @param key    the key
     * @param member the member
     */
    public void sismember(final String key, final String member) {
        sismember(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    /**
     * Srandmember.
     *
     * @param key the key
     */
    public void srandmember(final String key) {
        srandmember(SafeEncoder.encode(key));
    }

    public void srandmember(final String key, final int count) {
        srandmember(SafeEncoder.encode(key), count);
    }

    public void sscan(final String key, final String cursor, final ScanParams params) {
        sscan(SafeEncoder.encode(key), SafeEncoder.encode(cursor), params);
    }

    /**
     * Zadd.
     *
     * @param key    the key
     * @param score  the score
     * @param member the member
     */
    public void zadd(final String key, final double score, final String member) {
        zadd(SafeEncoder.encode(key), score, SafeEncoder.encode(member));
    }

    /**
     * Zrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrange(final String key, final long start, final long end) {
        zrange(SafeEncoder.encode(key), start, end);
    }

    /**
     * Zrem.
     *
     * @param key     the key
     * @param members the members
     */
    public void zrem(final String key, final String... members) {
        zrem(SafeEncoder.encode(key), SafeEncoder.encodeMany(members));
    }

    /**
     * Zincrby.
     *
     * @param key    the key
     * @param score  the score
     * @param member the member
     */
    public void zincrby(final String key, final double score, final String member) {
        zincrby(SafeEncoder.encode(key), score, SafeEncoder.encode(member));
    }

    /**
     * Zrank.
     *
     * @param key    the key
     * @param member the member
     */
    public void zrank(final String key, final String member) {
        zrank(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    /**
     * Zrevrank.
     *
     * @param key    the key
     * @param member the member
     */
    public void zrevrank(final String key, final String member) {
        zrevrank(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    /**
     * Zrevrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrevrange(final String key, final long start, final long end) {
        zrevrange(SafeEncoder.encode(key), start, end);
    }

    /**
     * Zrange with scores.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrangeWithScores(final String key, final long start, final long end) {
        zrangeWithScores(SafeEncoder.encode(key), start, end);
    }

    /**
     * Zrevrange with scores.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zrevrangeWithScores(final String key, final long start, final long end) {
        zrevrangeWithScores(SafeEncoder.encode(key), start, end);
    }

    /**
     * Zcard.
     *
     * @param key the key
     */
    public void zcard(final String key) {
        zcard(SafeEncoder.encode(key));
    }

    /**
     * Zscore.
     *
     * @param key    the key
     * @param member the member
     */
    public void zscore(final String key, final String member) {
        zscore(SafeEncoder.encode(key), SafeEncoder.encode(member));
    }

    /**
     * Zcount.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zcount(final String key, final double min, final double max) {
        zcount(SafeEncoder.encode(key), RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max));
    }

    /**
     * Zcount.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zcount(final String key, final String min, final String max) {
        zcount(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max));
    }

    /**
     * Zrange by score.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zrangeByScore(final String key, final double min, final double max) {
        zrangeByScore(SafeEncoder.encode(key), RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max));
    }

    /**
     * Zrange by score.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zrangeByScore(final String key, final String min, final String max) {
        zrangeByScore(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max));
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
    public void zrangeByScore(final String key, final double min, final double max, final int offset, int count) {
        zrangeByScore(SafeEncoder.encode(key), RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max), offset, count);
    }

    /**
     * Zrange by score with scores.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zrangeByScoreWithScores(final String key, final double min, final double max) {
        zrangeByScoreWithScores(SafeEncoder.encode(key), RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max));
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
    public void zrangeByScoreWithScores(final String key, final double min, final double max, final int offset,
                                        final int count) {
        zrangeByScoreWithScores(SafeEncoder.encode(key), RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max), offset, count);
    }

    /**
     * Zrevrange by score.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     */
    public void zrevrangeByScore(final String key, final double max, final double min) {
        zrevrangeByScore(SafeEncoder.encode(key), RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min));
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
    public void zrangeByScore(final String key, final String min, final String max, final int offset, int count) {
        zrangeByScore(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max), offset, count);
    }

    /**
     * Zrange by score with scores.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     */
    public void zrangeByScoreWithScores(final String key, final String min, final String max) {
        zrangeByScoreWithScores(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max));
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
    public void zrangeByScoreWithScores(final String key, final String min, final String max, final int offset,
                                        final int count) {
        zrangeByScoreWithScores(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max), offset,
                count);
    }

    /**
     * Zrevrange by score.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     */
    public void zrevrangeByScore(final String key, final String max, final String min) {
        zrevrangeByScore(SafeEncoder.encode(key), SafeEncoder.encode(max), SafeEncoder.encode(min));
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
    public void zrevrangeByScore(final String key, final double max, final double min, final int offset, int count) {
        zrevrangeByScore(SafeEncoder.encode(key), RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min), offset, count);
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
    public void zrevrangeByScore(final String key, final String max, final String min, final int offset, int count) {
        zrevrangeByScore(SafeEncoder.encode(key), SafeEncoder.encode(max), SafeEncoder.encode(min), offset, count);
    }

    /**
     * Zrevrange by score with scores.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     */
    public void zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        zrevrangeByScoreWithScores(SafeEncoder.encode(key), RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min));
    }

    /**
     * Zrevrange by score with scores.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     */
    public void zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        zrevrangeByScoreWithScores(SafeEncoder.encode(key), SafeEncoder.encode(max), SafeEncoder.encode(min));
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
    public void zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset,
                                           final int count) {
        zrevrangeByScoreWithScores(SafeEncoder.encode(key), RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min), offset, count);
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
    public void zrevrangeByScoreWithScores(final String key, final String max, final String min, final int offset,
                                           final int count) {
        zrevrangeByScoreWithScores(SafeEncoder.encode(key), SafeEncoder.encode(max), SafeEncoder.encode(min), offset,
                count);
    }

    /**
     * Zremrange by rank.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zremrangeByRank(final String key, final long start, final long end) {
        zremrangeByRank(SafeEncoder.encode(key), start, end);
    }

    /**
     * Zremrange by score.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zremrangeByScore(final String key, final double start, final double end) {
        zremrangeByScore(SafeEncoder.encode(key), RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    /**
     * Zremrange by score.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     */
    public void zremrangeByScore(final String key, final String start, final String end) {
        zremrangeByScore(SafeEncoder.encode(key), SafeEncoder.encode(start), SafeEncoder.encode(end));
    }

    public void zscan(final String key, final String cursor, final ScanParams params) {
        zscan(SafeEncoder.encode(key), SafeEncoder.encode(cursor), params);
    }

    /**
     * Strlen.
     *
     * @param key the key
     */
    public void strlen(final String key) {
        strlen(SafeEncoder.encode(key));
    }

    /**
     * Lpushx.
     *
     * @param key    the key
     * @param string the string
     */
    public void lpushx(final String key, final String string) {
        lpushx(SafeEncoder.encode(key), SafeEncoder.encode(string));
    }

    /**
     * Rpushx.
     *
     * @param key    the key
     * @param string the string
     */
    public void rpushx(final String key, final String string) {
        rpushx(SafeEncoder.encode(key), SafeEncoder.encode(string));
    }

    /**
     * Linsert.
     *
     * @param key   the key
     * @param where the where
     * @param pivot the pivot
     * @param value the value
     */
    public void linsert(final String key, final BinaryClient.LIST_POSITION where, final String pivot, final String value) {
        linsert(SafeEncoder.encode(key), where, SafeEncoder.encode(pivot), SafeEncoder.encode(value));
    }

    /**
     * Setbit.
     *
     * @param key    the key
     * @param offset the offset
     * @param value  the value
     */
    public void setbit(final String key, final long offset, final boolean value) {
        setbit(SafeEncoder.encode(key), offset, RedisProtocol.toByteArray(value ? 1 : 0));
    }

    /**
     * Gets the bit.
     *
     * @param key    the key
     * @param offset the offset
     * @return the bit
     */
    public void getbit(String key, long offset) {
        getbit(SafeEncoder.encode(key), offset);
    }

    /**
     * Setrange.
     *
     * @param key    the key
     * @param offset the offset
     * @param value  the value
     */
    public void setrange(String key, long offset, String value) {
        setrange(SafeEncoder.encode(key), offset, SafeEncoder.encode(value));
    }

    /**
     * Gets the range.
     *
     * @param key         the key
     * @param startOffset the start offset
     * @param endOffset   the end offset
     * @return the range
     */
    public void getrange(String key, long startOffset, long endOffset) {
        getrange(SafeEncoder.encode(key), startOffset, endOffset);
    }

    /**
     * Config set.
     *
     * @param parameter the parameter
     * @param value     the value
     */
    public void configSet(String parameter, String value) {
        configSet(SafeEncoder.encode(parameter), SafeEncoder.encode(value));
    }

    /**
     * Config get.
     *
     * @param pattern the pattern
     */
    public void configGet(String pattern) {
        configGet(SafeEncoder.encode(pattern));
    }

    /**
     * Zadd.
     *
     * @param key          the key
     * @param scoreMembers the score members
     */
    public void zadd(String key, Map<Double, String> scoreMembers) {
        HashMap<Double, byte[]> binaryScoreMembers = new HashMap<Double, byte[]>();

        for (Map.Entry<Double, String> entry : scoreMembers.entrySet()) {
            binaryScoreMembers.put(entry.getKey(), SafeEncoder.encode(entry.getValue()));
        }

        zaddBinary(SafeEncoder.encode(key), binaryScoreMembers);
    }

    public void zadd2(String key, Map<String, Double> scoreMembers) {
        HashMap<byte[], Double> binaryScoreMembers = new HashMap<byte[], Double>();
        for (Map.Entry<String, Double> entry : scoreMembers.entrySet()) {
            binaryScoreMembers.put(SafeEncoder.encode(entry.getKey()), entry.getValue());
        }

        zaddBinary2(SafeEncoder.encode(key), binaryScoreMembers);
    }


    /**
     * Object refcount.
     *
     * @param key the key
     */
    public void objectRefcount(String key) {
        objectRefcount(SafeEncoder.encode(key));
    }

    /**
     * Object idletime.
     *
     * @param key the key
     */
    public void objectIdletime(String key) {
        objectIdletime(SafeEncoder.encode(key));
    }

    /**
     * Object encoding.
     *
     * @param key the key
     */
    public void objectEncoding(String key) {
        objectEncoding(SafeEncoder.encode(key));
    }

    /**
     * @param keys the key
     */
    public void mget(final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];

        for (int i = 0; i < keys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }

        mget(bkeys);
    }

    public void mset(String... keysvalues) {
        final List<byte[]> params = new ArrayList<byte[]>();

        for (String keyvalue : keysvalues) {
            params.add(SafeEncoder.encode(keyvalue));
        }

        mset(params.toArray(new byte[params.size()][]));
    }

    public void dump(final String key) {
        dump(SafeEncoder.encode(key));
    }

    public void restore(final String key, final long ttl, final byte[] serializedValue) {
        restore(SafeEncoder.encode(key), ttl, serializedValue);
    }

}
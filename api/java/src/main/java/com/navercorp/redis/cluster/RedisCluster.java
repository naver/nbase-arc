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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.navercorp.redis.cluster.pipeline.BuilderFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Tuple;

/**
 * The Class RedisCluster.
 *
 * @author jaehong.kim
 */
public class RedisCluster extends BinaryRedisCluster implements
        RedisClusterCommands {

    /**
     * Instantiates a new redis cluster.
     *
     * @param host the host
     */
    public RedisCluster(final String host) {
        super(host);
    }

    /**
     * Instantiates a new redis cluster.
     *
     * @param host the host
     * @param port the port
     */
    public RedisCluster(final String host, final int port) {
        super(host, port);
    }

    /**
     * Instantiates a new redis cluster.
     *
     * @param host    the host
     * @param port    the port
     * @param timeout the timeout
     */
    public RedisCluster(final String host, final int port, final int timeout) {
        super(host, port, timeout);
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Keys

    public Long del(final String... keys) {
        client.del(keys);
        return client.getIntegerReply();
    }

    public Boolean exists(final String key) {
        client.exists(key);
        return client.getIntegerReply() == 1;
    }

    public Long expire(final String key, final int seconds) {
        client.expire(key, seconds);
        return client.getIntegerReply();
    }

    public Long expireAt(final String key, final long secondsTimestamp) {
        client.expireAt(key, secondsTimestamp);
        return client.getIntegerReply();
    }

    public Long pexpire(final String key, final long milliseconds) {
        client.pexpire(key, milliseconds);
        return client.getIntegerReply();
    }

    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        client.pexpireAt(key, millisecondsTimestamp);
        return client.getIntegerReply();
    }

    public Long objectRefcount(String string) {
        client.objectRefcount(string);
        return client.getIntegerReply();
    }

    public String objectEncoding(String string) {
        client.objectEncoding(string);
        return client.getBulkReply();
    }

    public Long objectIdletime(String string) {
        client.objectIdletime(string);
        return client.getIntegerReply();
    }

    public Long ttl(final String key) {
        client.ttl(key);
        return client.getIntegerReply();
    }

    public Long pttl(final String key) {
        client.pttl(key);
        return client.getIntegerReply();
    }

    public String type(final String key) {
        client.type(key);
        return client.getStatusCodeReply();
    }

    public Long persist(final String key) {
        client.persist(key);
        return client.getIntegerReply();
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Strings

    public Long append(final String key, final String value) {
        client.append(key, value);
        return client.getIntegerReply();
    }

    public Long decr(final String key) {
        client.decr(key);
        return client.getIntegerReply();
    }

    public Long decrBy(final String key, final long integer) {
        client.decrBy(key, integer);
        return client.getIntegerReply();
    }

    public String get(final String key) {
        client.get(key);
        return client.getBulkReply();
    }

    public Boolean getbit(String key, long offset) {
        client.getbit(key, offset);
        return client.getIntegerReply() == 1;
    }

    public String getrange(String key, long startOffset, long endOffset) {
        client.getrange(key, startOffset, endOffset);
        return client.getBulkReply();
    }

    public String substr(final String key, final int start, final int end) {
        client.substr(key, start, end);
        return client.getBulkReply();
    }

    public String getSet(final String key, final String value) {
        client.getSet(key, value);
        return client.getBulkReply();
    }

    public Long incr(final String key) {
        client.incr(key);
        return client.getIntegerReply();
    }

    public Long incrBy(final String key, final long integer) {
        client.incrBy(key, integer);
        return client.getIntegerReply();
    }

    public Double incrByFloat(final String key, final double increment) {
        client.incrByFloat(key, increment);
        String reply = client.getBulkReply();
        return (reply != null ? new Double(reply) : null);
    }

    public String set(final String key, String value) {
        client.set(key, value);
        return client.getStatusCodeReply();
    }

    public Boolean setbit(String key, long offset, boolean value) {
        client.setbit(key, offset, value);
        return client.getIntegerReply() == 1;
    }

    public String setex(final String key, final int seconds, final String value) {
        client.setex(key, seconds, value);
        return client.getStatusCodeReply();
    }

    public Long setnx(final String key, final String value) {
        client.setnx(key, value);
        return client.getIntegerReply();
    }

    public Long setrange(String key, long offset, String value) {
        client.setrange(key, offset, value);
        return client.getIntegerReply();
    }

    public Long strlen(final String key) {
        client.strlen(key);
        return client.getIntegerReply();
    }

    public List<String> mget(final String... keys) {
        client.mget(keys);
        return client.getMultiBulkReply();
    }

    public String psetex(final String key, final long milliseconds,
                         final String value) {
        client.psetex(key, milliseconds, value);
        return client.getStatusCodeReply();
    }

    public String mset(String... keysvalues) {
        client.mset(keysvalues);
        return client.getStatusCodeReply();
    }

    public Long bitcount(final String key) {
        client.bitcount(key);
        return client.getIntegerReply();
    }

    public Long bitcount(final String key, long start, long end) {
        client.bitcount(key, start, end);
        return client.getIntegerReply();
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Hashes

    public Long hdel(final String key, final String... fields) {
        client.hdel(key, fields);
        return client.getIntegerReply();
    }

    public Boolean hexists(final String key, final String field) {
        client.hexists(key, field);
        return client.getIntegerReply() == 1;
    }

    public String hget(final String key, final String field) {
        client.hget(key, field);
        return client.getBulkReply();
    }

    public Map<String, String> hgetAll(final String key) {
        client.hgetAll(key);
        return BuilderFactory.STRING_MAP
                .build(client.getBinaryMultiBulkReply());
    }

    public Long hincrBy(final String key, final String field, final long value) {
        client.hincrBy(key, field, value);
        return client.getIntegerReply();
    }

    public Double hincrByFloat(final String key, final String field,
                               double increment) {
        client.hincrByFloat(key, field, increment);
        String reply = client.getBulkReply();
        return (reply != null ? new Double(reply) : null);
    }

    public Set<String> hkeys(final String key) {
        client.hkeys(key);
        return BuilderFactory.STRING_SET
                .build(client.getBinaryMultiBulkReply());
    }

    public Long hlen(final String key) {
        client.hlen(key);
        return client.getIntegerReply();
    }

    public List<String> hmget(final String key, final String... fields) {
        client.hmget(key, fields);
        return client.getMultiBulkReply();
    }

    public String hmset(final String key, final Map<String, String> hash) {
        client.hmset(key, hash);
        return client.getStatusCodeReply();
    }

    public Long hset(final String key, final String field, final String value) {
        client.hset(key, field, value);
        return client.getIntegerReply();
    }

    public Long hsetnx(final String key, final String field, final String value) {
        client.hsetnx(key, field, value);
        return client.getIntegerReply();
    }

    public List<String> hvals(final String key) {
        client.hvals(key);
        final List<String> lresult = client.getMultiBulkReply();
        return lresult;
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Lists

    public String lindex(final String key, final long index) {
        client.lindex(key, index);
        return client.getBulkReply();
    }

    public Long linsert(final String key, final LIST_POSITION where,
                        final String pivot, final String value) {
        client.linsert(key, where, pivot, value);
        return client.getIntegerReply();
    }

    public Long llen(final String key) {
        client.llen(key);
        return client.getIntegerReply();
    }

    public String lpop(final String key) {
        client.lpop(key);
        return client.getBulkReply();
    }

    public Long lpush(final String key, final String... strings) {
        client.lpush(key, strings);
        return client.getIntegerReply();
    }

    public Long lpushx(final String key, final String string) {
        client.lpushx(key, string);
        return client.getIntegerReply();
    }

    public List<String> lrange(final String key, final long start,
                               final long end) {
        client.lrange(key, start, end);
        return client.getMultiBulkReply();
    }

    public Long lrem(final String key, final long count, final String value) {
        client.lrem(key, count, value);
        return client.getIntegerReply();
    }

    public String lset(final String key, final long index, final String value) {
        client.lset(key, index, value);
        return client.getStatusCodeReply();
    }

    public String ltrim(final String key, final long start, final long end) {
        client.ltrim(key, start, end);
        return client.getStatusCodeReply();
    }

    public String rpop(final String key) {
        client.rpop(key);
        return client.getBulkReply();
    }

    public Long rpush(final String key, final String... strings) {
        client.rpush(key, strings);
        return client.getIntegerReply();
    }

    public Long rpushx(final String key, final String string) {
        client.rpushx(key, string);
        return client.getIntegerReply();
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sets

    public Long sadd(final String key, final String... members) {
        client.sadd(key, members);
        return client.getIntegerReply();
    }

    public Long scard(final String key) {
        client.scard(key);
        return client.getIntegerReply();
    }

    public Boolean sismember(final String key, final String member) {
        client.sismember(key, member);
        return client.getIntegerReply() == 1;
    }

    public Set<String> smembers(final String key) {
        client.smembers(key);
        final List<String> members = client.getMultiBulkReply();
        return new HashSet<String>(members);
    }

    public String srandmember(final String key) {
        client.srandmember(key);
        return client.getBulkReply();
    }

    public List<String> srandmember(final String key, final int count) {
        client.srandmember(key, count);
        return client.getMultiBulkReply();
    }

    public Long srem(final String key, final String... members) {
        client.srem(key, members);
        return client.getIntegerReply();
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sorted Sets

    public Long zadd(final String key, final double score, final String member) {
        client.zadd(key, score, member);
        return client.getIntegerReply();
    }

    public Long zadd(final String key, final Map<Double, String> scoreMembers) {
        client.zadd(key, scoreMembers);
        return client.getIntegerReply();
    }

    public Long zadd2(final String key, final Map<String, Double> scoreMembers) {
        client.zadd2(key, scoreMembers);
        return client.getIntegerReply();
    }

    public Long zcard(final String key) {
        client.zcard(key);
        return client.getIntegerReply();
    }

    public Long zcount(final String key, final double min, final double max) {
        client.zcount(key, min, max);
        return client.getIntegerReply();
    }

    public Long zcount(final String key, final String min, final String max) {
        client.zcount(key, min, max);
        return client.getIntegerReply();
    }

    public Double zincrby(final String key, final double score,
                          final String member) {
        client.zincrby(key, score, member);
        String newscore = client.getBulkReply();
        return Double.valueOf(newscore);
    }

    public Set<String> zrange(final String key, final long start, final long end) {
        client.zrange(key, start, end);
        final List<String> members = client.getMultiBulkReply();
        return new LinkedHashSet<String>(members);
    }

    public Set<String> zrangeByScore(final String key, final double min,
                                     final double max) {
        client.zrangeByScore(key, min, max);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<String> zrangeByScore(final String key, final String min,
                                     final String max) {
        client.zrangeByScore(key, min, max);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<String> zrangeByScore(final String key, final double min,
                                     final double max, final int offset, final int count) {
        client.zrangeByScore(key, min, max, offset, count);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<String> zrangeByScore(final String key, final String min,
                                     final String max, final int offset, final int count) {
        client.zrangeByScore(key, min, max, offset, count);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<Tuple> zrangeWithScores(final String key, final long start,
                                       final long end) {
        client.zrangeWithScores(key, start, end);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final double min, final double max) {
        client.zrangeByScoreWithScores(key, min, max);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final String min, final String max) {
        client.zrangeByScoreWithScores(key, min, max);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final double min, final double max, final int offset,
                                              final int count) {
        client.zrangeByScoreWithScores(key, min, max, offset, count);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key,
                                              final String min, final String max, final int offset,
                                              final int count) {
        client.zrangeByScoreWithScores(key, min, max, offset, count);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Long zrank(final String key, final String member) {
        client.zrank(key, member);
        return client.getIntegerReply();
    }

    public Long zrem(final String key, final String... members) {
        client.zrem(key, members);
        return client.getIntegerReply();
    }

    public Long zremrangeByRank(final String key, final long start,
                                final long end) {
        client.zremrangeByRank(key, start, end);
        return client.getIntegerReply();
    }

    public Long zremrangeByScore(final String key, final double start,
                                 final double end) {
        client.zremrangeByScore(key, start, end);
        return client.getIntegerReply();
    }

    public Long zremrangeByScore(final String key, final String start,
                                 final String end) {
        client.zremrangeByScore(key, start, end);
        return client.getIntegerReply();
    }

    public Set<String> zrevrange(final String key, final long start,
                                 final long end) {
        client.zrevrange(key, start, end);
        final List<String> members = client.getMultiBulkReply();
        return new LinkedHashSet<String>(members);
    }

    public Set<Tuple> zrevrangeWithScores(final String key, final long start,
                                          final long end) {
        client.zrevrangeWithScores(key, start, end);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<String> zrevrangeByScore(final String key, final double max,
                                        final double min) {
        client.zrevrangeByScore(key, max, min);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<String> zrevrangeByScore(final String key, final String max,
                                        final String min) {
        client.zrevrangeByScore(key, max, min);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<String> zrevrangeByScore(final String key, final double max,
                                        final double min, final int offset, final int count) {
        client.zrevrangeByScore(key, max, min, offset, count);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final double max, final double min) {
        client.zrevrangeByScoreWithScores(key, max, min);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final double max, final double min, final int offset,
                                                 final int count) {
        client.zrevrangeByScoreWithScores(key, max, min, offset, count);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final String max, final String min, final int offset,
                                                 final int count) {
        client.zrevrangeByScoreWithScores(key, max, min, offset, count);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Set<String> zrevrangeByScore(final String key, final String max,
                                        final String min, final int offset, final int count) {
        client.zrevrangeByScore(key, max, min, offset, count);
        return new LinkedHashSet<String>(client.getMultiBulkReply());
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key,
                                                 final String max, final String min) {
        client.zrevrangeByScoreWithScores(key, max, min);
        Set<Tuple> set = getTupledSet();
        return set;
    }

    public Long zrevrank(final String key, final String member) {
        client.zrevrank(key, member);
        return client.getIntegerReply();
    }

    public Double zscore(final String key, final String member) {
        client.zscore(key, member);
        final String score = client.getBulkReply();
        return (score != null ? new Double(score) : null);
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Connection

    public String ping() {
        client.ping();
        return client.getStatusCodeReply();
    }

    // ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Server

    public String info() {
        client.info();
        return client.getBulkReply();
    }

    public String info(final String section) {
        client.info(section);
        return client.getBulkReply();
    }

    public Long dbSize() {
        client.dbSize();
        return client.getIntegerReply();
    }

    public byte[] dump(final String key) {
        client.dump(key);
        return client.getBinaryBulkReply();
    }

    public String restore(final String key, final long ttl,
                          final byte[] serializedValue) {
        client.restore(key, ttl, serializedValue);
        return client.getStatusCodeReply();
    }

    /**
     * Quit.
     *
     * @return the string
     */
    public String quit() {
        client.quit();
        return client.getStatusCodeReply();
    }

    /**
     * Connect.
     */
    public void connect() {
        client.connect();
    }

    /**
     * Disconnect.
     */
    public void disconnect() {
        client.disconnect();
    }

    /**
     * Checks if is connected.
     *
     * @return true, if is connected
     */
    public boolean isConnected() {
        return client.isConnected();
    }

    /**
     * Gets the tupled set.
     *
     * @return the tupled set
     */
    private Set<Tuple> getTupledSet() {
        List<String> membersWithScores = client.getMultiBulkReply();
        Set<Tuple> set = new LinkedHashSet<Tuple>();
        Iterator<String> iterator = membersWithScores.iterator();
        while (iterator.hasNext()) {
            set.add(new Tuple(iterator.next(), Double.valueOf(iterator.next())));
        }
        return set;
    }
}
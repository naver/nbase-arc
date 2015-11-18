/*
 * @(#)BinaryRedisCluster.java 2013. 6. 21
 * 
 * Copyright 2013 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.navercorp.redis.cluster;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.navercorp.redis.cluster.connection.RedisProtocol;
import com.navercorp.redis.cluster.triples.TriplesRedisCluster;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Tuple;
import redis.clients.util.JedisByteHashMap;
import redis.clients.util.SafeEncoder;

/**
 * The Class BinaryRedisCluster.
 *
 * @author jaehong.kim
 */
public class BinaryRedisCluster extends TriplesRedisCluster implements BinaryRedisClusterCommands {

    /**
     * Instantiates a new binary redis cluster.
     *
     * @param host the host
     */
    public BinaryRedisCluster(final String host) {
        super(host);
    }

    /**
     * Instantiates a new binary redis cluster.
     *
     * @param host the host
     * @param port the port
     */
    public BinaryRedisCluster(final String host, final int port) {
        super(host, port);
    }

    /**
     * Instantiates a new binary redis cluster.
     *
     * @param host    the host
     * @param port    the port
     * @param timeout the timeout
     */
    public BinaryRedisCluster(final String host, final int port, final int timeout) {
        super(host, port, timeout);
        client = new RedisClusterClient(host, port);
        client.setTimeout(timeout);
    }

    public RedisClusterClient getClient() {
        return this.client;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Keys

    public Long del(final byte[]... key) {
        client.del(key);
        return client.getIntegerReply();
    }

    public Boolean exists(final byte[] key) {
        client.exists(key);
        return client.getIntegerReply() == 1;
    }

    public Long expire(final byte[] key, final int seconds) {
        client.expire(key, seconds);
        return client.getIntegerReply();
    }

    public Long expireAt(final byte[] key, final long secondsTimestamp) {
        client.expireAt(key, secondsTimestamp);
        return client.getIntegerReply();
    }

    public Long pexpire(final byte[] key, final long milliseconds) {
        client.pexpire(key, milliseconds);
        return client.getIntegerReply();
    }

    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        client.pexpireAt(key, millisecondsTimestamp);
        return client.getIntegerReply();
    }

    public byte[] objectEncoding(byte[] key) {
        client.objectEncoding(key);
        return client.getBinaryBulkReply();
    }

    public Long objectIdletime(byte[] key) {
        client.objectIdletime(key);
        return client.getIntegerReply();
    }

    public Long objectRefcount(byte[] key) {
        client.objectRefcount(key);
        return client.getIntegerReply();
    }

    public Long ttl(final byte[] key) {
        client.ttl(key);
        return client.getIntegerReply();
    }

    public Long pttl(final byte[] key) {
        client.pttl(key);
        return client.getIntegerReply();
    }

    public String type(final byte[] key) {
        client.type(key);
        return client.getStatusCodeReply();
    }

    public Long persist(final byte[] key) {
        client.persist(key);
        return client.getIntegerReply();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Strings

    public Long append(final byte[] key, final byte[] value) {
        client.append(key, value);
        return client.getIntegerReply();
    }

    public Long decr(final byte[] key) {
        client.decr(key);
        return client.getIntegerReply();
    }

    public Long decrBy(final byte[] key, final long integer) {
        client.decrBy(key, integer);
        return client.getIntegerReply();
    }

    public byte[] get(final byte[] key) {
        client.get(key);
        return client.getBinaryBulkReply();
    }

    public Boolean getbit(byte[] key, long offset) {
        client.getbit(key, offset);
        return client.getIntegerReply() == 1;
    }

    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        client.getrange(key, startOffset, endOffset);
        return client.getBinaryBulkReply();
    }

    public byte[] substr(final byte[] key, final int start, final int end) {
        client.substr(key, start, end);
        return client.getBinaryBulkReply();
    }

    public byte[] getSet(final byte[] key, final byte[] value) {
        client.getSet(key, value);
        return client.getBinaryBulkReply();
    }

    public Long incr(final byte[] key) {
        client.incr(key);
        return client.getIntegerReply();
    }

    public Long incrBy(final byte[] key, final long integer) {
        client.incrBy(key, integer);
        return client.getIntegerReply();
    }

    public Double incrByFloat(final byte[] key, final double increment) {
        client.incrByFloat(key, increment);
        final String reply = client.getBulkReply();
        return (reply != null ? new Double(reply) : null);
    }

    public String set(final byte[] key, final byte[] value) {
        client.set(key, value);
        return client.getStatusCodeReply();
    }

    public Boolean setbit(byte[] key, long offset, byte[] value) {
        client.setbit(key, offset, value);
        return client.getIntegerReply() == 1;
    }

    public String setex(final byte[] key, final int seconds, final byte[] value) {
        client.setex(key, seconds, value);
        return client.getStatusCodeReply();
    }

    public Long setnx(final byte[] key, final byte[] value) {
        client.setnx(key, value);
        return client.getIntegerReply();
    }

    public Long setrange(byte[] key, long offset, byte[] value) {
        client.setrange(key, offset, value);
        return client.getIntegerReply();
    }

    public Long strlen(final byte[] key) {
        client.strlen(key);
        return client.getIntegerReply();
    }

    public List<byte[]> mget(final byte[]... keys) {
        client.mget(keys);
        return client.getBinaryMultiBulkReply();
    }

    public String mset(final byte[]... keysvalues) {
        client.mset(keysvalues);
        return client.getStatusCodeReply();
    }

    public String psetex(final byte[] key, final long milliseconds, final byte[] value) {
        client.psetex(key, milliseconds, value);
        return client.getStatusCodeReply();
    }

    public Long bitcount(final byte[] key) {
        client.bitcount(key);
        return client.getIntegerReply();
    }

    public Long bitcount(final byte[] key, long start, long end) {
        client.bitcount(key, start, end);
        return client.getIntegerReply();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Hashes

    public Long hdel(final byte[] key, final byte[]... fields) {
        client.hdel(key, fields);
        return client.getIntegerReply();
    }

    public Boolean hexists(final byte[] key, final byte[] field) {
        client.hexists(key, field);
        return client.getIntegerReply() == 1;
    }

    public byte[] hget(final byte[] key, final byte[] field) {
        client.hget(key, field);
        return client.getBinaryBulkReply();
    }

    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        client.hgetAll(key);
        final List<byte[]> flatHash = client.getBinaryMultiBulkReply();
        final Map<byte[], byte[]> hash = new JedisByteHashMap();
        if (flatHash == null) {
            return hash;
        }

        final Iterator<byte[]> iterator = flatHash.iterator();
        if (iterator == null) {
            return hash;
        }

        while (iterator.hasNext()) {
            hash.put(iterator.next(), iterator.next());
        }

        return hash;
    }

    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        client.hincrBy(key, field, value);
        return client.getIntegerReply();
    }

    public Double hincrByFloat(final byte[] key, final byte[] field, double increment) {
        client.hincrByFloat(key, field, increment);
        String reply = client.getBulkReply();
        return (reply != null ? new Double(reply) : null);
    }

    public Set<byte[]> hkeys(final byte[] key) {
        client.hkeys(key);
        final List<byte[]> lresult = client.getBinaryMultiBulkReply();
        return new HashSet<byte[]>(lresult);
    }

    public Long hlen(final byte[] key) {
        client.hlen(key);
        return client.getIntegerReply();
    }

    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        client.hmget(key, fields);
        return client.getBinaryMultiBulkReply();
    }

    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        client.hmset(key, hash);
        return client.getStatusCodeReply();
    }

    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        client.hset(key, field, value);
        return client.getIntegerReply();
    }

    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        client.hsetnx(key, field, value);
        return client.getIntegerReply();
    }

    public List<byte[]> hvals(final byte[] key) {
        client.hvals(key);
        final List<byte[]> lresult = client.getBinaryMultiBulkReply();
        return lresult;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Lists

    public byte[] lindex(final byte[] key, final long index) {
        client.lindex(key, index);
        return client.getBinaryBulkReply();
    }

    public Long linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot, final byte[] value) {
        client.linsert(key, where, pivot, value);
        return client.getIntegerReply();
    }

    public Long llen(final byte[] key) {
        client.llen(key);
        return client.getIntegerReply();
    }

    public byte[] lpop(final byte[] key) {
        client.lpop(key);
        return client.getBinaryBulkReply();
    }

    public Long lpush(final byte[] key, final byte[]... strings) {
        client.lpush(key, strings);
        return client.getIntegerReply();
    }

    public Long lpushx(final byte[] key, final byte[] string) {
        client.lpushx(key, string);
        return client.getIntegerReply();
    }

    public List<byte[]> lrange(final byte[] key, final long start, final long end) {
        client.lrange(key, start, end);
        return client.getBinaryMultiBulkReply();
    }

    public Long lrem(final byte[] key, final long count, final byte[] value) {
        client.lrem(key, count, value);
        return client.getIntegerReply();
    }

    public String lset(final byte[] key, final long index, final byte[] value) {
        client.lset(key, index, value);
        return client.getStatusCodeReply();
    }

    public String ltrim(final byte[] key, final long start, final long end) {
        client.ltrim(key, start, end);
        return client.getStatusCodeReply();
    }

    public byte[] rpop(final byte[] key) {
        client.rpop(key);
        return client.getBinaryBulkReply();
    }

    public Long rpush(final byte[] key, final byte[]... strings) {
        client.rpush(key, strings);
        return client.getIntegerReply();
    }

    public Long rpushx(final byte[] key, final byte[] string) {
        client.rpushx(key, string);
        return client.getIntegerReply();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sets

    public Long sadd(final byte[] key, final byte[]... members) {
        client.sadd(key, members);
        return client.getIntegerReply();
    }

    public Long scard(final byte[] key) {
        client.scard(key);
        return client.getIntegerReply();
    }

    public Boolean sismember(final byte[] key, final byte[] member) {
        client.sismember(key, member);
        return client.getIntegerReply() == 1;
    }

    public Set<byte[]> smembers(final byte[] key) {
        client.smembers(key);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new HashSet<byte[]>(members);
    }

    public byte[] srandmember(final byte[] key) {
        client.srandmember(key);
        return client.getBinaryBulkReply();
    }

    public List<byte[]> srandmember(final byte[] key, final int count) {
        client.srandmember(key, count);
        return client.getBinaryMultiBulkReply();
    }

    public Long srem(final byte[] key, final byte[]... member) {
        client.srem(key, member);
        return client.getIntegerReply();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sorted Sets

    public Long zadd(final byte[] key, final double score, final byte[] member) {
        client.zadd(key, score, member);
        return client.getIntegerReply();
    }

    public Long zadd(final byte[] key, final Map<Double, byte[]> scoreMembers) {
        client.zaddBinary(key, scoreMembers);
        return client.getIntegerReply();
    }

    public Long zadd2(final byte[] key, final Map<byte[], Double> scoreMembers) {
        client.zaddBinary2(key, scoreMembers);
        return client.getIntegerReply();
    }

    public Long zcard(final byte[] key) {
        client.zcard(key);
        return client.getIntegerReply();
    }

    public Long zcount(final byte[] key, final byte[] min, final byte[] max) {
        client.zcount(key, min, max);
        return client.getIntegerReply();
    }

    public Long zcount(final byte[] key, final double min, final double max) {
        return zcount(key, RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max));
    }

    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        client.zincrby(key, score, member);
        String newscore = client.getBulkReply();
        return Double.valueOf(newscore);
    }

    public Set<byte[]> zrange(final byte[] key, final long start, final long end) {
        client.zrange(key, start, end);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new LinkedHashSet<byte[]>(members);
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        client.zrangeByScore(key, min, max);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                     final int count) {
        client.zrangeByScore(key, min, max, offset, count);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return zrangeByScore(key, RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max));
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
                                     final int count) {
        return zrangeByScore(key, RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max), offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        client.zrangeByScoreWithScores(key, min, max);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                              final int count) {
        client.zrangeByScoreWithScores(key, min, max, offset, count);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return zrangeByScoreWithScores(key, RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max));
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset,
                                              final int count) {
        return zrangeByScoreWithScores(key, RedisProtocol.toByteArray(min), RedisProtocol.toByteArray(max), offset, count);
    }

    public Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long end) {
        client.zrangeWithScores(key, start, end);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    public Long zrank(final byte[] key, final byte[] member) {
        client.zrank(key, member);
        return client.getIntegerReply();
    }

    public Long zrem(final byte[] key, final byte[]... members) {
        client.zrem(key, members);
        return client.getIntegerReply();
    }

    public Long zremrangeByRank(final byte[] key, final long start, final long end) {
        client.zremrangeByRank(key, start, end);
        return client.getIntegerReply();
    }

    public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        client.zremrangeByScore(key, start, end);
        return client.getIntegerReply();
    }

    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return zremrangeByScore(key, RedisProtocol.toByteArray(start), RedisProtocol.toByteArray(end));
    }

    public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {
        client.zrevrange(key, start, end);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new LinkedHashSet<byte[]>(members);
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        client.zrevrangeByScore(key, max, min);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset,
                                        final int count) {
        client.zrevrangeByScore(key, max, min, offset, count);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return zrevrangeByScore(key, RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min));
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset,
                                        final int count) {
        return zrevrangeByScore(key, RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min), offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        client.zrevrangeByScoreWithScores(key, max, min);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min,
                                                 final int offset, final int count) {
        client.zrevrangeByScoreWithScores(key, max, min, offset, count);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return zrevrangeByScoreWithScores(key, RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min));
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min,
                                                 final int offset, final int count) {
        return zrevrangeByScoreWithScores(key, RedisProtocol.toByteArray(max), RedisProtocol.toByteArray(min), offset, count);
    }

    public Set<Tuple> zrevrangeWithScores(final byte[] key, final long start, final long end) {
        client.zrevrangeWithScores(key, start, end);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    public Long zrevrank(final byte[] key, final byte[] member) {
        client.zrevrank(key, member);
        return client.getIntegerReply();
    }

    public Double zscore(final byte[] key, final byte[] member) {
        client.zscore(key, member);
        final String score = client.getBulkReply();
        return (score != null ? new Double(score) : null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Connection

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Server

    public byte[] dump(final byte[] key) {
        client.dump(key);
        return client.getBinaryBulkReply();
    }

    public String restore(final byte[] key, final long ttl, final byte[] serializedValue) {
        client.restore(key, ttl, serializedValue);
        return client.getStatusCodeReply();
    }

    /**
     * Gets the binary tupled set.
     *
     * @return the binary tupled set
     */
    private Set<Tuple> getBinaryTupledSet() {
        List<byte[]> membersWithScores = client.getBinaryMultiBulkReply();
        Set<Tuple> set = new LinkedHashSet<Tuple>();
        if (membersWithScores == null) {
            return set;
        }

        Iterator<byte[]> iterator = membersWithScores.iterator();
        if (iterator == null) {
            return set;
        }

        while (iterator.hasNext()) {
            set.add(new Tuple(iterator.next(), Double.valueOf(SafeEncoder.encode(iterator.next()))));
        }
        return set;
    }
}
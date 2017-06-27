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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.navercorp.redis.cluster.connection.RedisProtocol;
import com.navercorp.redis.cluster.pipeline.BuilderFactory;
import com.navercorp.redis.cluster.triples.TriplesRedisCluster;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.geo.GeoRadiusParam;
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
    
    public BinaryRedisCluster(final String host, final int port, final int timeout, boolean async) {
        super(host, port, timeout);
        client = new RedisClusterClient(host, port, async);
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

    public String set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx, final long time) {
        client.set(key, value, nxxx, expx, time);
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

    public List<Long> bitfield(final byte[] key, final byte[]... arguments) {
        client.bitfield(key, arguments);
        return client.getIntegerMultiBulkReply();
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

    public ScanResult<byte[]> scan(final byte[] cursor) {
        return scan(cursor, new ScanParams());
    }

    public ScanResult<byte[]> scan(final byte[] cursor, final ScanParams params) {
        client.scan(cursor, params);
        List<Object> result = client.getObjectMultiBulkReply();
        byte[] newcursor = (byte[]) result.get(0);
        @SuppressWarnings("unchecked")
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<byte[]>(newcursor, rawResults);
    }

    public ScanResult<byte[]> cscan(int partitionID, byte[] cursor) {
        return cscan(partitionID, cursor, new ScanParams());
    }

    public ScanResult<byte[]> cscan(int partitionID, byte[] cursor, ScanParams params) {
        client.cscan(partitionID, cursor, params);
        List<Object> result = client.getObjectMultiBulkReply();
        byte[] newcursor = (byte[]) result.get(0);
        @SuppressWarnings("unchecked")
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<byte[]>(newcursor, rawResults);
    }

    public String cscandigest() {
        client.cscandigest();
        return client.getBulkReply();
    }

    public Long cscanlen() {
        client.cscanlen();
        return client.getIntegerReply();
    }

    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor) {
        return sscan(key, cursor, new ScanParams());
    }

    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        client.sscan(key, cursor, params);
        List<Object> result = client.getObjectMultiBulkReply();
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<byte[]>(newcursor, rawResults);
    }

    public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor) {
        client.hscan(key, cursor, new ScanParams());
        List<Object> result = client.getObjectMultiBulkReply();
        String newcursor = new String((byte[]) result.get(0));
        List<Map.Entry<byte[], byte[]>> results = new ArrayList<Map.Entry<byte[], byte[]>>();
        @SuppressWarnings("unchecked")
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        Iterator<byte[]> iterator = rawResults.iterator();
        while (iterator.hasNext()) {
          results.add(new AbstractMap.SimpleEntry<byte[], byte[]>(iterator.next(), iterator.next()));
        }
        return new ScanResult<Map.Entry<byte[], byte[]>>(newcursor, results);
    }

    public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        client.hscan(key, cursor, params);
        List<Object> result = client.getObjectMultiBulkReply();
        byte[] newcursor = (byte[]) result.get(0);
        List<Map.Entry<byte[], byte[]>> results = new ArrayList<Map.Entry<byte[], byte[]>>();
        @SuppressWarnings("unchecked")
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        Iterator<byte[]> iterator = rawResults.iterator();
        while (iterator.hasNext()) {
            results.add(new AbstractMap.SimpleEntry<byte[], byte[]>(iterator.next(), iterator.next()));
        }
        return new ScanResult<Map.Entry<byte[], byte[]>>(newcursor, results);
    }

    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor) {
        return zscan(key, cursor, new ScanParams());
    }

    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        client.zscan(key, cursor, params);
        List<Object> result = client.getObjectMultiBulkReply();
        byte[] newcursor = (byte[]) result.get(0);
        List<Tuple> results = new ArrayList<Tuple>();
        @SuppressWarnings("unchecked")
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        Iterator<byte[]> iterator = rawResults.iterator();
        while (iterator.hasNext()) {
            results.add(new Tuple(iterator.next(), Double.valueOf(SafeEncoder.encode(iterator.next()))));
        }
        return new ScanResult<Tuple>(newcursor, results);
    }
    
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        client.hset(key, field, value);
        return client.getIntegerReply();
    }

    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        client.hsetnx(key, field, value);
        return client.getIntegerReply();
    }
    
    public Long hstrlen(byte[] key, byte[] field) {
        client.hstrlen(key, field);
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

    public Long zlexcount(final byte[] key, final byte[] min, final byte[] max) {
      client.zlexcount(key, min, max);
      return client.getIntegerReply();
    }

    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
      client.zrangeByLex(key, min, max);
      return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max,
        final int offset, final int count) {
      client.zrangeByLex(key, min, max, offset, count);
      return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
      client.zrevrangeByLex(key, max, min);
      return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
      client.zrevrangeByLex(key, max, min, offset, count);
      return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    public Long zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
      client.zremrangeByLex(key, min, max);
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
    
    public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
        client.geoadd(key, longitude, latitude, member);
        return client.getIntegerReply();
    }
    
    public Long geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
        client.geoadd(key, memberCoordinateMap);
        return client.getIntegerReply();
    }

    public Double geodist(byte[] key, byte[] member1, byte[] member2) {
        client.geodist(key, member1, member2);
        String dval = client.getBulkReply();
        return (dval != null ? new Double(dval) : null);
    }

    public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
        client.geodist(key, member1, member2, unit);
        String dval = client.getBulkReply();
        return (dval != null ? new Double(dval) : null);
    }

    public List<byte[]> geohash(byte[] key, byte[]... members) {
        client.geohash(key, members);
        return client.getBinaryMultiBulkReply();
    }

    public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
        client.geopos(key, members);
        return BuilderFactory.GEO_COORDINATE_LIST.build(client.getObjectMultiBulkReply());
    }

    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius,
            GeoUnit unit) {
        client.georadius(key, longitude, latitude, radius, unit);
        return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
    }

    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius, GeoUnit unit,
            GeoRadiusParam param) {
        client.georadius(key, longitude, latitude, radius, unit, param);
        return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
    }

    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit) {
        client.georadiusByMember(key, member, radius, unit);
        return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
    }

    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit,
            GeoRadiusParam param) {
        client.georadiusByMember(key, member, radius, unit, param);
        return BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT.build(client.getObjectMultiBulkReply());
    }

    public Long pfadd(final byte[] key, final byte[]... elements) {
      client.pfadd(key, elements);
      return client.getIntegerReply();
    }

    public Long pfcount(final byte[] key) {
      client.pfcount(key);
      return client.getIntegerReply();
    }
    
    public Long touch(byte[]... keys) {
        client.touch(keys);
        return client.getIntegerReply();
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
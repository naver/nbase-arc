/*
 * Copyright 2011-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.navercorp.redis.cluster.spring;

import static com.navercorp.redis.cluster.connection.RedisProtocol.Keyword.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.exceptions.JedisDataException;

import com.navercorp.redis.cluster.gateway.GatewayClient;
import com.navercorp.redis.cluster.pipeline.RedisClusterPipeline;
import com.navercorp.redis.cluster.pipeline.Response;
import com.navercorp.redis.cluster.util.ScanIteratorFactory;
import com.navercorp.redis.cluster.util.ScanIteratorFactory.ScanIterator;

/**
 * The Class RedisClusterConnection.
 * <p>
 * {@code RedisConnection} implementation on top of <a href="http://github.com/xetorthio/jedis">RedisCluster</a> library.
 *
 * @author Costin Leau
 */
public class RedisClusterConnection implements RedisConnection, RedisSessionOfHashListCommands,
        RedisSessionOfHashSetCommands {

    /**
     * The client.
     */
    private final GatewayClient client;
    private final RedisClusterExceptionConverter exceptionConverter = new RedisClusterExceptionConverter();

    private volatile RedisClusterPipeline pipeline;

    private boolean convertPipelineAndTxResults = true;
    private List<FutureResult<Response<?>>> pipelinedResults = new ArrayList<FutureResult<Response<?>>>();

    /**
     * Constructs a new <code>RedisClusterConnection</code> instance backed by a redisCluster pool.
     *
     * @param client the client
     */
    public RedisClusterConnection(GatewayClient client) {
        this.client = client;
    }

    /**
     * Specifies if pipelined results should be converted to the expected data type. If false, results of
     * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the Jedis driver
     *
     * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
     */
    public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
        this.convertPipelineAndTxResults = convertPipelineAndTxResults;
    }

    /*
     * @see org.springframework.data.redis.connection.RedisCommands#execute(java.lang.String, byte[][])
     */
    public Object execute(String command, byte[]... args) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnection#close()
     */
    public void close() throws DataAccessException {
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnection#isClosed()
     */
    public boolean isClosed() {
        return false;
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnection#getNativeConnection()
     */
    public Object getNativeConnection() {
        return this.client;
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnection#isPipelined()
     */
    public boolean isPipelined() {
        return pipeline != null;
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnection#isQueueing()
     */
    public boolean isQueueing() {
        return false;
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnection#openPipeline()
     */
    public void openPipeline() {
        if (pipeline == null) {
            pipeline = client.pipeline();
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnection#closePipeline()
     */
    public List<Object> closePipeline() {
        if (pipeline == null) {
            return Collections.emptyList();
        }

        try {
            return convertPipelineResults();
        } finally {
            pipeline.close();
            pipeline = null;
            pipelinedResults.clear();
        }
    }

    private List<Object> convertPipelineResults() {
        List<Object> results = new ArrayList<Object>();
        pipeline.sync();
        Exception cause = null;
        for (FutureResult<Response<?>> result : pipelinedResults) {
            try {
                Object data = result.get();
                if (!convertPipelineAndTxResults || !(result.isStatus())) {
                    results.add(data);
                }
            } catch (JedisDataException e) {
                DataAccessException dataAccessException = convertException(e);
                if (cause == null) {
                    cause = dataAccessException;
                }
                results.add(dataAccessException);
            } catch (DataAccessException e) {
                if (cause == null) {
                    cause = e;
                }
                results.add(e);
            }
        }
        if (cause != null) {
            throw new RedisPipelineException(cause, results);
        }
        return results;
    }

    private void pipeline(FutureResult<Response<?>> result) {
        pipelinedResults.add(result);
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters)
     */
    @Override
    public List<byte[]> sort(byte[] key, SortParameters params) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
     */
    @Override
    public Long sort(byte[] key, SortParameters params, byte[] sortKey) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown()
     */
    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#dbSize()
     */
    @Override
    public Long dbSize() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.dbSize()));
                return null;
            }

            return client.dbSize();
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnectionCommands#echo(byte[])
     */
    @Override
    public byte[] echo(byte[] message) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnectionCommands#ping()
     */
    @Override
    public String ping() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ping()));
                return null;
            }

            return client.ping();
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#del(byte[][])
     */
    @Override
    public Long del(byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.del(keys)));
                return null;
            }

            return client.del(keys);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#discard()
     */
    @Override
    public void discard() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#exec()
     */
    @Override
    public List<Object> exec() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#exists(byte[])
     */
    @Override
    public Boolean exists(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.exists(key)));
                return null;
            }

            return client.exists(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#expire(byte[], long)
     */
    @Override
    public Boolean expire(byte[] key, long seconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.expire(key, (int) seconds), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(client.expire(key, (int) seconds));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#expireAt(byte[], long)
     */
    @Override
    public Boolean expireAt(byte[] key, long unixTime) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.expireAt(key, unixTime), JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(client.expireAt(key, unixTime));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#keys(byte[])
     */
    @Override
    public Set<byte[]> keys(byte[] pattern) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#persist(byte[])
     */
    @Override
    public Boolean persist(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.persist(key), JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(client.persist(key));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#move(byte[], int)
     */
    @Override
    public Boolean move(byte[] key, int dbIndex) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#randomKey()
     */
    @Override
    public byte[] randomKey() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#rename(byte[], byte[])
     */
    @Override
    public void rename(byte[] oldName, byte[] newName) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#renameNX(byte[], byte[])
     */
    @Override
    public Boolean renameNX(byte[] oldName, byte[] newName) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnectionCommands#select(int)
     */
    @Override
    public void select(int dbIndex) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[])
     */
    @Override
    public Long ttl(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ttl(key)));
                return null;
            }

            return client.ttl(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Boolean pExpire(byte[] key, long milliseconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pexpire(key, milliseconds), JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(this.client.pexpire(key, milliseconds));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Boolean pExpireAt(byte[] key, long millisecondsTimestamp) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pexpireAt(key, millisecondsTimestamp),
                        JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(this.client.pexpireAt(key, millisecondsTimestamp));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long pTtl(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pttl(key)));
                return null;
            }

            return this.client.pttl(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public byte[] dump(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.dump(key)));
                return null;
            }

            return this.client.dump(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.restore(key, ttlInMillis, serializedValue)));
                return;
            }

            this.client.restore(key, ttlInMillis, serializedValue);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#type(byte[])
     */
    @Override
    public DataType type(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.type(key), JedisConverters.stringToDataType()));
                return null;
            }

            return JedisConverters.toDataType(client.type(key));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    //
    // String commands
    //

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#get(byte[])
     */
    @Override
    public byte[] get(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.get(key)));
                return null;
            }

            return client.get(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[])
     */
    @Override
    public void set(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.set(key, value)));
                return;
            }
            client.set(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#getSet(byte[], byte[])
     */
    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.getSet(key, value)));
                return null;
            }
            return client.getSet(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#append(byte[], byte[])
     */
    @Override
    public Long append(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.append(key, value)));
                return null;
            }
            return client.append(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#mGet(byte[][])
     */
    @Override
    public List<byte[]> mGet(byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.mget(keys)));
                return null;
            }

            return client.mget(keys);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#mSet(java.util.Map)
     */
    @Override
    public void mSet(Map<byte[], byte[]> tuples) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.mset(JedisConverters.toByteArrays((tuples)))));
                return;
            }

            client.mset(JedisConverters.toByteArrays(tuples));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#mSetNX(java.util.Map)
     */
    @Override
    public Boolean mSetNX(Map<byte[], byte[]> tuples) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#setEx(byte[], long, byte[])
     */
    @Override
    public void setEx(byte[] key, long seconds, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.setex(key, (int) seconds, value)));
                return;
            }

            client.setex(key, (int) seconds, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void pSetEx(byte[] key, long milliseconds, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.psetex(key, milliseconds, value)));
                return;
            }

            client.psetex(key, milliseconds, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }


    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#setNX(byte[], byte[])
     */
    @Override
    public Boolean setNX(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.setnx(key, value), JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(client.setnx(key, value));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#getRange(byte[], long, long)
     */
    @Override
    public byte[] getRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.getrange(key, start, end)));
                return null;
            }

            return client.getrange(key, start, end);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#decr(byte[])
     */
    @Override
    public Long decr(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.decr(key)));
                return null;
            }

            return client.decr(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#decrBy(byte[], long)
     */
    @Override
    public Long decrBy(byte[] key, long value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.decrBy(key, value)));
                return null;
            }

            return client.decrBy(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#incr(byte[])
     */
    @Override
    public Long incr(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.incr(key)));
                return null;
            }

            return client.incr(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#incrBy(byte[], long)
     */
    @Override
    public Long incrBy(byte[] key, long value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.incrBy(key, value)));
                return null;
            }

            return client.incrBy(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Double incrBy(byte[] key, double value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.incrByFloat(key, value)));
                return null;
            }

            return this.client.incrByFloat(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#getBit(byte[], long)
     */
    @Override
    public Boolean getBit(byte[] key, long offset) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.getbit(key, offset)));
                return null;
            }

            // compatibility check for RedisCluster 2.0.0
            Object getBit = client.getbit(key, offset);
            // RedisCluster 2.0
            if (getBit instanceof Long) {
                return (((Long) getBit) == 0 ? Boolean.FALSE : Boolean.TRUE);
            }
            // RedisCluster 2.1
            return ((Boolean) getBit);
        } catch (Exception ex) {
            throw convertException(ex);
        }

    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#setBit(byte[], long, boolean)
     */
    @Override
    public Boolean setBit(byte[] key, long offset, boolean value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.setbit(key, offset, JedisConverters.toBit(value))));
                return null;
            }

            return client.setbit(key, offset, JedisConverters.toBit(value));
        } catch (Exception ex) {
            throw convertException(ex);
        }

    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#setRange(byte[], byte[], long)
     */
    @Override
    public void setRange(byte[] key, byte[] value, long start) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.setrange(key, start, value)));
                return;
            }
            client.setrange(key, start, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#strLen(byte[])
     */
    @Override
    public Long strLen(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.strlen(key)));
                return null;
            }

            return client.strlen(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long bitCount(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.bitcount(key)));
                return null;
            }

            return this.client.bitcount(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long bitCount(byte[] key, long begin, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.bitcount(key, begin, end)));
                return null;
            }

            return this.client.bitcount(key, begin, end);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // List commands
    //

    @Override
    public Long lPush(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lpush(key, values)));
                return null;
            }

            return this.client.lpush(key, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long rPush(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.rpush(key, values)));
                return null;
            }

            return this.client.rpush(key, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#bLPop(int, byte[][])
     */
    @Override
    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#bRPop(int, byte[][])
     */
    @Override
    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lIndex(byte[], long)
     */
    @Override
    public byte[] lIndex(byte[] key, long index) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lindex(key, index)));
                return null;
            }

            return client.lindex(key, index);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lInsert(byte[], org.springframework.data.redis.connection.RedisListCommands.Position, byte[], byte[])
     */
    @Override
    public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.linsert(key, JedisConverters.toListPosition(where), pivot, value)));
                return null;
            }

            return client.linsert(key, JedisConverters.toListPosition(where), pivot, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lLen(byte[])
     */
    @Override
    public Long lLen(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.llen(key)));
                return null;
            }

            return client.llen(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lPop(byte[])
     */
    @Override
    public byte[] lPop(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lpop(key)));
                return null;
            }

            return client.lpop(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lRange(byte[], long, long)
     */
    @Override
    public List<byte[]> lRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lrange(key, start, end)));
                return null;
            }

            return client.lrange(key, start, end);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lRem(byte[], long, byte[])
     */
    @Override
    public Long lRem(byte[] key, long count, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lrem(key, count, value)));
                return null;
            }

            return client.lrem(key, count, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lSet(byte[], long, byte[])
     */
    @Override
    public void lSet(byte[] key, long index, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.lset(key, index, value)));
                return;
            }

            client.lset(key, index, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lTrim(byte[], long, long)
     */
    @Override
    public void lTrim(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.ltrim(key, start, end)));
                return;
            }

            client.ltrim(key, start, end);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#rPop(byte[])
     */
    @Override
    public byte[] rPop(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.rpop(key)));
                return null;
            }

            return client.rpop(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#rPopLPush(byte[], byte[])
     */
    @Override
    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#bRPopLPush(int, byte[], byte[])
     */
    @Override
    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lPushX(byte[], byte[])
     */
    @Override
    public Long lPushX(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lpushx(key, value)));
                return null;
            }

            return client.lpushx(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#rPushX(byte[], byte[])
     */
    @Override
    public Long rPushX(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.rpushx(key, value)));
                return null;
            }

            return client.rpushx(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    //
    // Set commands
    //

    @Override
    public Long sAdd(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sadd(key, values)));
                return null;
            }

            return this.client.sadd(key, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sCard(byte[])
     */
    @Override
    public Long sCard(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.scard(key)));
                return null;
            }

            return client.scard(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sDiff(byte[][])
     */
    @Override
    public Set<byte[]> sDiff(byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sDiffStore(byte[], byte[][])
     */
    @Override
    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sInter(byte[][])
     */
    @Override
    public Set<byte[]> sInter(byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sInterStore(byte[], byte[][])
     */
    @Override
    public Long sInterStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sIsMember(byte[], byte[])
     */
    @Override
    public Boolean sIsMember(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sismember(key, value)));
                return null;
            }

            return client.sismember(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sMembers(byte[])
     */
    @Override
    public Set<byte[]> sMembers(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.smembers(key)));
                return null;
            }

            return client.smembers(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sMove(byte[], byte[], byte[])
     */
    @Override
    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[])
     */
    @Override
    public byte[] sPop(byte[] key) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[])
     */
    @Override
    public byte[] sRandMember(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.srandmember(key)));
                return null;
            }

            return client.srandmember(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public List<byte[]> sRandMember(byte[] key, long count) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.srandmember(key, (int) count)));
                return null;
            }

            return this.client.srandmember(key, (int) count);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long sRem(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.srem(key, values)));
                return null;
            }

            return this.client.srem(key, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sUnion(byte[][])
     */
    @Override
    public Set<byte[]> sUnion(byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sUnionStore(byte[], byte[][])
     */
    @Override
    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // ZSet commands
    //

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], double, byte[])
     */
    @Override
    public Boolean zAdd(byte[] key, double score, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zadd(key, score, value), JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(client.zadd(key, score, value));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long zAdd(byte[] key, Set<Tuple> tuples) {
        try {
            if (isPipelined()) {
                throw new UnsupportedOperationException("not supported pipeline command!");
            }
            Map<byte[], Double> args = zAddArgs(tuples);
            try {
                return this.client.zadd2(key, args);
            } catch (Exception ex) {
                throw convertException(ex);
            }
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    private Map<byte[], Double> zAddArgs(Set<Tuple> tuples) {
        Map<byte[], Double> args = new HashMap<byte[], Double>();
        for (Tuple tuple : tuples) {
            args.put(tuple.getValue(), tuple.getScore());
        }

        return args;
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zCard(byte[])
     */
    @Override
    public Long zCard(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zcard(key)));
                return null;
            }

            return client.zcard(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], double, double)
     */
    @Override
    public Long zCount(byte[] key, double min, double max) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zcount(key, min, max)));
                return null;
            }

            return client.zcount(key, min, max);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zIncrBy(byte[], double, byte[])
     */
    @Override
    public Double zIncrBy(byte[] key, double increment, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zincrby(key, increment, value)));
                return null;
            }

            return client.zincrby(key, increment, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, int[], byte[][])
     */
    @Override
    public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], byte[][])
     */
    @Override
    public Long zInterStore(byte[] destKey, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRange(byte[], long, long)
     */
    @Override
    public Set<byte[]> zRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrange(key, start, end)));
                return null;
            }

            return client.zrange(key, start, end);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeWithScores(byte[], long, long)
     */
    @Override
    public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeWithScores(key, start, end),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.toTupleSet(client.zrangeWithScores(key, start, end));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double)
     */
    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScore(key, min, max)));
                return null;
            }

            return client.zrangeByScore(key, min, max);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
        try {
            final String keyStr = new String(key, "UTF-8");
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScore(keyStr, min, max)));
                return null;
            }

            return JedisConverters.stringSetToByteSet().convert(client.zrangeByScore(keyStr, min, max));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
        try {
            final String keyStr = new String(key, "UTF-8");
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScore(keyStr, min, max, (int) offset, (int) count)));
                return null;
            }

            return JedisConverters.stringSetToByteSet().convert(client.zrangeByScore(keyStr, min, max, (int) offset, (int) count));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double)
     */
    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScoreWithScores(key, min, max),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.toTupleSet(client.zrangeByScoreWithScores(key, min, max));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeWithScores(byte[], long, long)
     */
    @Override
    public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeWithScores(key, start, end),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.toTupleSet(client.zrevrangeWithScores(key, start, end));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double, long, long)
     */
    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScore(key, min, max, (int) offset, (int) count)));
                return null;
            }

            return client.zrangeByScore(key, min, max, (int) offset, (int) count);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double, long, long)
     */
    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScoreWithScores(key, min, max, (int) offset, (int) count),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.toTupleSet(client.zrangeByScoreWithScores(key, min, max, (int) offset, (int) count));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double, long, long)
     */
    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScore(key, max, min, (int) offset, (int) count)));
                return null;
            }

            return client.zrevrangeByScore(key, max, min, (int) offset, (int) count);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double)
     */
    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScore(key, max, min)));
                return null;
            }

            return client.zrevrangeByScore(key, max, min);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double, long, long)
     */
    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScoreWithScores(key, max, min, (int) offset, (int) count),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.toTupleSet(client.zrevrangeByScoreWithScores(key, max, min, (int) offset, (int) count));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double)
     */
    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScoreWithScores(key, max, min),
                        JedisConverters.tupleSetToTupleSet()));
            }

            return JedisConverters.toTupleSet(client.zrevrangeByScoreWithScores(key, max, min));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRank(byte[], byte[])
     */
    @Override
    public Long zRank(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrank(key, value)));
                return null;
            }

            return client.zrank(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long zRem(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrem(key, values)));
                return null;
            }

            return this.client.zrem(key, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRange(byte[], long, long)
     */
    @Override
    public Long zRemRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zremrangeByRank(key, start, end)));
                return null;
            }

            return client.zremrangeByRank(key, start, end);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], double, double)
     */
    @Override
    public Long zRemRangeByScore(byte[] key, double min, double max) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zremrangeByScore(key, min, max)));
                return null;
            }

            return client.zremrangeByScore(key, min, max);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRange(byte[], long, long)
     */
    @Override
    public Set<byte[]> zRevRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrange(key, start, end)));
                return null;
            }

            return client.zrevrange(key, start, end);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRank(byte[], byte[])
     */
    @Override
    public Long zRevRank(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrank(key, value)));
                return null;
            }
            return client.zrevrank(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zScore(byte[], byte[])
     */
    @Override
    public Double zScore(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zscore(key, value)));
                return null;
            }
            return client.zscore(key, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, int[], byte[][])
     */
    @Override
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], byte[][])
     */
    @Override
    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // Hash commands
    //

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hSet(byte[], byte[], byte[])
     */
    @Override
    public Boolean hSet(byte[] key, byte[] field, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hset(key, field, value), JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(client.hset(key, field, value));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hSetNX(byte[], byte[], byte[])
     */
    @Override
    public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hsetnx(key, field, value), JedisConverters.longToBoolean()));
                return null;
            }

            return JedisConverters.toBoolean(client.hsetnx(key, field, value));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long hDel(byte[] key, byte[]... fields) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hdel(key, fields)));
                return null;
            }

            return this.client.hdel(key, fields);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hExists(byte[], byte[])
     */
    @Override
    public Boolean hExists(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hexists(key, field)));
                return null;
            }

            return client.hexists(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hGet(byte[], byte[])
     */
    @Override
    public byte[] hGet(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hget(key, field)));
                return null;
            }

            return client.hget(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hGetAll(byte[])
     */
    @Override
    public Map<byte[], byte[]> hGetAll(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hgetAll(key)));
                return null;
            }

            return client.hgetAll(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hIncrBy(byte[], byte[], long)
     */
    @Override
    public Long hIncrBy(byte[] key, byte[] field, long delta) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hincrBy(key, field, delta)));
                return null;
            }

            return client.hincrBy(key, field, delta);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Double hIncrBy(byte[] key, byte[] field, double delta) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hincrByFloat(key, field, delta)));
                return null;
            }

            return this.client.hincrByFloat(key, field, delta);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hKeys(byte[])
     */
    @Override
    public Set<byte[]> hKeys(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hkeys(key)));
                return null;
            }
            return client.hkeys(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hLen(byte[])
     */
    @Override
    public Long hLen(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hlen(key)));
                return null;
            }
            return client.hlen(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hMGet(byte[], byte[][])
     */
    @Override
    public List<byte[]> hMGet(byte[] key, byte[]... fields) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hmget(key, fields)));
                return null;
            }
            return client.hmget(key, fields);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hMSet(byte[], java.util.Map)
     */
    @Override
    public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.hmset(key, tuple)));
                return;
            }
            client.hmset(key, tuple);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hVals(byte[])
     */
    @Override
    public List<byte[]> hVals(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hvals(key)));
                return null;
            }
            return client.hvals(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#publish(byte[], byte[])
     */
    @Override
    public Long publish(byte[] arg0, byte[] arg1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#getSubscription()
     */
    @Override
    public Subscription getSubscription() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#isSubscribed()
     */
    @Override
    public boolean isSubscribed() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#pSubscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
     */
    @Override
    public void pSubscribe(MessageListener arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#subscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
     */
    @Override
    public void subscribe(MessageListener arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // Scripting commands
    //

    @Override
    public void scriptFlush() {
        throw new UnsupportedOperationException("not supported command!");
    }

    @Override
    public void scriptKill() {
        throw new UnsupportedOperationException("not supported command!");
    }

    @Override
    public String scriptLoad(byte[] script) {
        throw new UnsupportedOperationException("not supported command!");
    }

    @Override
    public List<Boolean> scriptExists(String... scriptSha1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    @Override
    public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        throw new UnsupportedOperationException("not supported command!");
    }

    @Override
    public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#multi()
     */
    @Override
    public void multi() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#unwatch()
     */
    @Override
    public void unwatch() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#watch(byte[][])
     */
    @Override
    public void watch(byte[]... arg0) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#bgSave()
     */
    @Override
    public void bgSave() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#bgWriteAof()
     */
    @Override
    public void bgWriteAof() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#flushAll()
     */
    @Override
    public void flushAll() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#flushDb()
     */
    @Override
    public void flushDb() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#getConfig(java.lang.String)
     */
    @Override
    public List<String> getConfig(String param) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#info()
     */
    @Override
    public Properties info() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.info(), JedisConverters.stringToProps()));
                return null;
            }

            return JedisConverters.toProperties(this.client.info());
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Properties info(String section) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.info(section), JedisConverters.stringToProps()));
                return null;
            }

            return JedisConverters.toProperties(this.client.info(section));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#lastSave()
     */
    @Override
    public Long lastSave() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#resetConfigStats()
     */
    @Override
    public void resetConfigStats() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#save()
     */
    @Override
    public void save() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#setConfig(java.lang.String, java.lang.String)
     */
    @Override
    public void setConfig(String param, String value) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssGet(byte[], byte[], byte[])
     */
    @Override
    public Set<byte[]> ssGet(byte[] key, byte[] field, byte[] name) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssget(key, field, name)));
                return null;
            }

            return this.client.ssget(key, field, name);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssMGet(byte[], byte[], byte[][])
     */
    @Override
    public Map<byte[], Set<byte[]>> ssMGet(byte[] key, byte[] field, byte[]... names) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssmget(key, field, names)));
                return null;
            }

            return this.client.ssmget(key, field, names);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssKeys(byte[])
     */
    @Override
    public Set<byte[]> ssKeys(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sskeys(key)));
                return null;
            }

            return this.client.sskeys(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssKeys(byte[], byte[])
     */
    @Override
    public Set<byte[]> ssKeys(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sskeys(key, field)));
                return null;
            }

            return this.client.sskeys(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssAdd(byte[], byte[], byte[], byte[][])
     */
    @Override
    public Long ssAdd(byte[] key, byte[] field, byte[] name, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssadd(key, field, name, values)));
                return null;
            }

            return this.client.ssadd(key, field, name, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssAdd(byte[], byte[], byte[], long, byte[][])
     */
    @Override
    public Long ssAdd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssadd(key, field, name, expireSeconds, values)));
                return null;
            }

            return this.client.ssadd(key, field, name, expireSeconds, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long ssAddAt(byte[] key, byte[] field, byte[] name, long millisecondsTimestamp, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssaddAt(key, field, name, millisecondsTimestamp, values)));
                return null;
            }

            return this.client.ssaddAt(key, field, name, millisecondsTimestamp, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssSet(byte[], byte[], byte[], byte[][])
     */
    @Override
    public Long ssSet(byte[] key, byte[] field, byte[] name, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssset(key, field, name, values)));
                return null;
            }

            return this.client.ssset(key, field, name, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssSet(byte[], byte[], byte[], long, byte[][])
     */
    @Override
    public Long ssSet(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssset(key, field, name, expireSeconds, values)));
                return null;
            }

            return this.client.ssset(key, field, name, expireSeconds, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssDel(byte[])
     */
    @Override
    public Long ssDel(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssdel(key)));
                return null;
            }

            return this.client.ssdel(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssRem(byte[], byte[])
     */
    @Override
    public Long ssRem(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssrem(key, field)));
                return null;
            }

            return this.client.ssrem(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssRem(byte[], byte[], byte[])
     */
    @Override
    public Long ssRem(byte[] key, byte[] field, byte[] name) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssrem(key, field, name)));
                return null;
            }

            return this.client.ssrem(key, field, name);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssRem(byte[], byte[], byte[], byte[][])
     */
    @Override
    public Long ssRem(byte[] key, byte[] field, byte[] name, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssrem(key, field, name, values)));
                return null;
            }

            return this.client.ssrem(key, field, name, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssCount(byte[], byte[], byte[])
     */
    @Override
    public Long ssCount(byte[] key, byte[] field, byte[] name) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sscount(key, field, name)));
                return null;
            }

            return this.client.sscount(key, field, name);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssExists(byte[], byte[], byte[], byte[])
     */
    @Override
    public Boolean ssExists(byte[] key, byte[] field, byte[] name, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssexists(key, field, name, value)));
                return null;
            }

            return this.client.ssexists(key, field, name, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssExpire(byte[], long)
     */
    @Override
    public Long ssExpire(byte[] key, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssexpire(key, expireSeconds)));
                return null;
            }

            return this.client.ssexpire(key, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssExpire(byte[], byte[], long)
     */
    @Override
    public Long ssExpire(byte[] key, byte[] field, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssexpire(key, field, expireSeconds)));
                return null;
            }

            return this.client.ssexpire(key, field, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssExpire(byte[], byte[], byte[], long)
     */
    @Override
    public Long ssExpire(byte[] key, byte[] field, byte[] name, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssexpire(key, field, name, expireSeconds)));
                return null;
            }

            return this.client.ssexpire(key, field, name, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssExpire(byte[], byte[], byte[], byte[], long)
     */
    @Override
    public Long ssExpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssexpire(key, field, name, value, expireSeconds)));
                return null;
            }

            return this.client.ssexpire(key, field, name, value, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssTTL(byte[], byte[], byte[], byte[])
     */
    @Override
    public Long ssTTL(byte[] key, byte[] field, byte[] name, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssttl(key, field, name, value)));
                return null;
            }

            return this.client.ssttl(key, field, name, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public List<byte[]> ssVals(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ssvals(key, field)));
                return null;
            }

            return this.client.ssvals(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slGet(byte[], byte[], byte[])
     */
    @Override
    public List<byte[]> slGet(byte[] key, byte[] field, byte[] name) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slget(key, field, name)));
                return null;
            }

            return this.client.slget(key, field, name);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slMGet(byte[], byte[], byte[][])
     */
    @Override
    public Map<byte[], List<byte[]>> slMGet(byte[] key, byte[] field, byte[]... names) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slmget(key, field, names)));
                return null;
            }

            return this.client.slmget(key, field, names);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slKeys(byte[])
     */
    @Override
    public Set<byte[]> slKeys(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slkeys(key)));
                return null;
            }

            return this.client.slkeys(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slKeys(byte[], byte[])
     */
    @Override
    public Set<byte[]> slKeys(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slkeys(key, field)));
                return null;
            }

            return this.client.slkeys(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slAdd(byte[], byte[], byte[], byte[][])
     */
    @Override
    public Long slAdd(byte[] key, byte[] field, byte[] name, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sladd(key, field, name, values)));
                return null;
            }

            return this.client.sladd(key, field, name, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slAdd(byte[], byte[], byte[], long, byte[][])
     */
    @Override
    public Long slAdd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sladd(key, field, name, expireSeconds, values)));
                return null;
            }

            return this.client.sladd(key, field, name, expireSeconds, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long slAddAt(byte[] key, byte[] field, byte[] name, long millisecondsTimestamp, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sladdAt(key, field, name, millisecondsTimestamp, values)));
                return null;
            }

            return this.client.sladdAt(key, field, name, millisecondsTimestamp, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slSet(byte[], byte[], byte[], byte[][])
     */
    @Override
    public Long slSet(byte[] key, byte[] field, byte[] name, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slset(key, field, name, values)));
                return null;
            }

            return this.client.slset(key, field, name, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slSet(byte[], byte[], byte[], long, byte[][])
     */
    @Override
    public Long slSet(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slset(key, field, name, expireSeconds, values)));
                return null;
            }

            return this.client.slset(key, field, name, expireSeconds, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slDel(byte[])
     */
    @Override
    public Long slDel(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sldel(key)));
                return null;
            }

            return this.client.sldel(key);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slRem(byte[], byte[])
     */
    @Override
    public Long slRem(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slrem(key, field)));
                return null;
            }
            return this.client.slrem(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slRem(byte[], byte[], byte[])
     */
    @Override
    public Long slRem(byte[] key, byte[] field, byte[] name) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slrem(key, field, name)));
                return null;
            }

            return this.client.slrem(key, field, name);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slRem(byte[], byte[], byte[], byte[][])
     */
    @Override
    public Long slRem(byte[] key, byte[] field, byte[] name, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slrem(key, field, name, values)));
                return null;
            }

            return this.client.slrem(key, field, name, values);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slCount(byte[], byte[], byte[])
     */
    @Override
    public Long slCount(byte[] key, byte[] field, byte[] name) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slcount(key, field, name)));
                return null;
            }

            return this.client.slcount(key, field, name);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slExists(byte[], byte[], byte[], byte[])
     */
    @Override
    public Boolean slExists(byte[] key, byte[] field, byte[] name, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slexists(key, field, name, value)));
                return null;
            }

            return this.client.slexists(key, field, name, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slExpire(byte[], long)
     */
    @Override
    public Long slExpire(byte[] key, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slexpire(key, expireSeconds)));
                return null;
            }

            return this.client.slexpire(key, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slExpire(byte[], byte[], long)
     */
    @Override
    public Long slExpire(byte[] key, byte[] field, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slexpire(key, field, expireSeconds)));
                return null;
            }

            return this.client.slexpire(key, field, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slExpire(byte[], byte[], byte[], long)
     */
    @Override
    public Long slExpire(byte[] key, byte[] field, byte[] name, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slexpire(key, field, name, expireSeconds)));
                return null;
            }

            return this.client.slexpire(key, field, name, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slExpire(byte[], byte[], byte[], byte[], long)
     */
    @Override
    public Long slExpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slexpire(key, field, name, expireSeconds)));
                return null;
            }

            return this.client.slexpire(key, field, name, value, expireSeconds);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /*
     * @see RedisSessionOfHashListCommands#slTTL(byte[], byte[], byte[], byte[])
     */
    @Override
    public Long slTTL(byte[] key, byte[] field, byte[] name, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slttl(key, field, name, value)));
                return null;
            }

            return this.client.slttl(key, field, name, value);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public List<byte[]> slVals(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.slvals(key, field)));
                return null;
            }

            return this.client.slvals(key, field);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    private class JedisResult extends FutureResult<Response<?>> {
        public <T> JedisResult(Response<T> resultHolder, Converter<T, ?> converter) {
            super(resultHolder, converter);
        }

        public <T> JedisResult(Response<T> resultHolder) {
            super(resultHolder);
        }

        @SuppressWarnings("unchecked")
        public Object get() {
            if (convertPipelineAndTxResults && converter != null) {
                return converter.convert(resultHolder.get());
            }
            return resultHolder.get();
        }
    }

    private class JedisStatusResult extends JedisResult {
        public JedisStatusResult(Response<?> resultHolder) {
            super(resultHolder);
            setStatus(true);
        }
    }

    private DataAccessException convertException(Exception ex) {
        return exceptionConverter.convert(ex);
    }

    @Override
    public void bgReWriteAof() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public List<RedisClientInfo> getClientList() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public String getClientName() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public void killClient(String arg0, int arg1) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public void setClientName(byte[] arg0) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public void shutdown(ShutdownOption arg0) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public void slaveOf(String arg0, int arg1) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public void slaveOfNoOne() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public Long time() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public <T> T evalSha(byte[] arg0, ReturnType arg1, int arg2, byte[]... arg3) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public Long pfAdd(byte[] arg0, byte[]... arg1) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pfadd(arg0, arg1)));
                return null;
            }

            return this.client.pfadd(arg0, arg1);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void pfMerge(byte[] arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    abstract static class DefaultScanCursor<T, T2> implements Cursor<T> {
        enum State {
            READY, OPEN, CLOSED
        };

        protected final ScanIterator<T2> iterator;
        private State state;

        protected DefaultScanCursor(ScanIterator<T2> iterator) {
            this.iterator = iterator;
            this.state = State.READY;
        }

        @Override
        public boolean hasNext() {
            assertCursorIsOpen();
            return iterator.hasNext();
        }

        abstract public T next();

        protected void assertCursorIsOpen() {
            if (state != State.OPEN) {
                throw new InvalidDataAccessApiUsageException(
                        "Cannot access closed cursor. Did you forget to call open()?");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove is not supported operation!");
        }

        @Override
        public void close() throws IOException {
            state = State.CLOSED;
        }

        @Override
        public boolean isClosed() {
            return state == State.CLOSED;
        }

        @Override
        public long getCursorId() {
            return iterator.getCursor();
        }

        @Override
        public Cursor<T> open() {
            if (state != State.READY) {
                throw new InvalidDataAccessApiUsageException("Cursor already " + state + ". Cannot (re)open it.");
            }
            state = State.OPEN;
            return this;
        }

        @Override
        public long getPosition() {
            return iterator.getPosition();
        }
    }

    static Cursor<byte[]> createScanCursor(
        final GatewayClient client, final ScanOptions options) { 
    
        return new DefaultScanCursor<byte[], byte[]>(
                ScanIteratorFactory.createScanIterator(client, JedisConverters.toScanParams(options))) {
            @Override
            public byte[] next() {
                assertCursorIsOpen();
                return iterator.next();
            }
        };
    }
    
    static Cursor<byte[]> createSScanCursor(final GatewayClient client, final byte[] key,
            final ScanOptions options) {

        return new DefaultScanCursor<byte[], byte[]>(
                ScanIteratorFactory.createSScanIterator(client, key, JedisConverters.toScanParams(options))) {
            @Override
            public byte[] next() {
                assertCursorIsOpen();
                return iterator.next();
            }
        };
    }

    private static final Converter<redis.clients.jedis.Tuple, Tuple> TUPLE_CONVERTER;
    static {
        TUPLE_CONVERTER = new Converter<redis.clients.jedis.Tuple, Tuple>() {
            public Tuple convert(redis.clients.jedis.Tuple source) {
                return source != null ? new DefaultTuple(source.getBinaryElement(), source.getScore()) : null;
            }
        };
    }

    static Cursor<Tuple> createZScanCursor(
        final GatewayClient client, final byte[] key, final ScanOptions options) {
        
        return new DefaultScanCursor<Tuple, redis.clients.jedis.Tuple>( 
                ScanIteratorFactory.createZScanIterator(client, key, JedisConverters.toScanParams(options))) {
            @Override
            public Tuple next() {
                assertCursorIsOpen();
                return TUPLE_CONVERTER.convert(iterator.next());
            }
        };
    }

    static Cursor<Entry<byte[], byte[]>> createHScanCursor(
        final GatewayClient client, final byte[] key, final ScanOptions options) {
        
        return new DefaultScanCursor<Entry<byte[], byte[]>, Entry<byte[], byte[]>>(
                ScanIteratorFactory.createHScanIterator(client, key, JedisConverters.toScanParams(options))) {
            @Override
            public Entry<byte[], byte[]> next() {
                assertCursorIsOpen();
                return iterator.next();
            }
        };
    }

    @Override
    public Cursor<byte[]> scan(ScanOptions options) {
        try {
            if (isPipelined()) {
                throw new UnsupportedOperationException("'SCAN' cannot be called in pipeline mode.");
            }

            Cursor<byte[]> cursor = createScanCursor(client, options);
            cursor.open();
            return cursor;
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }
    
    @Override
    public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
        try {
            if (isPipelined()) {
                throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline mode.");
            }

            Cursor<byte[]> cursor = createSScanCursor(this.client, key, options);
            cursor.open();
            return cursor;
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }
    
    @Override
    public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
        try {
            if (isPipelined()) {
                throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline mode.");
            }

            Cursor<Tuple> cursor = createZScanCursor(this.client, key, options);
            cursor.open();
            return cursor;
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }
    
    @Override
    public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
        try {
            if (isPipelined()) {
                throw new UnsupportedOperationException("'HSCAN' cannot be called in pipeline mode.");
            }

            Cursor<Entry<byte[], byte[]>> cursor = createHScanCursor(this.client, key, options);
            cursor.open();
            return cursor;
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long ttl(byte[] key, TimeUnit timeUnit) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ttl(key), JedisConverters.secondsToTimeUnit(timeUnit)));
                return null;
            }
            
            return JedisConverters.secondsToTimeUnit(this.client.ttl(key), timeUnit);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long pTtl(byte[] key, TimeUnit timeUnit) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ttl(key), JedisConverters.millisecondsToTimeUnit(timeUnit)));
                return null;
            }
            
            return JedisConverters.millisecondsToTimeUnit(this.client.pttl(key), timeUnit);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
        try {
            if (isPipelined()) {
                switch (option) {
                case SET_IF_ABSENT:
                    pipeline(new JedisStatusResult(
                            pipeline.set(key, value, XX.raw, PX.raw, expiration.getExpirationTimeInSeconds())));
                    break;
                case SET_IF_PRESENT:
                    pipeline(new JedisStatusResult(
                            pipeline.set(key, value, XX.raw, PX.raw, expiration.getExpirationTimeInSeconds())));
                    break;
                case UPSERT:
                    pipeline(new JedisStatusResult(
                            pipeline.set(key, value, XX.raw, PX.raw, expiration.getExpirationTimeInSeconds())));
                    break;
                }
            }

            switch (option) {
            case SET_IF_ABSENT:
                this.client.set(key, value, XX.raw, PX.raw, expiration.getExpirationTimeInSeconds());
                break;
            case SET_IF_PRESENT:
                this.client.set(key, value, XX.raw, PX.raw, expiration.getExpirationTimeInSeconds());
                break;
            case UPSERT:
                this.client.set(key, value, XX.raw, PX.raw, expiration.getExpirationTimeInSeconds());
                break;
            }
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }
    
    private byte[] minToBytesForZRange(Range range) {
        return JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.MINUS_BYTES);
    }
    
    private byte[] maxToBytesForZRange(Range range) {
        return JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.PLUS_BYTES);
    }
    
    private byte[] minToBytesForZRangeByLex(Range range) {
        return JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
    }
    
    private byte[] maxToBytesForZRangeByLex(Range range) {
        return JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScoreWithScores(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range)),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.tupleSetToTupleSet()
                    .convert(this.client.zrangeByScoreWithScores(key,
                            minToBytesForZRange(range),
                            maxToBytesForZRange(range)));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScoreWithScores(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range), 
                        limit.getOffset(), limit.getCount()), JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.tupleSetToTupleSet()
                    .convert(this.client.zrangeByScoreWithScores(key,
                            minToBytesForZRange(range),
                            maxToBytesForZRange(range),
                            limit.getOffset(), limit.getCount()));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScore(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range))));
                return null;
            }

            return this.client.zrevrangeByScore(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScore(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range),
                        limit.getOffset(), limit.getCount())));
                return null;
            }

            return this.client.zrevrangeByScore(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range),
                    limit.getOffset(), limit.getCount());
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScoreWithScores(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range)),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.tupleSetToTupleSet().convert(
                    this.client.zrevrangeByScoreWithScores(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range)));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeByScoreWithScores(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range),
                        limit.getOffset(), limit.getCount()),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }

            return JedisConverters.tupleSetToTupleSet().convert(
                    this.client.zrevrangeByScoreWithScores(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range),
                    limit.getOffset(), limit.getCount()));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long zCount(byte[] key, Range range) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zcount(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range))));
                return null;
            }

            return this.client.zcount(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long zRemRangeByScore(byte[] key, Range range) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zremrangeByScore(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range))));
                return null;
            }

            return this.client.zremrangeByScore(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScore(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range))));
                return null;
            }

            return this.client.zrangeByScore(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeByScore(key,
                        minToBytesForZRange(range),
                        maxToBytesForZRange(range),
                        limit.getOffset(), limit.getCount())));
                return null;
            }

            return this.client.zrangeByScore(key, 
                    minToBytesForZRange(range),
                    maxToBytesForZRange(range),
                    limit.getOffset(), limit.getCount());
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(
                        pipeline.zrangeByLex(key, JedisConverters.MINUS_BYTES, JedisConverters.PLUS_BYTES)));
                return null;
            }

            return this.client.zrangeByLex(key, JedisConverters.MINUS_BYTES, JedisConverters.PLUS_BYTES);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key, Range range) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(
                        pipeline.zrangeByLex(key, minToBytesForZRangeByLex(range),
                                maxToBytesForZRangeByLex(range))));
                return null;
            }

            return this.client.zrangeByLex(key, minToBytesForZRangeByLex(range),
                    maxToBytesForZRangeByLex(range));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(
                        pipeline.zrangeByLex(key, minToBytesForZRangeByLex(range),
                                maxToBytesForZRangeByLex(range), limit.getOffset(),
                                limit.getCount())));
                return null;
            }

            return this.client.zrangeByLex(key, minToBytesForZRangeByLex(range),
                    maxToBytesForZRangeByLex(range), limit.getOffset(),
                    limit.getCount());
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option, long timeout) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public Long geoAdd(byte[] key, Point point, byte[] member) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geoadd(key, point.getX(), point.getY(), member)));
                return null;
            }

            return this.client.geoadd(key, point.getX(), point.getY(), member);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geoadd(key, location.getPoint().getX(), location.getPoint().getY(),
                        location.getName())));
                return null;
            }

            return this.client.geoadd(key, location.getPoint().getX(), location.getPoint().getY(), location.getName());
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long geoAdd(byte[] key, Map<byte[], Point> memberPointMap) {
        try {
            Map<byte[], GeoCoordinate> memberCoordinateMap = new HashMap<byte[], GeoCoordinate>();
            for (Entry<byte[], Point> point : memberPointMap.entrySet()) {
                memberCoordinateMap.put(point.getKey(),
                        new GeoCoordinate(point.getValue().getX(), point.getValue().getY()));
            }

            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geoadd(key, memberCoordinateMap)));
                return null;
            }

            return this.client.geoadd(key, memberCoordinateMap);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
        try {
            Map<byte[], GeoCoordinate> memberCoordinateMap = new HashMap<byte[], GeoCoordinate>();
            for (GeoLocation<byte[]> location : locations) {
                memberCoordinateMap.put(location.getName(),
                        new GeoCoordinate(location.getPoint().getX(), location.getPoint().getY()));
            }

            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geoadd(key, memberCoordinateMap)));
                return null;
            }

            return this.client.geoadd(key, memberCoordinateMap);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geodist(key, member1, member2, GeoUnit.M),
                        JedisConverters.distanceConverterForMetric(Metrics.NEUTRAL)));
                return null;
            }

            return JedisConverters.distanceConverterForMetric(Metrics.NEUTRAL)
                    .convert(this.client.geodist(key, member1, member2, GeoUnit.M));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geodist(key, member1, member2, JedisConverters.toGeoUnit(metric)),
                        JedisConverters.distanceConverterForMetric(metric)));
                return null;
            }

            return JedisConverters.distanceConverterForMetric(metric)
                    .convert(this.client.geodist(key, member1, member2, JedisConverters.toGeoUnit(metric)));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public List<String> geoHash(byte[] key, byte[]... members) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geohash(key, members),
                        JedisConverters.bytesListToStringListConverter()));
                return null;
            }

            return JedisConverters.bytesListToStringListConverter().convert(this.client.geohash(key, members));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public List<Point> geoPos(byte[] key, byte[]... members) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geopos(key, members), JedisConverters.geoCoordinateToPointConverter()));
                return null;
            }

            return JedisConverters.geoCoordinateToPointConverter().convert(client.geopos(key, members));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
                        within.getRadius().getValue(), GeoUnit.valueOf(within.getRadius().getUnit().toUpperCase())),
                        JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())));
                return null;
            }

            return JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())
                    .convert(client.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
                            within.getRadius().getValue(),
                            GeoUnit.valueOf(within.getRadius().getUnit().toUpperCase())));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
                        within.getRadius().getValue(), GeoUnit.valueOf(within.getRadius().getUnit().toUpperCase()),
                        JedisConverters.toGeoRadiusParam(args)),
                        JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())));
                return null;
            }

            return JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())
                    .convert(client.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
                            within.getRadius().getValue(), GeoUnit.valueOf(within.getRadius().getUnit().toUpperCase()),
                            JedisConverters.toGeoRadiusParam(args)));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.georadiusByMember(key, member, radius, GeoUnit.M),
                        JedisConverters.geoRadiusResponseToGeoResultsConverter(Metrics.NEUTRAL)));
                return null;
            }

            return JedisConverters.geoRadiusResponseToGeoResultsConverter(Metrics.NEUTRAL)
                    .convert(client.georadiusByMember(key, member, radius, GeoUnit.M));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(
                        pipeline.georadiusByMember(key, member, radius.getValue(), GeoUnit.valueOf(radius.getUnit().toUpperCase())),
                        JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric())));
                return null;
            }

            return JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric()).convert(
                    client.georadiusByMember(key, member, radius.getValue(), GeoUnit.valueOf(radius.getUnit().toUpperCase())));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
            GeoRadiusCommandArgs args) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(
                        pipeline.georadiusByMember(key, member, radius.getValue(), GeoUnit.valueOf(radius.getUnit().toUpperCase()),
                                JedisConverters.toGeoRadiusParam(args)),
                        JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric())));
                return null;
            }

            return JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric())
                    .convert(client.georadiusByMember(key, member, radius.getValue(), GeoUnit.valueOf(radius.getUnit().toUpperCase()),
                            JedisConverters.toGeoRadiusParam(args)));
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public Long geoRemove(byte[] key, byte[]... members) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrem(key, members)));
                return null;
            }

            return client.zrem(key, members);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public RedisSentinelConnection getSentinelConnection() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    @Override
    public Long pfCount(byte[]... keys) {
        if (keys.length > 1) {
            throw new UnsupportedOperationException("multi key pfcount is not supported!");
        }

        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pfcount(keys[0])));
                return null;
            }

            return client.pfcount(keys[0]);
        } catch (Exception ex) {
            throw convertException(ex);
        }
    }
}
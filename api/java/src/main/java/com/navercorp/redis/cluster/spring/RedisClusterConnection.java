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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.core.types.RedisClientInfo;

import redis.clients.jedis.exceptions.JedisDataException;

import com.navercorp.redis.cluster.gateway.GatewayClient;
import com.navercorp.redis.cluster.pipeline.RedisClusterPipeline;
import com.navercorp.redis.cluster.pipeline.Response;

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
    public List<byte[]> sort(byte[] key, SortParameters params) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
     */
    public Long sort(byte[] key, SortParameters params, byte[] sortKey) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown()
     */
    public void shutdown() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#dbSize()
     */
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
    public byte[] echo(byte[] message) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnectionCommands#ping()
     */
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
    public void discard() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#exec()
     */
    public List<Object> exec() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#exists(byte[])
     */
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
    public Set<byte[]> keys(byte[] pattern) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#persist(byte[])
     */
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
    public Boolean move(byte[] key, int dbIndex) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#randomKey()
     */
    public byte[] randomKey() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#rename(byte[], byte[])
     */
    public void rename(byte[] oldName, byte[] newName) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#renameNX(byte[], byte[])
     */
    public Boolean renameNX(byte[] oldName, byte[] newName) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnectionCommands#select(int)
     */
    public void select(int dbIndex) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[])
     */
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
    public Boolean mSetNX(Map<byte[], byte[]> tuples) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisStringCommands#setEx(byte[], long, byte[])
     */
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

    public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // List commands
    //

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
    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#bRPop(int, byte[][])
     */
    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lIndex(byte[], long)
     */
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
    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#bRPopLPush(int, byte[], byte[])
     */
    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisListCommands#lPushX(byte[], byte[])
     */
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
    public Set<byte[]> sDiff(byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sDiffStore(byte[], byte[][])
     */
    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sInter(byte[][])
     */
    public Set<byte[]> sInter(byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sInterStore(byte[], byte[][])
     */
    public Long sInterStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sIsMember(byte[], byte[])
     */
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
    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[])
     */
    public byte[] sPop(byte[] key) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[])
     */
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
    public Set<byte[]> sUnion(byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisSetCommands#sUnionStore(byte[], byte[][])
     */
    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // ZSet commands
    //

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], double, byte[])
     */
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
    public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], byte[][])
     */
    public Long zInterStore(byte[] destKey, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zRange(byte[], long, long)
     */
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
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], byte[][])
     */
    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // Hash commands
    //

    /*
     * @see org.springframework.data.redis.connection.RedisHashCommands#hSet(byte[], byte[], byte[])
     */
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
    public Long publish(byte[] arg0, byte[] arg1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#getSubscription()
     */
    public Subscription getSubscription() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#isSubscribed()
     */
    public boolean isSubscribed() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#pSubscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
     */
    public void pSubscribe(MessageListener arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisPubSubCommands#subscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
     */
    public void subscribe(MessageListener arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    //
    // Scripting commands
    //

    public void scriptFlush() {
        throw new UnsupportedOperationException("not supported command!");
    }

    public void scriptKill() {
        throw new UnsupportedOperationException("not supported command!");
    }

    public String scriptLoad(byte[] script) {
        throw new UnsupportedOperationException("not supported command!");
    }

    public List<Boolean> scriptExists(String... scriptSha1) {
        throw new UnsupportedOperationException("not supported command!");
    }

    public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        throw new UnsupportedOperationException("not supported command!");
    }

    public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#multi()
     */
    public void multi() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#unwatch()
     */
    public void unwatch() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisTxCommands#watch(byte[][])
     */
    public void watch(byte[]... arg0) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#bgSave()
     */
    public void bgSave() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#bgWriteAof()
     */
    public void bgWriteAof() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#flushAll()
     */
    public void flushAll() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#flushDb()
     */
    public void flushDb() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#getConfig(java.lang.String)
     */
    public List<String> getConfig(String param) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#info()
     */
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
    public Long lastSave() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#resetConfigStats()
     */
    public void resetConfigStats() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#save()
     */
    public void save() {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see org.springframework.data.redis.connection.RedisServerCommands#setConfig(java.lang.String, java.lang.String)
     */
    public void setConfig(String param, String value) {
        throw new UnsupportedOperationException("not supported command!");
    }

    /*
     * @see RedisSessionOfHashSetCommands#ssGet(byte[], byte[], byte[])
     */
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

    public void bgReWriteAof() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public List<RedisClientInfo> getClientList() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public String getClientName() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public void killClient(String arg0, int arg1) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public void setClientName(byte[] arg0) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public void shutdown(ShutdownOption arg0) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public void slaveOf(String arg0, int arg1) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public void slaveOfNoOne() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public Long time() {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public <T> T evalSha(byte[] arg0, ReturnType arg1, int arg2, byte[]... arg3) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public Long pfAdd(byte[] arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public Long pfCount(byte[]... arg0) {
        throw new UnsupportedOperationException("not supported operation!");
    }

    public void pfMerge(byte[] arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not supported operation!");
    }

//	public RedisSentinelConnection getSentinelConnection() {
//		throw new UnsupportedOperationException("not supported operation!");
//	}
}
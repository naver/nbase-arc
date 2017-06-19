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

package com.navercorp.redis.cluster.gateway;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.navercorp.redis.cluster.BinaryRedisClusterCommands;
import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.RedisClusterCommands;
import com.navercorp.redis.cluster.async.BackgroundPool;
import com.navercorp.redis.cluster.pipeline.RedisClusterPipeline;
import com.navercorp.redis.cluster.triples.BinaryTriplesRedisClusterCommands;
import com.navercorp.redis.cluster.triples.TriplesRedisClusterCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * redis cluster<br>
 * <p>
 * GatewayClient
 * <p>
 * <b>Spring Bean</b><br>
 * <pre>
 * &lt;bean id="gatewayConfig" class="com.navercorp.redis.cluster.GatewayConfig"&gt;
 * &lt;property name="ipAddress" value="10.96.250.205:6379,10.96.250.207:6379,10.96.250.209:6379"/&gt;
 * &lt;/bean&gt;
 *
 * &lt;bean id="gatewayClient" class="com.navercorp.redis.cluster.GatewayClient" destroy-method="destroy"&gt;
 *  &lt;constructor-arg ref="gatewayConfig"/&gt;
 * &lt;/bean&gt;
 *
 * </pre>
 * <p>
 * <b>Example</b><br>
 * <pre>
 * GatewayConfig config = new GatewayConfig();
 * config.setIpAddress(address); // address is [ip:port,...]
 * or config.setDomainAddress(address); // address is domain:port
 * GatewayClient client = new GatewayClient(config);
 *
 * String result = client.get("foo");
 * client.destroy();
 * </pre>
 * <p>
 * <p>
 * <b>Pipeline</b><br>
 * <pre>
 * RedisClusterPipeline pipeline = null;
 * try {
 *   pipeline = gatewayClient.pipeline();
 *   pipeline.setTimeout(10000); // pipeline timeout
 *   pipeline.incr("foo");
 *   pipeline.incr("bar");
 *
 *   List&lt;Object&gt; result = pipeline.syncAndReturnAll();
 * } finally {
 *   if(pipeline != null) {
 *       pipeline.close();
 *   }
 * }
 * </pre>
 * <p>
 *
 * @author seongminwoo
 * @author jaehong.kim
 */
public class GatewayClient implements RedisClusterCommands, BinaryRedisClusterCommands, TriplesRedisClusterCommands,
        BinaryTriplesRedisClusterCommands {

    private final Logger log = LoggerFactory.getLogger(GatewayClient.class);

    /**
     * The gateway.
     */
    private Gateway gateway;
    private final int maxRetry;
    private BackgroundPool backgroundPool;

    /**
     * Instantiates a new gateway client.
     *
     * @param config the config
     */
    public GatewayClient(final GatewayConfig config) {
        this.gateway = new Gateway(config);
        this.maxRetry = config.getMaxRetry();
        this.backgroundPool = new BackgroundPool(config.getBackgroundPoolSize());
    }

    /**
     * destroy.
     */
    public void destroy() {
        this.gateway.destroy();
        this.backgroundPool.shutdown();
    }

    /**
     * Gets the gateway.
     *
     * @return the gateway
     */
    Gateway getGateway() {
        return this.gateway;
    }

    public RedisClusterPipeline pipeline() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(gateway);
        return pipeline;
    }

    public List<Object> pipelineCallback(final GatewayPipelineCallback action) {
        RedisClusterPipeline pipeline = null;
        try {
            pipeline = new RedisClusterPipeline(gateway);
            action.doInRedisCluster(pipeline);
            return pipeline.syncAndReturnAll();
        } finally {
            if (pipeline != null) {
                pipeline.close();
            }
        }
    }

    public void executeBackgroundPool(final Runnable task) {
        this.backgroundPool.getPool().execute(task);
    }

    /**
     * execute to action(redis in command)
     *
     * @param <T>    the generic type
     * @param action {@link RedisClusterCallback}
     * @return object returned by the action
     */
    public <T> T execute(RedisClusterCallback<T> action) {
        GatewayException exception = null;
        for (int i = 0; i <= this.maxRetry; i++) {
            try {
                return executeCallback(action, i);
            } catch (GatewayException e) {
                if (!e.isRetryable()) {
                    throw e;
                } else {
                    exception = e;
                    if (i < this.maxRetry) {
                        log.warn(
                                "[GatewayClient] Failed to action. auto retrying ... " + (i + 1) + "/" + this.maxRetry, e);
                    }
                }
            }
        }

        throw exception;
    }

    private <T> T executeCallback(final RedisClusterCallback<T> action, final int count) {
        final long startedTime = System.currentTimeMillis();
        final int hash = action.getPartitionNumber();
        final GatewayServer server = gateway.getServer(hash, action.getState());

        RedisCluster redis = null;
        try {
            redis = server.getResource(); // JedisConnectionException
            redis.getConnection().allocPc(hash, action.getState(), false);
            final T result = action.doInRedisCluster(redis); // JedisConnectionException, JedisDataException, JedisException or RuntimeException.
            server.returnResource(redis);
            return result;
        } catch (JedisConnectionException ex) {
            final String message = toExecuteInfo(count, startedTime, server, redis);
            server.setValid(false); // drop.
            server.returnBrokenResource(redis);
            throw new GatewayException(message, ex, true);
        } catch (Exception ex) {
            if ((ex instanceof IOException)) {
                final String message = toExecuteInfo(count, startedTime, server, redis);
                server.setValid(false); // drop.
                server.returnBrokenResource(redis);
                throw new GatewayException(message, ex, true);
            }
            final String message = toExecuteInfo(count, startedTime, server, redis);
            server.returnResource(redis);
            throw new GatewayException(message, ex);
        }
    }

    // time, retry, gateway server information.
    String toExecuteInfo(final int tryCount, final long startedTime, final GatewayServer server,
                         final RedisCluster redis) {
        final StringBuilder sb = new StringBuilder();
        final long executedTime = System.currentTimeMillis() - startedTime;
        sb.append("time=").append(executedTime).append(", ");
        sb.append("retry=").append(tryCount).append(", ");
        sb.append("gateway=").append(server);
        if (redis != null) {
            sb.append(", ").append("connect=").append(redis.connectInfo());
        }

        return sb.toString();
    }

    /* 
     * @see RedisClusterCommands#ping()
	 */
    public String ping() {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ping();
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#del(java.lang.String)
	 */
    public Long del(final String... keys) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.del(keys);
            }

            public int getPartitionNumber() {
                if (keys == null || keys.length < 1) {
                    return GatewayPartitionNumber.NOT_MATCHED;
                }
                return GatewayPartitionNumber.get(keys[0]);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#del(byte[])
	 */
    public Long del(final byte[]... keys) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.del(keys);
            }

            public int getPartitionNumber() {
                if (keys == null || keys.length < 1) {
                    return GatewayPartitionNumber.NOT_MATCHED;
                }
                return GatewayPartitionNumber.get(keys[0]);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#exists(java.lang.String)
	 */
    public Boolean exists(final String key) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.exists(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#exists(byte[])
	 */
    public Boolean exists(final byte[] key) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.exists(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#expire(java.lang.String, int)
	 */
    public Long expire(final String key, final int seconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.expire(key, seconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#expire(byte[], int)
	 */
    public Long expire(final byte[] key, final int seconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.expire(key, seconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#expireAt(java.lang.String, long)
	 */
    public Long expireAt(final String key, final long secondsTimestamp) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.expireAt(key, secondsTimestamp);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#expireAt(byte[], long)
	 */
    public Long expireAt(final byte[] key, final long secondsTimestamp) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.expireAt(key, secondsTimestamp);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long pexpire(final String key, final long milliseconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.pexpire(key, milliseconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long pexpire(final byte[] key, final long milliseconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.pexpire(key, milliseconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.pexpireAt(key, millisecondsTimestamp);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.pexpireAt(key, millisecondsTimestamp);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#ttl(java.lang.String)
	 */
    public Long ttl(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ttl(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#ttl(byte[])
	 */
    public Long ttl(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ttl(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long pttl(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.pttl(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long pttl(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.pttl(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#type(java.lang.String)
	 */
    public String type(final String key) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.type(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#type(byte[])
	 */
    public String type(final byte[] key) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.type(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long persist(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.persist(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long persist(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.persist(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#get(java.lang.String)
	 */
    public String get(final String key) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.get(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#get(byte[])
	 */
    public byte[] get(final byte[] key) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.get(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#set(java.lang.String, java.lang.String)
	 */
    public String set(final String key, final String value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.set(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public String set(final String key, final String value, final String nxxx, final String expx, final long time) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.set(key, value, nxxx, expx, time);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }
    
    /* 
	 * @see BinaryRedisClusterCommands#set(byte[], byte[])
	 */
    public String set(final byte[] key, final byte[] value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.set(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public String set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx, final long time) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.set(key, value, nxxx, expx, time);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }
    
    /* 
	 * @see RedisClusterCommands#getSet(java.lang.String, java.lang.String)
	 */
    public String getSet(final String key, final String value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.getSet(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#getSet(byte[], byte[])
	 */
    public byte[] getSet(final byte[] key, final byte[] value) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.getSet(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#append(java.lang.String, java.lang.String)
	 */
    public Long append(final String key, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.append(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#append(byte[], byte[])
	 */
    public Long append(final byte[] key, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.append(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#setex(java.lang.String, int, java.lang.String)
	 */
    public String setex(final String key, final int seconds, final String value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setex(key, seconds, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#setex(byte[], int, byte[])
	 */
    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setex(key, seconds, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#setnx(java.lang.String, java.lang.String)
	 */
    public Long setnx(final String key, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setnx(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#setnx(byte[], byte[])
	 */
    public Long setnx(final byte[] key, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setnx(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#substr(java.lang.String, int, int)
	 */
    public String substr(final String key, final int start, final int end) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.substr(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#substr(byte[], int, int)
	 */
    public byte[] substr(final byte[] key, final int start, final int end) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.substr(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#getrange(java.lang.String, long, long)
	 */
    public String getrange(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.getrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#getrange(byte[], long, long)
	 */
    public byte[] getrange(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.getrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#objectRefcount(java.lang.String)
	 */
    public Long objectRefcount(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.objectRefcount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#objectRefcount(byte[])
	 */
    public Long objectRefcount(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.objectRefcount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#objectEncoding(java.lang.String)
	 */
    public String objectEncoding(final String key) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.objectEncoding(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#objectEncoding(byte[])
	 */
    public byte[] objectEncoding(final byte[] key) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.objectEncoding(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#objectIdletime(java.lang.String)
	 */
    public Long objectIdletime(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.objectIdletime(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#objectIdletime(byte[])
	 */
    public Long objectIdletime(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.objectIdletime(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#decr(java.lang.String)
	 */
    public Long decr(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.decr(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#decr(byte[])
	 */
    public Long decr(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.decr(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#decrBy(java.lang.String, long)
	 */
    public Long decrBy(final String key, final long value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.decrBy(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#decrBy(byte[], long)
	 */
    public Long decrBy(final byte[] key, final long value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.decrBy(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#incr(java.lang.String)
	 */
    public Long incr(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.incr(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#incr(byte[])
	 */
    public Long incr(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.incr(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#incrBy(java.lang.String, long)
	 */
    public Long incrBy(final String key, final long value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.incrBy(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#incrBy(byte[], long)
	 */
    public Long incrBy(final byte[] key, final long value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.incrBy(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Double incrByFloat(final String key, final double increment) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.incrByFloat(key, increment);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Double incrByFloat(final byte[] key, final double increment) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.incrByFloat(key, increment);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#getbit(java.lang.String, long)
	 */
    public Boolean getbit(final String key, final long offset) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.getbit(key, offset);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#getbit(byte[], long)
	 */
    public Boolean getbit(final byte[] key, final long offset) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.getbit(key, offset);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#setbit(java.lang.String, long, boolean)
	 */
    public Boolean setbit(final String key, final long offset, final boolean value) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setbit(key, offset, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#setbit(byte[], long, byte[])
	 */
    public Boolean setbit(final byte[] key, final long offset, final byte[] value) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setbit(key, offset, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#setrange(java.lang.String, long, java.lang.String)
	 */
    public Long setrange(final String key, final long offset, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setrange(key, offset, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#setrange(byte[], long, byte[])
	 */
    public Long setrange(final byte[] key, final long offset, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.setrange(key, offset, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#strlen(java.lang.String)
	 */
    public Long strlen(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.strlen(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#strlen(byte[])
	 */
    public Long strlen(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.strlen(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /**
     * @see RedisClusterCommands#mget(java.lang.String[])
     */
    public List<String> mget(final String... keys) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.mget(keys);
            }

            public int getPartitionNumber() {
                if (keys == null || keys.length < 1) {
                    return GatewayPartitionNumber.NOT_MATCHED;
                }
                return GatewayPartitionNumber.get(keys[0]);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /**
     * @see BinaryRedisClusterCommands#mget(byte[][])
     */
    public List<byte[]> mget(final byte[]... keys) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.mget(keys);
            }

            public int getPartitionNumber() {
                if (keys == null || keys.length < 1) {
                    return GatewayPartitionNumber.NOT_MATCHED;
                }
                return GatewayPartitionNumber.get(keys[0]);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /**
     * @see BinaryRedisClusterCommands#mset(byte[][])
     */
    public String mset(final byte[]... keysvalues) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.mset(keysvalues);
            }

            public int getPartitionNumber() {
                if (keysvalues == null || keysvalues.length < 1) {
                    return GatewayPartitionNumber.NOT_MATCHED;
                }
                return GatewayPartitionNumber.get(keysvalues[0]);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /**
     * @see RedisClusterCommands#mset(java.lang.String[])
     */
    public String mset(final String... keysvalues) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.mset(keysvalues);
            }

            public int getPartitionNumber() {
                if (keysvalues == null || keysvalues.length < 1) {
                    return GatewayPartitionNumber.NOT_MATCHED;
                }
                return GatewayPartitionNumber.get(keysvalues[0]);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public String psetex(final String key, final long milliseconds, final String value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.psetex(key, milliseconds, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public String psetex(final byte[] key, final long milliseconds, final byte[] value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.psetex(key, milliseconds, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long bitcount(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.bitcount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long bitcount(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.bitcount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long bitcount(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.bitcount(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long bitcount(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.bitcount(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    //
    // List commands
    //

    /* 
	 * @see RedisClusterCommands#lpush(java.lang.String, java.lang.String[])
	 */
    public Long lpush(final String key, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lpush(key, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#lpush(byte[], byte[][])
	 */
    public Long lpush(final byte[] key, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lpush(key, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#rpush(java.lang.String, java.lang.String[])
	 */
    public Long rpush(final String key, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.rpush(key, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#rpush(byte[], byte[][])
	 */
    public Long rpush(final byte[] key, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.rpush(key, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#lindex(java.lang.String, long)
	 */
    public String lindex(final String key, final long index) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lindex(key, index);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#lindex(byte[], long)
	 */
    public byte[] lindex(final byte[] key, final long index) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lindex(key, index);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#linsert(java.lang.String, redis.clients.jedis.BinaryClient.LIST_POSITION, java.lang.String, java.lang.String)
	 */
    public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.linsert(key, where, pivot, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#linsert(byte[], redis.clients.jedis.BinaryClient.LIST_POSITION, byte[], byte[])
	 */
    public Long linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.linsert(key, where, pivot, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#llen(java.lang.String)
	 */
    public Long llen(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.llen(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#llen(byte[])
	 */
    public Long llen(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.llen(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#lpop(java.lang.String)
	 */
    public String lpop(final String key) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lpop(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#lpop(byte[])
	 */
    public byte[] lpop(final byte[] key) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lpop(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#lrange(java.lang.String, long, long)
	 */
    public List<String> lrange(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#lrange(byte[], long, long)
	 */
    public List<byte[]> lrange(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#lrem(java.lang.String, long, java.lang.String)
	 */
    public Long lrem(final String key, final long count, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lrem(key, count, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#lrem(byte[], long, byte[])
	 */
    public Long lrem(final byte[] key, final long count, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lrem(key, count, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#lset(java.lang.String, long, java.lang.String)
	 */
    public String lset(final String key, final long index, final String value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lset(key, index, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#lset(byte[], long, byte[])
	 */
    public String lset(final byte[] key, final long index, final byte[] value) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lset(key, index, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#ltrim(java.lang.String, long, long)
	 */
    public String ltrim(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ltrim(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#ltrim(byte[], long, long)
	 */
    public String ltrim(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ltrim(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#rpop(java.lang.String)
	 */
    public String rpop(final String key) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.rpop(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#rpop(byte[])
	 */
    public byte[] rpop(final byte[] key) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.rpop(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#lpushx(java.lang.String, java.lang.String)
	 */
    public Long lpushx(final String key, final String string) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lpushx(key, string);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#lpushx(byte[], byte[])
	 */
    public Long lpushx(final byte[] key, final byte[] string) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.lpushx(key, string);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#rpushx(java.lang.String, java.lang.String)
	 */
    public Long rpushx(final String key, final String string) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.rpushx(key, string);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#rpushx(byte[], byte[])
	 */
    public Long rpushx(final byte[] key, final byte[] string) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.rpushx(key, string);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    //
    // Set commands
    //

    /* 
	 * @see RedisClusterCommands#sadd(java.lang.String, java.lang.String[])
	 */
    public Long sadd(final String key, final String... members) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sadd(key, members);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#sadd(byte[], byte[][])
	 */
    public Long sadd(final byte[] key, final byte[]... members) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sadd(key, members);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#scard(java.lang.String)
	 */
    public Long scard(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.scard(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#scard(byte[])
	 */
    public Long scard(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.scard(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#sismember(java.lang.String, java.lang.String)
	 */
    public Boolean sismember(final String key, final String member) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sismember(key, member);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#sismember(byte[], byte[])
	 */
    public Boolean sismember(final byte[] key, final byte[] member) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sismember(key, member);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#smembers(java.lang.String)
	 */
    public Set<String> smembers(final String key) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.smembers(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#smembers(byte[])
	 */
    public Set<byte[]> smembers(final byte[] key) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.smembers(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#srandmember(java.lang.String)
	 */
    public String srandmember(final String key) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.srandmember(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#srandmember(byte[])
	 */
    public byte[] srandmember(final byte[] key) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.srandmember(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public List<String> srandmember(final String key, final int count) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.srandmember(key, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#srandmember(byte[])
	 */
    public List<byte[]> srandmember(final byte[] key, final int count) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.srandmember(key, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#srem(java.lang.String, java.lang.String[])
	 */
    public Long srem(final String key, final String... member) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.srem(key, member);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#srem(byte[], byte[][])
	 */
    public Long srem(final byte[] key, final byte[]... member) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.srem(key, member);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    //
    // ZSet commands
    //

    /* 
	 * @see RedisClusterCommands#zadd(java.lang.String, double, java.lang.String)
	 */
    public Long zadd(final String key, final double score, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zadd(key, score, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zadd(byte[], double, byte[])
	 */
    public Long zadd(final byte[] key, final double score, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zadd(key, score, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zadd(java.lang.String, java.util.Map)
	 */
    public Long zadd(final String key, final Map<Double, String> scoreMembers) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zadd(key, scoreMembers);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zadd(byte[], java.util.Map)
	 */
    public Long zadd(final byte[] key, final Map<Double, byte[]> scoreMembers) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zadd(key, scoreMembers);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long zadd2(final String key, final Map<String, Double> scoreMembers) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zadd2(key, scoreMembers);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long zadd2(final byte[] key, final Map<byte[], Double> scoreMembers) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zadd2(key, scoreMembers);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zcard(java.lang.String)
	 */
    public Long zcard(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zcard(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zcard(byte[])
	 */
    public Long zcard(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zcard(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zcount(java.lang.String, double, double)
	 */
    public Long zcount(final String key, final double min, final double max) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zcount(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zcount(byte[], double, double)
	 */
    public Long zcount(final byte[] key, final double min, final double max) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zcount(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zcount(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long zcount(final String key, final String min, final String max) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zcount(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zcount(byte[], byte[], byte[])
	 */
    public Long zcount(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zcount(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zincrby(java.lang.String, double, java.lang.String)
	 */
    public Double zincrby(final String key, final double score, final String member) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zincrby(key, score, member);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zincrby(byte[], double, byte[])
	 */
    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zincrby(key, score, member);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrange(java.lang.String, long, long)
	 */
    public Set<String> zrange(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrange(byte[], long, long)
	 */
    public Set<byte[]> zrange(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeWithScores(java.lang.String, long, long)
	 */
    public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeWithScores(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeWithScores(byte[], long, long)
	 */
    public Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeWithScores(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScore(java.lang.String, double, double)
	 */
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScore(byte[], double, double)
	 */
    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScore(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScore(byte[], byte[], byte[])
	 */
    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScore(java.lang.String, java.lang.String, java.lang.String, int, int)
	 */
    public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset,
                                     final int count) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScore(byte[], byte[], byte[], int, int)
	 */
    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                     final int count) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScoreWithScores(java.lang.String, double, double)
	 */
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScoreWithScores(byte[], double, double)
	 */
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScoreWithScores(byte[], byte[], byte[])
	 */
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String, int, int)
	 */
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset,
                                              final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScoreWithScores(byte[], byte[], byte[], int, int)
	 */
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                              final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeWithScores(java.lang.String, long, long)
	 */
    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeWithScores(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeWithScores(byte[], long, long)
	 */
    public Set<Tuple> zrevrangeWithScores(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeWithScores(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScore(java.lang.String, double, double, int, int)
	 */
    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset,
                                     final int count) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScore(byte[], double, double, int, int)
	 */
    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
                                     final int count) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScore(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrangeByScoreWithScores(java.lang.String, double, double, int, int)
	 */
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset,
                                              final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrangeByScoreWithScores(byte[], double, double, int, int)
	 */
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset,
                                              final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScore(java.lang.String, double, double, int, int)
	 */
    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset,
                                        final int count) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScore(byte[], double, double, int, int)
	 */
    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset,
                                        final int count) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScore(java.lang.String, double, double)
	 */
    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScore(byte[], double, double)
	 */
    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScore(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScore(byte[], byte[], byte[])
	 */
    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScore(java.lang.String, java.lang.String, java.lang.String, int, int)
	 */
    public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset,
                                        final int count) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScore(byte[], byte[], byte[], int, int)
	 */
    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset,
                                        final int count) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScore(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScoreWithScores(java.lang.String, double, double, int, int)
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min,
                                                 final int offset, final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScoreWithScores(byte[], double, double, int, int)
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min,
                                                 final int offset, final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScoreWithScores(java.lang.String, double, double)
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScoreWithScores(byte[], double, double)
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScoreWithScores(byte[], byte[], byte[])
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String, int, int)
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min,
                                                 final int offset, final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrangeByScoreWithScores(byte[], byte[], byte[], int, int)
	 */
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min,
                                                 final int offset, final int count) {
        return this.execute(new RedisClusterCallback<Set<Tuple>>() {
            public Set<Tuple> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrank(java.lang.String, java.lang.String)
	 */
    public Long zrank(final String key, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrank(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrank(byte[], byte[])
	 */
    public Long zrank(final byte[] key, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrank(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrem(java.lang.String, java.lang.String[])
	 */
    public Long zrem(final String key, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrem(key, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrem(byte[], byte[][])
	 */
    public Long zrem(final byte[] key, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrem(key, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zremrangeByRank(java.lang.String, long, long)
	 */
    public Long zremrangeByRank(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zremrangeByRank(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zremrangeByRank(byte[], long, long)
	 */
    public Long zremrangeByRank(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zremrangeByRank(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }

        });
    }

    /* 
	 * @see RedisClusterCommands#zremrangeByScore(java.lang.String, double, double)
	 */
    public Long zremrangeByScore(final String key, final double start, final double end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zremrangeByScore(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zremrangeByScore(byte[], double, double)
	 */
    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zremrangeByScore(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zremrangeByScore(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long zremrangeByScore(final String key, final String start, final String end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zremrangeByScore(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zremrangeByScore(byte[], byte[], byte[])
	 */
    public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zremrangeByScore(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrange(java.lang.String, long, long)
	 */
    public Set<String> zrevrange(final String key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrange(byte[], long, long)
	 */
    public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrange(key, start, end);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zrevrank(java.lang.String, java.lang.String)
	 */
    public Long zrevrank(final String key, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrank(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });

    }

    /* 
	 * @see BinaryRedisClusterCommands#zrevrank(byte[], byte[])
	 */
    public Long zrevrank(final byte[] key, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zrevrank(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#zscore(java.lang.String, java.lang.String)
	 */
    public Double zscore(final String key, final String value) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zscore(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#zscore(byte[], byte[])
	 */
    public Double zscore(final byte[] key, final byte[] value) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.zscore(key, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    //
    // Hash commands
    //

    /* 
	 * @see RedisClusterCommands#hset(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long hset(final String key, final String field, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hset(key, field, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hset(byte[], byte[], byte[])
	 */
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hset(key, field, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hsetnx(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long hsetnx(final String key, final String field, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hsetnx(key, field, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hsetnx(byte[], byte[], byte[])
	 */
    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hsetnx(key, field, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hdel(java.lang.String, java.lang.String[])
	 */
    public Long hdel(final String key, final String... fields) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hdel(key, fields);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hdel(byte[], byte[][])
	 */
    public Long hdel(final byte[] key, final byte[]... fields) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hdel(key, fields);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hexists(java.lang.String, java.lang.String)
	 */
    public Boolean hexists(final String key, final String field) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hexists(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hexists(byte[], byte[])
	 */
    public Boolean hexists(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hexists(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hget(java.lang.String, java.lang.String)
	 */
    public String hget(final String key, final String field) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hget(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hget(byte[], byte[])
	 */
    public byte[] hget(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hget(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hgetAll(java.lang.String)
	 */
    public Map<String, String> hgetAll(final String key) {
        return this.execute(new RedisClusterCallback<Map<String, String>>() {
            public Map<String, String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hgetAll(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hgetAll(byte[])
	 */
    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        return this.execute(new RedisClusterCallback<Map<byte[], byte[]>>() {
            public Map<byte[], byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hgetAll(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hincrBy(java.lang.String, java.lang.String, long)
	 */
    public Long hincrBy(final String key, final String field, final long delta) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hincrBy(key, field, delta);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hincrBy(byte[], byte[], long)
	 */
    public Long hincrBy(final byte[] key, final byte[] field, final long delta) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hincrBy(key, field, delta);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Double hincrByFloat(final String key, final String field, final double increment) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hincrByFloat(key, field, increment);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Double hincrByFloat(final byte[] key, final byte[] field, final double increment) {
        return this.execute(new RedisClusterCallback<Double>() {
            public Double doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hincrByFloat(key, field, increment);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hkeys(java.lang.String)
	 */
    public Set<String> hkeys(final String key) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hkeys(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hkeys(byte[])
	 */
    public Set<byte[]> hkeys(final byte[] key) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hkeys(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hlen(java.lang.String)
	 */
    public Long hlen(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hlen(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hlen(byte[])
	 */
    public Long hlen(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hlen(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hmget(java.lang.String, java.lang.String[])
	 */
    public List<String> hmget(final String key, final String... fields) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hmget(key, fields);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hmget(byte[], byte[][])
	 */
    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hmget(key, fields);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hmset(java.lang.String, java.util.Map)
	 */
    public String hmset(final String key, final Map<String, String> tuple) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hmset(key, tuple);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hmset(byte[], java.util.Map)
	 */
    public String hmset(final byte[] key, final Map<byte[], byte[]> tuple) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hmset(key, tuple);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see RedisClusterCommands#hvals(java.lang.String)
	 */
    public List<String> hvals(final String key) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hvals(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryRedisClusterCommands#hvals(byte[])
	 */
    public List<byte[]> hvals(final byte[] key) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.hvals(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slget(java.lang.String, java.lang.String, java.lang.String)
	 */
    public List<String> slget(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slget(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slget(byte[], byte[], byte[])
	 */
    public List<byte[]> slget(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slget(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slmget(java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Map<String, List<String>> slmget(final String key, final String field, final String... names) {
        return this.execute(new RedisClusterCallback<Map<String, List<String>>>() {
            public Map<String, List<String>> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slmget(key, field, names);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slmget(byte[], byte[], byte[][])
	 */
    public Map<byte[], List<byte[]>> slmget(final byte[] key, final byte[] field, final byte[]... names) {
        return this.execute(new RedisClusterCallback<Map<byte[], List<byte[]>>>() {
            public Map<byte[], List<byte[]>> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slmget(key, field, names);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slkeys(java.lang.String)
	 */
    public Set<String> slkeys(final String key) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slkeys(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slkeys(byte[])
	 */
    public Set<byte[]> slkeys(final byte[] key) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slkeys(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slkeys(java.lang.String, java.lang.String)
	 */
    public Set<String> slkeys(final String key, final String field) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slkeys(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slkeys(byte[], byte[])
	 */
    public Set<byte[]> slkeys(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slkeys(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sladd(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Long sladd(final String key, final String field, final String name, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sladd(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sladd(byte[], byte[], byte[], byte[][])
	 */
    public Long sladd(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sladd(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sladd(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])
	 */
    public Long sladd(final String key, final String field, final String name, final long expireSeconds,
                      final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sladd(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sladd(byte[], byte[], byte[], long, byte[][])
	 */
    public Long sladd(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                      final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sladd(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long sladdAt(final byte[] key, final byte[] field, final byte[] name, final long millisecondsTimestamp,
                        final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sladdAt(key, field, name, millisecondsTimestamp, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long sladdAt(final String key, final String field, final String name, final long millisecondsTimestamp,
                        final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sladdAt(key, field, name, millisecondsTimestamp, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slset(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Long slset(final String key, final String field, final String name, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slset(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slset(byte[], byte[], byte[], byte[][])
	 */
    public Long slset(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slset(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slset(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])
	 */
    public Long slset(final String key, final String field, final String name, final long expireSeconds,
                      final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slset(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slset(byte[], byte[], byte[], long, byte[][])
	 */
    public Long slset(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                      final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slset(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sldel(java.lang.String)
	 */
    public Long sldel(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sldel(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sldel(byte[])
	 */
    public Long sldel(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sldel(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slrem(java.lang.String, java.lang.String)
	 */
    public Long slrem(final String key, final String field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slrem(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slrem(byte[], byte[])
	 */
    public Long slrem(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slrem(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slrem(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long slrem(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slrem(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slrem(byte[], byte[], byte[])
	 */
    public Long slrem(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slrem(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slrem(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Long slrem(final String key, final String field, final String name, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slrem(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slrem(byte[], byte[], byte[], byte[][])
	 */
    public Long slrem(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slrem(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slcount(java.lang.String)
	 */
    public Long slcount(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slcount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slcount(byte[])
	 */
    public Long slcount(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slcount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slcount(java.lang.String, java.lang.String)
	 */
    public Long slcount(final String key, final String field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slcount(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slcount(byte[], byte[])
	 */
    public Long slcount(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slcount(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slcount(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long slcount(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slcount(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slcount(byte[], byte[], byte[])
	 */
    public Long slcount(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slcount(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slexists(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
    public Boolean slexists(final String key, final String field, final String name, final String value) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexists(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slexists(byte[], byte[], byte[], byte[])
	 */
    public Boolean slexists(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexists(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slexpire(java.lang.String, long)
	 */
    public Long slexpire(final String key, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slexpire(byte[], long)
	 */
    public Long slexpire(final byte[] key, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slexpire(java.lang.String, java.lang.String, long)
	 */
    public Long slexpire(final String key, final String field, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, field, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slexpire(byte[], byte[], long)
	 */
    public Long slexpire(final byte[] key, final byte[] field, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, field, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slexpire(java.lang.String, java.lang.String, java.lang.String, long)
	 */
    public Long slexpire(final String key, final String field, final String name, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, field, name, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slexpire(byte[], byte[], byte[], long)
	 */
    public Long slexpire(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, field, name, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slexpire(java.lang.String, java.lang.String, java.lang.String, java.lang.String, long)
	 */
    public Long slexpire(final String key, final String field, final String name, final String value,
                         final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, field, name, value, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slexpire(byte[], byte[], byte[], byte[], long)
	 */
    public Long slexpire(final byte[] key, final byte[] field, final byte[] name, final byte[] value,
                         final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slexpire(key, field, name, value, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slttl(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long slttl(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slttl(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slttl(byte[], byte[], byte[])
	 */
    public Long slttl(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slttl(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#slttl(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long slttl(final String key, final String field, final String name, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slttl(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#slttl(byte[], byte[], byte[], byte[])
	 */
    public Long slttl(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slttl(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    @Override
    public List<String> slvals(final String key, final String field) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slvals(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    @Override
    public List<byte[]> slvals(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.slvals(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssget(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Set<String> ssget(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssget(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssget(byte[], byte[], byte[])
	 */
    public Set<byte[]> ssget(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssget(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssmget(java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Map<String, Set<String>> ssmget(final String key, final String field, final String... names) {
        return this.execute(new RedisClusterCallback<Map<String, Set<String>>>() {
            public Map<String, Set<String>> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssmget(key, field, names);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssmget(byte[], byte[], byte[][])
	 */
    public Map<byte[], Set<byte[]>> ssmget(final byte[] key, final byte[] field, final byte[]... names) {
        return this.execute(new RedisClusterCallback<Map<byte[], Set<byte[]>>>() {
            public Map<byte[], Set<byte[]>> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssmget(key, field, names);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sskeys(java.lang.String)
	 */
    public Set<String> sskeys(final String key) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sskeys(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sskeys(byte[])
	 */
    public Set<byte[]> sskeys(final byte[] key) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sskeys(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sskeys(java.lang.String, java.lang.String)
	 */
    public Set<String> sskeys(final String key, final String field) {
        return this.execute(new RedisClusterCallback<Set<String>>() {
            public Set<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sskeys(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sskeys(byte[], byte[])
	 */
    public Set<byte[]> sskeys(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sskeys(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssadd(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Long ssadd(final String key, final String field, final String name, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssadd(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssadd(byte[], byte[], byte[], byte[][])
	 */
    public Long ssadd(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssadd(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssadd(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])
	 */
    public Long ssadd(final String key, final String field, final String name, final long expireSeconds,
                      final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssadd(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssadd(byte[], byte[], byte[], long, byte[][])
	 */
    public Long ssadd(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                      final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssadd(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long ssaddAt(final String key, final String field, final String name, final long millisecondsTimestamp,
                        final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssaddAt(key, field, name, millisecondsTimestamp, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Long ssaddAt(final byte[] key, final byte[] field, final byte[] name, final long millisecondsTimestamp,
                        final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssaddAt(key, field, name, millisecondsTimestamp, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssset(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Long ssset(final String key, final String field, final String name, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssset(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssset(byte[], byte[], byte[], byte[][])
	 */
    public Long ssset(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssset(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssset(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])
	 */
    public Long ssset(final String key, final String field, final String name, final long expireSeconds,
                      final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssset(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssset(byte[], byte[], byte[], long, byte[][])
	 */
    public Long ssset(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                      final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssset(key, field, name, expireSeconds, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssdel(java.lang.String)
	 */
    public Long ssdel(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssdel(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssdel(byte[])
	 */
    public Long ssdel(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssdel(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssrem(java.lang.String, java.lang.String)
	 */
    public Long ssrem(final String key, final String field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssrem(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssrem(byte[], byte[])
	 */
    public Long ssrem(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssrem(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssrem(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long ssrem(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssrem(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssrem(byte[], byte[], byte[])
	 */
    public Long ssrem(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssrem(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssrem(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
	 */
    public Long ssrem(final String key, final String field, final String name, final String... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssrem(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssrem(byte[], byte[], byte[], byte[][])
	 */
    public Long ssrem(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssrem(key, field, name, values);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sscount(java.lang.String)
	 */
    public Long sscount(final String key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sscount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sscount(byte[])
	 */
    public Long sscount(final byte[] key) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sscount(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sscount(java.lang.String, java.lang.String)
	 */
    public Long sscount(final String key, final String field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sscount(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sscount(byte[], byte[])
	 */
    public Long sscount(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sscount(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#sscount(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long sscount(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sscount(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#sscount(byte[], byte[], byte[])
	 */
    public Long sscount(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.sscount(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssexists(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
    public Boolean ssexists(final String key, final String field, final String name, final String value) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexists(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssexists(byte[], byte[], byte[], byte[])
	 */
    public Boolean ssexists(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.execute(new RedisClusterCallback<Boolean>() {
            public Boolean doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexists(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssexpire(java.lang.String, long)
	 */
    public Long ssexpire(final String key, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssexpire(byte[], long)
	 */
    public Long ssexpire(final byte[] key, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssexpire(java.lang.String, java.lang.String, long)
	 */
    public Long ssexpire(final String key, final String field, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, field, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssexpire(byte[], byte[], long)
	 */
    public Long ssexpire(final byte[] key, final byte[] field, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, field, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssexpire(java.lang.String, java.lang.String, java.lang.String, long)
	 */
    public Long ssexpire(final String key, final String field, final String name, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, field, name, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssexpire(byte[], byte[], byte[], long)
	 */
    public Long ssexpire(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, field, name, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssexpire(java.lang.String, java.lang.String, java.lang.String, java.lang.String, long)
	 */
    public Long ssexpire(final String key, final String field, final String name, final String value,
                         final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, field, name, value, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssexpire(byte[], byte[], byte[], byte[], long)
	 */
    public Long ssexpire(final byte[] key, final byte[] field, final byte[] name, final byte[] value,
                         final long expireSeconds) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssexpire(key, field, name, value, expireSeconds);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssttl(java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long ssttl(final String key, final String field, final String name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssttl(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssttl(byte[], byte[], byte[])
	 */
    public Long ssttl(final byte[] key, final byte[] field, final byte[] name) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssttl(key, field, name);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see TriplesRedisClusterCommands#ssttl(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
    public Long ssttl(final String key, final String field, final String name, final String value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssttl(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    /* 
	 * @see BinaryTriplesRedisClusterCommands#ssttl(byte[], byte[], byte[], byte[])
	 */
    public Long ssttl(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssttl(key, field, name, value);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    @Override
    public List<String> ssvals(final String key, final String field) {
        return this.execute(new RedisClusterCallback<List<String>>() {
            public List<String> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssvals(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    @Override
    public List<byte[]> ssvals(final byte[] key, final byte[] field) {
        return this.execute(new RedisClusterCallback<List<byte[]>>() {
            public List<byte[]> doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.ssvals(key, field);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public String info() {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.info();
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public String info(final String section) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.info(section);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Long dbSize() {
        return this.execute(new RedisClusterCallback<Long>() {
            public Long doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.dbSize();
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public byte[] dump(final String key) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.dump(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public byte[] dump(final byte[] key) {
        return this.execute(new RedisClusterCallback<byte[]>() {
            public byte[] doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.dump(key);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public String restore(final String key, final long ttl, final byte[] serializedValue) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.restore(key, ttl, serializedValue);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public String restore(final byte[] key, final long ttl, final byte[] serializedValue) {
        return this.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                return redisCluster.restore(key, ttl, serializedValue);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("gateway=").append(this.gateway).append(", ");
        sb.append("maxRetry=").append(this.maxRetry).append(", ");
        sb.append("backgroundPool=").append(this.backgroundPool);
        sb.append("}");
        return sb.toString();
    }
}
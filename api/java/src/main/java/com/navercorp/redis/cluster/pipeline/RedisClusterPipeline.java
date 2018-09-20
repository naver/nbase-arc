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
package com.navercorp.redis.cluster.pipeline;

import static com.navercorp.redis.cluster.connection.RedisProtocol.toByteArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.RedisClusterClient;
import com.navercorp.redis.cluster.connection.RedisProtocol.Command;
import com.navercorp.redis.cluster.gateway.AffinityState;
import com.navercorp.redis.cluster.gateway.Gateway;
import com.navercorp.redis.cluster.gateway.GatewayException;
import com.navercorp.redis.cluster.gateway.GatewayPartitionNumber;
import com.navercorp.redis.cluster.gateway.GatewayServer;
import com.navercorp.redis.cluster.gateway.RedisClusterCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.util.SafeEncoder;

/**
 * @author jaehong.kim
 */
public class RedisClusterPipeline extends Queable {
    private final Logger log = LoggerFactory.getLogger(RedisClusterPipeline.class);

    private final Object lock = new Object();
    private final Gateway gateway;
    private GatewayServer server;
    private RedisCluster redis;
    private RedisClusterClient client;
    private long defaultExpireSeconds = 0;
    private String keyspace;
    private boolean closed;
    private boolean brokenResource;
    private int timeoutMillisec = -1;

    public RedisClusterPipeline(final Gateway gateway) {
        this.gateway = gateway;
    }

    public void setTimeout(final int timeoutMillisec) {
        synchronized (lock) {
            this.timeoutMillisec = timeoutMillisec;
            if (this.client != null) {
                this.client.commitActiveTimeout(this.timeoutMillisec);
            }
        }
    }

    public void setServer(GatewayServer server) {
        this.server = server;
        this.redis = server.getResource();
        this.client = redis.getClient();
        this.keyspace = redis.getKeyspace();
        if (this.timeoutMillisec != -1) {
            this.client.commitActiveTimeout(this.timeoutMillisec);
        }
    }

    public GatewayServer getServer() {
        return this.server;
    }

    /**
     * Synchronize pipeline by reading all responses. This operation close the
     * pipeline. In order to get return values from pipelined commands, capture
     * the different Response&lt;?&gt; of the commands you execute.
     */
    public void sync() {
        if (this.client == null) {
            return;
        }

        List<Object> unformatted = null;
        try {
            unformatted = this.client.getAll();
        } catch (Exception e) {
            this.brokenResource = true;
            throw new GatewayException("gateway=" + this.server + ", connect=" + this.redis.connectInfo(), e);
        }

        if (unformatted == null) {
            return;
        }

        for (Object o : unformatted) {
            try {
                generateResponse(o);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Syncronize pipeline by reading all responses. This operation close the
     * pipeline. Whenever possible try to avoid using this version and use
     * Pipeline.sync() as it won't go through all the responses and generate the
     * right response type (usually it is a waste of time).
     *
     * @return A list of all the responses in the order you executed them.
     * @see sync
     */
    public List<Object> syncAndReturnAll() {
        final List<Object> formatted = new ArrayList<Object>();

        if (client == null) {
            return formatted;
        }

        List<Object> unformatted = null;
        try {
            unformatted = client.getAll();
        } catch (Exception e) {
            this.brokenResource = true;
            throw new GatewayException("gateway=" + this.server + ", connect=" + this.redis.connectInfo(), e);
        }

        if (unformatted == null) {
            return formatted;
        }

        for (Object o : unformatted) {
            try {
                formatted.add(generateResponse(o).get());
            } catch (JedisDataException e) {
                formatted.add(e);
            }
        }

        return formatted;
    }

    public void close() {
        if (this.closed) {
            return;
        }

        try {
            // input stream clear.
            sync();
        } catch(Exception ignored) {
        }

        this.closed = true;
        if (this.server != null && this.redis != null && this.client != null) {
            if (this.timeoutMillisec != -1) {
                // rollback timeout.
                try {
                    this.client.rollbackActiveTimeout();
                } catch (Exception e) {
                    this.brokenResource = true;
                    log.warn("[Pipeline] Failed to rollback timeout", e);
                }
            }
            if (this.brokenResource) {
                this.server.returnBrokenResource(redis);
            } else {
                this.server.returnResource(redis);
            }
        }
    }

    private <T> T executeCallback(final PipelineCallback<T> action) {
        try {
            if (server == null || redis == null || client == null) {
                initGatewayServer(action.getPartitionNumber(), action.getState());
            }

            final T result = action.doInPipeline();
            return result;
        } catch (Exception ex) {
            this.brokenResource = true;
            final StringBuilder sb = new StringBuilder();
            sb.append("pipeline execute error. ");
            if (server != null) {
                sb.append("gateway=").append(this.server);
            }
            if (redis != null) {
                sb.append(", connect=").append(redis.connectInfo());
            }

            throw new GatewayException(sb.toString(), ex);
        }
    }

    private void initGatewayServer(final int partitionNumber, final AffinityState state) {
        synchronized (lock) {
            if (server == null || redis == null || client == null) {
                final GatewayServer server = gateway.getServer(partitionNumber, state);
                setServer(server);
                this.client.allocPc(partitionNumber, state, true);
            }
        }
    }

    public Response<Long> del(final String... keys) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.del(keys);
                return getResponse(BuilderFactory.LONG);
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

    public Response<Long> del(final byte[]... keys) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.del(keys);
                return getResponse(BuilderFactory.LONG);
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

    public Response<Boolean> exists(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.exists(key);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> exists(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.exists(key);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> expire(final String key, final int seconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.expire(key, seconds);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> expire(final byte[] key, final int seconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.expire(key, seconds);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> expireAt(final String key, final long secondsTimestamp) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.expireAt(key, secondsTimestamp);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> expireAt(final byte[] key, final long secondsTimestamp) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.expireAt(key, secondsTimestamp);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> pexpire(final String key, final long milliseconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pexpire(key, milliseconds);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> pexpire(final byte[] key, final long milliseconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pexpire(key, milliseconds);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> pexpireAt(final String key, final long millisecondsTimestamp) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pexpireAt(key, millisecondsTimestamp);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pexpireAt(key, millisecondsTimestamp);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ttl(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ttl(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> ttl(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ttl(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> pttl(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pttl(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> pttl(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pttl(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> type(final String key) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.type(key);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> type(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.type(key);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> persist(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.persist(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> persist(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.persist(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Strings

    public Response<Long> append(final String key, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.append(key, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> append(final byte[] key, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.append(key, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> decr(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.decr(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> decr(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.decr(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> decrBy(final String key, final long integer) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.decrBy(key, integer);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> decrBy(final byte[] key, final long integer) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.decrBy(key, integer);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> get(final String key) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.get(key);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> get(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.get(key);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> getbit(final String key, final long offset) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.getbit(key, offset);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> getbit(final byte[] key, final long offset) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.getbit(key, offset);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> getrange(final String key, final long startOffset, final long endOffset) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.getrange(key, startOffset, endOffset);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> getrange(final byte[] key, final long startOffset, final long endOffset) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.getrange(key, startOffset, endOffset);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> substr(final String key, final int start, final int end) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.substr(key, start, end);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> substr(final byte[] key, final int start, final int end) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.substr(key, start, end);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> getSet(final String key, final String value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.getSet(key, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<byte[]> getSet(final byte[] key, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.getSet(key, value);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> incr(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.incr(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> incr(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.incr(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> incrBy(final String key, final long integer) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.incrBy(key, integer);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> incrBy(final byte[] key, final long integer) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.incrBy(key, integer);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Double> incrByFloat(final String key, final double increment) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.incrByFloat(key, increment);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Double> incrByFloat(final byte[] key, final double increment) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.incrByFloat(key, increment);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> set(final String key, final String value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.set(key, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> set(final String key, final String value, final String nxxx, final String expx, final long time) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.set(key, value, nxxx, expx, time);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }
    
    public Response<String> set(final byte[] key, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.set(key, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx, final long time) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.set(key, value, nxxx, expx, time);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

	public Response<String> set(final byte[]... args) {
		return this.executeCallback(new PipelineCallback<Response<String>>() {
			public Response<String> doInPipeline() {
				client.execute(Command.SET, args);
				return getResponse(BuilderFactory.STRING);
			}

			public int getPartitionNumber() {
				return GatewayPartitionNumber.get(args[0]);
			}

			public AffinityState getState() {
				return AffinityState.WRITE;
			}
		});
	}

    public Response<Boolean> setbit(final String key, final long offset, final boolean value) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.setbit(key, offset, value);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Boolean> setbit(final byte[] key, final long offset, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.setbit(key, offset, value);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> setex(final String key, final int seconds, final String value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.setex(key, seconds, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> setex(final byte[] key, final int seconds, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.setex(key, seconds, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> psetex(final String key, final long milliseconds, final String value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.psetex(key, milliseconds, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> psetex(final byte[] key, final long milliseconds, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.psetex(key, milliseconds, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> setnx(final String key, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.setnx(key, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> setnx(final byte[] key, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.setnx(key, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> setrange(final String key, final long offset, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.setrange(key, offset, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> setrange(final byte[] key, final long offset, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.setrange(key, offset, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> strlen(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.strlen(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> strlen(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.strlen(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<String>> mget(final String... keys) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.mget(keys);
                return getResponse(BuilderFactory.STRING_LIST);
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

    public Response<List<byte[]>> mget(final byte[]... keys) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.mget(keys);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
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

    public Response<String> mset(final String... keysvalues) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.mset(keysvalues);
                return getResponse(BuilderFactory.STRING);
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

    public Response<String> mset(final byte[]... keysvalues) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.mset(keysvalues);
                return getResponse(BuilderFactory.STRING);
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

    public Response<Long> bitcount(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.bitcount(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> bitcount(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.bitcount(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> bitcount(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.bitcount(key, start, end);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> bitcount(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.bitcount(key, start, end);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<Long>> bitfield(final String key, final String... arguments) {
        return this.executeCallback(new PipelineCallback<Response<List<Long>>>() {
            public Response<List<Long>> doInPipeline() {
                client.bitfield(key, arguments);
                return getResponse(BuilderFactory.LONG_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<Long>> bitfield(final byte[] key, final byte[]... arguments) {
        return this.executeCallback(new PipelineCallback<Response<List<Long>>>() {
            public Response<List<Long>> doInPipeline() {
                client.bitfield(key, arguments);
                return getResponse(BuilderFactory.LONG_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Hashes

    public Response<Long> hdel(final String key, final String... fields) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hdel(key, fields);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> hdel(final byte[] key, final byte[]... fields) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hdel(key, fields);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Boolean> hexists(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.hexists(key, field);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> hexists(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.hexists(key, field);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> hget(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.hget(key, field);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> hget(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.hget(key, field);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Map<String, String>> hgetAll(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Map<String, String>>>() {
            public Response<Map<String, String>> doInPipeline() {
                client.hgetAll(key);
                return getResponse(BuilderFactory.STRING_MAP);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Map<byte[], byte[]>> hgetAll(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Map<byte[], byte[]>>>() {
            public Response<Map<byte[], byte[]>> doInPipeline() {
                client.hgetAll(key);
                return getResponse(BuilderFactory.BYTE_ARRAY_MAP);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> hincrBy(final String key, final String field, final long value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hincrBy(key, field, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> hincrBy(final byte[] key, final byte[] field, final long value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hincrBy(key, field, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Double> hincrByFloat(final String key, final String field, final double increment) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.hincrByFloat(key, field, increment);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Double> hincrByFloat(final byte[] key, final byte[] field, final double increment) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.hincrByFloat(key, field, increment);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Set<String>> hkeys(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.hkeys(key);
                return getResponse(BuilderFactory.STRING_SET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> hkeys(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.hkeys(key);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> hlen(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hlen(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> hlen(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hlen(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<String>> hmget(final String key, final String... fields) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.hmget(key, fields);
                return getResponse(BuilderFactory.STRING_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<byte[]>> hmget(final byte[] key, final byte[]... fields) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.hmget(key, fields);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> hmset(final String key, final Map<String, String> hash) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.hmset(key, hash);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.hmset(key, hash);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> hset(final String key, final String field, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hset(key, field, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> hset(final byte[] key, final byte[] field, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hset(key, field, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> hsetnx(final String key, final String field, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hsetnx(key, field, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hsetnx(key, field, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<List<String>> hvals(final String key) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.hvals(key);
                return getResponse(BuilderFactory.STRING_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<byte[]>> hvals(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.hvals(key);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Lists

    public Response<String> lindex(final String key, final long index) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.lindex(key, index);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> lindex(final byte[] key, final long index) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.lindex(key, index);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.linsert(key, where, pivot, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.linsert(key, where, pivot, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> llen(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.llen(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> llen(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.llen(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> lpop(final String key) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.lpop(key);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<byte[]> lpop(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.lpop(key);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> lpush(final String key, final String... strings) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.lpush(key, strings);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> lpush(final byte[] key, final byte[]... strings) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.lpush(key, strings);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> lpushx(final String key, final String string) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.lpushx(key, string);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> lpushx(final byte[] key, final byte[] string) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.lpushx(key, string);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<List<String>> lrange(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.lrange(key, start, end);
                return getResponse(BuilderFactory.STRING_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<byte[]>> lrange(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.lrange(key, start, end);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> lrem(final String key, final long count, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.lrem(key, count, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> lrem(final byte[] key, final long count, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.lrem(key, count, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> lset(final String key, final long index, final String value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.lset(key, index, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> lset(final byte[] key, final long index, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.lset(key, index, value);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> ltrim(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.ltrim(key, start, end);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> ltrim(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.ltrim(key, start, end);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> rpop(final String key) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.rpop(key);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<byte[]> rpop(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.rpop(key);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> rpush(final String key, final String... strings) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.rpush(key, strings);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> rpush(final byte[] key, final byte[]... strings) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.rpush(key, strings);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> rpushx(final String key, final String string) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.rpushx(key, string);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> rpushx(final byte[] key, final byte[] string) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.rpushx(key, string);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    // Sets
    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Response<Long> sadd(final String key, final String... members) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sadd(key, members);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sadd(final byte[] key, final byte[]... members) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sadd(key, members);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> scard(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.scard(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> scard(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.scard(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> sismember(final String key, final String member) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.sismember(key, member);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> sismember(final byte[] key, final byte[] member) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.sismember(key, member);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> smembers(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.smembers(key);
                return getResponse(BuilderFactory.STRING_SET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> smembers(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.smembers(key);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> srandmember(final String key) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.srandmember(key);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> srandmember(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.srandmember(key);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> srandmember(final String key, final int count) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.srandmember(key, count);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> srandmember(final byte[] key, final int count) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.srandmember(key, count);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> srem(final String key, final String... members) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.srem(key, members);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> srem(final byte[] key, final byte[]... members) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.srem(key, members);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sorted Sets

    public Response<Long> zadd(final String key, final double score, final String member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zadd(key, score, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zadd(final byte[] key, final double score, final byte[] member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zadd(key, score, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zadd(final String key, final Map<Double, String> scoreMembers) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zadd(key, scoreMembers);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zadd(final byte[] key, final Map<Double, byte[]> scoreMembers) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zaddBinary(key, scoreMembers);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zadd2(final String key, final Map<String, Double> scoreMembers) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zadd2(key, scoreMembers);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zadd2(final byte[] key, final Map<byte[], Double> scoreMembers) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zaddBinary2(key, scoreMembers);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zcard(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zcard(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zcard(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zcard(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zcount(final String key, final double min, final double max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zcount(key, min, max);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zcount(final byte[] key, final double min, final double max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zcount(key, toByteArray(min), toByteArray(max));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zcount(final String key, final String min, final String max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zcount(key, min, max);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zcount(final byte[] key, final byte[] min, final byte[] max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zcount(key, min, max);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Double> zincrby(final String key, final double score, final String member) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.zincrby(key, score, member);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Double> zincrby(final byte[] key, final double score, final byte[] member) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.zincrby(key, score, member);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Set<String>> zrange(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrange(key, start, end);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrange(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrange(key, start, end);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrangeByScore(final String key, final double min, final double max) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrangeByScore(key, min, max);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrangeByScore(final byte[] key, final double min, final double max) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrangeByScore(key, toByteArray(min), toByteArray(max));
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrangeByScore(final String key, final String min, final String max) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrangeByScore(key, min, max);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrangeByScore(key, min, max);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrangeByScore(final String key, final double min, final double max, final int offset,
                                               final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrangeByScore(key, min, max, offset, count);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
                                               final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrangeByScore(key, toByteArray(min), toByteArray(max), offset, count);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrangeByScore(final String key, final String min, final String max, final int offset,
                                               final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrangeByScore(key, min, max, offset, count);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                               final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrangeByScore(key, min, max, offset, count);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeWithScores(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeWithScores(key, start, end);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeWithScores(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeWithScores(key, start, end);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, min, max);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, toByteArray(min), toByteArray(max));
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final String min, final String max) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, min, max);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, min, max);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max,
                                                        final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, min, max, offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final double min, final double max,
                                                        final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, toByteArray(min), toByteArray(max), offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final String min, final String max,
                                                        final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, min, max, offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max,
                                                        final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrangeByScoreWithScores(key, min, max, offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zrank(final String key, final String member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zrank(key, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zrank(final byte[] key, final byte[] member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zrank(key, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zrem(final String key, final String... members) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zrem(key, members);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zrem(final byte[] key, final byte[]... members) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zrem(key, members);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zremrangeByRank(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByRank(key, start, end);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zremrangeByRank(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByRank(key, start, end);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zremrangeByScore(final String key, final double start, final double end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByScore(key, start, end);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zremrangeByScore(final byte[] key, final double start, final double end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByScore(key, toByteArray(start), toByteArray(end));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zremrangeByScore(final String key, final String start, final String end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByScore(key, start, end);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByScore(key, start, end);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Set<String>> zrevrange(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrevrange(key, start, end);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrevrange(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrevrange(key, start, end);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeWithScores(final String key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeWithScores(key, start, end);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeWithScores(final byte[] key, final long start, final long end) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeWithScores(key, start, end);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrevrangeByScore(key, max, min);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrevrangeByScore(key, toByteArray(max), toByteArray(min));
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrevrangeByScore(final String key, final String max, final String min) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrevrangeByScore(key, max, min);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrevrangeByScore(key, max, min);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min,
                                                  final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrevrangeByScore(key, max, min, offset, count);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final double max, final double min,
                                                  final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrevrangeByScore(key, toByteArray(max), toByteArray(min), offset, count);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, max, min);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, toByteArray(max), toByteArray(min));
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min,
                                                           final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, max, min, offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min,
                                                           final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, toByteArray(max), toByteArray(min), offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final String max, final String min,
                                                           final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, max, min, offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min,
                                                           final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, max, min, offset, count);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrevrangeByScore(final String key, final String max, final String min,
                                                  final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrevrangeByScore(key, max, min, offset, count);
                return getResponse(BuilderFactory.STRING_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min,
                                                  final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrevrangeByScore(key, max, min, offset, count);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, max, min);
                return getResponse(BuilderFactory.TUPLE_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        return this.executeCallback(new PipelineCallback<Response<Set<Tuple>>>() {
            public Response<Set<Tuple>> doInPipeline() {
                client.zrevrangeByScoreWithScores(key, max, min);
                return getResponse(BuilderFactory.TUPLE_ZSET_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zrevrank(final String key, final String member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zrevrank(key, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zrevrank(final byte[] key, final byte[] member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zrevrank(key, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Double> zscore(final byte[] key, final byte[] member) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.zscore(key, member);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Double> zscore(final String key, final String member) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.zscore(key, member);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> ping() {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.ping();
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    // Triples
    public Response<List<String>> slget(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.slget(keyspace, key, field, name);
                return getResponse(BuilderFactory.STRING_LIST_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<byte[]>> slget(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.slget(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Map<String, List<String>>> slmget(final String key, final String field, final String... names) {
        return this.executeCallback(new PipelineCallback<Response<Map<String, List<String>>>>() {
            public Response<Map<String, List<String>>> doInPipeline() {
                client.slmget(keyspace, key, field, names);
                return getResponse(BuilderFactory.STRING_LIST_MAP);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Map<byte[], List<byte[]>>> slmget(final byte[] key, final byte[] field, final byte[]... names) {
        return this.executeCallback(new PipelineCallback<Response<Map<byte[], List<byte[]>>>>() {
            public Response<Map<byte[], List<byte[]>>> doInPipeline() {
                client.slmget(SafeEncoder.encode(keyspace), key, field, names);
                return getResponse(BuilderFactory.BYTE_LIST_MAP);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> slkeys(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.slkeys(keyspace, key);
                return getResponse(BuilderFactory.STRING_SET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> slkeys(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.slkeys(SafeEncoder.encode(keyspace), key);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> slkeys(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.slkeys(keyspace, key, field);
                return getResponse(BuilderFactory.STRING_SET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> slkeys(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.slkeys(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> sladd(final String key, final String field, final String name, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sladd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sladd(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sladd(SafeEncoder.encode(keyspace), key, field, name,
                        TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sladd(final String key, final String field, final String name, final long expireSeconds,
                                final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sladd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sladd(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                                final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sladd(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds),
                        values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sladdAt(final String key, final String field, final String name,
                                  final long millisecondsTimestamp, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sladdAt(keyspace, key, field, name, millisecondsTimestamp, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sladdAt(final byte[] key, final byte[] field, final byte[] name,
                                  final long millisecondsTimestamp, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sladdAt(SafeEncoder.encode(keyspace), key, field, name, millisecondsTimestamp, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slset(final String key, final String field, final String name, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slset(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slset(SafeEncoder.encode(keyspace), key, field, name,
                        TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slset(final String key, final String field, final String name, final long expireSeconds,
                                final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slset(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                                final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slset(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds),
                        values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sldel(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sldel(keyspace, key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sldel(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sldel(SafeEncoder.encode(keyspace), key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slrem(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slrem(keyspace, key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slrem(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slrem(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slrem(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slrem(keyspace, key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slrem(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slrem(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slrem(final String key, final String field, final String name, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slrem(keyspace, key, field, name, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slrem(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slrem(SafeEncoder.encode(keyspace), key, field, name, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slcount(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slcount(keyspace, key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slcount(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slcount(SafeEncoder.encode(keyspace), key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slcount(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slcount(keyspace, key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slcount(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slcount(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slcount(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slcount(keyspace, key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slcount(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slcount(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> slexists(final String key, final String field, final String name, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.slexists(keyspace, key, field, name, value);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> slexists(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.slexists(SafeEncoder.encode(keyspace), key, field, name, value);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slexpire(final String key, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(keyspace, key, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slexpire(final byte[] key, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(SafeEncoder.encode(keyspace), key, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slexpire(final String key, final String field, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(keyspace, key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slexpire(final byte[] key, final byte[] field, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(SafeEncoder.encode(keyspace), key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slexpire(final String key, final String field, final String name, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slexpire(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(SafeEncoder.encode(keyspace), key, field, name,
                        TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slexpire(final String key, final String field, final String name, final String value,
                                   final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(keyspace, key, field, name, value, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slexpire(final byte[] key, final byte[] field, final byte[] name, final byte[] value,
                                   final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slexpire(SafeEncoder.encode(keyspace), key, field, name, value,
                        TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> slttl(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slttl(keyspace, key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slttl(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slttl(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slttl(final String key, final String field, final String name, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slttl(keyspace, key, field, name, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> slttl(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.slttl(SafeEncoder.encode(keyspace), key, field, name, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<String>> slvals(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.slvals(keyspace, key, field);
                return getResponse(BuilderFactory.STRING_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<byte[]>> slvals(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.slvals(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }


    public Response<Set<String>> ssget(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.ssget(keyspace, key, field, name);
                return getResponse(BuilderFactory.STRING_SET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> ssget(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.ssget(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Map<String, Set<String>>> ssmget(final String key, final String field, final String... names) {
        return this.executeCallback(new PipelineCallback<Response<Map<String, Set<String>>>>() {
            public Response<Map<String, Set<String>>> doInPipeline() {
                client.ssmget(keyspace, key, field, names);
                return getResponse(BuilderFactory.STRING_SET_MAP);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Map<byte[], Set<byte[]>>> ssmget(final byte[] key, final byte[] field, final byte[]... names) {
        return this.executeCallback(new PipelineCallback<Response<Map<byte[], Set<byte[]>>>>() {
            public Response<Map<byte[], Set<byte[]>>> doInPipeline() {
                client.ssmget(SafeEncoder.encode(keyspace), key, field, names);
                return getResponse(BuilderFactory.BYTE_SET_MAP);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> sskeys(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.sskeys(keyspace, key);
                return getResponse(BuilderFactory.STRING_SET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> sskeys(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.sskeys(SafeEncoder.encode(keyspace), key);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> sskeys(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.sskeys(keyspace, key, field);
                return getResponse(BuilderFactory.STRING_SET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> sskeys(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.sskeys(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET_NOTNULL);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> ssadd(final String key, final String field, final String name, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssadd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssadd(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssadd(SafeEncoder.encode(keyspace), key, field, name,
                        TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssadd(final String key, final String field, final String name, final long expireSeconds,
                                final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssadd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssadd(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                                final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssadd(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds),
                        values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssaddAt(final String key, final String field, final String name,
                                  final long millisecondsTimestamp, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssaddAt(keyspace, key, field, name, millisecondsTimestamp, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssaddAt(final byte[] key, final byte[] field, final byte[] name,
                                  final long millisecondsTimestamp, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssaddAt(SafeEncoder.encode(keyspace), key, field, name, millisecondsTimestamp, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssset(final String key, final String field, final String name, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssset(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssset(SafeEncoder.encode(keyspace), key, field, name,
                        TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssset(final String key, final String field, final String name, final long expireSeconds,
                                final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssset(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds,
                                final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssset(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds),
                        values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssdel(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssdel(keyspace, key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssdel(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssdel(SafeEncoder.encode(keyspace), key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssrem(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssrem(keyspace, key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssrem(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssrem(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssrem(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssrem(keyspace, key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssrem(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssrem(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssrem(final String key, final String field, final String name, final String... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssrem(keyspace, key, field, name, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssrem(final byte[] key, final byte[] field, final byte[] name, final byte[]... values) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssrem(SafeEncoder.encode(keyspace), key, field, name, values);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> sscount(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sscount(keyspace, key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> sscount(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sscount(SafeEncoder.encode(keyspace), key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> sscount(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sscount(keyspace, key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> sscount(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sscount(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> sscount(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sscount(keyspace, key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> sscount(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.sscount(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> ssexists(final String key, final String field, final String name, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.ssexists(keyspace, key, field, name, value);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Boolean> ssexists(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Boolean>>() {
            public Response<Boolean> doInPipeline() {
                client.ssexists(SafeEncoder.encode(keyspace), key, field, name, value);
                return getResponse(BuilderFactory.BOOLEAN);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> ssexpire(final String key, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(keyspace, key, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssexpire(final byte[] key, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(SafeEncoder.encode(keyspace), key, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssexpire(final String key, final String field, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(keyspace, key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssexpire(final byte[] key, final byte[] field, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(SafeEncoder.encode(keyspace), key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssexpire(final String key, final String field, final String name, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssexpire(final byte[] key, final byte[] field, final byte[] name, final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(SafeEncoder.encode(keyspace), key, field, name,
                        TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssexpire(final String key, final String field, final String name, final String value,
                                   final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(keyspace, key, field, name, value, TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssexpire(final byte[] key, final byte[] field, final byte[] name, final byte[] value,
                                   final long expireSeconds) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssexpire(SafeEncoder.encode(keyspace), key, field, name, value,
                        TimeUnit.SECONDS.toMillis(expireSeconds));
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> ssttl(final String key, final String field, final String name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssttl(keyspace, key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> ssttl(final byte[] key, final byte[] field, final byte[] name) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssttl(SafeEncoder.encode(keyspace), key, field, name);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> ssttl(final String key, final String field, final String name, final String value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssttl(keyspace, key, field, name, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> ssttl(final byte[] key, final byte[] field, final byte[] name, final byte[] value) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.ssttl(SafeEncoder.encode(keyspace), key, field, name, value);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<String>> ssvals(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.ssvals(keyspace, key, field);
                return getResponse(BuilderFactory.STRING_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<byte[]>> ssvals(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.ssvals(SafeEncoder.encode(keyspace), key, field);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> dbSize() {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.dbSize();
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> info() {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.info();
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> info(final String section) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.info(section);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.NOT_MATCHED;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<byte[]> dump(final String key) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.dump(key);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> restore(final String key, final long ttl, final byte[] serializedValue) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.restore(key, ttl, serializedValue);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<String> restore(final byte[] key, final long ttl, final byte[] serializedValue) {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.restore(key, ttl, serializedValue);
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<byte[]> dump(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<byte[]>>() {
            public Response<byte[]> doInPipeline() {
                client.dump(key);
                return getResponse(BuilderFactory.BYTE_ARRAY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> geoadd(final String key, final double longitude, final double latitude, final String member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.geoadd(key, longitude, latitude, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> geoadd(final byte[] key, final double longitude, final double latitude, final byte[] member) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.geoadd(key, longitude, latitude, member);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> geoadd(final String key, final Map<String, GeoCoordinate> memberCoordinateMap) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.geoadd(key, memberCoordinateMap);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> geoadd(final byte[] key, final Map<byte[], GeoCoordinate> memberCoordinateMap) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.geoadd(key, memberCoordinateMap);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Double> geodist(final String key, final String member1, final String member2) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.geodist(key, member1, member2);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Double> geodist(final byte[] key, final byte[] member1, final byte[] member2) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.geodist(key, member1, member2);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Double> geodist(final String key, final String member1, final String member2, final GeoUnit unit) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.geodist(key, member1, member2, unit);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Double> geodist(final byte[] key, final byte[] member1, final byte[] member2, final GeoUnit unit) {
        return this.executeCallback(new PipelineCallback<Response<Double>>() {
            public Response<Double> doInPipeline() {
                client.geodist(key, member1, member2, unit);
                return getResponse(BuilderFactory.DOUBLE);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<String>> geohash(final String key, final String... members) {
        return this.executeCallback(new PipelineCallback<Response<List<String>>>() {
            public Response<List<String>> doInPipeline() {
                client.geohash(key, members);
                return getResponse(BuilderFactory.STRING_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<byte[]>> geohash(final byte[] key, final byte[]... members) {
        return this.executeCallback(new PipelineCallback<Response<List<byte[]>>>() {
            public Response<List<byte[]>> doInPipeline() {
                client.geohash(key, members);
                return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoCoordinate>> geopos(final String key, final String... members) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoCoordinate>>>() {
            public Response<List<GeoCoordinate>> doInPipeline() {
                client.geopos(key, members);
                return getResponse(BuilderFactory.GEO_COORDINATE_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoCoordinate>> geopos(final byte[] key, final byte[]... members) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoCoordinate>>>() {
            public Response<List<GeoCoordinate>> doInPipeline() {
                client.geopos(key, members);
                return getResponse(BuilderFactory.GEO_COORDINATE_LIST);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoRadiusResponse>> georadius(final String key, final double longitude, final double latitude,
            final double radius, final GeoUnit unit) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadius(key, longitude, latitude, radius, unit);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoRadiusResponse>> georadius(final byte[] key, final double longitude, final double latitude,
            final double radius, final GeoUnit unit) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadius(key, longitude, latitude, radius, unit);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoRadiusResponse>> georadius(final String key, final double longitude, final double latitude,
            final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadius(key, longitude, latitude, radius, unit, param);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoRadiusResponse>> georadius(final byte[] key, final double longitude, final double latitude,
            final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadius(key, longitude, latitude, radius, unit, param);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoRadiusResponse>> georadiusByMember(final String key, final String member,
            final double radius, final GeoUnit unit) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadiusByMember(key, member, radius, unit);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }
    
    public Response<List<GeoRadiusResponse>> georadiusByMember(final byte[] key, final byte[] member,
            final double radius, final GeoUnit unit) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadiusByMember(key, member, radius, unit);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoRadiusResponse>> georadiusByMember(final String key, final String member,
            final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadiusByMember(key, member, radius, unit, param);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<List<GeoRadiusResponse>> georadiusByMember(final byte[] key, final byte[] member,
            final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return this.executeCallback(new PipelineCallback<Response<List<GeoRadiusResponse>>>() {
            public Response<List<GeoRadiusResponse>> doInPipeline() {
                client.georadiusByMember(key, member, radius, unit, param);
                return getResponse(BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> pfadd(final String key, final String... elements) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pfadd(key, elements);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> pfadd(final byte[] key, final byte[]... elements) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pfadd(key, elements);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> pfcount(final String key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pfcount(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> pfcount(final byte[] key) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.pfcount(key);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> touch(final String... keys) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.touch(keys);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(keys[0]);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> touch(final byte[]... keys) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.touch(keys);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(keys[0]);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<String>> scan(final String cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<String>>>() {
            public Response<ScanResult<String>> doInPipeline() {
                client.scan(cursor, new ScanParams());
                return getResponse(BuilderFactory.SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get();
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<byte[]>> scan(final byte[] cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<byte[]>>>() {
            public Response<ScanResult<byte[]>> doInPipeline() {
                client.scan(cursor, new ScanParams());
                return getResponse(BuilderFactory.SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get();
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<String>> scan(final String cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<String>>>() {
            public Response<ScanResult<String>> doInPipeline() {
                client.scan(cursor, params);
                return getResponse(BuilderFactory.SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get();
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<byte[]>> scan(final byte[] cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<byte[]>>>() {
            public Response<ScanResult<byte[]>> doInPipeline() {
                client.scan(cursor, params);
                return getResponse(BuilderFactory.SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get();
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<String>> cscan(final int partitionID, final String cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<String>>>() {
            public Response<ScanResult<String>> doInPipeline() {
                client.cscan(partitionID, cursor);
                return getResponse(BuilderFactory.SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return partitionID;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<byte[]>> cscan(final int partitionID, final byte[] cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<byte[]>>>() {
            public Response<ScanResult<byte[]>> doInPipeline() {
                client.cscan(partitionID, cursor);
                return getResponse(BuilderFactory.SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return partitionID;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<String>> cscan(final int partitionID, final String cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<String>>>() {
            public Response<ScanResult<String>> doInPipeline() {
                client.cscan(partitionID, cursor, params);
                return getResponse(BuilderFactory.SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return partitionID;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<byte[]>> cscan(final int partitionID, final byte[] cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<byte[]>>>() {
            public Response<ScanResult<byte[]>> doInPipeline() {
                client.cscan(partitionID, cursor, params);
                return getResponse(BuilderFactory.SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return partitionID;
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<String> cscandigest() {
        return this.executeCallback(new PipelineCallback<Response<String>>() {
            public Response<String> doInPipeline() {
                client.cscandigest();
                return getResponse(BuilderFactory.STRING);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get();
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> cscanlen() {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.cscanlen();
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get();
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }
    
    public Response<ScanResult<String>> sscan(final String key, final String cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<String>>>() {
            public Response<ScanResult<String>> doInPipeline() {
                client.sscan(key, cursor, new ScanParams());
                return getResponse(BuilderFactory.SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }
    
    public Response<ScanResult<byte[]>> sscan(final byte[] key, final byte[] cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<byte[]>>>() {
            public Response<ScanResult<byte[]>> doInPipeline() {
                client.sscan(key, cursor, new ScanParams());
                return getResponse(BuilderFactory.SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<String>> sscan(final String key, final String cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<String>>>() {
            public Response<ScanResult<String>> doInPipeline() {
                client.sscan(key, cursor, params);
                return getResponse(BuilderFactory.SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<byte[]>> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<byte[]>>>() {
            public Response<ScanResult<byte[]>> doInPipeline() {
                client.sscan(key, cursor, params);
                return getResponse(BuilderFactory.SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<Map.Entry<String, String>>> hscan(final String key, final String cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Map.Entry<String, String>>>>() {
            public Response<ScanResult<Map.Entry<String, String>>> doInPipeline() {
                client.hscan(key, cursor, new ScanParams());
                return getResponse(BuilderFactory.MAP_SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<Map.Entry<byte[], byte[]>>> hscan(final byte[] key, final byte[] cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Map.Entry<byte[], byte[]>>>>() {
            public Response<ScanResult<Map.Entry<byte[], byte[]>>> doInPipeline() {
                client.hscan(key, cursor, new ScanParams());
                return getResponse(BuilderFactory.MAP_SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<Map.Entry<String, String>>> hscan(final String key, final String cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Map.Entry<String, String>>>>() {
            public Response<ScanResult<Map.Entry<String, String>>> doInPipeline() {
                client.hscan(key, cursor, params);
                return getResponse(BuilderFactory.MAP_SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<Map.Entry<byte[], byte[]>>> hscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Map.Entry<byte[], byte[]>>>>() {
            public Response<ScanResult<Map.Entry<byte[], byte[]>>> doInPipeline() {
                client.hscan(key, cursor, params);
                return getResponse(BuilderFactory.MAP_SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }



    public Response<ScanResult<Tuple>> zscan(final String key, final String cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Tuple>>>() {
            public Response<ScanResult<Tuple>> doInPipeline() {
                client.zscan(key, cursor, new ScanParams());
                return getResponse(BuilderFactory.TUPLE_SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<Tuple>> zscan(final byte[] key, final byte[] cursor) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Tuple>>>() {
            public Response<ScanResult<Tuple>> doInPipeline() {
                client.zscan(key, cursor, new ScanParams());
                return getResponse(BuilderFactory.TUPLE_SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<Tuple>> zscan(final String key, final String cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Tuple>>>() {
            public Response<ScanResult<Tuple>> doInPipeline() {
                client.zscan(key, cursor, params);
                return getResponse(BuilderFactory.TUPLE_SCAN_RESULT);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<ScanResult<Tuple>> zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return this.executeCallback(new PipelineCallback<Response<ScanResult<Tuple>>>() {
            public Response<ScanResult<Tuple>> doInPipeline() {
                client.zscan(key, cursor, params);
                return getResponse(BuilderFactory.TUPLE_SCAN_RESULT_BINARY);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }
    
    public Response<Long> hstrlen(final String key, final String field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hstrlen(key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }
    
    public Response<Long> hstrlen(final byte[] key, final byte[] field) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.hstrlen(key, field);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zlexcount(final String key, final String min, final String max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zlexcount(key, min, max);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zlexcount(final byte[] key, final byte[] min, final byte[] max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zlexcount(key, min, max);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrangeByLex(final String key, final String min, final String max) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrangeByLex(key, min, max);
                return getResponse(BuilderFactory.STRING_SET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrangeByLex(key, min, max);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrangeByLex(final String key, final String min, final String max, final int offset,
            final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrangeByLex(key, min, max, offset, count);
                return getResponse(BuilderFactory.STRING_SET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrangeByLex(final byte[] key, final byte[] min, final byte[] max, final int offset,
            final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrangeByLex(key, min, max, offset, count);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrevrangeByLex(final String key, final String max, final String min) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrevrangeByLex(key, max, min);
                return getResponse(BuilderFactory.STRING_SET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrevrangeByLex(key, max, min);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<String>> zrevrangeByLex(final String key, final String max, final String min, final int offset,
            final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<String>>>() {
            public Response<Set<String>> doInPipeline() {
                client.zrevrangeByLex(key, max, min, offset, count);
                return getResponse(BuilderFactory.STRING_SET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Set<byte[]>> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min, final int offset, final int count) {
        return this.executeCallback(new PipelineCallback<Response<Set<byte[]>>>() {
            public Response<Set<byte[]>> doInPipeline() {
                client.zrevrangeByLex(key, max, min, offset, count);
                return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.READ;
            }
        });
    }

    public Response<Long> zremrangeByLex(final String key, final String min, final String max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByLex(key, min, max);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }

    public Response<Long> zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return this.executeCallback(new PipelineCallback<Response<Long>>() {
            public Response<Long> doInPipeline() {
                client.zremrangeByLex(key, min, max);
                return getResponse(BuilderFactory.LONG);
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get(key);
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });
    }
    
    private interface PipelineCallback<T> {
        public T doInPipeline();

        public int getPartitionNumber();

        public AffinityState getState();
    }
}

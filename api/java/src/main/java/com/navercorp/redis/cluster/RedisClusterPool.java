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

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.redis.cluster.connection.RedisProtocol;

/**
 * The Class RedisClusterPool.
 *
 * @author seongminwoo
 */
public class RedisClusterPool extends Pool<RedisCluster> {

    /**
     * The log.
     */
    private long timeout;

    /**
     * Return the number of instances currently borrowed from this pool.
     *
     * @return the number of instances currently borrowed from this pool
     */
    public int getNumActive() {
        return internalPool.getNumActive();
    }

    public int getNumIdle() {
        return internalPool.getNumIdle();
    }

    public long getMaxWait() {
        return internalPool.getMaxBorrowWaitTimeMillis();
    }

    public long getTimeout() {
        return this.timeout;
    }

    public int getMaxTotal() {
        return this.internalPool.getMaxTotal();
    }


    /**
     * Instantiates a new redis cluster pool.
     *
     * @param host the host
     * @param port the port
     */
    public RedisClusterPool(String host, int port) {
        this(new RedisClusterPoolConfig(), host, port, RedisProtocol.DEFAULT_TIMEOUT);
    }

    /**
     * Instantiates a new redis cluster pool.
     *
     * @param poolConfig the pool config
     * @param host       the host
     * @param port       the port
     */
    public RedisClusterPool(final RedisClusterPoolConfig poolConfig, final String host, final int port) {
        this(poolConfig, host, port, RedisProtocol.DEFAULT_TIMEOUT);
    }

    public RedisClusterPool(final RedisClusterPoolConfig poolConfig, final String host, int port, int timeout) {
        this(poolConfig, host, port, timeout, null);
    }

    /**
     * Instantiates a new redis cluster pool.
     *
     * @param poolConfig the pool config
     * @param host       the host
     * @param port       the port
     * @param timeout    the timeout
     * @param password   the password
     */
    public RedisClusterPool(final RedisClusterPoolConfig poolConfig, final String host, int port, int timeout,
                            final String keyspace) {
        super(poolConfig, new RedisClusterFactory(host, port, timeout, keyspace));
        this.timeout = timeout;
    }

    public void clear() {
        internalPool.clear();
    }

    public void stop() {
        internalPool.setTimeBetweenEvictionRunsMillis(-1);
        internalPool.setMaxTotal(0);
        internalPool.setMaxIdle(0);
        internalPool.setMinIdle(0);
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class RedisClusterFactory implements PooledObjectFactory<RedisCluster> {

        /**
         * The log.
         */
        private final Logger log = LoggerFactory.getLogger(RedisClusterFactory.class);

        /**
         * The host.
         */
        private final String host;

        /**
         * The port.
         */
        private final int port;

        /**
         * The timeout.
         */
        private final int timeout;

        private final String keyspace;

        /**
         * Instantiates a new redis cluster factory.
         *
         * @param host     the host
         * @param port     the port
         * @param timeout  the timeout
         * @param password the password
         */
        public RedisClusterFactory(final String host, final int port, final int timeout, final String keyspace) {
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            this.keyspace = keyspace;
        }

        @Override
        public void activateObject(PooledObject<RedisCluster> p) throws Exception {
        }

        @Override
        public PooledObject<RedisCluster> makeObject() throws Exception {
            final RedisCluster redis = new RedisCluster(this.host, this.port, this.timeout);
            if (this.keyspace != null) {
                redis.setKeyspace(this.keyspace);
            }
            redis.connect();
            return new DefaultPooledObject<RedisCluster>(redis);
        }

        @Override
        public void destroyObject(PooledObject<RedisCluster> p) throws Exception {
            final RedisCluster redisCluster = p.getObject();
            if (redisCluster.isConnected()) {
                try {
                    redisCluster.disconnect();
                } catch (Exception e) {
                    log.warn("[RedisClusterFactory] Failed to destroy connection", e);
                }
            }
        }

        @Override
        public boolean validateObject(PooledObject<RedisCluster> p) {
            final RedisCluster redisCluster = p.getObject();
            try {
                if (!redisCluster.isConnected()) {
                    return false;
                }

                final String response = redisCluster.ping();
                if (response != null && response.equals("PONG")) {
                    return true;
                }

                return false;
            } catch (final Exception e) {
                log.warn("[RedisClusterFactory] Failed to validate connection " + redisCluster, e);
                return false;
            }
        }

        @Override
        public void passivateObject(PooledObject<RedisCluster> p) throws Exception {
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("host=").append(host).append(", ");
            sb.append("post=").append(port).append(", ");
            sb.append("timeout=").append(timeout);
            sb.append("}");

            return sb.toString();
        }

    }
}
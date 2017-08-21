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

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.nbasearc.gcp.GatewayConnectionPool;
import com.navercorp.nbasearc.gcp.VirtualConnection;
import com.navercorp.redis.cluster.connection.RedisProtocol;

/**
 * The Class RedisClusterPool.
 *
 */
public class RedisClusterPoolAsync extends Pool<RedisCluster> {

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
     * @param poolConfig the pool config
     * @param host       the host
     * @param port       the port
     * @param timeout    the timeout
     * @param keyspace   the keyspace
     * @param gcp        the GatewayConnectionPool 
     */
    public RedisClusterPoolAsync(final RedisClusterPoolConfig poolConfig, final String host, int port, int timeout,
                            final String keyspace, final GatewayConnectionPool gcp) {
        super(poolConfig, new RedisClusterFactory(host, port, timeout, keyspace, gcp));
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

        private final GatewayConnectionPool gcp;
        
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
        public RedisClusterFactory(final String host, final int port, final int timeout, final String keyspace, final GatewayConnectionPool gcp) {
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            this.keyspace = keyspace;
            this.gcp = gcp;
        }

        @Override
        public void activateObject(PooledObject<RedisCluster> p) throws Exception {
        }

        @Override
        public PooledObject<RedisCluster> makeObject() throws Exception {
            final RedisCluster redis = new RedisCluster(host, port, timeout, true);
            if (this.keyspace != null) {
                redis.setKeyspace(this.keyspace);
            }
            redis.getConnection().setVc(gcp.newVc(512));
            
            return new DefaultPooledObject<RedisCluster>(redis);
        }

        @Override
        public void destroyObject(PooledObject<RedisCluster> p) throws Exception {
            final RedisCluster redisCluster = p.getObject();
            redisCluster.disconnect();
        }

        @Override
        public boolean validateObject(PooledObject<RedisCluster> p) {
            return true;
        }

        @Override
        public void passivateObject(PooledObject<RedisCluster> p) throws Exception {
            final RedisCluster redisCluster = p.getObject();
            redisCluster.passivate();
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
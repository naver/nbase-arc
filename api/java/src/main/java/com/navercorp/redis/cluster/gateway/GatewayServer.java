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

import com.navercorp.nbasearc.gcp.GatewayConnectionPool;
import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.RedisClusterPoolConfig;

/**
 * The Class GatewayServer.
 *
 * @author jaehong.kim
 */
public class GatewayServer {
    private GatewayServerImpl impl;

    /**
     * The Constant DEFAULT_POOL_CONFIG.
     */
    private static final RedisClusterPoolConfig DEFAULT_POOL_CONFIG = new RedisClusterPoolConfig();

    /**
     * The Constant DEFAULT_TIMEOUT_MILLISEC.
     */
    private static final int DEFAULT_TIMEOUT_MILLISEC = 1000;
    
    /**
     * Instantiates a new gateway server.
     *
     * @param address the address
     */
    public GatewayServer(final GatewayAddress address) {
        this(address, DEFAULT_POOL_CONFIG, DEFAULT_TIMEOUT_MILLISEC, null);
    }

    /**
     * Instantiates a new gateway server.
     *
     * @param address         the address
     * @param poolConfig      the pool config
     * @param timeoutMillisec the timeout millisec
     */
    public GatewayServer(final GatewayAddress address, final RedisClusterPoolConfig poolConfig,
                         final int timeoutMillisec, final String keyspace) {
        impl = new GatewayServerSync(address, poolConfig, timeoutMillisec, keyspace);
    }

    public GatewayServer(final GatewayAddress address, final RedisClusterPoolConfig poolConfig,
            final int timeoutMillisec, final String keyspace, final GatewayConnectionPool gcp) {
        impl = new GatewayServerAsync(address, poolConfig, timeoutMillisec, keyspace, gcp);
    }

    public int preload(final long syncTimeUnitMillis, final long connectPerDelayMillis,
                       final RedisClusterPoolConfig poolConfig) {
        return impl.preload(syncTimeUnitMillis, connectPerDelayMillis, poolConfig);
    }

    public RedisCluster getResource() {
        return impl.getResource();
    }

    public void returnResource(final RedisCluster redis) {
        impl.returnResource(redis);
    }

    public void returnBrokenResource(final RedisCluster redis) {
        impl.returnBrokenResource(redis);
    }

    /**
     * Ping.
     *
     * @return true, if successful
     * @throws HealthCheckException the health check exception
     */
    public boolean ping() {
        return impl.ping();
    }

    public void flush() {
        impl.flush();
    }

    public void close() {
        impl.close();
    }

    public void destroy() {
        impl.destroy();
    }

    /**
     * Gets the address.
     *
     * @return the address
     */
    public GatewayAddress getAddress() {
        return impl.getAddress();
    }

    public boolean hasActiveConnection() {
        return impl.hasActiveConnection();
    }

    public boolean isFullConnection() {
        return impl.isFullConnection();
    }

    public long getMaxWait() {
        return impl.getMaxWait();
    }

    public long getTimeout() {
        return impl.getTimeout();
    }

    /**
     * Sets the valid.
     *
     * @param valid the new valid
     */
    public void setValid(final boolean valid) {
        impl.setValid(valid);
    }

    /**
     * Checks if is valid.
     *
     * @return true, if is valid
     */
    public boolean isValid() {
        return impl.isValid();
    }

    public void setExist(final boolean exist) {
        impl.setExist(exist);
    }

    public boolean isExist() {
        return impl.isExist();
    }

    /*
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return impl.toString();
    }
}
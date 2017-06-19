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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.navercorp.nbasearc.gcp.GatewayConnectionPool;
import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.RedisClusterPoolAsync;
import com.navercorp.redis.cluster.RedisClusterPoolConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisException;

/**
 * The Class GatewayServer.
 *
 * @author seunghoo.han
 */
class GatewayServerAsync implements GatewayServerImpl {
    private static final String CLIENT_SYNC_KEY_PREFIX = "##@@*25419530;CLIENT_SYNC;^##@&&*$#;620278919530;";

    /**
     * The log.
     */
    private final Logger log = LoggerFactory.getLogger(GatewayServerAsync.class);

    /**
     * The Constant DEFAULT_POOL_CONFIG.
     */
    private static final RedisClusterPoolConfig DEFAULT_POOL_CONFIG = new RedisClusterPoolConfig();

    /**
     * The Constant DEFAULT_TIMEOUT_MILLISEC.
     */
    private static final int DEFAULT_TIMEOUT_MILLISEC = 1000;

    /**
     * The address.
     */
    private GatewayAddress address;

    /**
     * The pool.
     */
    private RedisClusterPoolAsync pool;

    /**
     * The valid.
     */
    private final AtomicBoolean valid = new AtomicBoolean(true);

    /**
     * The exist.
     */
    private final AtomicBoolean exist = new AtomicBoolean(true);

    /**
     * Instantiates a new gateway server.
     *
     * @param address the address
     */

    public GatewayServerAsync(GatewayAddress address) {
        throw new UnsupportedOperationException();
    }

    public GatewayServerAsync(GatewayAddress address, RedisClusterPoolConfig poolConfig, int timeoutMillisec,
            String keyspace) {
        throw new UnsupportedOperationException();
    }
    
    public GatewayServerAsync(GatewayAddress address, RedisClusterPoolConfig poolConfig, int timeoutMillisec,
            String keyspace, GatewayConnectionPool gcp) {
        this.address = address;
        this.pool = new RedisClusterPoolAsync(
                poolConfig, address.getHost(), address.getPort(), timeoutMillisec, keyspace, gcp);
    }

    public int preload(final long syncTimeUnitMillis, final long connectPerDelayMillis,
                       final RedisClusterPoolConfig poolConfig) {
        return 0;
    }

    public RedisCluster getResource() {
        return this.pool.getResource();
    }

    public void returnResource(final RedisCluster redis) {
        if (redis == null) {
            return;
        }

        try {
            pool.returnResource(redis);
        } catch (JedisException e) {
            log.error("[GatewayServer] Failed to return resource", e);
        }
    }

    public void returnBrokenResource(final RedisCluster redis) {
        if (redis == null) {
            return;
        }

        try {
            pool.returnBrokenResource(redis);
        } catch (JedisException e) {
            log.error("[GatewayServer] Failed to return resource", e);
        }
    }

    /**
     * Ping.
     *
     * @return true, if successful
     * @throws HealthCheckException the health check exception
     */
    public boolean ping() {
        try {
            return ping(getResource());
        } catch (Exception e) {
            log.error("[GatewayServer] Failed to health check " + toString(), e);
        }

        return false;
    }

    private boolean ping(final RedisCluster redis) {
        boolean success = false;

        try {
            final String result = redis.ping();
            if (result != null && result.equals("PONG")) {
                success = true;
            }
            if (success) {
                returnResource(redis);
            } else {
                returnBrokenResource(redis);
            }
        } catch (Exception ex) {
            log.error("[GatewayServer] Failed to health check " + toString(), ex);
            returnBrokenResource(redis);
        }

        return success;
    }

    long sync() {
        long number = 0;
        RedisCluster redis = null;
        final String key = CLIENT_SYNC_KEY_PREFIX + address.toString()
                + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
        try {
            redis = getResource();

            number = redis.incr(key);
            redis.expire(key, 60);

            returnResource(redis);
        } catch (Exception ex) {
            log.error("[GatewayServer] Failed to health check " + toString(), ex);
            returnBrokenResource(redis);
        }

        return number;
    }

    public void flush() {
        final int count = pool.getNumIdle();
        pool.clear();

        log.debug("[GatewayServer] Flush connection {} to {}", count, pool.getNumIdle());
    }

    public void close() {
        pool.stop();
        pool.clear();
    }

    public void destroy() {
        try {
            pool.destroy();
        } catch (Exception e) {
            log.error("[GatewayServer] Failed to destory. " + toString(), e);
        }
    }

    /**
     * Gets the address.
     *
     * @return the address
     */
    public GatewayAddress getAddress() {
        return address;
    }

    public boolean hasActiveConnection() {
        return this.pool.getNumActive() > 0;
    }

    public boolean isFullConnection() {
        return this.pool.getMaxTotal() == -1 ? false : this.pool.getMaxTotal() == this.pool.getNumActive();
    }

    public long getMaxWait() {
        return this.pool.getMaxWait();
    }

    public long getTimeout() {
        return this.pool.getTimeout();
    }

    /**
     * Sets the valid.
     *
     * @param valid the new valid
     */
    public void setValid(final boolean valid) {
        this.valid.set(valid);
        if (!this.valid.get()) {
            flush();
        }
    }

    /**
     * Checks if is valid.
     *
     * @return true, if is valid
     */
    public boolean isValid() {
        return valid.get();
    }

    public void setExist(final boolean exist) {
        this.exist.set(exist);
    }

    public boolean isExist() {
        return exist.get();
    }

    /*
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("address=").append(this.address).append(", ");
        if (this.pool != null) {
            sb.append("conn.active=").append(this.pool.getNumActive()).append(", ");
            sb.append("conn.idle=").append(this.pool.getNumIdle()).append(", ");
        }
        sb.append("valid=").append(this.valid).append(", ");
        sb.append("exist=").append(this.exist);
        sb.append("}");

        return sb.toString();
    }
}
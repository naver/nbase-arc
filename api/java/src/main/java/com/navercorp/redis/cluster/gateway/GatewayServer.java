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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.RedisClusterPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisException;

import com.navercorp.redis.cluster.RedisClusterPool;

/**
 * The Class GatewayServer.
 *
 * @author jaehong.kim
 */
public class GatewayServer {
    private static final String CLIENT_SYNC_KEY_PREFIX = "##@@*25419530;CLIENT_SYNC;^##@&&*$#;620278919530;";

    /**
     * The log.
     */
    private final Logger log = LoggerFactory.getLogger(GatewayServer.class);

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
    private final GatewayAddress address;

    /**
     * The pool.
     */
    private final RedisClusterPool pool;

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
        this.address = address;
        this.pool = new RedisClusterPool(poolConfig, address.getHost(), address.getPort(), timeoutMillisec, keyspace);
    }

    public int preload(final long syncTimeUnitMillis, final long connectPerDelayMillis,
                       final RedisClusterPoolConfig poolConfig) {
        if (syncTimeUnitMillis > 0) {
            try {
                final long syncNumber = sync();
                log.debug("[GatewayServer] Preload sync number {}", syncNumber);
                if (syncNumber > 0) {
                    log.info("[GatewayServer] Preload sync {}ms", syncNumber * syncTimeUnitMillis);
                    TimeUnit.MILLISECONDS.sleep(syncNumber * syncTimeUnitMillis);
                }
            } catch (Exception e) {
                log.warn("[GatewayServer] Failed to preload sync", e);
            }
        }

        // If initialSize > 0, preload the pool
        List<RedisCluster> preloadPool = new ArrayList<RedisCluster>();
        int max = poolConfig.getInitialSize();
        if (max > poolConfig.getMaxTotal()) {
            max = poolConfig.getMaxTotal();
        }

        for (int i = 0; i < max; i++) {
            try {
                if (connectPerDelayMillis > 0) {
                    TimeUnit.MILLISECONDS.sleep(connectPerDelayMillis);
                }
                preloadPool.add(getResource());
            } catch (Exception e) {
                log.warn("[GatewayServer] Failed to connect on preload", e);
            }
        }

        int count = 0;
        for (RedisCluster redis : preloadPool) {
            try {
                log.info("[GatewayServer] Preload connect {}", redis.connectInfo());
                if (ping(redis)) {
                    count++;
                }
            } catch (Exception e) {
                log.error("[GatewayServer] Failed to preload return resource " + redis, e);
            }
        }

        return count;
    }

    public RedisCluster getResource() {
        return this.pool.getResource();
    }

    public void returnResource(final RedisCluster redis) {
        if (redis == null) {
            return;
        }

        try {
            if (isValid()) {
                pool.returnResource(redis);
            } else {
                returnBrokenResource(redis);
            }
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
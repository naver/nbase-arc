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

import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.RedisClusterPoolConfig;

interface GatewayServerImpl {
    public int preload(final long syncTimeUnitMillis, final long connectPerDelayMillis,
            final RedisClusterPoolConfig poolConfig);

    public RedisCluster getResource();

    public void returnResource(final RedisCluster redis);

    public void returnBrokenResource(final RedisCluster redis);

    public boolean ping();

    public void flush();

    public void close();

    public void destroy();

    public GatewayAddress getAddress();

    public boolean hasActiveConnection();

    public boolean isFullConnection();

    public long getMaxWait();

    public long getTimeout();

    public void setValid(final boolean valid);

    public boolean isValid();

    public void setExist(final boolean exist);

    public boolean isExist();
}
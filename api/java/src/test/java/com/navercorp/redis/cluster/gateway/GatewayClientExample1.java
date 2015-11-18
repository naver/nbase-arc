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

import com.navercorp.redis.cluster.RedisClusterPoolConfig;
import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author jaehong.kim
 */
public class GatewayClientExample1 {

    public void usage() {
        final GatewayConfig config = new GatewayConfig();
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());

        final GatewayClient client = new GatewayClient(config);
        client.set("name", "clark kent");
        String name = client.get("name");

        System.out.println(name);
        client.destroy();
    }

    public void config() throws Exception {
        final GatewayConfig config = new GatewayConfig();
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());
        config.setTimeoutMillisec(1000);
        config.setHealthCheckUsed(true);
        config.setHealthCheckThreadSize(3);
        config.setHealthCheckPeriodSeconds(10);
        config.setGatewaySelectorMethod("round-robin");

        RedisClusterPoolConfig poolConfig = new RedisClusterPoolConfig();
        poolConfig.setInitialSize(8);
        poolConfig.setMaxActive(8);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(0);
        poolConfig.setMaxWait(1000);
        poolConfig.setMinEvictableIdleTimeMillis(-1);
        poolConfig.setNumTestsPerEvictionRun(2);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(300000);
        config.setPoolConfig(poolConfig);

        final GatewayClient client = new GatewayClient(config);
        client.set("foo", "bar");
        client.destroy();
    }
}

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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.navercorp.redis.cluster.RedisClusterPoolConfig;
import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author jaehong.kim
 */
public class GatewayClientTestMain {
    private static final String KEY = "tmain_key01";
    private static final String VAL = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    public void start() throws Exception {
        GatewayConfig config = new GatewayConfig();
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());
        config.setHealthCheckPeriodSeconds(10);
        config.setHealthCheckThreadSize(3);
        config.setHealthCheckUsed(true);
        config.setTimeoutMillisec(1000);

        RedisClusterPoolConfig poolConfig = new RedisClusterPoolConfig();
        poolConfig.setMaxActive(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMaxWait(1000);
        poolConfig.setMinEvictableIdleTimeMillis(1000);
        poolConfig.setMinIdle(3);
        poolConfig.setNumTestsPerEvictionRun(-1);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        config.setPoolConfig(poolConfig);

        final GatewayClient client = new GatewayClient(config);

        Runnable operation = new Runnable() {
            public void run() {
                client.setex(KEY, 1, VAL);
                final String value = client.get(KEY);
                if (value == null || !value.equals(VAL)) {
                    System.out.println("Invalid value " + value);
                }
            }
        };

        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.scheduleAtFixedRate(operation, 0, 100, TimeUnit.MILLISECONDS);

        while (true) {
            TimeUnit.SECONDS.sleep(10);
        }
    }

    public static void main(String[] args) throws Exception {
        GatewayClientTestMain main = new GatewayClientTestMain();
        main.start();
    }
}

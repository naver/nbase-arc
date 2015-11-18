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
import java.util.concurrent.atomic.AtomicInteger;

import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author jaehong.kim
 */
public class GatewayTestMain {

    public void failover() throws Exception {
        final AtomicInteger total = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();

        GatewayConfig config = new GatewayConfig();
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());
        config.setHealthCheckUsed(false);

        final Gateway manager = new Gateway(config);
        Runnable operation = new Runnable() {
            public void run() {
                try {
                    GatewayServer server = manager.getServer(-1, AffinityState.READ);
                    RedisCluster redis = server.getResource();
                    try {
                        redis.get("foo");
                    } catch (Exception e) {
                        failed.incrementAndGet();
                    } finally {
                        server.returnResource(redis);
                    }
                } catch (Exception e) {
                    failed.incrementAndGet();
                    e.printStackTrace();
                }
                total.incrementAndGet();
            }
        };

        Runnable flush = new Runnable() {

            @Override
            public void run() {
                GatewayServer server = manager.getServer(-1, AffinityState.READ);
                server.flush();
            }
        };

        ScheduledExecutorService operationScheduler = Executors.newSingleThreadScheduledExecutor();
        operationScheduler.scheduleAtFixedRate(operation, 0, 100, TimeUnit.MILLISECONDS);

        ScheduledExecutorService flushScheduler = Executors.newSingleThreadScheduledExecutor();
        flushScheduler.scheduleAtFixedRate(flush, 0, 1000, TimeUnit.MILLISECONDS);

        TimeUnit.SECONDS.sleep(99999999);

        // invalid.
        manager.getServer(-1, AffinityState.READ).setValid(false);
        TimeUnit.SECONDS.sleep(10000);

        // invalid.
        for (GatewayServer server : manager.getServers()) {
            server.setValid(false);
        }
        TimeUnit.SECONDS.sleep(10000);

        for (int i = 0; i < 100; i++) {
            manager.getServer(-1, AffinityState.READ).setValid(false);
            TimeUnit.MILLISECONDS.sleep(100);
        }
        TimeUnit.SECONDS.sleep(100000);

        operationScheduler.shutdownNow();
        flushScheduler.shutdownNow();
        manager.destroy();

        System.out.println("Total: " + total.get());
        System.out.println("Failed: " + failed.get());
    }

    public static void main(String[] args) throws Exception {
        GatewayTestMain main = new GatewayTestMain();
        main.failover();
    }
}

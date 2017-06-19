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

import static org.junit.Assert.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.navercorp.redis.cluster.util.TestEnvUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.navercorp.redis.cluster.RedisClusterPoolConfig;

/**
 * @author jaehong.kim
 */
public class GatewayServerTest {

    @Ignore
    @Test
    public void instance() {
        GatewayServer server = new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()));
        assertEquals(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort(), server.getAddress().getName());
        assertEquals(true, server.isValid());
    }

    @Ignore
    @Test
    public void ping() {
        GatewayServer server01 = new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()));
        assertEquals(true, server01.ping());

        // expected Exception.
        GatewayServer server02 = new GatewayServer(new GatewayAddress(1, "1.1.1.1:1111"));
        assertEquals(false, server02.ping());
    }

    @Ignore
    @Test
    public void sync() throws Exception {
        int max = 10;
        ExecutorService threadPool = Executors.newFixedThreadPool(max);
        final CountDownLatch latch = new CountDownLatch(max);
        final CyclicBarrier barrier = new CyclicBarrier(max + 1);

        for (int i = 0; i < max; i++) {
            final Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        barrier.await();
                    } catch (InterruptedException e) {
                    } catch (BrokenBarrierException e) {
                    }

                    GatewayServer server01 = new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()));

                    RedisClusterPoolConfig poolConfig = new RedisClusterPoolConfig();
                    int number = server01.preload(100, 100, poolConfig);

                    System.out.println(number);
                    latch.countDown();
                }
            };
            threadPool.execute(runnable);
        }

        barrier.await(10, TimeUnit.SECONDS);
        latch.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void isFullConnection() {
        GatewayServer server01 = new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()));
        System.out.println(server01);

        for (int i = 0; i < 8; i++) {
            server01.getResource();
            System.out.println(server01);
        }

        System.out.println(server01.isFullConnection());
    }

}


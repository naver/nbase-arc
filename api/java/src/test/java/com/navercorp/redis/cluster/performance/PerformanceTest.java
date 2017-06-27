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

package com.navercorp.redis.cluster.performance;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.redis.cluster.async.AsyncAction;
import com.navercorp.redis.cluster.async.AsyncResult;
import com.navercorp.redis.cluster.async.AsyncResultHandler;
import com.navercorp.redis.cluster.connection.RedisProtocol.Keyword;
import com.navercorp.redis.cluster.gateway.GatewayClient;
import com.navercorp.redis.cluster.gateway.GatewayConfig;
import com.navercorp.redis.cluster.pipeline.RedisClusterPipeline;
import com.navercorp.redis.cluster.util.TestEnvUtils;


/**
 * @author seongminwoo
 */
public class PerformanceTest {
    /**
     *
     */
    private static final String KEY_PREFIX = "performance_test";
    private static final int THREAD_SIZE = 8;
    private static final int DATA_SIZE = 64; //byte
    private static final int POOL_SIZE = 60;
    private static final long RUNNING_MS = 86400 * 1000;
    final static long END_TTIME_MS = System.currentTimeMillis() + RUNNING_MS;

    private static final Logger log = LoggerFactory.getLogger(PerformanceTest.class);

    int runCount = 100;
    GatewayClient client;
    ExecutorService threadPool;
    CountDownLatch latch = new CountDownLatch(THREAD_SIZE);
    byte[] data = new byte[DATA_SIZE];

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        GatewayConfig config = new GatewayConfig();
        config.getPoolConfig().setMaxTotal(POOL_SIZE);
        config.getPoolConfig().setMaxActive(POOL_SIZE);
        config.getPoolConfig().setMaxIdle(POOL_SIZE);
        config.setZkAddress(TestEnvUtils.getZkAddress());
        config.setClusterName(TestEnvUtils.getClusterName());
        client = new GatewayClient(config);

        threadPool = Executors.newFixedThreadPool(THREAD_SIZE);
        Arrays.fill(data, (byte) 1);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        threadPool.shutdown();
        client.destroy();
    }

    private void doRead() {
        byte[] result = client.get((KEY_PREFIX + Thread.currentThread().getId()).getBytes());

        if (!Arrays.equals(data, result)) {
            throw new IllegalStateException("data error!");
        }
    }
    
    private void doWrite() {
        client.set((KEY_PREFIX + Thread.currentThread().getId()).getBytes(), data);
    }
    
    private void doReadWriteWithRatio(float readRatio) {
        int readRatioPercent = (int) (readRatio * 100);
        int randomNumber = new Random().nextInt(100);

        if (randomNumber <= readRatioPercent) {
            doRead();
        } else {
            doWrite();
        }
    }
    
    @Ignore
    @Test
    public void sync() throws InterruptedException {
        for (int i = 0; i < THREAD_SIZE; i++) {
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    doWrite();
                    while (System.currentTimeMillis() < END_TTIME_MS) {
                        doReadWriteWithRatio(0.8f);
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
    }
    
    
    
    private void asyncAction() {
        if (System.currentTimeMillis() > END_TTIME_MS) {
            latch.countDown();
            return;
        }
        
        AsyncResultHandler<String> handler = new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> result) {
                assertFalse(result.toString(), result.isFailed());
                asyncAction();
            }
        };
        
        new AsyncAction<String>(client, handler) {
            public String action() throws Exception {
                client.set("foo" + Thread.currentThread().getId(), "bar" + Thread.currentThread().getId());
                client.del("foo" + Thread.currentThread().getId());
                return client.get("foo" + Thread.currentThread().getId());
            }
        }.run();
    }
    
    @Ignore
    @Test
    public void async() throws InterruptedException {
        for (int i = 0; i < 60; i++) {
            asyncAction();
        }
        latch.await();
    }
    
    @Ignore
    @Test
    public void pipeline() throws InterruptedException {
        for (int i = 0; i < THREAD_SIZE; i++) {
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    RedisClusterPipeline pipeline = client.pipeline();
                    while (System.currentTimeMillis() < END_TTIME_MS) {
                        final int MAX = 3;
                        for (int i = 0; i < MAX; i++) {
                            pipeline.set(KEY_PREFIX + Thread.currentThread().getId(), String.valueOf(Thread.currentThread().getId()));
                            pipeline.get(KEY_PREFIX + Thread.currentThread().getId());
                            pipeline.del(KEY_PREFIX + Thread.currentThread().getId());
                        }
                        List<Object> results = pipeline.syncAndReturnAll();
                        assertEquals(results.size(), 3 * MAX);
                        for (int i = 0; i < MAX; i++) {
                            assertEquals(results.get(0), "OK");
                            assertEquals(results.get(1), String.valueOf(Thread.currentThread().getId()));
                            assertEquals(results.get(2), 1L);
                        }
                        pipeline.sync();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
    }
    
    @Ignore
    @Test
    public void cafe2() throws InterruptedException {
        for (int i = 0; i < THREAD_SIZE; i++) {
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    Random r = new Random(System.currentTimeMillis());
                    while (true) {
                        try {
                            String k = "arc_" + r.nextInt();
                            for (int i = 0; i < 100; i++) {
                                client.zadd(k, i, "value" + i);
                            }
                            client.zrevrangeByScore(k, 0, 100);
                            client.zrevrangeByScoreWithScores(k, 0, 100, 0, 20);
                            for (int i = 0; i < 100; i++) {
                                client.zrem(k, "value" + i);
                            }
                            client.objectRefcount(k);
                            client.del(k);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        Thread.sleep(86400 * 1000);
    }
}


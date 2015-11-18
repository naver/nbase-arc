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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.navercorp.redis.cluster.gateway.GatewayClient;
import com.navercorp.redis.cluster.gateway.GatewayConfig;
import com.navercorp.redis.cluster.util.TestEnvUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author seongminwoo
 */
public class PerformanceTestZookeeperMain {
    /**
     *
     */
    private static final String TEST_KEY = "performance_test";
    private static final int THREAD_SIZE = 300;
    private static final int DATA_SIZE = 64; //byte
    private static final int POOL_SIZE = 60;

    private static final Logger log = LoggerFactory.getLogger(PerformanceTestZookeeperMain.class);

    int runCount = 100;
    GatewayClient client;
    ExecutorService threadPool;
    byte[] data = new byte[DATA_SIZE];

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        GatewayConfig config = new GatewayConfig();
        config.getPoolConfig().setMaxActive(POOL_SIZE);
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

    private void doWrite() {
        client.set(TEST_KEY.getBytes(), data);
    }

    /**
     * write operation(set) 100%
     *
     * @throws InterruptedException
     */
    @Test
    public void writeAll() throws InterruptedException {
        long start = System.currentTimeMillis();

        final AtomicInteger operationCounter = new AtomicInteger(0);
        final AtomicInteger errorCounter = new AtomicInteger(0);

        final CountDownLatch latch = new CountDownLatch(runCount);

        for (int i = 0; i < runCount; i++) {
            threadPool.execute(new Runnable() {
                public void run() {
                    try {
                        doWrite();
                        operationCounter.incrementAndGet();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        errorCounter.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        long end = System.currentTimeMillis();

        System.out.println("operationCounter : " + operationCounter);
        System.out.println("errorCounter : " + errorCounter);
        System.out.println("running time : " + (end - start) + "ms");
        System.out.println("TPS : " + operationCounter.get() * 1000 / (end - start));
    }

    private void doRead() {
        byte[] result = client.get(TEST_KEY.getBytes());

        if (!Arrays.equals(data, result)) {
            throw new IllegalStateException("data error!");
        }
    }

    /**
     * read operation(get) 100%
     *
     * @throws InterruptedException
     */
    @Test
    public void readAll() throws InterruptedException {
        long start = System.currentTimeMillis();

        final AtomicInteger operationCounter = new AtomicInteger(0);
        final AtomicInteger errorCounter = new AtomicInteger(0);

        final CountDownLatch latch = new CountDownLatch(runCount);

        for (int i = 0; i < runCount; i++) {
            threadPool.execute(new Runnable() {
                public void run() {
                    try {
                        doRead();
                        operationCounter.incrementAndGet();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        errorCounter.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        long end = System.currentTimeMillis();

        System.out.println("operationCounter : " + operationCounter);
        System.out.println("errorCounter : " + errorCounter);
        System.out.println("elapsed time : " + (end - start) + "ms");
        long tps = operationCounter.get() * 1000 / (end - start);
        float responseTime = 1.0f / tps;
        System.out.println("TPS : " + tps);
        System.out.println("responseTime : " + responseTime);
    }

    /**
     * read(get) 80%, write(set) 20%
     *
     * @throws InterruptedException
     */
    @Test
    public void lineSenario() throws InterruptedException {
        long start = System.currentTimeMillis();

        final AtomicInteger operationCounter = new AtomicInteger(0);
        final AtomicInteger errorCounter = new AtomicInteger(0);

        final CountDownLatch latch = new CountDownLatch(runCount);

        for (int i = 0; i < runCount; i++) {
            threadPool.execute(new Runnable() {
                public void run() {
                    try {
                        doReadWriteWithRatio(0.8f); //read 80%, write20%
                        operationCounter.incrementAndGet();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        errorCounter.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        long end = System.currentTimeMillis();

        System.out.println("operationCounter : " + operationCounter);
        System.out.println("errorCounter : " + errorCounter);
        System.out.println("elapsed time : " + (end - start) + "ms");
        long tps = operationCounter.get() * 1000 / (end - start);
        float responseTime = 1.0f / tps;
        System.out.println("TPS : " + tps);
        System.out.println("responseTime : " + responseTime);
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

    /**
     * read(get) 80%, write(set) 20%
     *
     * @throws InterruptedException
     */
    @Ignore
    @Test
    public void lineSenarioPeriod() throws InterruptedException {
        doWrite();

        long start = System.currentTimeMillis();

        final AtomicLong operationCounter = new AtomicLong(0);
        final AtomicLong errorCounter = new AtomicLong(0);


        while (true) {
            threadPool.execute(new Runnable() {
                public void run() {
                    try {
                        doReadWriteWithRatio(0.8f); //read 80%, write20%
                        operationCounter.incrementAndGet();
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                        errorCounter.incrementAndGet();
                    } finally {
                    }
                }
            });

            TimeUnit.MICROSECONDS.sleep(1);

            if (System.currentTimeMillis() - start > 1 * 24 * 3600 * 1000) { // 1 days
                break;
            }
        }

        long end = System.currentTimeMillis();

        log.info("operationCounter : " + operationCounter);
        log.info("errorCounter : " + errorCounter);
        log.info("elapsed time : " + (end - start) + "ms");
        long tps = operationCounter.get() * 1000 / (end - start);
        float responseTime = 1.0f / tps;
        log.info("TPS : " + tps);
        log.info("responseTime : " + responseTime);
    }
}

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

package com.navercorp.redis.cluster;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author jaehong.kim
 */
public class RedisClusterPoolTestMain {

    public void suddenDestory() throws Exception {
        final RedisClusterPool pool = new RedisClusterPool(TestEnvUtils.getHost(), TestEnvUtils.getPort());
        Runnable operation = new Runnable() {
            public void run() {
                try {
                    RedisCluster redis = pool.getResource();
                    try {
                        System.out.println(redis.get("foo"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        pool.returnResource(redis);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.scheduleAtFixedRate(operation, 0, 1000, TimeUnit.MILLISECONDS);

        TimeUnit.SECONDS.sleep(3);
        pool.destroy();

        TimeUnit.SECONDS.sleep(3);

        ses.shutdownNow();
    }

    public void checkPool() throws Exception {
        RedisClusterPoolConfig config = new RedisClusterPoolConfig();
        config.setInitialSize(60);
        config.setMaxActive(60);
        config.setMaxIdle(60);
        config.setMaxWait(1000);

        long startTime = System.currentTimeMillis();
        final AtomicLong count = new AtomicLong(0);
        final RedisClusterPool pool = new RedisClusterPool(config, TestEnvUtils.getHost(), TestEnvUtils.getPort());
        Runnable operation = new Runnable() {
            public void run() {
                while (true) {
                    try {
                        RedisCluster redis = null;
                        long startTime = System.currentTimeMillis();
                        redis = pool.getResource();
                        try {
                            pool.returnResource(redis);
                            count.incrementAndGet();
                        } catch (Exception e) {
                            if (redis != null) {
                                pool.returnBrokenResource(redis);
                            }
                            e.printStackTrace();
                        }
                        if (System.currentTimeMillis() - startTime >= 1000) {
                            System.out.println("TIME OUT");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        ExecutorService ses = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 20; i++) {
            ses.execute(operation);
        }

        while (true) {
            long runTime = System.currentTimeMillis() - startTime;
            if (runTime > TimeUnit.HOURS.toMillis(1)) {
                break;
            }
            System.out.println("TPS: " + (count.get() / (float) runTime) * 1000);
            TimeUnit.SECONDS.sleep(10);
        }

        pool.destroy();

        TimeUnit.SECONDS.sleep(3);

        ses.shutdownNow();

    }

    public void checkPoolCreateEverytime() throws Exception {
        long startTime = System.currentTimeMillis();
        final AtomicLong count = new AtomicLong(0);
        final AtomicBoolean isTerminate = new AtomicBoolean(false);

        Runnable operation = new Runnable() {
            public void run() {
                RedisClusterPool pool = null;

                while (true) {
                    try {
                        final int initialSize = 60;
                        RedisClusterPoolConfig config = new RedisClusterPoolConfig();
                        config.setInitialSize(initialSize);
                        config.setMaxActive(initialSize);
                        config.setMaxIdle(initialSize);
                        config.setMaxWait(1000);
                        pool = new RedisClusterPool(config, TestEnvUtils.getHost(), TestEnvUtils.getPort());
                        RedisCluster redis = null;
                        long startTime = System.currentTimeMillis();
                        redis = pool.getResource();


                        try {
                            redis.set("test1", "test1");
                            pool.returnResource(redis);
                            count.incrementAndGet();
                        } catch (Exception e) {
                            if (redis != null) {
                                pool.returnBrokenResource(redis);
                            }
                            e.printStackTrace();
                            isTerminate.set(true);
                            return;
                        }
                        if (System.currentTimeMillis() - startTime >= 1000) {
                            System.out.println("TIME OUT");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    } finally {
                        if (pool != null) {
                            pool.destroy();
                        }
                    }
                }
            }
        };

        final int size = 20;
        ExecutorService ses = Executors.newFixedThreadPool(size);
        for (int i = 0; i < size; i++) {
            ses.execute(operation);
        }

        while (true) {
            long runTime = System.currentTimeMillis() - startTime;
            if (runTime > TimeUnit.HOURS.toMillis(1) || isTerminate.get()) {
                break;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("TOTAL: ").append(count.get()).append(",TPS: ").append((count.get() * 1000 / runTime));
            System.out.println(sb.toString());
            TimeUnit.SECONDS.sleep(10);
        }

        TimeUnit.SECONDS.sleep(3);

        ses.shutdownNow();

    }

    public static void main(String[] args) throws Exception {
        RedisClusterPoolTestMain main = new RedisClusterPoolTestMain();
        // main.suddenDestory();
        //main.checkPool();
        main.checkPoolCreateEverytime();
    }
}

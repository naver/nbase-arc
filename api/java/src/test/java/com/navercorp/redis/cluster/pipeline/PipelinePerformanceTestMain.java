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

package com.navercorp.redis.cluster.pipeline;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.navercorp.redis.cluster.async.AsyncAction;
import com.navercorp.redis.cluster.gateway.GatewayClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.async.AsyncResult;
import com.navercorp.redis.cluster.async.AsyncResultHandler;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-pipeline.xml")
public class PipelinePerformanceTestMain {
    private static final int COUNT = 1000;
    private static final String PIPELINE_KEY = "pipeline_incr_perf";
    private static final String NORMAL_KEY = "normal_incr_perf";
    private static final String ASYNC_KEY = "async_incr_perf";
    private static final String ASYNC_AND_PIPELINE_KEY = "async_pipeline_incr_perf";


    @Autowired
    GatewayClient gatewayClient;

    @Before
    public void before() {
        gatewayClient.del(PIPELINE_KEY);
        gatewayClient.del(NORMAL_KEY);
        gatewayClient.del(ASYNC_KEY);
        gatewayClient.del(ASYNC_AND_PIPELINE_KEY);
    }

    @Test
    public void performance() throws Exception {
        normal(COUNT);
        async(COUNT);
        pipeline(COUNT);
        asyncAndPipeline(COUNT);
    }

    private void pipeline(final int count) {
        RedisClusterPipeline pipeline = gatewayClient.pipeline();
        pipeline.setTimeout(0);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            pipeline.incr(PIPELINE_KEY);
        }
        List<Object> result = pipeline.syncAndReturnAll();
        pipeline.close();
        assertEquals(count, result.size());

        long time = System.currentTimeMillis() - startTime;
        System.out.println("pipeline count=" + count + ", time=" + time);

    }

    private void normal(final int count) {
        long startTime = System.currentTimeMillis();
        long result = 0;
        for (int i = 0; i < count; i++) {
            result = gatewayClient.incr(NORMAL_KEY);
        }
        assertEquals(count, result);

        long time = System.currentTimeMillis() - startTime;
        System.out.println("serial count=" + count + ", time=" + time);
    }

    private void async(final int count) throws Exception {
        long startTime = System.currentTimeMillis();
        final CountDownLatch latch1 = new CountDownLatch(1);
        AsyncResultHandler<Long> handler = new AsyncResultHandler<Long>() {
            public void handle(AsyncResult<Long> result) {
                if (result.getResult() == count) {
                    latch1.countDown();
                }
            }
        };

        for (int i = 0; i < count; i++) {
            new AsyncAction<Long>(gatewayClient, handler) {
                public Long action() throws Exception {
                    return gatewayClient.incr(ASYNC_KEY);
                }
            }.run();
        }
        latch1.await();
        long time = System.currentTimeMillis() - startTime;
        System.out.println("async count=" + count + ", time=" + time);
    }


    private void asyncAndPipeline(final int count) throws Exception {
        long startTime = System.currentTimeMillis();
        final CountDownLatch latch1 = new CountDownLatch(1);
        AsyncResultHandler<Long> handler = new AsyncResultHandler<Long>() {
            public void handle(AsyncResult<Long> result) {
                if (result.getResult() == count) {
                    latch1.countDown();
                }
            }
        };

        for (int i = 0; i < count / 1000; i++) {
            new AsyncAction<Long>(gatewayClient, handler) {
                public Long action() throws Exception {
                    RedisClusterPipeline pipeline = gatewayClient.pipeline();
                    pipeline.setTimeout(0);
                    for (int task = 0; task < 1000; task++) {
                        pipeline.incr(ASYNC_AND_PIPELINE_KEY);
                    }
                    List<Object> result = pipeline.syncAndReturnAll();
                    pipeline.close();

                    return (Long) result.get(result.size() - 1);
                }
            }.run();
        }
        latch1.await();
        long time = System.currentTimeMillis() - startTime;
        System.out.println("async&pipeline count=" + count + ", time=" + time);
    }
}


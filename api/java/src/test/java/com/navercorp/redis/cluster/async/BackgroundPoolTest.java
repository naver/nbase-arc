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

package com.navercorp.redis.cluster.async;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class BackgroundPoolTest {

    @Test
    public void action() throws Exception {
        BackgroundPool pool = new BackgroundPool(1);

        final CountDownLatch latch = new CountDownLatch(1);
        AsyncResultHandler<String> handler = new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> result) {
                System.out.println(result.getResult());
                latch.countDown();
            }
        };

        pool.getPool().execute(new Runnable() {
            public void run() {
                latch.countDown();
            }
        });
        latch.await();

        pool.shutdown();
    }
}


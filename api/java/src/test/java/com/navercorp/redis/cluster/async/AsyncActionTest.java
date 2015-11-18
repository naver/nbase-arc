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

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;

import com.navercorp.redis.cluster.gateway.GatewayClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-gatewayclient.xml")
public class AsyncActionTest {

    @Autowired
    GatewayClient gatewayClient;

    @Test
    public void operations() throws Exception {
        // not found handler
        final CountDownLatch latch0 = new CountDownLatch(1);
        new AsyncAction<Void>(gatewayClient) {
            public Void action() throws Exception {
                gatewayClient.del("test_asyncaction_key");
                gatewayClient.incr("test_asyncaction_key");
                gatewayClient.incr("test_asyncaction_key");
                gatewayClient.incr("test_asyncaction_key");
                gatewayClient.incr("test_asyncaction_key");
                long value = gatewayClient.incr("test_asyncaction_key");
                assertEquals(5L, value);
                latch0.countDown();
                return null;
            }
        }.run();
        latch0.await();

        // handler
        final CountDownLatch latch1 = new CountDownLatch(1);
        AsyncResultHandler<String> handler = new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> result) {
                assertEquals("bar", result.getResult());
                latch1.countDown();
            }
        };

        new AsyncAction<String>(gatewayClient, handler) {
            public String action() throws Exception {
                gatewayClient.del("foo");
                gatewayClient.set("foo", "bar");
                return gatewayClient.get("foo");
            }
        }.run();
        latch1.await();
    }
}

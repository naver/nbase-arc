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

package com.navercorp.redis.cluster.example;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.async.AsyncAction;
import com.navercorp.redis.cluster.async.AsyncResult;
import com.navercorp.redis.cluster.async.AsyncResultHandler;
import com.navercorp.redis.cluster.gateway.GatewayClient;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-gatewayclient.xml")
public class RedisClusterAsyncExample1 {

    @Autowired
    GatewayClient client;

    @Before
    public void before() {
        client.del("foo");
    }

    @Test
    public void async() {
        AsyncResultHandler<String> handler = new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> result) {
                if (result.isFailed()) {
                    System.out.println(result.getCause());
                } else {
                    System.out.println(result.getResult());
                }
            }
        };

        new AsyncAction<String>(client, handler) {
            public String action() throws Exception {
                client.del("foo");
                client.set("foo", "bar");
                return client.get("foo");
            }
        }.run();
    }
}

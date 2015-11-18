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

import java.util.List;

import com.navercorp.redis.cluster.pipeline.RedisClusterPipeline;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.gateway.GatewayClient;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-gatewayclient.xml")
public class RedisClusterPipelineExample1 {

    @Autowired
    GatewayClient gatewayClient;

    @Before
    public void before() {
        gatewayClient.del("foo");
    }

    @Test
    public void pipeline() {
        RedisClusterPipeline pipeline = gatewayClient.pipeline();
        try {
            pipeline.incr("foo");
            pipeline.incr("foo");
            pipeline.incr("foo");
            pipeline.incr("foo");
            List<Object> result = pipeline.syncAndReturnAll();
            System.out.println(result);
        } finally {
            pipeline.close();
        }
    }
}

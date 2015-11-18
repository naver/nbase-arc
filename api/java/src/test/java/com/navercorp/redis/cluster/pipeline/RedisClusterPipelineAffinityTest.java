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


import com.navercorp.redis.cluster.gateway.GatewayClient;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-affinity.xml")
public class RedisClusterPipelineAffinityTest {

    @Autowired
    GatewayClient client;

    @Ignore
    @Test
    public void pipeline() {

        RedisClusterPipeline pipeline = null;

        final String key = "pipelin_affinity_key";
        try {
            pipeline = client.pipeline();
            pipeline.del(key);

            for (int i = 0; i < 100; i++) {
                pipeline.incr(key);
            }

            pipeline.info();

            for (Object result : pipeline.syncAndReturnAll()) {
                System.out.println(result);
            }
        } finally {
            if (pipeline != null) {
                pipeline.close();
            }
        }
    }

    @Test
    public void info() {
        System.out.println(client.info());
    }


}

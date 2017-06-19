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

import java.util.List;

import com.navercorp.redis.cluster.util.TestEnvUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class GatewayClientExample2 {

    private GatewayClient client;

    @Before
    public void before() {
        final GatewayConfig config = new GatewayConfig();
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());

        this.client = new GatewayClient(config);
    }

    @After
    public void after() {
        this.client.destroy();
    }

    @Test
    public void value() {
        client.set("name", "clark kent");
        String result = client.get("name");

        System.out.println(result);
    }

    @Test
    public void hash() {
        client.hset("superhero", "phantom", "dc");
        client.hset("superhero", "superman", "dc");
        client.hset("superhero", "batman", "dc");
        client.hset("superhero", "iron man", "marvel");

        String value = client.hget("superhero", "superman");
        System.out.println(value);
    }

    @Test
    public void sessionOfHashList() throws Exception {
        client.sladd("superman", "review", "netizen", 10, "good");
        client.sladd("superman", "review", "netizen", 10, "wonderful");
        client.sladd("superman", "review", "netizen", 10, "bad");
        client.sladd("superman", "review", "netizen", 10, "awesome");
        client.sladd("superman", "review", "netizen", 10, "good");
        client.sladd("superman", "review", "netizen", 10, "bad");

        List<String> result = client.slget("superman", "review", "netizen");
        System.out.println(result);
    }
}


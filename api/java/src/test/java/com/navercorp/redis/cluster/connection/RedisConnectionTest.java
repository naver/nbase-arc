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

package com.navercorp.redis.cluster.connection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author jaehong.kim
 */
public class RedisConnectionTest {

    private RedisConnection client;

    @Before
    public void setUp() throws Exception {
        client = new RedisConnection();
    }

    @After
    public void tearDown() throws Exception {
        client.disconnect();
    }

    @Test(expected = JedisConnectionException.class)
    public void checkUnkownHost() {
        client.setHost("someunknownhost");
        client.connect();
    }

    @Test(expected = JedisConnectionException.class)
    public void checkWrongPort() {
        client.setHost("localhost");
        client.setPort(55665);
        client.connect();
    }
}

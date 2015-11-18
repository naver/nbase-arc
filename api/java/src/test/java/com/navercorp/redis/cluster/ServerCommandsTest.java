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

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class ServerCommandsTest extends RedisClusterTestBase {

    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_BKEY_0);
    }

    @Test
    public void ping() {
        String status = redis.ping();
        assertEquals("PONG", status);
    }

    @Test
    public void dbSize() {
        long beforeSize = redis.dbSize();
        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        long afterSize = redis.dbSize();
        assertNotEquals(beforeSize, afterSize);
    }

    @Test
    public void info() {
        System.out.println(redis.info());
        System.out.println(redis.info("keyspace"));
    }
}

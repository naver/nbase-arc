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

import redis.clients.util.SafeEncoder;

/**
 * @author jaehong.kim
 */
public class ObjectCommandsTest extends RedisClusterTestBase {
    private String key = "mylist";
    private byte[] binaryKey = SafeEncoder.encode(key);

    @Override
    public void clear() {
        redis.del(key);
    }

    @Test
    public void objectRefcount() {
        redis.lpush(key, "hello world");
        Long refcount = redis.objectRefcount(key);
        assertEquals(new Long(1), refcount);

        // Binary
        refcount = redis.objectRefcount(binaryKey);
        assertEquals(new Long(1), refcount);

    }

    @Test
    public void objectEncoding() {
        redis.lpush(key, "hello world");
        String encoding = redis.objectEncoding(key);
        assertEquals("quicklist", encoding);

        // Binary
        encoding = SafeEncoder.encode(redis.objectEncoding(binaryKey));
        assertEquals("quicklist", encoding);
    }

    @Test
    public void objectIdletime() throws InterruptedException {
        redis.lpush(key, "hello world");

        // Wait a little bit more than 10 seconds so the idle time is 10
        // seconds.
        Thread.sleep(10001);
        Long time = redis.objectIdletime(key);
        assertEquals(new Long(10), time);

        // Binary
        time = redis.objectIdletime(binaryKey);
        assertEquals(new Long(10), time);
    }

}
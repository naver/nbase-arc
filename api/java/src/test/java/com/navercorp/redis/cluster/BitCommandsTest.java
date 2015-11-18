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
public class BitCommandsTest extends RedisClusterTestBase {

    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_KEY_1);
        redis.del(REDIS_KEY_2);
    }

    @Test
    public void setAndgetbit() {
        boolean bit = redis.setbit(REDIS_KEY_0, 0, true);
        assertEquals(false, bit);

        bit = redis.getbit(REDIS_KEY_0, 0);
        assertEquals(true, bit);

        boolean bbit = redis.setbit(REDIS_KEY_1.getBytes(), 0, "1".getBytes());
        assertFalse(bbit);

        bbit = redis.getbit(REDIS_KEY_1.getBytes(), 0);
        assertTrue(bbit);
    }

    @Test
    public void setAndgetrange() {
        redis.set(REDIS_KEY_2, "Hello World");
        long reply = redis.setrange(REDIS_KEY_2, 6, "Jedis");
        assertEquals(11, reply);

        assertEquals(redis.get(REDIS_KEY_2), "Hello Jedis");

        assertEquals("Hello", redis.getrange(REDIS_KEY_2, 0, 4));
        assertEquals("Jedis", redis.getrange(REDIS_KEY_2, 6, 11));
    }

    @Test
    public void bitCount() {
        redis.setbit(REDIS_KEY_0, 16, true);
        redis.setbit(REDIS_KEY_0, 24, true);
        redis.setbit(REDIS_KEY_0, 40, true);
        redis.setbit(REDIS_KEY_0, 56, true);

        long c4 = redis.bitcount(REDIS_KEY_0);
        assertEquals(4, c4);

        long c3 = redis.bitcount(REDIS_KEY_0, 2L, 5L);
        assertEquals(3, c3);
    }
}
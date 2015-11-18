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

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * @author jaehong.kim
 */
public class StringValuesCommandsTest extends RedisClusterTestBase {
    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_VALUE_0);
        redis.del(REDIS_KEY_1);
    }

    @Test
    public void setAndGet() {
        String status = redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals("OK", status);

        String value = redis.get(REDIS_KEY_0);
        assertEquals(REDIS_VALUE_0, value);

        assertEquals(null, redis.get(REDIS_VALUE_0));
    }

    @Test
    public void getSet() {
        String value = redis.getSet(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(null, value);
        value = redis.get(REDIS_KEY_0);
        assertEquals(REDIS_VALUE_0, value);
    }

    @Test
    public void setnx() {
        long status = redis.setnx(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(1, status);
        assertEquals(REDIS_VALUE_0, redis.get(REDIS_KEY_0));

        status = redis.setnx(REDIS_KEY_0, "bar2");
        assertEquals(0, status);
        assertEquals(REDIS_VALUE_0, redis.get(REDIS_KEY_0));
    }

    @Test
    public void setex() {
        String status = redis.setex(REDIS_KEY_0, 20, REDIS_VALUE_0);
        assertEquals("OK", status);
        long ttl = redis.ttl(REDIS_KEY_0);
        assertTrue(ttl > 0 && ttl <= 20);
    }

    @Test(expected = JedisDataException.class)
    public void incrWrongValue() {
        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        redis.incr(REDIS_KEY_0);
    }

    @Test
    public void incr() {
        long value = redis.incr(REDIS_KEY_0);
        assertEquals(1, value);
        value = redis.incr(REDIS_KEY_0);
        assertEquals(2, value);
    }

    @Test(expected = JedisDataException.class)
    public void incrByWrongValue() {
        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        redis.incrBy(REDIS_KEY_0, 2);
    }

    @Test
    public void incrBy() {
        long value = redis.incrBy(REDIS_KEY_0, 2);
        assertEquals(2, value);
        value = redis.incrBy(REDIS_KEY_0, 2);
        assertEquals(4, value);
    }

    @Test(expected = JedisDataException.class)
    public void decrWrongValue() {
        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        redis.decr(REDIS_KEY_0);
    }

    @Test
    public void decr() {
        long value = redis.decr(REDIS_KEY_0);
        assertEquals(-1, value);
        value = redis.decr(REDIS_KEY_0);
        assertEquals(-2, value);
    }

    @Test(expected = JedisDataException.class)
    public void decrByWrongValue() {
        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        redis.decrBy(REDIS_KEY_0, 2);
    }

    @Test
    public void decrBy() {
        long value = redis.decrBy(REDIS_KEY_0, 2);
        assertEquals(-2, value);
        value = redis.decrBy(REDIS_KEY_0, 2);
        assertEquals(-4, value);
    }

    @Test
    public void append() {
        long value = redis.append(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(REDIS_VALUE_0.length(), value);
        assertEquals(REDIS_VALUE_0, redis.get(REDIS_KEY_0));
        value = redis.append(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(REDIS_VALUE_0.length() * 2, value);
        assertEquals(REDIS_VALUE_0 + REDIS_VALUE_0, redis.get(REDIS_KEY_0));
    }

    @Test
    public void substr() {
        redis.set(REDIS_KEY_1, "This is a string");
        assertEquals("This", redis.substr(REDIS_KEY_1, 0, 3));
        assertEquals("ing", redis.substr(REDIS_KEY_1, -3, -1));
        assertEquals("This is a string", redis.substr(REDIS_KEY_1, 0, -1));
        assertEquals(" string", redis.substr(REDIS_KEY_1, 9, 100000));
    }

    @Test
    public void strlen() {
        redis.set(REDIS_KEY_1, "This is a string");
        assertEquals("This is a string".length(), redis.strlen(REDIS_KEY_1).intValue());
    }

    @Test
    public void incrLargeNumbers() {
        long value = redis.incr(REDIS_KEY_0);
        assertEquals(1, value);
        assertEquals(1L + Integer.MAX_VALUE, (long) redis.incrBy(REDIS_KEY_0, Integer.MAX_VALUE));
    }

    @Test(expected = JedisDataException.class)
    public void incrReallyLargeNumbers() {
        redis.set(REDIS_KEY_0, Long.toString(Long.MAX_VALUE));
        long value = redis.incr(REDIS_KEY_0);
        assertEquals(Long.MIN_VALUE, value);
    }

    @Test
    public void psetex() {
        String status = redis.psetex(REDIS_KEY_0, 20000, REDIS_VALUE_0);
        assertEquals("OK", status);
        long ttl = redis.ttl(REDIS_KEY_0);
        assertTrue(ttl > 0 && ttl <= 20000);
    }

    @Test
    public void incrByFloat() {
        double value = redis.incrByFloat(REDIS_KEY_0, 10.5);
        assertEquals(10.5, value, 0.0);
        value = redis.incrByFloat(REDIS_KEY_0, 0.1);
        assertEquals(10.6, value, 0.0);
    }
}
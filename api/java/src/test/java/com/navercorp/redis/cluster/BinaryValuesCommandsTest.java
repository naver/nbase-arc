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

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.exceptions.JedisDataException;

import com.navercorp.redis.cluster.connection.RedisProtocol.Keyword;

/**
 * @author jaehong.kim
 */
public class BinaryValuesCommandsTest extends RedisClusterTestBase {
    byte[] binaryValue;

    @Override
    public void clear() {
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BVALUE_0);
    }

    @Before
    public void startUp() {
        StringBuilder sb = new StringBuilder();

        for (int n = 0; n < 1000; n++) {
            sb.append("A");
        }

        binaryValue = sb.toString().getBytes();
    }

    @Test
    public void setAndGet() {
        String status = redis.set(REDIS_BKEY_0, binaryValue);
        assertTrue(Keyword.OK.name().equalsIgnoreCase(status));

        byte[] value = redis.get(REDIS_BKEY_0);
        assertTrue(Arrays.equals(binaryValue, value));

        assertNull(redis.get(REDIS_BVALUE_0));
    }

    @Test
    public void setNxExAndGet() {
        String status = redis.set(REDIS_BKEY_0, binaryValue, REDIS_BNX, REDIS_BEX, 2);
        assertTrue("OK".equalsIgnoreCase(status));

        byte[] value = redis.get(REDIS_BKEY_0);
        assertTrue(Arrays.equals(binaryValue, value));
    }

    @Test
    public void setIfNotExistAndGet() {
        String status = redis.set(REDIS_BKEY_0, binaryValue);
        assertTrue("OK".equalsIgnoreCase(status));
        String statusFail = redis.set(REDIS_BKEY_0, binaryValue, REDIS_BNX, REDIS_BEX, 2);
        assertNull(statusFail);

        byte[] value = redis.get(REDIS_BKEY_0);
        assertTrue(Arrays.equals(binaryValue, value));
    }

    @Test
    public void setIfExistAndGet() {
        String status = redis.set(REDIS_BKEY_0, binaryValue);
        assertTrue("OK".equalsIgnoreCase(status));
        String statusSuccess = redis.set(REDIS_BKEY_0, binaryValue, REDIS_BXX, REDIS_BEX, 2);
        assertTrue("OK".equalsIgnoreCase(statusSuccess));

        byte[] value = redis.get(REDIS_BKEY_0);
        assertTrue(Arrays.equals(binaryValue, value));
    }

    @Test
    public void setFailIfNotExistAndGet() {
        String statusFail = redis.set(REDIS_BKEY_0, binaryValue, REDIS_BXX, REDIS_BEX, 2);
        assertNull(statusFail);
    }

    @Test
    public void setAndExpireMillis() {
        String status = redis.set(REDIS_BKEY_0, binaryValue, REDIS_BNX, REDIS_BPX, 2000);
        assertTrue("OK".equalsIgnoreCase(status));
        long ttl = redis.ttl(REDIS_BKEY_0);
        assertTrue(ttl > 0 && ttl <= 2);
    }

    @Test
    public void setAndExpire() {
        String status = redis.set(REDIS_BKEY_0, binaryValue, REDIS_BNX, REDIS_BEX, 2);
        assertTrue("OK".equalsIgnoreCase(status));
        long ttl = redis.ttl(REDIS_BKEY_0);
        assertTrue(ttl > 0 && ttl <= 2);
    }
        
    @Test
    public void getSet() {
        byte[] value = redis.getSet(REDIS_BKEY_0, binaryValue);
        assertNull(value);
        value = redis.get(REDIS_BKEY_0);
        assertTrue(Arrays.equals(binaryValue, value));
    }

    @Test
    public void setnx() {
        long status = redis.setnx(REDIS_BKEY_0, binaryValue);
        assertEquals(1, status);
        assertTrue(Arrays.equals(binaryValue, redis.get(REDIS_BKEY_0)));

        status = redis.setnx(REDIS_BKEY_0, REDIS_BVALUE_0);
        assertEquals(0, status);
        assertTrue(Arrays.equals(binaryValue, redis.get(REDIS_BKEY_0)));
    }

    @Test
    public void setex() {
        String status = redis.setex(REDIS_BKEY_0, 20, binaryValue);
        assertEquals(Keyword.OK.name(), status);
        long ttl = redis.ttl(REDIS_BKEY_0);
        assertTrue(ttl > 0 && ttl <= 20);
    }

    @Test(expected = JedisDataException.class)
    public void incrWrongValue() {
        redis.set(REDIS_BKEY_0, binaryValue);
        redis.incr(REDIS_BKEY_0);
    }

    @Test
    public void incr() {
        long value = redis.incr(REDIS_BKEY_0);
        assertEquals(1, value);
        value = redis.incr(REDIS_BKEY_0);
        assertEquals(2, value);
    }

    @Test(expected = JedisDataException.class)
    public void incrByWrongValue() {
        redis.set(REDIS_BKEY_0, binaryValue);
        redis.incrBy(REDIS_BKEY_0, 2);
    }

    @Test
    public void incrBy() {
        long value = redis.incrBy(REDIS_BKEY_0, 2);
        assertEquals(2, value);
        value = redis.incrBy(REDIS_BKEY_0, 2);
        assertEquals(4, value);
    }

    @Test(expected = JedisDataException.class)
    public void decrWrongValue() {
        redis.set(REDIS_BKEY_0, binaryValue);
        redis.decr(REDIS_BKEY_0);
    }

    @Test
    public void decr() {
        long value = redis.decr(REDIS_BKEY_0);
        assertEquals(-1, value);
        value = redis.decr(REDIS_BKEY_0);
        assertEquals(-2, value);
    }

    @Test(expected = JedisDataException.class)
    public void decrByWrongValue() {
        redis.set(REDIS_BKEY_0, binaryValue);
        redis.decrBy(REDIS_BKEY_0, 2);
    }

    @Test
    public void decrBy() {
        long value = redis.decrBy(REDIS_BKEY_0, 2);
        assertEquals(-2, value);
        value = redis.decrBy(REDIS_BKEY_0, 2);
        assertEquals(-4, value);
    }

    @Test
    public void append() {
        byte[] first512 = new byte[512];
        System.arraycopy(binaryValue, 0, first512, 0, 512);
        long value = redis.append(REDIS_BKEY_0, first512);
        assertEquals(512, value);
        assertTrue(Arrays.equals(first512, redis.get(REDIS_BKEY_0)));

        byte[] rest = new byte[binaryValue.length - 512];
        System.arraycopy(binaryValue, 512, rest, 0, binaryValue.length - 512);
        value = redis.append(REDIS_BKEY_0, rest);
        assertEquals(binaryValue.length, value);

        assertTrue(Arrays.equals(binaryValue, redis.get(REDIS_BKEY_0)));
    }

    @Test
    public void substr() {
        redis.set(REDIS_BKEY_0, binaryValue);

        byte[] first512 = new byte[512];
        System.arraycopy(binaryValue, 0, first512, 0, 512);
        byte[] rfirst512 = redis.substr(REDIS_BKEY_0, 0, 511);
        assertTrue(Arrays.equals(first512, rfirst512));

        byte[] last512 = new byte[512];
        System.arraycopy(binaryValue, binaryValue.length - 512, last512, 0, 512);
        assertTrue(Arrays.equals(last512, redis.substr(REDIS_BKEY_0, -512, -1)));

        assertTrue(Arrays.equals(binaryValue, redis.substr(REDIS_BKEY_0, 0, -1)));

        assertTrue(Arrays.equals(last512,
                redis.substr(REDIS_BKEY_0, binaryValue.length - 512, 100000)));
    }

    @Test
    public void strlen() {
        redis.set(REDIS_BKEY_0, binaryValue);
        assertEquals(binaryValue.length, redis.strlen(REDIS_BKEY_0).intValue());
    }
}

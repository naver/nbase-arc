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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class HashesCommandsTest extends RedisClusterTestBase {

    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_KEY_1);
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BKEY_1);
        redis.del(REDIS_BVALUE_0);
    }

    @Test
    public void hset() {
        long status = redis.hset(REDIS_KEY_0, REDIS_KEY_1, REDIS_VALUE_0);
        assertEquals(1, status);
        status = redis.hset(REDIS_KEY_0, REDIS_KEY_1, REDIS_KEY_0);
        assertEquals(0, status);

        // Binary
        long bstatus = redis.hset(REDIS_BKEY_0, REDIS_BKEY_1, REDIS_BVALUE_0);
        assertEquals(1, bstatus);
        bstatus = redis.hset(REDIS_BKEY_0, REDIS_BKEY_1, REDIS_BKEY_0);
        assertEquals(0, bstatus);

    }

    @Test
    public void hget() {
        redis.hset(REDIS_KEY_0, REDIS_KEY_1, REDIS_VALUE_0);
        assertEquals(null, redis.hget(REDIS_KEY_1, REDIS_KEY_0));
        assertEquals(null, redis.hget(REDIS_KEY_0, REDIS_VALUE_0));
        assertEquals(REDIS_VALUE_0, redis.hget(REDIS_KEY_0, REDIS_KEY_1));

        // Binary
        redis.hset(REDIS_BKEY_0, REDIS_BKEY_1, REDIS_BVALUE_0);
        assertEquals(null, redis.hget(REDIS_BKEY_1, REDIS_BKEY_0));
        assertEquals(null, redis.hget(REDIS_BKEY_0, REDIS_BVALUE_0));
        assertArrayEquals(REDIS_BVALUE_0, redis.hget(REDIS_BKEY_0, REDIS_BKEY_1));
    }

    @Test
    public void hsetnx() {
        long status = redis.hsetnx(REDIS_KEY_0, REDIS_KEY_1, REDIS_VALUE_0);
        assertEquals(1, status);
        assertEquals(REDIS_VALUE_0, redis.hget(REDIS_KEY_0, REDIS_KEY_1));

        status = redis.hsetnx(REDIS_KEY_0, REDIS_KEY_1, REDIS_KEY_0);
        assertEquals(0, status);
        assertEquals(REDIS_VALUE_0, redis.hget(REDIS_KEY_0, REDIS_KEY_1));

        status = redis.hsetnx(REDIS_KEY_0, REDIS_VALUE_0, REDIS_KEY_1);
        assertEquals(1, status);
        assertEquals(REDIS_KEY_1, redis.hget(REDIS_KEY_0, REDIS_VALUE_0));

        // Binary
        long bstatus = redis.hsetnx(REDIS_BKEY_0, REDIS_BKEY_1, REDIS_BVALUE_0);
        assertEquals(1, bstatus);
        assertArrayEquals(REDIS_BVALUE_0, redis.hget(REDIS_BKEY_0, REDIS_BKEY_1));

        bstatus = redis.hsetnx(REDIS_BKEY_0, REDIS_BKEY_1, REDIS_BKEY_0);
        assertEquals(0, bstatus);
        assertArrayEquals(REDIS_BVALUE_0, redis.hget(REDIS_BKEY_0, REDIS_BKEY_1));

        bstatus = redis.hsetnx(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_1);
        assertEquals(1, bstatus);
        assertArrayEquals(REDIS_BKEY_1, redis.hget(REDIS_BKEY_0, REDIS_BVALUE_0));

    }

    @Test
    public void hmset() {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put(REDIS_KEY_1, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_0, REDIS_KEY_1);
        String status = redis.hmset(REDIS_KEY_0, hash);
        assertEquals("OK", status);
        assertEquals(REDIS_VALUE_0, redis.hget(REDIS_KEY_0, REDIS_KEY_1));
        assertEquals(REDIS_KEY_1, redis.hget(REDIS_KEY_0, REDIS_VALUE_0));

        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        String bstatus = redis.hmset(REDIS_BKEY_0, bhash);
        assertEquals("OK", bstatus);
        assertArrayEquals(REDIS_BVALUE_0, redis.hget(REDIS_BKEY_0, REDIS_BKEY_1));
        assertArrayEquals(REDIS_BKEY_1, redis.hget(REDIS_BKEY_0, REDIS_BVALUE_0));

    }

    @Test
    public void hmget() {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put(REDIS_KEY_1, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_0, REDIS_KEY_1);
        redis.hmset(REDIS_KEY_0, hash);

        List<String> values = redis.hmget(REDIS_KEY_0, REDIS_KEY_1, REDIS_VALUE_0, REDIS_KEY_0);
        List<String> expected = new ArrayList<String>();
        expected.add(REDIS_VALUE_0);
        expected.add(REDIS_KEY_1);
        expected.add(null);

        assertEquals(expected, values);

        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        redis.hmset(REDIS_BKEY_0, bhash);

        List<byte[]> bvalues = redis.hmget(REDIS_BKEY_0, REDIS_BKEY_1, REDIS_BVALUE_0, REDIS_BKEY_0);
        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(REDIS_BVALUE_0);
        bexpected.add(REDIS_BKEY_1);
        bexpected.add(null);

        assertEquals(bexpected, bvalues);
    }

    @Test
    public void hincrBy() {
        long value = redis.hincrBy(REDIS_KEY_0, REDIS_KEY_1, 1);
        assertEquals(1, value);
        value = redis.hincrBy(REDIS_KEY_0, REDIS_KEY_1, -1);
        assertEquals(0, value);
        value = redis.hincrBy(REDIS_KEY_0, REDIS_KEY_1, -10);
        assertEquals(-10, value);

        // Binary
        long bvalue = redis.hincrBy(REDIS_BKEY_0, REDIS_BKEY_1, 1);
        assertEquals(1, bvalue);
        bvalue = redis.hincrBy(REDIS_BKEY_0, REDIS_BKEY_1, -1);
        assertEquals(0, bvalue);
        bvalue = redis.hincrBy(REDIS_BKEY_0, REDIS_BKEY_1, -10);
        assertEquals(-10, bvalue);

    }

    @Test
    public void hincrByAt() {
        long status = redis.hset(REDIS_KEY_0, REDIS_KEY_1, "10.50");
        assertEquals(1, status);

        double value = redis.hincrByFloat(REDIS_KEY_0, REDIS_KEY_1, 0.1);
        System.out.println(value);

        redis.hset(REDIS_KEY_0, REDIS_KEY_1, "5.0e3");

        value = redis.hincrByFloat(REDIS_KEY_0, REDIS_KEY_1, 2.0e2);
        System.out.println(value);
    }

    @Test
    public void hexists() {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put(REDIS_KEY_1, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_0, REDIS_KEY_1);
        redis.hmset(REDIS_KEY_0, hash);

        assertFalse(redis.hexists(REDIS_KEY_1, REDIS_KEY_0));
        assertFalse(redis.hexists(REDIS_KEY_0, REDIS_KEY_0));
        assertTrue(redis.hexists(REDIS_KEY_0, REDIS_KEY_1));

        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        redis.hmset(REDIS_BKEY_0, bhash);

        assertFalse(redis.hexists(REDIS_BKEY_1, REDIS_BKEY_0));
        assertFalse(redis.hexists(REDIS_BKEY_0, REDIS_BKEY_0));
        assertTrue(redis.hexists(REDIS_BKEY_0, REDIS_BKEY_1));

    }

    @Test
    public void hdel() {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put(REDIS_KEY_1, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_0, REDIS_KEY_1);
        redis.hmset(REDIS_KEY_0, hash);

        assertEquals(0, redis.hdel(REDIS_KEY_1, REDIS_KEY_0).intValue());
        assertEquals(0, redis.hdel(REDIS_KEY_0, REDIS_KEY_0).intValue());
        assertEquals(1, redis.hdel(REDIS_KEY_0, REDIS_KEY_1).intValue());
        assertEquals(null, redis.hget(REDIS_KEY_0, REDIS_KEY_1));

        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        redis.hmset(REDIS_BKEY_0, bhash);

        assertEquals(0, redis.hdel(REDIS_BKEY_1, REDIS_BKEY_0).intValue());
        assertEquals(0, redis.hdel(REDIS_BKEY_0, REDIS_BKEY_0).intValue());
        assertEquals(1, redis.hdel(REDIS_BKEY_0, REDIS_BKEY_1).intValue());
        assertEquals(null, redis.hget(REDIS_BKEY_0, REDIS_BKEY_1));

    }

    @Test
    public void hlen() {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put(REDIS_KEY_1, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_0, REDIS_KEY_1);
        redis.hmset(REDIS_KEY_0, hash);

        assertEquals(0, redis.hlen(REDIS_KEY_1).intValue());
        assertEquals(2, redis.hlen(REDIS_KEY_0).intValue());

        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        redis.hmset(REDIS_BKEY_0, bhash);

        assertEquals(0, redis.hlen(REDIS_BKEY_1).intValue());
        assertEquals(2, redis.hlen(REDIS_BKEY_0).intValue());

    }

    @Test
    public void hkeys() {
        Map<String, String> hash = new LinkedHashMap<String, String>();
        hash.put(REDIS_KEY_1, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_0, REDIS_KEY_1);
        redis.hmset(REDIS_KEY_0, hash);

        Set<String> keys = redis.hkeys(REDIS_KEY_0);
        Set<String> expected = new LinkedHashSet<String>();
        expected.add(REDIS_KEY_1);
        expected.add(REDIS_VALUE_0);
        assertEquals(expected, keys);

        // Binary
        Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        redis.hmset(REDIS_BKEY_0, bhash);

        Set<byte[]> bkeys = redis.hkeys(REDIS_BKEY_0);
        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(REDIS_BKEY_1);
        bexpected.add(REDIS_BVALUE_0);
        assertEquals(bexpected, bkeys);
    }

    @Test
    public void hvals() {
        Map<String, String> hash = new LinkedHashMap<String, String>();
        hash.put(REDIS_KEY_1, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_0, REDIS_KEY_1);
        redis.hmset(REDIS_KEY_0, hash);

        List<String> vals = redis.hvals(REDIS_KEY_0);
        assertEquals(2, vals.size());
        assertTrue(vals.contains(REDIS_KEY_1));
        assertTrue(vals.contains(REDIS_VALUE_0));

        // Binary
        Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        redis.hmset(REDIS_BKEY_0, bhash);

        List<byte[]> bvals = redis.hvals(REDIS_BKEY_0);

        assertEquals(2, bvals.size());
        assertTrue(arrayContains(bvals, REDIS_BKEY_1));
        assertTrue(arrayContains(bvals, REDIS_BVALUE_0));
    }

    @Test
    public void hgetAll() {
        Map<String, String> h = new HashMap<String, String>();
        h.put(REDIS_KEY_1, REDIS_VALUE_0);
        h.put(REDIS_VALUE_0, REDIS_KEY_1);
        redis.hmset(REDIS_KEY_0, h);

        Map<String, String> hash = redis.hgetAll(REDIS_KEY_0);
        assertEquals(2, hash.size());
        assertEquals(REDIS_VALUE_0, hash.get(REDIS_KEY_1));
        assertEquals(REDIS_KEY_1, hash.get(REDIS_VALUE_0));

        // Binary
        Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
        bh.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        bh.put(REDIS_BVALUE_0, REDIS_BKEY_1);
        redis.hmset(REDIS_BKEY_0, bh);
        Map<byte[], byte[]> bhash = redis.hgetAll(REDIS_BKEY_0);

        assertEquals(2, bhash.size());
        assertArrayEquals(REDIS_BVALUE_0, bhash.get(REDIS_BKEY_1));
        assertArrayEquals(REDIS_BKEY_1, bhash.get(REDIS_BVALUE_0));
    }
}

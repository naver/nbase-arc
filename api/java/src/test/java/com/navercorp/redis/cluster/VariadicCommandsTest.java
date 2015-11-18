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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class VariadicCommandsTest extends RedisClusterTestBase {
    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_VALUE_0);
        redis.del(REDIS_VALUE_1);
        redis.del(REDIS_VALUE_2);
        redis.del(REDIS_BKEY_1);
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BVALUE_0);
        redis.del(REDIS_BVALUE_1);
    }

    @Test
    public void hdel() {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put(REDIS_VALUE_0, REDIS_VALUE_2);
        hash.put(REDIS_VALUE_2, REDIS_VALUE_0);
        hash.put(REDIS_VALUE_1, REDIS_VALUE_0);
        redis.hmset(REDIS_KEY_0, hash);

        assertEquals(0, redis.hdel(REDIS_VALUE_0, REDIS_KEY_0, "foo1").intValue());
        assertEquals(0, redis.hdel(REDIS_KEY_0, REDIS_KEY_0, "foo1").intValue());
        assertEquals(2, redis.hdel(REDIS_KEY_0, REDIS_VALUE_0, REDIS_VALUE_1).intValue());
        assertEquals(null, redis.hget(REDIS_KEY_0, REDIS_VALUE_0));

        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(REDIS_BVALUE_0, REDIS_BVALUE_1);
        bhash.put(REDIS_BVALUE_1, REDIS_BVALUE_0);
        bhash.put(REDIS_BKEY_1, REDIS_BVALUE_0);
        redis.hmset(REDIS_BKEY_0, bhash);

        assertEquals(0, redis.hdel(REDIS_BVALUE_0, REDIS_BKEY_0, REDIS_BKEY_2).intValue());
        assertEquals(0, redis.hdel(REDIS_BKEY_0, REDIS_BKEY_0, REDIS_BKEY_2).intValue());
        assertEquals(2, redis.hdel(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_1).intValue());
        assertEquals(null, redis.hget(REDIS_BKEY_0, REDIS_BVALUE_0));

    }

    @Test
    public void rpush() {
        long size = redis.rpush(REDIS_KEY_0, REDIS_VALUE_0, REDIS_KEY_0);
        assertEquals(2, size);

        List<String> expected = new ArrayList<String>();
        expected.add(REDIS_VALUE_0);
        expected.add(REDIS_KEY_0);

        List<String> values = redis.lrange(REDIS_KEY_0, 0, -1);
        assertEquals(expected, values);

        // Binary
        size = redis.rpush(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_0);
        assertEquals(2, size);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(REDIS_BVALUE_0);
        bexpected.add(REDIS_BKEY_0);

        List<byte[]> bvalues = redis.lrange(REDIS_BKEY_0, 0, -1);
        assertEquals(bexpected, bvalues);

    }

    @Test
    public void lpush() {
        long size = redis.lpush(REDIS_KEY_0, REDIS_VALUE_0, REDIS_KEY_0);
        assertEquals(2, size);

        List<String> expected = new ArrayList<String>();
        expected.add(REDIS_KEY_0);
        expected.add(REDIS_VALUE_0);

        List<String> values = redis.lrange(REDIS_KEY_0, 0, -1);
        assertEquals(expected, values);

        // Binary
        size = redis.lpush(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_0);
        assertEquals(2, size);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(REDIS_BKEY_0);
        bexpected.add(REDIS_BVALUE_0);

        List<byte[]> bvalues = redis.lrange(REDIS_BKEY_0, 0, -1);
        assertEquals(bexpected, bvalues);

    }

    @Test
    public void sadd() {
        long status = redis.sadd(REDIS_KEY_0, REDIS_VALUE_0, "foo1");
        assertEquals(2, status);

        status = redis.sadd(REDIS_KEY_0, REDIS_VALUE_0, REDIS_VALUE_2);
        assertEquals(1, status);

        status = redis.sadd(REDIS_KEY_0, REDIS_VALUE_0, "foo1");
        assertEquals(0, status);

        status = redis.sadd(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_2);
        assertEquals(2, status);

        status = redis.sadd(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BVALUE_1);
        assertEquals(1, status);

        status = redis.sadd(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_2);
        assertEquals(0, status);

    }

    @Test
    public void zadd() {
        Map<Double, String> scoreMembers = new HashMap<Double, String>();
        scoreMembers.put(1d, REDIS_VALUE_0);
        scoreMembers.put(10d, REDIS_KEY_0);

        long status = redis.zadd(REDIS_KEY_0, scoreMembers);
        assertEquals(2, status);

        scoreMembers.clear();
        scoreMembers.put(0.1d, REDIS_VALUE_2);
        scoreMembers.put(2d, REDIS_VALUE_0);

        status = redis.zadd(REDIS_KEY_0, scoreMembers);
        assertEquals(1, status);

        Map<Double, byte[]> bscoreMembers = new HashMap<Double, byte[]>();
        bscoreMembers.put(1d, REDIS_BVALUE_0);
        bscoreMembers.put(10d, REDIS_BKEY_0);

        status = redis.zadd(REDIS_BKEY_0, bscoreMembers);
        assertEquals(2, status);

        bscoreMembers.clear();
        bscoreMembers.put(0.1d, REDIS_BVALUE_1);
        bscoreMembers.put(2d, REDIS_BVALUE_0);

        status = redis.zadd(REDIS_BKEY_0, bscoreMembers);
        assertEquals(1, status);

    }

    @Test
    public void zrem() {
        redis.zadd(REDIS_KEY_0, 1d, REDIS_VALUE_0);
        redis.zadd(REDIS_KEY_0, 2d, REDIS_VALUE_2);
        redis.zadd(REDIS_KEY_0, 3d, "foo1");

        long status = redis.zrem(REDIS_KEY_0, REDIS_VALUE_0, REDIS_VALUE_2);

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("foo1");

        assertEquals(2, status);
        assertEquals(expected, redis.zrange(REDIS_KEY_0, 0, 100));

        status = redis.zrem(REDIS_KEY_0, REDIS_VALUE_0, REDIS_VALUE_2);
        assertEquals(0, status);

        status = redis.zrem(REDIS_KEY_0, REDIS_VALUE_0, "foo1");
        assertEquals(1, status);

        // Binary
        redis.zadd(REDIS_BKEY_0, 1d, REDIS_BVALUE_0);
        redis.zadd(REDIS_BKEY_0, 2d, REDIS_BVALUE_1);
        redis.zadd(REDIS_BKEY_0, 3d, REDIS_BKEY_2);

        status = redis.zrem(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BVALUE_1);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(REDIS_BKEY_0);

        assertEquals(2, status);
        assertEquals(bexpected, redis.zrange(REDIS_BKEY_0, 0, 100));

        status = redis.zrem(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BVALUE_1);
        assertEquals(0, status);

        status = redis.zrem(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_2);
        assertEquals(1, status);
    }
}
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
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class SetCommandsTest extends RedisClusterTestBase {
    final byte[] ba = {0x0A};
    final byte[] bb = {0x0B};
    final byte[] bc = {0x0C};
    final byte[] bd = {0x0D};
    final byte[] bx = {0x42};

    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_KEY_1);
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BKEY_1);
        redis.del(REDIS_BKEY_2);
    }

    @Test
    public void sadd() {
        long status = redis.sadd(REDIS_KEY_0, "a");
        assertEquals(1, status);

        status = redis.sadd(REDIS_KEY_0, "a");
        assertEquals(0, status);

        long bstatus = redis.sadd(REDIS_BKEY_0, ba);
        assertEquals(1, bstatus);

        bstatus = redis.sadd(REDIS_BKEY_0, ba);
        assertEquals(0, bstatus);

    }

    @Test
    public void smembers() {
        redis.sadd(REDIS_KEY_0, "a");
        redis.sadd(REDIS_KEY_0, "b");

        Set<String> expected = new HashSet<String>();
        expected.add("a");
        expected.add("b");

        Set<String> members = redis.smembers(REDIS_KEY_0);

        assertEquals(expected, members);

        // Binary
        redis.sadd(REDIS_BKEY_0, ba);
        redis.sadd(REDIS_BKEY_0, bb);

        Set<byte[]> bexpected = new HashSet<byte[]>();
        bexpected.add(bb);
        bexpected.add(ba);

        Set<byte[]> bmembers = redis.smembers(REDIS_BKEY_0);

        assertEquals(bexpected, bmembers);
    }

    @Test
    public void srem() {
        redis.sadd(REDIS_KEY_0, "a");
        redis.sadd(REDIS_KEY_0, "b");

        long status = redis.srem(REDIS_KEY_0, "a");

        Set<String> expected = new HashSet<String>();
        expected.add("b");

        assertEquals(1, status);
        assertEquals(expected, redis.smembers(REDIS_KEY_0));

        status = redis.srem(REDIS_KEY_0, REDIS_KEY_1);

        assertEquals(0, status);

        // Binary

        redis.sadd(REDIS_BKEY_0, ba);
        redis.sadd(REDIS_BKEY_0, bb);

        long bstatus = redis.srem(REDIS_BKEY_0, ba);

        Set<byte[]> bexpected = new HashSet<byte[]>();
        bexpected.add(bb);

        assertEquals(1, bstatus);
        assertEquals(bexpected, redis.smembers(REDIS_BKEY_0));

        bstatus = redis.srem(REDIS_BKEY_0, REDIS_BKEY_1);

        assertEquals(0, bstatus);

    }

    @Test
    public void scard() {
        redis.sadd(REDIS_KEY_0, "a");
        redis.sadd(REDIS_KEY_0, "b");

        long card = redis.scard(REDIS_KEY_0);

        assertEquals(2, card);

        card = redis.scard(REDIS_KEY_1);
        assertEquals(0, card);

        // Binary
        redis.sadd(REDIS_BKEY_0, ba);
        redis.sadd(REDIS_BKEY_0, bb);

        long bcard = redis.scard(REDIS_BKEY_0);

        assertEquals(2, bcard);

        bcard = redis.scard(REDIS_BKEY_1);
        assertEquals(0, bcard);

    }

    @Test
    public void sismember() {
        redis.sadd(REDIS_KEY_0, "a");
        redis.sadd(REDIS_KEY_0, "b");

        assertTrue(redis.sismember(REDIS_KEY_0, "a"));

        assertFalse(redis.sismember(REDIS_KEY_0, "c"));

        // Binary
        redis.sadd(REDIS_BKEY_0, ba);
        redis.sadd(REDIS_BKEY_0, bb);

        assertTrue(redis.sismember(REDIS_BKEY_0, ba));

        assertFalse(redis.sismember(REDIS_BKEY_0, bc));

    }

    @Test
    public void srandmember() {
        redis.sadd(REDIS_KEY_0, "a");
        redis.sadd(REDIS_KEY_0, "b");

        String member = redis.srandmember(REDIS_KEY_0);

        assertTrue("a".equals(member) || "b".equals(member));
        assertEquals(2, redis.smembers(REDIS_KEY_0).size());

        member = redis.srandmember(REDIS_KEY_1);
        assertNull(member);

        // Binary
        redis.sadd(REDIS_BKEY_0, ba);
        redis.sadd(REDIS_BKEY_0, bb);

        byte[] bmember = redis.srandmember(REDIS_BKEY_0);

        assertTrue(Arrays.equals(ba, bmember) || Arrays.equals(bb, bmember));
        assertEquals(2, redis.smembers(REDIS_BKEY_0).size());

        bmember = redis.srandmember(REDIS_BKEY_1);
        assertNull(bmember);

    }
}
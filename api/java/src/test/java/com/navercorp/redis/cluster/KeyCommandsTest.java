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
public class KeyCommandsTest extends RedisClusterTestBase {

    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_KEY_1);
        redis.del(REDIS_KEY_2);
        redis.del(REDIS_KEY_3);

        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BKEY_1);
        redis.del(REDIS_BKEY_2);
        redis.del(REDIS_BKEY_3);

        redis.del(REDIS_BVALUE_0);
        redis.del(REDIS_BVALUE_1);
        redis.del(REDIS_BVALUE_2);
        redis.del(REDIS_BVALUE_3);
    }

    @Test
    public void exists() {
        String status = redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals("OK", status);

        status = redis.set(REDIS_BKEY_0, REDIS_BVALUE_0);
        assertEquals("OK", status);

        boolean reply = redis.exists(REDIS_KEY_0);
        assertTrue(reply);

        reply = redis.exists(REDIS_BKEY_0);
        assertTrue(reply);

        long lreply = redis.del(REDIS_KEY_0);
        assertEquals(1, lreply);

        lreply = redis.del(REDIS_BKEY_0);
        assertEquals(1, lreply);

        reply = redis.exists(REDIS_KEY_0);
        assertFalse(reply);

        reply = redis.exists(REDIS_BKEY_0);
        assertFalse(reply);

        status = redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals("OK", status);
        status = redis.set(REDIS_KEY_1, REDIS_VALUE_1);
        assertEquals("OK", status);
        lreply = redis.exists(REDIS_KEY_0, REDIS_KEY_1, "NOT_EXIST");
        assertEquals(2, lreply);

        status = redis.set(REDIS_BKEY_0, REDIS_BVALUE_0);
        assertEquals("OK", status);
        status = redis.set(REDIS_BKEY_1, REDIS_BVALUE_1);
        assertEquals("OK", status);
        lreply = redis.exists(REDIS_BKEY_0, REDIS_BKEY_1, "NOT_EXIST".getBytes());
        assertEquals(2, lreply);
    }

    @Test
    public void del() {
        redis.set(REDIS_KEY_1, REDIS_VALUE_1);

        long reply = redis.del(REDIS_KEY_1);
        assertEquals(1, reply);

        Boolean breply = redis.exists(REDIS_KEY_1);
        assertFalse(breply);
        breply = redis.exists(REDIS_KEY_2);
        assertFalse(breply);
        breply = redis.exists(REDIS_KEY_3);
        assertFalse(breply);

        redis.set(REDIS_KEY_1, REDIS_VALUE_1);

        reply = redis.del(REDIS_KEY_1);
        assertEquals(1, reply);

        reply = redis.del(REDIS_KEY_1);
        assertEquals(0, reply);

        // Binary ...
        redis.set(REDIS_BKEY_1, REDIS_BVALUE_1);
        redis.set(REDIS_BKEY_2, REDIS_BVALUE_2);
        redis.set(REDIS_BKEY_3, REDIS_BVALUE_3);

        reply = redis.del(REDIS_BKEY_1);
        assertEquals(1, reply);

        redis.del(REDIS_BKEY_2);
        redis.del(REDIS_BKEY_3);

        breply = redis.exists(REDIS_BKEY_1);
        assertFalse(breply);
        breply = redis.exists(REDIS_BKEY_2);
        assertFalse(breply);
        breply = redis.exists(REDIS_BKEY_3);
        assertFalse(breply);

        redis.set(REDIS_BKEY_1, REDIS_BVALUE_1);

        reply = redis.del(REDIS_BKEY_1);
        assertEquals(1, reply);

        reply = redis.del(REDIS_BKEY_1);
        assertEquals(0, reply);
    }

    @Test
    public void type() {
        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        String status = redis.type(REDIS_KEY_0);
        assertEquals("string", status);

        // Binary
        redis.set(REDIS_BKEY_0, REDIS_BVALUE_0);
        status = redis.type(REDIS_BKEY_0);
        assertEquals("string", status);
    }

    @Test
    public void expire() {
        long status = redis.expire(REDIS_KEY_0, 20);
        assertEquals(0, status);

        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        status = redis.expire(REDIS_KEY_0, 20);
        assertEquals(1, status);

        // Binary
        long bstatus = redis.expire(REDIS_BKEY_0, 20);
        assertEquals(0, bstatus);

        redis.set(REDIS_BKEY_0, REDIS_BVALUE_0);
        bstatus = redis.expire(REDIS_BKEY_0, 20);
        assertEquals(1, bstatus);

    }

    @Test
    public void expireAt() {
//		System.out.println(redis.set(REDIS_KEY_0, "1"));
//		System.out.println("TTL: " + redis.ttl(REDIS_KEY_0));
//		System.out.println(redis.expireAt(REDIS_KEY_0, System.currentTimeMillis() + 1000));
//		System.out.println(redis.expireAt(REDIS_KEY_0, 0));
//		System.out.println("TTL: " + redis.ttl(REDIS_KEY_0));
        System.out.println(redis.get(REDIS_KEY_0));
        System.out.println(redis.decr(REDIS_KEY_0));
        System.out.println("TTL: " + redis.ttl(REDIS_KEY_0));


//		long unixTime = (System.currentTimeMillis() / 1000L) + 20;
//
//		long status = redis.expireAt(REDIS_KEY_0, unixTime);
//		assertEquals(0, status);
//
//		redis.set(REDIS_KEY_0, REDIS_VALUE_0);
//		unixTime = (System.currentTimeMillis() / 1000L) + 20;
//		status = redis.expireAt(REDIS_KEY_0, unixTime);
//		assertEquals(1, status);
//
//		// Binary
//		long bstatus = redis.expireAt(REDIS_BKEY_0, unixTime);
//		assertEquals(0, bstatus);
//
//		redis.set(REDIS_BKEY_0, REDIS_BVALUE_0);
//		unixTime = (System.currentTimeMillis() / 1000L) + 20;
//		bstatus = redis.expireAt(REDIS_BKEY_0, unixTime);
//		assertEquals(1, bstatus);

    }

    @Test
    public void ttl() {
        long ttl = redis.ttl(REDIS_KEY_0);
        assertEquals(-2, ttl);

        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        ttl = redis.ttl(REDIS_KEY_0);
        assertEquals(-1, ttl);

        redis.expire(REDIS_KEY_0, 20);
        ttl = redis.ttl(REDIS_KEY_0);
        assertTrue(ttl >= 0 && ttl <= 20);

        // Binary
        long bttl = redis.ttl(REDIS_BKEY_0);
        assertEquals(-2, bttl);

        redis.set(REDIS_BKEY_0, REDIS_BVALUE_0);
        bttl = redis.ttl(REDIS_BKEY_0);
        assertEquals(-1, bttl);

        redis.expire(REDIS_BKEY_0, 20);
        bttl = redis.ttl(REDIS_BKEY_0);
        assertTrue(bttl >= 0 && bttl <= 20);
    }

    @Test
    public void dumpAndRestore() {
        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        byte[] sv = redis.dump(REDIS_KEY_0);
        redis.restore(REDIS_KEY_1, 0, sv);
        assertTrue(redis.exists(REDIS_KEY_1));
    }

    @Test
    public void pexpire() {
        long status = redis.pexpire(REDIS_KEY_0, 10000);
        assertEquals(0, status);

        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        status = redis.pexpire(REDIS_KEY_0, 10000);
        assertEquals(1, status);
    }

    @Test
    public void pexpireAt() {
        long millisecondsTimestamp = System.currentTimeMillis() + 10000;
        long status = redis.pexpireAt(REDIS_KEY_0, millisecondsTimestamp);
        assertEquals(0, status);

        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        millisecondsTimestamp = System.currentTimeMillis() + 10000;
        status = redis.pexpireAt(REDIS_KEY_0, millisecondsTimestamp);
        assertEquals(1, status);
    }

    @Test
    public void pttl() {
        assertFalse(redis.exists(REDIS_KEY_0));
        long pttl = redis.pttl(REDIS_KEY_0);
        assertEquals(-2, pttl);

        redis.set(REDIS_KEY_0, REDIS_VALUE_0);
        pttl = redis.pttl(REDIS_KEY_0);
        assertEquals(-1, pttl);

        redis.pexpire(REDIS_KEY_0, 20000);
        pttl = redis.pttl(REDIS_KEY_0);
        assertTrue(pttl >= 0 && pttl <= 20000);
    }

    @Test
    public void persist() {
        redis.setex(REDIS_KEY_0, 60 * 60, REDIS_VALUE_0);
        assertTrue(redis.ttl(REDIS_KEY_0) > 0);
        long status = redis.persist(REDIS_KEY_0);
        assertEquals(1, status);
        assertEquals(-1, redis.ttl(REDIS_KEY_0).intValue());

        // binary
        redis.setex(REDIS_BKEY_0, 60 * 60, REDIS_BVALUE_0);
        assertTrue(redis.ttl(REDIS_BKEY_0) > 0);
        long bstatus = redis.persist(REDIS_BKEY_0);
        assertEquals(1, bstatus);
        assertEquals(-1, redis.ttl(REDIS_BKEY_0).intValue());
    }

}
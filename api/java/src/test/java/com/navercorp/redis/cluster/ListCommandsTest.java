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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.Client;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * @author jaehong.kim
 */
public class ListCommandsTest extends RedisClusterTestBase {
    final byte[] bA = {0x0A};
    final byte[] bB = {0x0B};
    final byte[] bC = {0x0C};
    final byte[] b1 = {0x01};
    final byte[] b2 = {0x02};
    final byte[] b3 = {0x03};
    final byte[] bhello = {0x04, 0x02};
    final byte[] bx = {0x02, 0x04};
    final byte[] bdst = {0x11, 0x12, 0x13, 0x14};

    @Override
    public void clear() {
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BVALUE_0);
        redis.del(REDIS_BVALUE_1);
    }

    @Test
    public void rpush() {
        long size = redis.rpush(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(1, size);
        size = redis.rpush(REDIS_KEY_0, REDIS_KEY_0);
        assertEquals(2, size);
        size = redis.rpush(REDIS_KEY_0, REDIS_VALUE_0, REDIS_KEY_0);
        assertEquals(4, size);

        // Binary
        long bsize = redis.rpush(REDIS_BKEY_0, REDIS_BVALUE_0);
        assertEquals(1, bsize);
        bsize = redis.rpush(REDIS_BKEY_0, REDIS_BKEY_0);
        assertEquals(2, bsize);
        bsize = redis.rpush(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_0);
        assertEquals(4, bsize);

    }

    @Test
    public void lpush() {
        long size = redis.lpush(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(1, size);
        size = redis.lpush(REDIS_KEY_0, REDIS_KEY_0);
        assertEquals(2, size);
        size = redis.lpush(REDIS_KEY_0, REDIS_VALUE_0, REDIS_KEY_0);
        assertEquals(4, size);

        // Binary
        long bsize = redis.lpush(REDIS_BKEY_0, REDIS_BVALUE_0);
        assertEquals(1, bsize);
        bsize = redis.lpush(REDIS_BKEY_0, REDIS_BKEY_0);
        assertEquals(2, bsize);
        bsize = redis.lpush(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BKEY_0);
        assertEquals(4, bsize);

    }

    @Test
    public void llen() {
        assertEquals(0, redis.llen(REDIS_KEY_0).intValue());
        redis.lpush(REDIS_KEY_0, REDIS_VALUE_0);
        redis.lpush(REDIS_KEY_0, REDIS_VALUE_1);
        assertEquals(2, redis.llen(REDIS_KEY_0).intValue());

        // Binary
        assertEquals(0, redis.llen(REDIS_BKEY_0).intValue());
        redis.lpush(REDIS_BKEY_0, REDIS_BVALUE_0);
        redis.lpush(REDIS_BKEY_0, REDIS_BVALUE_1);
        assertEquals(2, redis.llen(REDIS_BKEY_0).intValue());

    }

    @Test
    public void llenNotOnList() {
        try {
            redis.set(REDIS_KEY_0, REDIS_VALUE_0);
            redis.llen(REDIS_KEY_0);
            fail("JedisDataException expected");
        } catch (final JedisDataException e) {
        }

        // Binary
        try {
            redis.set(REDIS_BKEY_0, REDIS_BVALUE_0);
            redis.llen(REDIS_BKEY_0);
            fail("JedisDataException expected");
        } catch (final JedisDataException e) {
        }

    }

    @Test
    public void lrange() {
        redis.rpush(REDIS_KEY_0, "a");
        redis.rpush(REDIS_KEY_0, "b");
        redis.rpush(REDIS_KEY_0, "c");

        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");
        expected.add("c");

        List<String> range = redis.lrange(REDIS_KEY_0, 0, 2);
        assertEquals(expected, range);

        range = redis.lrange(REDIS_KEY_0, 0, 20);
        assertEquals(expected, range);

        expected = new ArrayList<String>();
        expected.add("b");
        expected.add("c");

        range = redis.lrange(REDIS_KEY_0, 1, 2);
        assertEquals(expected, range);

        expected = new ArrayList<String>();
        range = redis.lrange(REDIS_KEY_0, 2, 1);
        assertEquals(expected, range);

        // Binary
        redis.rpush(REDIS_BKEY_0, bA);
        redis.rpush(REDIS_BKEY_0, bB);
        redis.rpush(REDIS_BKEY_0, bC);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bA);
        bexpected.add(bB);
        bexpected.add(bC);

        List<byte[]> brange = redis.lrange(REDIS_BKEY_0, 0, 2);
        Assert.assertEquals(bexpected, brange);

        brange = redis.lrange(REDIS_BKEY_0, 0, 20);
        Assert.assertEquals(bexpected, brange);

        bexpected = new ArrayList<byte[]>();
        bexpected.add(bB);
        bexpected.add(bC);

        brange = redis.lrange(REDIS_BKEY_0, 1, 2);
        Assert.assertEquals(bexpected, brange);

        bexpected = new ArrayList<byte[]>();
        brange = redis.lrange(REDIS_BKEY_0, 2, 1);
        Assert.assertEquals(bexpected, brange);

    }

    @Test
    public void ltrim() {
        redis.lpush(REDIS_KEY_0, "1");
        redis.lpush(REDIS_KEY_0, "2");
        redis.lpush(REDIS_KEY_0, "3");
        String status = redis.ltrim(REDIS_KEY_0, 0, 1);

        List<String> expected = new ArrayList<String>();
        expected.add("3");
        expected.add("2");

        assertEquals("OK", status);
        assertEquals(2, redis.llen(REDIS_KEY_0).intValue());
        assertEquals(expected, redis.lrange(REDIS_KEY_0, 0, 100));

        // Binary
        redis.lpush(REDIS_BKEY_0, b1);
        redis.lpush(REDIS_BKEY_0, b2);
        redis.lpush(REDIS_BKEY_0, b3);
        String bstatus = redis.ltrim(REDIS_BKEY_0, 0, 1);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(b3);
        bexpected.add(b2);

        assertEquals("OK", bstatus);
        assertEquals(2, redis.llen(REDIS_BKEY_0).intValue());
        assertEquals(bexpected, redis.lrange(REDIS_BKEY_0, 0, 100));

    }

    @Test
    public void lindex() {
        redis.lpush(REDIS_KEY_0, "1");
        redis.lpush(REDIS_KEY_0, "2");
        redis.lpush(REDIS_KEY_0, "3");

        List<String> expected = new ArrayList<String>();
        expected.add("3");
        expected.add(REDIS_VALUE_0);
        expected.add("1");

        String status = redis.lset(REDIS_KEY_0, 1, REDIS_VALUE_0);

        assertEquals("OK", status);
        assertEquals(expected, redis.lrange(REDIS_KEY_0, 0, 100));

        // Binary
        redis.lpush(REDIS_BKEY_0, b1);
        redis.lpush(REDIS_BKEY_0, b2);
        redis.lpush(REDIS_BKEY_0, b3);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(b3);
        bexpected.add(REDIS_BVALUE_0);
        bexpected.add(b1);

        String bstatus = redis.lset(REDIS_BKEY_0, 1, REDIS_BVALUE_0);

        assertEquals("OK", bstatus);
        assertEquals(bexpected, redis.lrange(REDIS_BKEY_0, 0, 100));
    }

    @Test
    public void lset() {
        redis.lpush(REDIS_KEY_0, "1");
        redis.lpush(REDIS_KEY_0, "2");
        redis.lpush(REDIS_KEY_0, "3");

        assertEquals("3", redis.lindex(REDIS_KEY_0, 0));
        assertEquals(null, redis.lindex(REDIS_KEY_0, 100));

        // Binary
        redis.lpush(REDIS_BKEY_0, b1);
        redis.lpush(REDIS_BKEY_0, b2);
        redis.lpush(REDIS_BKEY_0, b3);

        assertArrayEquals(b3, redis.lindex(REDIS_BKEY_0, 0));
        assertEquals(null, redis.lindex(REDIS_BKEY_0, 100));

    }

    @Test
    public void lrem() {
        redis.lpush(REDIS_KEY_0, "hello");
        redis.lpush(REDIS_KEY_0, "hello");
        redis.lpush(REDIS_KEY_0, "x");
        redis.lpush(REDIS_KEY_0, "hello");
        redis.lpush(REDIS_KEY_0, "c");
        redis.lpush(REDIS_KEY_0, "b");
        redis.lpush(REDIS_KEY_0, "a");

        long count = redis.lrem(REDIS_KEY_0, -2, "hello");

        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");
        expected.add("c");
        expected.add("hello");
        expected.add("x");

        assertEquals(2, count);
        assertEquals(expected, redis.lrange(REDIS_KEY_0, 0, 1000));
        assertEquals(0, redis.lrem(REDIS_VALUE_0, 100, REDIS_KEY_0).intValue());

        // Binary
        redis.lpush(REDIS_BKEY_0, bhello);
        redis.lpush(REDIS_BKEY_0, bhello);
        redis.lpush(REDIS_BKEY_0, bx);
        redis.lpush(REDIS_BKEY_0, bhello);
        redis.lpush(REDIS_BKEY_0, bC);
        redis.lpush(REDIS_BKEY_0, bB);
        redis.lpush(REDIS_BKEY_0, bA);

        long bcount = redis.lrem(REDIS_BKEY_0, -2, bhello);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bA);
        bexpected.add(bB);
        bexpected.add(bC);
        bexpected.add(bhello);
        bexpected.add(bx);

        assertEquals(2, bcount);
        assertEquals(bexpected, redis.lrange(REDIS_BKEY_0, 0, 1000));
        assertEquals(0, redis.lrem(REDIS_BVALUE_0, 100, REDIS_BKEY_0).intValue());

    }

    @Test
    public void lpop() {
        redis.rpush(REDIS_KEY_0, "a");
        redis.rpush(REDIS_KEY_0, "b");
        redis.rpush(REDIS_KEY_0, "c");

        String element = redis.lpop(REDIS_KEY_0);
        assertEquals("a", element);

        List<String> expected = new ArrayList<String>();
        expected.add("b");
        expected.add("c");

        assertEquals(expected, redis.lrange(REDIS_KEY_0, 0, 1000));
        redis.lpop(REDIS_KEY_0);
        redis.lpop(REDIS_KEY_0);

        element = redis.lpop(REDIS_KEY_0);
        assertEquals(null, element);

        // Binary
        redis.rpush(REDIS_BKEY_0, bA);
        redis.rpush(REDIS_BKEY_0, bB);
        redis.rpush(REDIS_BKEY_0, bC);

        byte[] belement = redis.lpop(REDIS_BKEY_0);
        assertArrayEquals(bA, belement);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bB);
        bexpected.add(bC);

        assertEquals(bexpected, redis.lrange(REDIS_BKEY_0, 0, 1000));
        redis.lpop(REDIS_BKEY_0);
        redis.lpop(REDIS_BKEY_0);

        belement = redis.lpop(REDIS_BKEY_0);
        assertEquals(null, belement);

    }

    @Test
    public void rpop() {
        redis.rpush(REDIS_KEY_0, "a");
        redis.rpush(REDIS_KEY_0, "b");
        redis.rpush(REDIS_KEY_0, "c");

        String element = redis.rpop(REDIS_KEY_0);
        assertEquals("c", element);

        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");

        assertEquals(expected, redis.lrange(REDIS_KEY_0, 0, 1000));
        redis.rpop(REDIS_KEY_0);
        redis.rpop(REDIS_KEY_0);

        element = redis.rpop(REDIS_KEY_0);
        assertEquals(null, element);

        // Binary
        redis.rpush(REDIS_BKEY_0, bA);
        redis.rpush(REDIS_BKEY_0, bB);
        redis.rpush(REDIS_BKEY_0, bC);

        byte[] belement = redis.rpop(REDIS_BKEY_0);
        assertArrayEquals(bC, belement);

        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bA);
        bexpected.add(bB);

        assertEquals(bexpected, redis.lrange(REDIS_BKEY_0, 0, 1000));
        redis.rpop(REDIS_BKEY_0);
        redis.rpop(REDIS_BKEY_0);

        belement = redis.rpop(REDIS_BKEY_0);
        assertEquals(null, belement);


        redis.del(REDIS_KEY_0);
        String value = "\"foo\":var";
        System.out.println(value);
        redis.rpush(REDIS_KEY_0, value);
        final String result = redis.rpop(REDIS_KEY_0);
        System.out.println(result);


    }

    @Test
    public void lpushx() {
        long status = redis.lpushx(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(0, status);

        redis.lpush(REDIS_KEY_0, "a");
        status = redis.lpushx(REDIS_KEY_0, "b");
        assertEquals(2, status);

        // Binary
        long bstatus = redis.lpushx(REDIS_BKEY_0, REDIS_BVALUE_0);
        assertEquals(0, bstatus);

        redis.lpush(REDIS_BKEY_0, bA);
        bstatus = redis.lpushx(REDIS_BKEY_0, bB);
        assertEquals(2, bstatus);

    }

    @Test
    public void rpushx() {
        long status = redis.rpushx(REDIS_KEY_0, REDIS_VALUE_0);
        assertEquals(0, status);

        redis.lpush(REDIS_KEY_0, "a");
        status = redis.rpushx(REDIS_KEY_0, "b");
        assertEquals(2, status);

        // Binary
        long bstatus = redis.rpushx(REDIS_BKEY_0, REDIS_BVALUE_0);
        assertEquals(0, bstatus);

        redis.lpush(REDIS_BKEY_0, bA);
        bstatus = redis.rpushx(REDIS_BKEY_0, bB);
        assertEquals(2, bstatus);
    }

    @Test
    public void linsert() {
        long status = redis.linsert(REDIS_KEY_0, Client.LIST_POSITION.BEFORE, REDIS_VALUE_0, REDIS_VALUE_1);
        assertEquals(0, status);

        redis.lpush(REDIS_KEY_0, "a");
        status = redis.linsert(REDIS_KEY_0, Client.LIST_POSITION.AFTER, "a", "b");
        assertEquals(2, status);

        List<String> actual = redis.lrange(REDIS_KEY_0, 0, 100);
        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");

        assertEquals(expected, actual);

        status = redis.linsert(REDIS_KEY_0, Client.LIST_POSITION.BEFORE, REDIS_VALUE_0, REDIS_VALUE_1);
        assertEquals(-1, status);

        // Binary
        long bstatus = redis.linsert(REDIS_BKEY_0, Client.LIST_POSITION.BEFORE, REDIS_BVALUE_0, REDIS_BVALUE_1);
        assertEquals(0, bstatus);

        redis.lpush(REDIS_BKEY_0, bA);
        bstatus = redis.linsert(REDIS_BKEY_0, Client.LIST_POSITION.AFTER, bA, bB);
        assertEquals(2, bstatus);

        List<byte[]> bactual = redis.lrange(REDIS_BKEY_0, 0, 100);
        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bA);
        bexpected.add(bB);

        Assert.assertEquals(bexpected, bactual);

        bstatus = redis.linsert(REDIS_BKEY_0, Client.LIST_POSITION.BEFORE, REDIS_BVALUE_0, REDIS_BVALUE_1);
        assertEquals(-1, bstatus);

    }
}

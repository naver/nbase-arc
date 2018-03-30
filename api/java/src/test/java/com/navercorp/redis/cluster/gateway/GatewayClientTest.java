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

package com.navercorp.redis.cluster.gateway;

import static com.navercorp.redis.cluster.connection.RedisProtocol.Keyword.INCRBY;
import static com.navercorp.redis.cluster.connection.RedisProtocol.Keyword.OVERFLOW;
import static com.navercorp.redis.cluster.connection.RedisProtocol.Keyword.SAT;
import static org.junit.Assert.*;
import static redis.clients.jedis.ScanParams.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.RedisClusterTestBase;
import com.navercorp.redis.cluster.async.AsyncAction;
import com.navercorp.redis.cluster.pipeline.RedisClusterPipeline;
import com.navercorp.redis.cluster.util.TestEnvUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.ScanOptions.ScanOptionsBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.Tuple;
import redis.clients.util.SafeEncoder;

import com.navercorp.redis.cluster.async.AsyncResult;
import com.navercorp.redis.cluster.async.AsyncResultHandler;

/**
 * @author seongminwoo
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-gatewayclient.xml")
public class GatewayClientTest {
    Logger log = LoggerFactory.getLogger(GatewayClientTest.class);

    /**
     *
     */
    private static final int EXPIRE_SEC = 1;

    private static final String KEY = "key";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";

    private static final String VALUE = "value1";
    private static final String VALUE2 = "value2";

    private static final String KEY_NUMBER = "keyint1";

    private static final long VALUE_NUMBER = 1;

    private static final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
    private static final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
    
    private static final byte[] ba = { 0x0A };
    private static final byte[] bb = { 0x0B };
    private static final byte[] bc = { 0x0C };

    private static final byte[] bInclusiveB = { 0x5B, 0x0B };
    private static final byte[] bExclusiveC = { 0x28, 0x0C };
    private static final byte[] bLexMinusInf = { 0x2D };
    private static final byte[] bLexPlusInf = { 0x2B };

    private static final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
    private static final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
    private static final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };
    private static final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };
    
    @Autowired
    GatewayClient gatewayClient;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        gatewayClient.del(KEY);
        gatewayClient.del(KEY2);
        gatewayClient.del(KEY3);
        gatewayClient.del("MyList");
        gatewayClient.del("myhash");
        gatewayClient.del("myset");
        gatewayClient.del("test");
        gatewayClient.del("pipeline_callback_incr");
        gatewayClient.ssdel("serviceCode");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void springCreateNotNull() {
        assertNotNull(gatewayClient);
    }

    @Test
    public void springCreateSetCommand() {
        assertNotNull(gatewayClient);
        String result = gatewayClient.execute(new RedisClusterCallback<String>() {
            public String doInRedisCluster(RedisCluster redisCluster) {
                String statusCode = redisCluster.setex("redis_key_0", 1, "redis_value_0");
                return statusCode;
            }

            public int getPartitionNumber() {
                return GatewayPartitionNumber.get("redis_key_0");
            }

            public AffinityState getState() {
                return AffinityState.WRITE;
            }
        });

        assertEquals("OK", result);
    }

    /**
     * JedisConnectionException Test
     */
    @Test
    public void testWrongGatewayOneIncludeAddress() {
        GatewayConfig config = new GatewayConfig();
        config.setIpAddress("1.1.1.1:6000, " + TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()); // invalid ip.
        GatewayClient client = new GatewayClient(config);
        Gateway gateway = client.getGateway();
        for (GatewayServer server : gateway.getServers()) {
            if (server.getAddress().getName().equals("1.1.1.1:6000")) {
                assertFalse(server.isValid());
            }
        }

        client.destroy();
    }

    @Test
    public void testDestroy() {
        GatewayConfig config = new GatewayConfig();
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());
        GatewayClient client = new GatewayClient(config);
        client.destroy();
    }

    /**
     * Test method for {@link GatewayClient#del(java.lang.String)}.
     */
    @Test
    public void testDelString() {
        gatewayClient.set(KEY, VALUE);
        assertEquals(VALUE, gatewayClient.get(KEY));
        gatewayClient.del(KEY);
        assertNull(gatewayClient.get(KEY));
    }

    /**
     * Test method for {@link GatewayClient#exists(java.lang.String)}.
     */
    @Test
    public void testExistsString() {
        gatewayClient.exists(KEY);
    }

    /**
     * Test method for {@link GatewayClient#expire(java.lang.String, int)}.
     */
    @Test
    public void testExpireStringInt() {
        gatewayClient.expire(KEY, EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#expireAt(java.lang.String, long)}.
     */
    @Test
    public void testExpireAtStringLong() {
        gatewayClient.expireAt(KEY, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#ttl(java.lang.String)}.
     */
    @Test
    public void testTtlString() {
        gatewayClient.ttl(KEY);
    }

    /**
     * Test method for {@link GatewayClient#type(java.lang.String)}.
     */
    @Test
    public void testTypeString() {
        gatewayClient.type(KEY);
    }

    /**
     * Test method for {@link GatewayClient#get(java.lang.String)}.
     */
    @Test
    public void testGetString() {
        gatewayClient.get(KEY);
    }

    /**
     * Test method for {@link GatewayClient#set(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSetStringString() {
        gatewayClient.set(KEY, VALUE);
    }

    /**
     * Test method for {@link GatewayClient#getSet(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testGetSetStringString() {
        gatewayClient.set(KEY, VALUE);
        String val = gatewayClient.getSet(KEY, VALUE2);
        assertEquals(VALUE, val);
        assertEquals(VALUE2, gatewayClient.get(KEY));
    }

    /**
     * Test method for {@link GatewayClient#append(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testAppendStringString() {
        gatewayClient.append(KEY, VALUE);
        String val = gatewayClient.get(KEY);
        assertEquals(VALUE, val);

        gatewayClient.append(KEY, VALUE2);
        String val2 = gatewayClient.get(KEY);
        assertEquals(VALUE + VALUE2, val2);
    }

    /**
     * Test method for {@link GatewayClient#setex(java.lang.String, int, java.lang.String)}.
     */
    @Test
    public void testSetexStringIntString() {
        gatewayClient.setex(KEY, EXPIRE_SEC, VALUE);
        assertEquals(VALUE, gatewayClient.get(KEY));

        try {
            Thread.sleep(1100);
        } catch (InterruptedException e) {
        }

        assertNull(gatewayClient.get(KEY));

    }

    /**
     * Test method for {@link GatewayClient#setnx(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSetnxStringString() {
        gatewayClient.setnx(KEY, VALUE);
    }

    /**
     * Test method for {@link GatewayClient#getrange(java.lang.String, long, long)}.
     */
    @Test
    public void testGetrangeStringLongLong() {
        gatewayClient.set(KEY, VALUE);
        String getrange = gatewayClient.getrange(KEY, 0, 2);
        assertEquals("val", getrange);
    }

    /**
     * Test method for {@link GatewayClient#objectRefcount(java.lang.String)}.
     */
    @Test
    public void testObjectRefcountString() {
        gatewayClient.objectRefcount(KEY);
    }

    /**
     * Test method for {@link GatewayClient#objectEncoding(java.lang.String)}.
     */
    @Test
    public void testObjectEncodingString() {
        gatewayClient.set(KEY, VALUE);
        String objectEncoding = gatewayClient.objectEncoding(KEY);
        System.out.println("objectEncoding : " + objectEncoding);
    }

    /**
     * Test method for {@link GatewayClient#objectIdletime(java.lang.String)}.
     */
    @Test
    public void testObjectIdletimeString() {
        gatewayClient.set(KEY, VALUE);
        Long objectIdletime = gatewayClient.objectIdletime(KEY);
        System.out.println("objectIdletime : " + objectIdletime);

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
        }

        objectIdletime = gatewayClient.objectIdletime(KEY);
        System.out.println("objectIdletime : " + objectIdletime);
    }

    /**
     * Test method for {@link GatewayClient#decr(java.lang.String)}.
     */
    @Test
    public void testDecrString() {
        gatewayClient.set(KEY_NUMBER, String.valueOf(VALUE_NUMBER));
        long decr = gatewayClient.decr(KEY_NUMBER);
        assertEquals(VALUE_NUMBER - 1, decr);
    }

    /**
     * Test method for {@link GatewayClient#decrBy(java.lang.String, long)}.
     */
    @Test
    public void testDecrByStringLong() {
        gatewayClient.set(KEY_NUMBER, String.valueOf(VALUE_NUMBER));
        long decrBy = gatewayClient.decrBy(KEY_NUMBER, 2);
        assertEquals(VALUE_NUMBER - 2, decrBy);
    }

    /**
     * Test method for {@link GatewayClient#incr(java.lang.String)}.
     */
    @Test
    public void testIncrString() {
        gatewayClient.set(KEY_NUMBER, String.valueOf(VALUE_NUMBER));
        long incr = gatewayClient.incr(KEY_NUMBER);
        assertEquals(VALUE_NUMBER + 1, incr);
    }

    @Test
    public void incrOverFlow() {
        gatewayClient.set(KEY_NUMBER, String.valueOf(Long.MAX_VALUE - 1));
        long value = gatewayClient.incr(KEY_NUMBER);
        assertEquals(Long.MAX_VALUE, value);
        
        try {
            gatewayClient.incr(KEY_NUMBER);
            fail("incr operation is limited to 64 bit signed integers.");
        } catch (GatewayException e) {
            assertTrue(e.getCause() instanceof JedisDataException);
        }
    }

    /**
     * Test method for {@link GatewayClient#incrBy(java.lang.String, long)}.
     */
    @Test
    public void testIncrByStringLong() {
        gatewayClient.set(KEY_NUMBER, String.valueOf(VALUE_NUMBER));
        long incrBy = gatewayClient.incrBy(KEY_NUMBER, 2);
        assertEquals(VALUE_NUMBER + 2, incrBy);
    }

    /**
     * Test method for {@link GatewayClient#setbit(java.lang.String, long, boolean)}.
     */
    @Test
    public void testSetbitStringLongBoolean() {
        gatewayClient.set(KEY, VALUE);
        gatewayClient.setbit(KEY, 2, true);
        Boolean beforeSetbit = gatewayClient.setbit(KEY, 2, false);
        assertEquals(true, beforeSetbit);

        Boolean getbit = gatewayClient.getbit(KEY, 2);
        assertEquals(false, getbit);
    }

    /**
     * Test method for {@link GatewayClient#setrange(java.lang.String, long, java.lang.String)}.
     */
    @Test
    public void testSetrangeStringLongString() {
        gatewayClient.set(KEY, "Hello World");
        gatewayClient.setrange(KEY, 6, "Redis");
        assertEquals("Hello Redis", gatewayClient.get(KEY));
    }

    /**
     * Test method for {@link GatewayClient#strlen(java.lang.String)}.
     */
    @Test
    public void testStrlenString() {
        gatewayClient.set(KEY, VALUE);
        long strlen = gatewayClient.strlen(KEY);
        assertEquals(VALUE.length(), strlen);
    }

    /**
     * Test method for {@link GatewayClient#lpush(java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testLpushStringStringArray() {
        long lpush = gatewayClient.lpush(KEY, VALUE, VALUE2);
        assertEquals(2, lpush);
    }

    /**
     * Test method for {@link GatewayClient#rpush(java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testRpushStringStringArray() {
        long rpush = gatewayClient.rpush(KEY, VALUE, VALUE2);
        assertEquals(2, rpush);
    }

    /**
     * Test method for {@link GatewayClient#lindex(java.lang.String, long)}.
     */
    @Test
    public void testLindexStringLong() {
        gatewayClient.rpush(KEY, VALUE, VALUE2);
        String lindex = gatewayClient.lindex(KEY, -1);
        assertEquals(VALUE2, lindex);
    }

    /**
     * Test method for {@link GatewayClient#linsert(java.lang.String, redis.clients.jedis.BinaryClient.LIST_POSITION, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testLinsertStringLIST_POSITIONStringString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hello"));
        actual.add(gatewayClient.rpush("MyList", "world"));
        actual.add(gatewayClient.linsert("MyList", LIST_POSITION.AFTER, "hello", "big"));
        actual.add(gatewayClient.lrange("MyList", 0, -1));
        actual.add(gatewayClient.linsert("MyList", LIST_POSITION.BEFORE, "big", "very"));
        actual.add(gatewayClient.lrange("MyList", 0, -1));
        assertEquals(
                Arrays.asList(new Object[]{1l, 2l, 3l, Arrays.asList(new String[]{"hello", "big", "world"}), 4l,
                        Arrays.asList(new String[]{"hello", "very", "big", "world"})}), actual);
    }

    /**
     * Test method for {@link GatewayClient#llen(java.lang.String)}.
     */
    @Test
    public void testLlenString() {
        gatewayClient.rpush("MyList", "hello");
        gatewayClient.rpush("MyList", "world");
        long llen = gatewayClient.llen("MyList");
        assertEquals(2, llen);
    }

    /**
     * Test method for {@link GatewayClient#lpop(java.lang.String)}.
     */
    @Test
    public void testLpopString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hello"));
        actual.add(gatewayClient.rpush("MyList", "world"));
        actual.add(gatewayClient.lpop("MyList"));
        assertEquals(Arrays.asList(new Object[]{1l, 2l, "hello"}), actual);
    }

    /**
     * Test method for {@link GatewayClient#lrange(java.lang.String, long, long)}.
     */
    @Test
    public void testLrangeStringLongLong() {
        gatewayClient.rpush("MyList", "hello");
        gatewayClient.rpush("MyList", "world");
        gatewayClient.rpush("MyList", "!");
        List<String> lrange = gatewayClient.lrange("MyList", 0, 1);
        assertEquals(2, lrange.size());
    }

    /**
     * Test method for {@link GatewayClient#lrem(java.lang.String, long, java.lang.String)}.
     */
    @Test
    public void testLremStringLongString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hello"));
        actual.add(gatewayClient.rpush("MyList", "big"));
        actual.add(gatewayClient.rpush("MyList", "world"));
        actual.add(gatewayClient.rpush("MyList", "hello"));
        actual.add(gatewayClient.lrem("MyList", 2, "hello"));
        actual.add(gatewayClient.lrange("MyList", 0, -1));
        assertEquals(Arrays.asList(new Object[]{1l, 2l, 3l, 4l, 2l, Arrays.asList(new String[]{"big", "world"})}),
                actual);
    }

    /**
     * Test method for {@link GatewayClient#lset(java.lang.String, long, java.lang.String)}.
     */
    @Test
    public void testLsetStringLongString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hello"));
        actual.add(gatewayClient.rpush("MyList", "big"));
        actual.add(gatewayClient.rpush("MyList", "world"));
        gatewayClient.lset("MyList", 1, "cruel");
        actual.add(gatewayClient.lrange("MyList", 0, -1));
        assertEquals(Arrays.asList(new Object[]{1l, 2l, 3l, Arrays.asList(new String[]{"hello", "cruel", "world"})}),
                actual);
    }

    /**
     * Test method for {@link GatewayClient#ltrim(java.lang.String, long, long)}.
     */
    @Test
    public void testLtrimStringLongLong() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hello"));
        actual.add(gatewayClient.rpush("MyList", "big"));
        actual.add(gatewayClient.rpush("MyList", "world"));
        gatewayClient.ltrim("MyList", 1, -1);
        actual.add(gatewayClient.lrange("MyList", 0, -1));
        assertEquals(Arrays.asList(new Object[]{1l, 2l, 3l, Arrays.asList(new String[]{"big", "world"})}), actual);
    }

    /**
     * Test method for {@link GatewayClient#rpop(java.lang.String)}.
     */
    @Test
    public void testRpopString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hello"));
        actual.add(gatewayClient.rpush("MyList", "world"));
        actual.add(gatewayClient.rpop("MyList"));
        assertEquals(Arrays.asList(new Object[]{1l, 2l, "world"}), actual);
    }

    /**
     * Test method for {@link GatewayClient#lpushx(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testLpushxStringString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hi"));
        actual.add(gatewayClient.lpushx("MyList", "foo"));
        actual.add(gatewayClient.lrange("MyList", 0, -1));
        assertEquals(Arrays.asList(new Object[]{1l, 2l, Arrays.asList(new String[]{"foo", "hi"})}), actual);
    }

    /**
     * Test method for {@link GatewayClient#rpushx(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testRpushxStringString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.rpush("MyList", "hi"));
        actual.add(gatewayClient.rpushx("MyList", "foo"));
        actual.add(gatewayClient.lrange("MyList", 0, -1));
        assertEquals(Arrays.asList(new Object[]{1l, 2l, Arrays.asList(new String[]{"hi", "foo"})}), actual);
    }

    /**
     * Test method for {@link GatewayClient#sadd(java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSaddStringStringArray() {
        gatewayClient.sadd("myset", "foo");
        gatewayClient.sadd("myset", "bar");
        assertEquals(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar"})), gatewayClient.smembers("myset"));
    }

    /**
     * Test method for {@link GatewayClient#scard(java.lang.String)}.
     */
    @Test
    public void testScardString() {
        gatewayClient.sadd("myset", "foo");
        gatewayClient.sadd("myset", "bar");
        assertEquals(Long.valueOf(2), gatewayClient.scard("myset"));
    }

    /**
     * Test method for {@link GatewayClient#sismember(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSismemberStringString() {
        gatewayClient.sadd("myset", "foo");
        gatewayClient.sadd("myset", "bar");
        assertTrue(gatewayClient.sismember("myset", "foo"));
        assertFalse(gatewayClient.sismember("myset", "baz"));
    }

    /**
     * Test method for {@link GatewayClient#smembers(java.lang.String)}.
     */
    @Test
    public void testSmembersString() {
        gatewayClient.sadd("myset", "foo");
        gatewayClient.sadd("myset", "bar");
        gatewayClient.smembers("myset");
    }

    /**
     * Test method for {@link GatewayClient#srandmember(java.lang.String)}.
     */
    @Test
    public void testSrandmemberString() {
        gatewayClient.sadd("myset", "foo");
        gatewayClient.sadd("myset", "bar");
        assertTrue(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar"})).contains(gatewayClient.srandmember("myset")));
    }

    /**
     * Test method for {@link GatewayClient#srem(java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSremStringStringArray() {
        gatewayClient.sadd("myset", "foo");
        gatewayClient.sadd("myset", "bar");
        assertTrue(gatewayClient.srem("myset", "foo") == 1);
        assertTrue(gatewayClient.srem("myset", "baz") != 1);
        assertEquals(new HashSet<String>(Collections.singletonList("bar")), gatewayClient.smembers("myset"));
    }

    /**
     * Test method for {@link GatewayClient#zadd(java.lang.String, double, java.lang.String)}.
     */
    @Test
    public void testZaddStringDoubleString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James", "Bob"})),
                gatewayClient.zrange("myset", 0, -1));
    }

    /**
     * Test method for {@link GatewayClient#zadd(java.lang.String, java.util.Map)}.
     */
    @Test
    public void testZaddStringMapOfDoubleString() {
        Map<Double, String> scoreMembers = new HashMap<Double, String>();
        scoreMembers.put(Double.valueOf(2), "Bob");
        scoreMembers.put(Double.valueOf(1), "James");
        gatewayClient.zadd("myset", scoreMembers);
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James", "Bob"})),
                gatewayClient.zrange("myset", 0, -1));
    }

    /**
     * Test method for {@link GatewayClient#zcard(java.lang.String)}.
     */
    @Test
    public void testZcardString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(Long.valueOf(2), gatewayClient.zcard("myset"));
    }

    /**
     * Test method for {@link GatewayClient#zcount(java.lang.String, double, double)}.
     */
    @Test
    public void testZcountStringDoubleDouble() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        gatewayClient.zadd("myset", 4, "Joe");
        assertEquals(Long.valueOf(2), gatewayClient.zcount("myset", 1, 2));
    }

    /**
     * Test method for {@link GatewayClient#zcount(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZcountStringStringString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        gatewayClient.zadd("myset", 4, "Joe");
        assertEquals(Long.valueOf(2), gatewayClient.zcount("myset", "1", "2"));
    }

    /**
     * Test method for {@link GatewayClient#zincrby(java.lang.String, double, java.lang.String)}.
     */
    @Test
    public void testZincrbyStringDoubleString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        gatewayClient.zadd("myset", 4, "Joe");
        gatewayClient.zincrby("myset", 2, "Joe");
        assertEquals(new LinkedHashSet<String>(Collections.singletonList("Joe")),
                gatewayClient.zrangeByScore("myset", 6, 6));
    }

    /**
     * Test method for {@link GatewayClient#zrange(java.lang.String, long, long)}.
     */
    @Test
    public void testZrangeStringLongLong() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James", "Bob"})),
                gatewayClient.zrange("myset", 0, -1));
    }

    /**
     * Test method for {@link GatewayClient#zrangeWithScores(java.lang.String, long, long)}.
     */
    @Test
    public void testZrangeWithScoresStringLongLong() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(
                new LinkedHashSet<Tuple>(Arrays.asList(new Tuple[]{new Tuple("James", 1d), new Tuple("Bob", 2d)})),
                gatewayClient.zrangeWithScores("myset", 0, -1));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScore(java.lang.String, double, double)}.
     */
    @Test
    public void testZrangeByScoreStringDoubleDouble() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James"})),
                gatewayClient.zrangeByScore("myset", 1, 1));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScore(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZrangeByScoreStringStringString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James"})),
                gatewayClient.zrangeByScore("myset", "1", "1"));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScore(java.lang.String, java.lang.String, java.lang.String, int, int)}.
     */
    @Test
    public void testZrangeByScoreStringStringStringIntInt() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James"})),
                gatewayClient.zrangeByScore("myset", 1, 1));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScoreWithScores(java.lang.String, double, double)}.
     */
    @Test
    public void testZrangeByScoreWithScoresStringDoubleDouble() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<Tuple>(Arrays.asList(new Tuple[]{new Tuple("Bob", 2d)})),
                gatewayClient.zrangeByScoreWithScores("myset", 2d, 5d));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZrangeByScoreWithScoresStringStringString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<Tuple>(Arrays.asList(new Tuple[]{new Tuple("Bob", 2d)})),
                gatewayClient.zrangeByScoreWithScores("myset", "2", "5"));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String, int, int)}.
     */
    @Test
    public void testZrangeByScoreWithScoresStringStringStringIntInt() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeWithScores(java.lang.String, long, long)}.
     */
    @Test
    public void testZrevrangeWithScoresStringLongLong() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(
                new LinkedHashSet<Tuple>(Arrays.asList(new Tuple[]{new Tuple("Bob", 2d), new Tuple("James", 1d)})),
                gatewayClient.zrevrangeWithScores("myset", 0, -1));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScore(java.lang.String, double, double, int, int)}.
     */
    @Test
    public void testZrangeByScoreStringDoubleDoubleIntInt() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James"})),
                gatewayClient.zrangeByScore("myset", 1, 1));
    }

    /**
     * Test method for {@link GatewayClient#zrangeByScoreWithScores(java.lang.String, double, double, int, int)}.
     */
    @Test
    public void testZrangeByScoreWithScoresStringDoubleDoubleIntInt() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(new LinkedHashSet<Tuple>(Arrays.asList(new Tuple[]{new Tuple("Bob", 2d)})),
                gatewayClient.zrangeByScoreWithScores("myset", 2d, 5d));
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScore(java.lang.String, double, double, int, int)}.
     */
    @Test
    public void testZrevrangeByScoreStringDoubleDoubleIntInt() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(
                new LinkedHashSet<Tuple>(Arrays.asList(new Tuple[]{new Tuple("Bob", 2d), new Tuple("James", 1d)})),
                gatewayClient.zrevrangeWithScores("myset", 0, -1));
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScore(java.lang.String, double, double)}.
     */
    @Test
    public void testZrevrangeByScoreStringDoubleDouble() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScore(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZrevrangeByScoreStringStringString() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScore(java.lang.String, java.lang.String, java.lang.String, int, int)}.
     */
    @Test
    public void testZrevrangeByScoreStringStringStringIntInt() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScoreWithScores(java.lang.String, double, double, int, int)}.
     */
    @Test
    public void testZrevrangeByScoreWithScoresStringDoubleDoubleIntInt() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScoreWithScores(java.lang.String, double, double)}.
     */
    @Test
    public void testZrevrangeByScoreWithScoresStringDoubleDouble() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZrevrangeByScoreWithScoresStringStringString() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrangeByScoreWithScores(java.lang.String, java.lang.String, java.lang.String, int, int)}.
     */
    @Test
    public void testZrevrangeByScoreWithScoresStringStringStringIntInt() {
    }

    /**
     * Test method for {@link GatewayClient#zrank(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZrankStringString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(Long.valueOf(0), gatewayClient.zrank("myset", "James"));
        assertEquals(Long.valueOf(1), gatewayClient.zrank("myset", "Bob"));
    }

    /**
     * Test method for {@link GatewayClient#zrem(java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testZremStringStringArray() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertTrue(gatewayClient.zrem("myset", "James") == 1);
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob"})),
                gatewayClient.zrange("myset", 0l, -1l));
    }

    /**
     * Test method for {@link GatewayClient#zremrangeByRank(java.lang.String, long, long)}.
     */
    @Test
    public void testZremrangeByRankStringLongLong() {
        gatewayClient.del("myset");
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        Long zRemRange = gatewayClient.zremrangeByRank("myset", 0l, 3l);
        assertEquals(Long.valueOf(2), zRemRange);
        assertTrue(gatewayClient.zrange("myset", 0l, -1l).isEmpty());
    }

    /**
     * Test method for {@link GatewayClient#zremrangeByScore(java.lang.String, double, double)}.
     */
    @Test
    public void testZremrangeByScoreStringDoubleDouble() {
        gatewayClient.del("myset");
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(Long.valueOf(1), gatewayClient.zremrangeByScore("myset", 0d, 1d));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob"})),
                gatewayClient.zrange("myset", 0l, -1l));
    }

    /**
     * Test method for {@link GatewayClient#zremrangeByScore(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZremrangeByScoreStringStringString() {
        gatewayClient.del("myset");
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        assertEquals(Long.valueOf(1), gatewayClient.zremrangeByScore("myset", "0", "1"));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob"})),
                gatewayClient.zrange("myset", 0l, -1l));
    }

    /**
     * Test method for {@link GatewayClient#zrevrange(java.lang.String, long, long)}.
     */
    @Test
    public void testZrevrangeStringLongLong() {
    }

    /**
     * Test method for {@link GatewayClient#zrevrank(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZrevrankStringString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        gatewayClient.zadd("myset", 3, "Joe");
        assertEquals(Long.valueOf(0), gatewayClient.zrevrank("myset", "Joe"));
    }

    /**
     * Test method for {@link GatewayClient#zscore(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testZscoreStringString() {
        gatewayClient.zadd("myset", 2, "Bob");
        gatewayClient.zadd("myset", 1, "James");
        gatewayClient.zadd("myset", 3, "Joe");
        assertEquals(Double.valueOf(3d), gatewayClient.zscore("myset", "Joe"));
    }

    /**
     * Test method for {@link GatewayClient#hset(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testHsetStringStringString() {
        List<Object> actual = new ArrayList<Object>();
        String hash = getClass() + ":hashtest";
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String value1 = "foo";
        String value2 = "bar";
        actual.add(gatewayClient.hset(hash, key1, value1));
        actual.add(gatewayClient.hset(hash, key2, value2));
        actual.add(gatewayClient.hget(hash, key1));
        actual.add(gatewayClient.hgetAll(hash));
        Map<String, String> expected = new HashMap<String, String>();
        expected.put(key1, value1);
        expected.put(key2, value2);
        gatewayClient.del(hash);
    }

    /**
     * Test method for {@link GatewayClient#hsetnx(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Ignore
    @Test
    public void testHsetnxStringStringString() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.hsetnx("myhash", "key1", "foo"));
        actual.add(gatewayClient.hsetnx("myhash", "key1", "bar"));
        actual.add(gatewayClient.hget("myhash", "key1"));
        assertEquals(Arrays.asList(new Object[]{1, 0, "foo"}), actual);
    }

    /**
     * Test method for {@link GatewayClient#hdel(java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testHdelStringStringArray() {
        gatewayClient.hset("test", "key", "val");
        assertTrue(gatewayClient.hdel("test", "key") == 1);
        assertTrue(gatewayClient.hdel("test", "foo") != 1);
        assertFalse(gatewayClient.hexists("test", "key"));
    }

    /**
     * Test method for {@link GatewayClient#hincrBy(java.lang.String, java.lang.String, long)}.
     */
    @Ignore
    @Test
    public void testHincrByStringStringLong() {
        List<Object> actual = new ArrayList<Object>();
        actual.add(gatewayClient.hset("test", "key", "2"));
        actual.add(gatewayClient.hincrBy("test", "key", 3l));
        actual.add(gatewayClient.hget("test", "key"));
        assertEquals(Arrays.asList(new Object[]{1, 5l, "5"}), actual);
    }

    /**
     * Test method for {@link GatewayClient#hkeys(java.lang.String)}.
     */
    @Test
    public void testHkeysString() {
        gatewayClient.hset("test", "key", "2");
        gatewayClient.hset("test", "key2", "2");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"key", "key2"})),
                gatewayClient.hkeys("test"));
    }

    /**
     * Test method for {@link GatewayClient#hlen(java.lang.String)}.
     */
    @Test
    public void testHlenString() {
    }

    /**
     * Test method for {@link GatewayClient#hmset(java.lang.String, java.util.Map)}.
     */
    @Test
    public void testHmsetStringMapOfStringString() {
        List<Object> actual = new ArrayList<Object>();
        Map<String, String> tuples = new HashMap<String, String>();
        tuples.put("key", "foo");
        tuples.put("key2", "bar");
        gatewayClient.hmset("test", tuples);
        actual.add(gatewayClient.hmget("test", "key", "key2"));
        assertEquals(Arrays.asList(new Object[]{Arrays.asList(new String[]{"foo", "bar"})}), actual);
    }

    /**
     * Test method for {@link GatewayClient#hvals(java.lang.String)}.
     */
    @Test
    public void testHvalsString() {
    }

    /**
     * Test method for {@link GatewayClient#slget(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlgetStringStringString() {
        gatewayClient.slget("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#slmget(java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSlmgetStringStringStringArray() {
        gatewayClient.slmget("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#slkeys(java.lang.String)}.
     */
    @Test
    public void testSlkeysString() {
        gatewayClient.slkeys("serviceCode");
    }

    /**
     * Test method for {@link GatewayClient#slkeys(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlkeysStringString() {
        gatewayClient.slkeys("serviceCode", "uid");
    }

    /**
     * Test method for {@link GatewayClient#sladd(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSladdStringStringStringStringArray() {
        gatewayClient.sladd("serviceCode", "uid", "key", "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#sladd(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])}.
     */
    @Test
    public void testSladdStringStringStringLongStringArray() {
        gatewayClient.sladd("serviceCode", "uid", "key", EXPIRE_SEC, "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#slset(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSlsetStringStringStringStringArray() {
        gatewayClient.slset("serviceCode", "uid", "key", "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#slset(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])}.
     */
    @Test
    public void testSlsetStringStringStringLongStringArray() {
        gatewayClient.slset("serviceCode", "uid", "key", EXPIRE_SEC, "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#sldel(java.lang.String)}.
     */
    @Test
    public void testSldelString() {
        gatewayClient.sldel("serviceCode");
    }

    /**
     * Test method for {@link GatewayClient#slrem(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlremStringString() {
        gatewayClient.slrem("serviceCode", "uid");
    }

    /**
     * Test method for {@link GatewayClient#slrem(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlremStringStringString() {
        gatewayClient.slrem("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#slrem(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSlremStringStringStringStringArray() {
        gatewayClient.slrem("serviceCode", "uid", "key", "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#slcount(java.lang.String)}.
     */
    @Test
    public void testSlcountString() {
        gatewayClient.slcount("serviceCode");
    }

    /**
     * Test method for {@link GatewayClient#slcount(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlcountStringString() {
        gatewayClient.slcount("serviceCode", "uid");
    }

    /**
     * Test method for {@link GatewayClient#slcount(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlcountStringStringString() {
        gatewayClient.slcount("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#slexists(java.lang.String, java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlexistsStringStringStringString() {
        gatewayClient.slexists("serviceCode", "uid", "key", "value1");
    }

    /**
     * Test method for {@link GatewayClient#slexpire(java.lang.String, long)}.
     */
    @Test
    public void testSlexpireStringLong() {
        gatewayClient.slexpire("serviceCode", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#slexpire(java.lang.String, java.lang.String, long)}.
     */
    @Test
    public void testSlexpireStringStringLong() {
        gatewayClient.slexpire("serviceCode", "uid", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#slexpire(java.lang.String, java.lang.String, java.lang.String, long)}.
     */
    @Test
    public void testSlexpireStringStringStringLong() {
        gatewayClient.slexpire("serviceCode", "uid", "key", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#slexpire(java.lang.String, java.lang.String, java.lang.String, java.lang.String, long)}.
     */
    @Test
    public void testSlexpireStringStringStringStringLong() {
        gatewayClient.slexpire("serviceCode", "uid", "key", "value1", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#slttl(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlttlStringStringString() {
        gatewayClient.slttl("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#slttl(java.lang.String, java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSlttlStringStringStringString() {
        gatewayClient.slttl("serviceCode", "uid", "key", "value1");
    }

    /**
     * Test method for {@link GatewayClient#ssget(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSsgetStringStringString() {
        gatewayClient.ssget("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#ssmget(java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSsmgetStringStringStringArray() {
        gatewayClient.ssmget("serviceCode", "uid", "key1", "key2");
    }

    /**
     * Test method for {@link GatewayClient#sskeys(java.lang.String)}.
     */
    @Test
    public void testSskeysString() {
        gatewayClient.sskeys("serviceCode");
    }

    /**
     * Test method for {@link GatewayClient#sskeys(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSskeysStringString() {
        gatewayClient.sskeys("serviceCode", "uid");
    }

    /**
     * Test method for {@link GatewayClient#ssadd(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSsaddStringStringStringStringArray() {
        gatewayClient.ssadd("serviceCode", "uid", "key", "value1");
    }

    /**
     * Test method for {@link GatewayClient#ssadd(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])}.
     */
    @Test
    public void testSsaddStringStringStringLongStringArray() {
        gatewayClient.ssadd("serviceCode", "uid", "key", EXPIRE_SEC, "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#ssset(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSssetStringStringStringStringArray() {
        gatewayClient.ssset("serviceCode", "uid", "key", "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#ssset(java.lang.String, java.lang.String, java.lang.String, long, java.lang.String[])}.
     */
    @Test
    public void testSssetStringStringStringLongStringArray() {
        gatewayClient.ssset("serviceCode", "uid", "key", EXPIRE_SEC, "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#ssdel(java.lang.String)}.
     */
    @Test
    public void testSsdelString() {
        gatewayClient.ssdel("serviceCode");
    }

    /**
     * Test method for {@link GatewayClient#ssrem(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSsremStringString() {
        gatewayClient.ssrem("serviceCode", "uid");
    }

    /**
     * Test method for {@link GatewayClient#ssrem(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSsremStringStringString() {
        gatewayClient.ssrem("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#ssrem(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])}.
     */
    @Test
    public void testSsremStringStringStringStringArray() {
        gatewayClient.ssrem("serviceCode", "uid", "key", "value1", "value2");
    }

    /**
     * Test method for {@link GatewayClient#sscount(java.lang.String)}.
     */
    @Test
    public void testSscountString() {
        gatewayClient.sscount("serviceCode");
    }

    /**
     * Test method for {@link GatewayClient#sscount(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSscountStringString() {
        gatewayClient.sscount("serviceCode", "uid");
    }

    /**
     * Test method for {@link GatewayClient#sscount(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSscountStringStringString() {
        gatewayClient.sscount("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#ssexists(java.lang.String, java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSsexistsStringStringStringString() {
        gatewayClient.ssexists("serviceCode", "uid", "key", "value1");
    }

    /**
     * Test method for {@link GatewayClient#ssexpire(java.lang.String, long)}.
     */
    @Test
    public void testSsexpireStringLong() {
        gatewayClient.ssexpire("serviceCode", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#ssexpire(java.lang.String, java.lang.String, long)}.
     */
    @Test
    public void testSsexpireStringStringLong() {
        gatewayClient.ssexpire("serviceCode", "uid", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#ssexpire(java.lang.String, java.lang.String, java.lang.String, long)}.
     */
    @Test
    public void testSsexpireStringStringStringLong() {
        gatewayClient.ssexpire("serviceCode", "uid", "key", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#ssexpire(java.lang.String, java.lang.String, java.lang.String, java.lang.String, long)}.
     */
    @Test
    public void testSsexpireStringStringStringStringLong() {
        gatewayClient.ssexpire("serviceCode", "uid", "key", "value1", EXPIRE_SEC);
    }

    /**
     * Test method for {@link GatewayClient#ssttl(java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSsttlStringStringString() {
        gatewayClient.ssttl("serviceCode", "uid", "key");
    }

    /**
     * Test method for {@link GatewayClient#ssttl(java.lang.String, java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testSsttlStringStringStringString() {
        gatewayClient.ssttl("serviceCode", "uid", "key", "value1");
    }

    @Test
    public void testMsetStringArrays() {
        String mset = gatewayClient.mset(KEY, VALUE, KEY2, VALUE2);
        log.debug("mset : {}", mset);

        assertEquals(VALUE, gatewayClient.get(KEY));
        assertEquals(VALUE2, gatewayClient.get(KEY2));

        assertArrayEquals(SafeEncoder.encode(VALUE), gatewayClient.get(SafeEncoder.encode(KEY)));
        assertArrayEquals(SafeEncoder.encode(VALUE2), gatewayClient.get(SafeEncoder.encode(KEY2)));
    }

    @Test
    public void testMsetStringByteArrays() {
        String mset = gatewayClient.mset(SafeEncoder.encode(KEY), SafeEncoder.encode(VALUE), SafeEncoder.encode(KEY2),
                SafeEncoder.encode(VALUE2));
        log.debug("mset : {}", mset);

        assertArrayEquals(SafeEncoder.encode(VALUE), gatewayClient.get(SafeEncoder.encode(KEY)));
        assertArrayEquals(SafeEncoder.encode(VALUE2), gatewayClient.get(SafeEncoder.encode(KEY2)));

        assertEquals(VALUE, gatewayClient.get(KEY));
        assertEquals(VALUE2, gatewayClient.get(KEY2));
    }

    @Test
    public void testMgetStringArrays() {
        String mset = gatewayClient.mset(KEY, VALUE, KEY2, VALUE2);
        log.debug("mset : {}", mset);

        List<String> mget = gatewayClient.mget(KEY, KEY2);
        assertEquals(VALUE, mget.get(0));
        assertEquals(VALUE2, mget.get(1));
    }

    @Test
    public void testMgetStringArraysNoData() {
        List<String> mget = gatewayClient.mget(KEY);
        log.debug("mget : {}", mget);
        assertNotNull(mget);
        assertEquals(null, mget.get(0));
    }

    @Test
    public void testMgetStringByteArrays() {
        String mset = gatewayClient.mset(SafeEncoder.encode(KEY), SafeEncoder.encode(VALUE), SafeEncoder.encode(KEY2),
                SafeEncoder.encode(VALUE2));
        log.debug("mset : {}", mset);

        List<byte[]> mgetByte = gatewayClient.mget(SafeEncoder.encode(KEY), SafeEncoder.encode(KEY2));
        assertArrayEquals(SafeEncoder.encode(VALUE), mgetByte.get(0));
        assertArrayEquals(SafeEncoder.encode(VALUE2), mgetByte.get(1));
    }

    @Test
    public void testMgetStringByteWithNotExistingKeyArrays() {
        String mset = gatewayClient.mset(SafeEncoder.encode(KEY), SafeEncoder.encode(VALUE), SafeEncoder.encode(KEY2),
                SafeEncoder.encode(VALUE2));
        log.debug("mset : {}", mset);

        List<byte[]> mgetByte = gatewayClient.mget(SafeEncoder.encode(KEY), SafeEncoder.encode(KEY2),
                SafeEncoder.encode(KEY3));
        assertArrayEquals(SafeEncoder.encode(VALUE), mgetByte.get(0));
        assertArrayEquals(SafeEncoder.encode(VALUE2), mgetByte.get(1));
        assertEquals(null, mgetByte.get(2));
    }

    @Test
    public void testDelStringArray() {
        String mset = gatewayClient.mset(KEY, VALUE, KEY2, VALUE2);
        log.debug("mset : {}", mset);

        assertEquals(VALUE, gatewayClient.get(KEY));
        assertEquals(VALUE2, gatewayClient.get(KEY2));

        gatewayClient.del(KEY, KEY2);

        assertNull(gatewayClient.get(KEY));
        assertNull(gatewayClient.get(KEY2));
    }

    @Test
    public void retry() {
        GatewayConfig config = new GatewayConfig();
        config.setIpAddress("111.111.111.111:1111"); // invalid address.
        config.setMaxRetry(3);
        GatewayClient client = new GatewayClient(config);
        try {
            client.get("foo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelStringByteArray() {
        String mset = gatewayClient.mset(KEY, VALUE, KEY2, VALUE2);
        log.debug("mset : {}", mset);

        assertEquals(VALUE, gatewayClient.get(KEY));
        assertEquals(VALUE2, gatewayClient.get(KEY2));

        gatewayClient.del(SafeEncoder.encode(KEY), SafeEncoder.encode(KEY2));
        assertNull(gatewayClient.get(KEY));
        assertNull(gatewayClient.get(KEY2));

        assertNull(gatewayClient.get(SafeEncoder.encode(KEY)));
        assertNull(gatewayClient.get(SafeEncoder.encode(KEY2)));
    }

    @Test
    public void doPipeline() {
        RedisClusterPipeline pipeline = gatewayClient.pipeline();

        try {
            pipeline.incr("test");
            pipeline.incr("test");
            pipeline.incr("test");
            pipeline.incr("test");

            List<Object> result = pipeline.syncAndReturnAll();
            log.debug("result : {}", result);
            Object[] expected = {1L, 2L, 3L, 4L};
            assertArrayEquals(expected, result.toArray());
        } finally {
            pipeline.close();
        }
    }

    @Test
    public void pipelineCallback() {
        List<Object> result = gatewayClient.pipelineCallback(new GatewayPipelineCallback() {

            @Override
            public void doInRedisCluster(RedisClusterPipeline pipeline) {
                pipeline.incr("pipeline_callback_incr");
                pipeline.incr("pipeline_callback_incr");
                pipeline.incr("pipeline_callback_incr");
                pipeline.incr("pipeline_callback_incr");
            }
        });

        log.debug("result : {}", result);
        Object[] expected = {1L, 2L, 3L, 4L};
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void bitfieldCommands() {
        List<Long> reply = gatewayClient.bitfield(KEY, INCRBY.toString(), "u2", "100", "1", OVERFLOW.toString(),
                SAT.toString(), INCRBY.toString(), "u2", "102", "1");
        assertEquals(2, reply.size());
        assertTrue(1 == reply.get(0));
        assertTrue(1 == reply.get(1));

        reply = gatewayClient.bitfield(KEY.getBytes(), INCRBY.raw, "u2".getBytes(), "100".getBytes(), "1".getBytes(),
                OVERFLOW.raw, SAT.raw, INCRBY.raw, "u2".getBytes(), "102".getBytes(), "1".getBytes());
        assertEquals(2, reply.size());
        assertTrue(2 == reply.get(0));
        assertTrue(2 == reply.get(1));
    }
    
    @Test
    public void geoCommands() {
        double longitude = 1.0;
        double latitude = 2.0;
        String member = "Seoul";
        assertEquals(Long.valueOf(1), gatewayClient.geoadd(KEY, longitude, latitude, member));

        Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<String, GeoCoordinate>();
        memberCoordinateMap.put("Daejon", new GeoCoordinate(2.0, 3.0));
        memberCoordinateMap.put("Daegu", new GeoCoordinate(30.0, 40.0));
        assertEquals(Long.valueOf(2), gatewayClient.geoadd(KEY, memberCoordinateMap));
        assertEquals(157222, gatewayClient.geodist(KEY, "Seoul", "Daejon").intValue());
        assertEquals(157, gatewayClient.geodist(KEY, "Seoul", "Daejon", GeoUnit.KM).intValue());
        
        List<String> hashes = gatewayClient.geohash(KEY, "Seoul", "Daejon");
        assertEquals("s02equ04ve0", hashes.get(0));
        assertEquals("s093jd0k720", hashes.get(1));
        
        List<GeoCoordinate> coords = gatewayClient.geopos(KEY, "Seoul", "Daejon");
        assertEquals(2, coords.size());
        
        List<GeoRadiusResponse> members = gatewayClient.georadius(KEY, 1.0, 2.0, 1000.0, GeoUnit.KM);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());

        members = gatewayClient.georadius(KEY, 1.0, 2.0, 1000.0, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
        
        members = gatewayClient.georadiusByMember(KEY, "Seoul", 1000.0, GeoUnit.KM);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
        
        members = gatewayClient.georadiusByMember(KEY, "Seoul", 1000.0, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
    }
    
    @Test
    public void hyperLogLogCommands() {
        assertEquals(Long.valueOf(1), gatewayClient.pfadd(KEY, "a", "b", "c", "d", "e", "f", "g"));
        assertEquals(Long.valueOf(7), gatewayClient.pfcount(KEY));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void hscan() {
        gatewayClient.hset(KEY, "b", "b");
        gatewayClient.hset(KEY, "a", "a");

        ScanResult<Map.Entry<String, String>> result = gatewayClient.hscan(KEY, SCAN_POINTER_START);

        assertEquals(0, result.getCursor());
        assertFalse(result.getResult().isEmpty());

        // binary
        gatewayClient.hset(KEY.getBytes(), bbar, bcar);

        ScanResult<Map.Entry<byte[], byte[]>> bResult = gatewayClient.hscan(KEY.getBytes(), SCAN_POINTER_START_BINARY);

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
    }

    @Test
    public void scan() {
        gatewayClient.set(KEY, "b");
        gatewayClient.set(KEY2, "a");

        ScanResult<String> result = gatewayClient.scan(SCAN_POINTER_START);

        assertNotNull(result.getStringCursor());
        assertFalse(result.getResult().isEmpty());

        // binary
        ScanResult<byte[]> bResult = gatewayClient.scan(SCAN_POINTER_START_BINARY);

        assertNotNull(bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
    }

    @Test
    public void cscan() {
        assertTrue(gatewayClient.cscanlen() > 0);
        assertNotNull(gatewayClient.cscandigest());
        
        // String
        gatewayClient.set(KEY, "b");
        gatewayClient.set(KEY2, "a");

        ScanResult<String> result = gatewayClient.cscan(0, SCAN_POINTER_START);
        assertNotNull(result.getStringCursor());
        assertFalse(result.getResult().isEmpty());
        
        result = gatewayClient.cscan(0, SCAN_POINTER_START, new ScanParams().match(KEY + "*").count(10));
        assertNotNull(result.getStringCursor());
        assertFalse(result.getResult().isEmpty());

        // binary
        ScanResult<byte[]> bResult = gatewayClient.cscan(0, SCAN_POINTER_START_BINARY);
        assertNotNull(bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        bResult = gatewayClient.cscan(0, SCAN_POINTER_START_BINARY, new ScanParams().match(KEY + "*").count(10));
        assertNotNull(bResult.getStringCursor());
        assertFalse(bResult.getResult().isEmpty());
    }
    
    @Test
    public void sscan() {
        gatewayClient.sadd(KEY, "a", "b");

        ScanResult<String> result = gatewayClient.sscan(KEY, SCAN_POINTER_START);

        assertEquals(SCAN_POINTER_START, result.getStringCursor());
        assertFalse(result.getResult().isEmpty());

        // binary
        gatewayClient.sadd(KEY.getBytes(), ba, bb);

        ScanResult<byte[]> bResult = gatewayClient.sscan(KEY.getBytes(), SCAN_POINTER_START_BINARY);

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
    }

    @Test
    public void zscan() {
        gatewayClient.zadd(KEY, 1, "a");
        gatewayClient.zadd(KEY, 2, "b");

        ScanResult<Tuple> result = gatewayClient.zscan(KEY, SCAN_POINTER_START);

        assertEquals(SCAN_POINTER_START, result.getStringCursor());
        assertFalse(result.getResult().isEmpty());

        // binary
        gatewayClient.zadd(KEY.getBytes(), 1, ba);
        gatewayClient.zadd(KEY.getBytes(), 1, bb);

        ScanResult<Tuple> bResult = gatewayClient.zscan(KEY.getBytes(), SCAN_POINTER_START_BINARY);

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
    }

    @Test
    public void zscanMatch() {
        ScanParams params = new ScanParams();
        params.match("a*");

        gatewayClient.zadd(KEY, 2, "b");
        gatewayClient.zadd(KEY, 1, "a");
        gatewayClient.zadd(KEY, 11, "aa");
        ScanResult<Tuple> result = gatewayClient.zscan(KEY, SCAN_POINTER_START, params);

        assertEquals(SCAN_POINTER_START, result.getStringCursor());
        assertFalse(result.getResult().isEmpty());

        // binary
        params = new ScanParams();
        params.match(bbarstar);

        gatewayClient.zadd(KEY.getBytes(), 2, bbar1);
        gatewayClient.zadd(KEY.getBytes(), 1, bbar2);
        gatewayClient.zadd(KEY.getBytes(), 11, bbar3);
        ScanResult<Tuple> bResult = gatewayClient.zscan(KEY.getBytes(), SCAN_POINTER_START_BINARY, params);

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
    }

    @Test
    public void zscanCount() {
        ScanParams params = new ScanParams();
        params.count(2);

        gatewayClient.zadd(KEY, 1, "a1");
        gatewayClient.zadd(KEY, 2, "a2");
        gatewayClient.zadd(KEY, 3, "a3");
        gatewayClient.zadd(KEY, 4, "a4");
        gatewayClient.zadd(KEY, 5, "a5");

        ScanResult<Tuple> result = gatewayClient.zscan(KEY, SCAN_POINTER_START, params);

        assertFalse(result.getResult().isEmpty());

        // binary
        params = new ScanParams();
        params.count(2);

        gatewayClient.zadd(KEY.getBytes(), 2, bbar1);
        gatewayClient.zadd(KEY.getBytes(), 1, bbar2);
        gatewayClient.zadd(KEY.getBytes(), 11, bbar3);

        ScanResult<Tuple> bResult = gatewayClient.zscan(KEY.getBytes(), SCAN_POINTER_START_BINARY, params);

        assertFalse(bResult.getResult().isEmpty());
    }

    @Test
    public void zlexcount() {
        gatewayClient.zadd(KEY, 1, "a");
        gatewayClient.zadd(KEY, 1, "b");
        gatewayClient.zadd(KEY, 1, "c");
        gatewayClient.zadd(KEY, 1, "aa");

        long result = gatewayClient.zlexcount(KEY, "[aa", "(c");
        assertEquals(2, result);

        result = gatewayClient.zlexcount(KEY, "-", "+");
        assertEquals(4, result);

        result = gatewayClient.zlexcount(KEY, "-", "(c");
        assertEquals(3, result);

        result = gatewayClient.zlexcount(KEY, "[aa", "+");
        assertEquals(3, result);
    }

    @Test
    public void zlexcountBinary() {
        // Binary
        gatewayClient.zadd(KEY.getBytes(), 1, ba);
        gatewayClient.zadd(KEY.getBytes(), 1, bc);
        gatewayClient.zadd(KEY.getBytes(), 1, bb);

        long result = gatewayClient.zlexcount(KEY.getBytes(), bInclusiveB, bExclusiveC);
        assertEquals(1, result);

        result = gatewayClient.zlexcount(KEY.getBytes(), bLexMinusInf, bLexPlusInf);
        assertEquals(3, result);
    }

    @Test
    public void zremrangeByLex() {
        gatewayClient.zadd(KEY, 1, "a");
        gatewayClient.zadd(KEY, 1, "b");
        gatewayClient.zadd(KEY, 1, "c");
        gatewayClient.zadd(KEY, 1, "aa");

        long result = gatewayClient.zremrangeByLex(KEY, "[aa", "(c");

        assertEquals(2, result);

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("a");
        expected.add("c");

        assertEquals(expected, gatewayClient.zrangeByLex(KEY, "-", "+"));
    }

    @Test
    public void zrangeByLexBinary() {
        // binary
        gatewayClient.zadd(KEY.getBytes(), 1, ba);
        gatewayClient.zadd(KEY.getBytes(), 1, bc);
        gatewayClient.zadd(KEY.getBytes(), 1, bb);

        Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
        bExpected.add(bb);

        RedisClusterTestBase.assertByteArraySetEquals(bExpected, gatewayClient.zrangeByLex(KEY.getBytes(), bInclusiveB, bExclusiveC));

        bExpected.clear();
        bExpected.add(ba);
        bExpected.add(bb);

        // with LIMIT
        RedisClusterTestBase.assertByteArraySetEquals(bExpected, gatewayClient.zrangeByLex(KEY.getBytes(), bLexMinusInf, bLexPlusInf, 0, 2));
    }

    @Test
    public void zremrangeByLexBinary() {
        gatewayClient.zadd(KEY.getBytes(), 1, ba);
        gatewayClient.zadd(KEY.getBytes(), 1, bc);
        gatewayClient.zadd(KEY.getBytes(), 1, bb);

        long bresult = gatewayClient.zremrangeByLex(KEY.getBytes(), bInclusiveB, bExclusiveC);

        assertEquals(1, bresult);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(ba);
        bexpected.add(bc);

        RedisClusterTestBase.assertByteArraySetEquals(bexpected, gatewayClient.zrangeByLex(KEY.getBytes(), bLexMinusInf, bLexPlusInf));
    }

    @Test
    public void zrevrangeByLex() {
        gatewayClient.zadd(KEY, 1, "aa");
        gatewayClient.zadd(KEY, 1, "c");
        gatewayClient.zadd(KEY, 1, "bb");
        gatewayClient.zadd(KEY, 1, "d");

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("c");
        expected.add("bb");

        // exclusive aa ~ inclusive c
        assertEquals(expected, gatewayClient.zrevrangeByLex(KEY, "[c", "(aa"));

        expected.clear();
        expected.add("c");
        expected.add("bb");

        // with LIMIT
        assertEquals(expected, gatewayClient.zrevrangeByLex(KEY, "+", "-", 1, 2));
    }

    @Test
    public void zrevrangeByLexBinary() {
        // binary
        gatewayClient.zadd(KEY.getBytes(), 1, ba);
        gatewayClient.zadd(KEY.getBytes(), 1, bc);
        gatewayClient.zadd(KEY.getBytes(), 1, bb);

        Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
        bExpected.add(bb);

        RedisClusterTestBase.assertByteArraySetEquals(bExpected, gatewayClient.zrevrangeByLex(KEY.getBytes(), bExclusiveC, bInclusiveB));

        bExpected.clear();
        bExpected.add(bb);
        bExpected.add(ba);

        // with LIMIT
        RedisClusterTestBase.assertByteArraySetEquals(bExpected,
                gatewayClient.zrevrangeByLex(KEY.getBytes(), bLexPlusInf, bLexMinusInf, 0, 2));
    }

    @Test
    public void hstrlen() {
        gatewayClient.hset(KEY, "myhash", "k1");
        Long response = gatewayClient.hstrlen("myhash", "k1");
        assertEquals(0l, response.longValue());
    }

    @Test
    public void touch() {
        gatewayClient.set(KEY, VALUE);
        gatewayClient.set(KEY2, VALUE);
        gatewayClient.set(KEY3, VALUE);
        assertEquals(Long.valueOf(3), gatewayClient.touch(KEY, KEY2, KEY3));
    }

    @Test
    public void doAsync() {
        AsyncResultHandler<String> handler = new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> result) {
                if (result.isFailed()) {
                    System.out.println(result.getCause());
                } else {
                    System.out.println(result.getResult());
                }
            }
        };

        new AsyncAction<String>(gatewayClient, handler) {
            public String action() throws Exception {
                gatewayClient.del("foo");
                gatewayClient.set("foo", "bar");
                return gatewayClient.get("foo");
            }
        }.run();
    }
    
    @Test
    public void scanWholeMatchingKeys() {
        String[] keys = {"test:string:scan1", "test:string:scan2", "test:string:scan3"};
        ScanOptions options = new ScanOptionsBuilder()
                .match("test:string:scan*")
                .count(2)
                .build();
        
        for (int i = 0; i < keys.length; i++) {
            gatewayClient.del(keys[i]);
        }

        // Scan
        List<String> scanResults = new ArrayList<String>();
        for (int i = 0; i < keys.length; i++) {
            gatewayClient.setrange(keys[i], 5, keys[i]);
        }
        
        ScanResult<String> result = gatewayClient.scan("0", JedisConverters.toScanParams(options));
        while (result.getCursor() != 0 || result.getResult().size() > 0) {
            if (result.getResult().size() > 0) {
                scanResults.addAll(result.getResult());
            }
            result = gatewayClient.scan(result.getStringCursor(), JedisConverters.toScanParams(options));
            
            if (result.getCursor() == 0) {
                break;
            }
        }
        
        for (String key : keys) {
            assertTrue(key + " isn't scaned", scanResults.contains(key));
        }
    }
}

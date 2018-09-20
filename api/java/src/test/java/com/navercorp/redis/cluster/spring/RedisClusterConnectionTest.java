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

package com.navercorp.redis.cluster.spring;

import static org.junit.Assert.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.*;
import static org.springframework.data.geo.Metrics.*;
import static org.springframework.data.redis.core.ScanOptions.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.DefaultSortParameters;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.ScanOptions.ScanOptionsBuilder;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.gateway.GatewayClient;

/**
 * @author seongminwoo
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class RedisClusterConnectionTest {

    @Autowired
    RedisClusterConnectionFactory connectionFactory;
    RedisClusterConnection byteConnection;

    protected StringRedisConnection connection;
    protected RedisSerializer<Object> serializer = new JdkSerializationRedisSerializer();
    protected RedisSerializer<String> stringSerializer = new StringRedisSerializer();

    private static final byte[] EMPTY_ARRAY = new byte[0];

    protected List<Object> actual = new ArrayList<Object>();
    
    private final String KEY = "test_key";

    static final String KEY_1 = "key-1";
    static final String KEY_2 = "key-2";
    static final String KEY_3 = "key-3";
    static final String SAME_SLOT_KEY_1 = "{key}-1";
    static final String SAME_SLOT_KEY_2 = "{key}-2";
    static final String SAME_SLOT_KEY_3 = "{key}-3";
    static final String VALUE_1 = "value-1";
    static final String VALUE_2 = "value-2";
    static final String VALUE_3 = "value-3";
    
    static final byte[] KEY_1_BYTES = JedisConverters.toBytes(KEY_1);
    static final byte[] KEY_2_BYTES = JedisConverters.toBytes(KEY_2);
    static final byte[] KEY_3_BYTES = JedisConverters.toBytes(KEY_3);

    static final byte[] SAME_SLOT_KEY_1_BYTES = JedisConverters.toBytes(SAME_SLOT_KEY_1);
    static final byte[] SAME_SLOT_KEY_2_BYTES = JedisConverters.toBytes(SAME_SLOT_KEY_2);
    static final byte[] SAME_SLOT_KEY_3_BYTES = JedisConverters.toBytes(SAME_SLOT_KEY_3);

    static final byte[] VALUE_1_BYTES = JedisConverters.toBytes(VALUE_1);
    static final byte[] VALUE_2_BYTES = JedisConverters.toBytes(VALUE_2);
    static final byte[] VALUE_3_BYTES = JedisConverters.toBytes(VALUE_3);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        byteConnection = connectionFactory.getConnection();
        connection = new DefaultStringRedisConnection(byteConnection);
        clear();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        clear();
        connection.close();
        connection = null;
    }


    private void clear() {
        connection.del("sfoo");
        connection.del("sortlist");
        connection.del("dbparam");
        connection.del("PopList");
        connection.del("poplist");
        connection.del("MyList");
        connection.del("mylist");
        connection.del("myset");
        connection.del("otherset");
        connection.del("myhash");
        connection.del("alist");
        connection.del("test");
        connection.del("keytest");
        connection.del("some");
        connection.del("renametest");
        connection.del("newnxtest");
        connection.del("exp3");
        connection.del("pop2");
        connection.del("testlist");
        connection.del("test_incr_key");
        connection.del("geo");
        
        connection.del(KEY);
        connection.del(KEY_1);
        connection.del(KEY_2);
        connection.del(KEY_3);
        connection.del(SAME_SLOT_KEY_1);
        connection.del(SAME_SLOT_KEY_2);
        connection.del(SAME_SLOT_KEY_3);
        connection.del(KEY_1_BYTES);
        connection.del(KEY_2_BYTES);
        connection.del(KEY_3_BYTES);
        connection.del(SAME_SLOT_KEY_1_BYTES);
        connection.del(SAME_SLOT_KEY_2_BYTES);
        connection.del(SAME_SLOT_KEY_3_BYTES);
    }

    @Test
    public void testExpire() throws Exception {
        connection.set("exp", "true");
        assertTrue(connection.expire("exp", 1));
        assertFalse(exists("exp", 3000l));

        connection.del("exp");
    }

    @Test
    public void testExpireAt() throws Exception {
        connection.set("exp2", "true");
        assertTrue(connection.expireAt("exp2", System.currentTimeMillis() / 1000 + 1));
        assertFalse(exists("exp2", 3000l));

        connection.del("exp2");
    }

    @Test
    public void testPersist() throws Exception {
        connection.set("exp3", "true");
        actual.add(connection.expire("exp3", 1));
        actual.add(connection.persist("exp3"));
        Thread.sleep(1500);
        actual.add(connection.exists("exp3"));
        verifyResults(Arrays.asList(new Object[]{true, true, true}), actual);

        connection.del("exp3");
    }

    @Test
    public void testSetEx() throws Exception {
        connection.setEx("expy", 1l, "yep");
        assertEquals("yep", connection.get("expy"));
        assertFalse(exists("expy", 3000l));

        connection.del("expy");
    }

	@Test
	public void setWithOptions() {
		// XX
		connection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(60), SetOption.ifAbsent());
		assertArrayEquals(VALUE_1_BYTES, connection.get(KEY_1_BYTES));
		assertTrue(connection.pTtl(KEY_1_BYTES) > 0);

		// XX for an already existing key. The value of KEY_1_BYTES must not be changed.
		connection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.seconds(60), SetOption.ifAbsent());
		assertArrayEquals(VALUE_1_BYTES, connection.get(KEY_1_BYTES));

		// NX for an already existing key. The valud of KEY_1_BYTES must be changed.
		connection.set(KEY_1_BYTES, VALUE_3_BYTES, Expiration.seconds(60), SetOption.ifPresent());
		assertArrayEquals(VALUE_3_BYTES, connection.get(KEY_1_BYTES));

		// NX for not existing key.
		connection.del(KEY_1_BYTES);
		connection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.seconds(60), SetOption.ifPresent());
		assertEquals(Boolean.FALSE, connection.exists(KEY_1_BYTES));

		// No expiration
		connection.del(KEY_1_BYTES);
		connection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.persistent(), SetOption.upsert());
		assertEquals(Long.valueOf(-1), connection.pTtl(KEY_1_BYTES));
		
		// pipeline mode
		connection.openPipeline();
		try {
			connection.set(KEY_1_BYTES, VALUE_1_BYTES, Expiration.seconds(60), SetOption.ifAbsent());
			connection.get(KEY_1_BYTES);
			connection.set(KEY_1_BYTES, VALUE_2_BYTES, Expiration.seconds(60), SetOption.ifAbsent());
			connection.get(KEY_1_BYTES);
			connection.set(KEY_1_BYTES, VALUE_3_BYTES, Expiration.seconds(60), SetOption.ifPresent());
			connection.get(KEY_1_BYTES);
		} finally {
			List<Object> results = connection.closePipeline();
			assertEquals(3, results.size());
			assertArrayEquals(VALUE_1_BYTES, (byte[]) results.get(0));
			assertArrayEquals(VALUE_1_BYTES, (byte[]) results.get(1));
			assertArrayEquals(VALUE_3_BYTES, (byte[]) results.get(2));
		}
	}

    @Test(expected = UnsupportedOperationException.class)
    public void testBRPopTimeout() throws Exception {
        actual.add(connection.bRPop(1, "alist"));
        Thread.sleep(1500l);
        verifyResults(Arrays.asList(new Object[]{null}), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBLPopTimeout() throws Exception {
        actual.add(connection.bLPop(1, "alist"));
        Thread.sleep(1500l);
        verifyResults(Arrays.asList(new Object[]{null}), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBRPopLPushTimeout() throws Exception {
        actual.add(connection.bRPopLPush(1, "alist", "foo"));
        Thread.sleep(1500l);
        verifyResults(Arrays.asList(new Object[]{null}), actual);
    }

    @Test
    public void testSetAndGet() {
        String key = "sfoo";
        String value = "blabla";
        connection.set(key.getBytes(), value.getBytes());
        actual.add(connection.get(key));
        verifyResults(new ArrayList<Object>(Collections.singletonList(value)), actual);
    }

    @Test
    public void testByteValue() {
        String value = UUID.randomUUID().toString();
        Person person = new Person(value, value, 1, new Address(value, 2));
        String key = getClass() + ":byteValue";
        byte[] rawKey = stringSerializer.serialize(key);

        connection.set(rawKey, serializer.serialize(person));
        byte[] rawValue = connection.get(rawKey);
        assertNotNull(rawValue);
        assertEquals(person, serializer.deserialize(rawValue));

        connection.del(rawKey);
    }

    @Test
    public void testPingPong() throws Exception {
        actual.add(connection.ping());
        verifyResults(new ArrayList<Object>(Collections.singletonList("PONG")), actual);
    }

    @Test
    public void testBitSet() throws Exception {
        String key = "bitset-test";
        connection.setBit(key, 0, false);
        connection.setBit(key, 1, true);
        actual.add(connection.getBit(key, 0));
        actual.add(connection.getBit(key, 1));
        verifyResults(Arrays.asList(new Object[]{false, true}), actual);

        connection.del(key);
    }

    @Test
    public void testInfo() throws Exception {
        Properties info = connection.info();
        assertNotNull(info);
        System.out.println(info);
    }

    @Ignore
    @Test
    public void testNullKey() throws Exception {
        connection.decr(EMPTY_ARRAY);
        try {
            connection.decr((String) null);
            fail("Decrement should fail with null key");
        } catch (Exception ex) {
            // expected
        }
    }

    @Ignore
    @Test
    public void testNullValue() throws Exception {
        byte[] key = UUID.randomUUID().toString().getBytes();
        connection.append(key, EMPTY_ARRAY);
        try {
            connection.append(key, null);
            fail("Append should fail with null value");
        } catch (DataAccessException ex) {
            // expected
        }

        connection.del(key);
    }

    @Ignore
    @Test
    public void testHashNullKey() throws Exception {
        byte[] key = UUID.randomUUID().toString().getBytes();
        connection.hExists(key, EMPTY_ARRAY);

        try {
            connection.hExists(key, null);
            fail("hExists should fail with null key");
        } catch (DataAccessException ex) {
            // expected
        }
    }

    @Ignore
    @Test
    public void testHashNullValue() throws Exception {
        byte[] key = UUID.randomUUID().toString().getBytes();
        byte[] field = "random".getBytes();

        connection.hSet(key, field, EMPTY_ARRAY);
        try {
            connection.hSet(key, field, null);
            fail("hSet should fail with null value");
        } catch (DataAccessException ex) {
            // expected
        }

        connection.del(key);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSort() {
        actual.add(connection.rPush("sortlist", "foo"));
        actual.add(connection.rPush("sortlist", "bar"));
        actual.add(connection.rPush("sortlist", "baz"));
        actual.add(connection.sort("sortlist", new DefaultSortParameters(null, Order.ASC, true)));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 3l,
                        Arrays.asList(new String[]{"bar", "baz", "foo"})}), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSortStore() {
        actual.add(connection.rPush("sortlist", "foo"));
        actual.add(connection.rPush("sortlist", "bar"));
        actual.add(connection.rPush("sortlist", "baz"));
        actual.add(connection.sort("sortlist", new DefaultSortParameters(null, Order.ASC, true),
                "newlist"));
        actual.add(connection.lRange("newlist", 0, 9));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 3l, 3l,
                        Arrays.asList(new String[]{"bar", "baz", "foo"})}), actual);
    }

    @Test
    public void testDbSize() {
        connection.set("dbparam", "foo");
        assertTrue(connection.dbSize() > 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFlushDb() {
        connection.flushDb();
        actual.add(connection.dbSize());
        verifyResults(Arrays.asList(new Object[]{0l}), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetConfig() {
        List<String> config = connection.getConfig("*");
        assertTrue(!config.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEcho() {
        actual.add(connection.echo("Hello World"));
        verifyResults(Arrays.asList(new Object[]{"Hello World"}), actual);
    }

    @Test
    public void testExists() {
        connection.set("existent", "true");
        actual.add(connection.exists("existent"));
        actual.add(connection.exists("nonexistent"));
        verifyResults(Arrays.asList(new Object[]{true, false}), actual);

        connection.del("existent");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeys() throws Exception {
        connection.set("keytest", "true");
        assertTrue(connection.keys("key*").contains("keytest"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRandomKey() {
        connection.set("some", "thing");
        assertNotNull(connection.randomKey());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRename() {
        connection.set("renametest", "testit");
        connection.rename("renametest", "newrenametest");
        actual.add(connection.get("newrenametest"));
        actual.add(connection.exists("renametest"));
        verifyResults(Arrays.asList(new Object[]{"testit", false}), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRenameNx() {
        connection.set("nxtest", "testit");
        actual.add(connection.renameNX("nxtest", "newnxtest"));
        actual.add(connection.get("newnxtest"));
        actual.add(connection.exists("nxtest"));
        verifyResults(Arrays.asList(new Object[]{true, "testit", false}), actual);
    }

    @Test
    public void testTtl() {
        connection.set("whatup", "yo");
        actual.add(connection.ttl("whatup"));
        verifyResults(Arrays.asList(new Object[]{-1L}), actual);

        connection.del("whatup");
    }

    @Test
    public void testType() {
        connection.set("something", "yo");
        assertEquals(DataType.STRING, connection.type("something"));

        connection.del("something");
    }

    @Test
    public void testGetSet() {
        connection.set("testGS", "1");
        actual.add(connection.getSet("testGS", "2"));
        actual.add(connection.get("testGS"));
        verifyResults(Arrays.asList(new Object[]{"1", "2"}), actual);

        connection.del("testGS");
    }

    public void testMSet() {
        Map<String, String> vals = new HashMap<String, String>();
        vals.put("color", "orange");
        vals.put("size", "1");
        connection.mSetString(vals);
        actual.add(connection.mGet("color", "size"));
        verifyResults(
                Arrays.asList(new Object[]{Arrays.asList(new String[]{"orange", "1"})}),
                actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMSetNx() {
        Map<String, String> vals = new HashMap<String, String>();
        vals.put("height", "5");
        vals.put("width", "1");
        connection.mSetNXString(vals);
        actual.add(connection.mGet("height", "width"));
        verifyResults(Arrays.asList(new Object[]{Arrays.asList(new String[]{"5", "1"})}),
                actual);
    }

    @Test
    public void testSetNx() {
        actual.add(connection.setNX("notaround", "54"));
        actual.add(connection.get("notaround"));
        actual.add(connection.setNX("notaround", "55"));
        actual.add(connection.get("notaround"));
        verifyResults(Arrays.asList(new Object[]{true, "54", false, "54"}), actual);

        connection.del("notaround");
    }

    @Test
    public void testGetRangeSetRange() {
        connection.set("rangekey", "supercalifrag");
        actual.add(connection.getRange("rangekey", 0l, 2l));
        connection.setRange("rangekey", "ck", 2);
        actual.add(connection.get("rangekey"));
        verifyResults(Arrays.asList(new Object[]{"sup", "suckrcalifrag"}), actual);

        connection.del("rangekey");
    }

    @Test
    public void testDecrByIncrBy() {
        connection.set("tdb", "4");
        actual.add(connection.decrBy("tdb", 3l));
        actual.add(connection.incrBy("tdb", 7l));
        verifyResults(Arrays.asList(new Object[]{1l, 8l}), actual);

        connection.del("tdb");
    }

    @Test
    public void testIncDecr() {
        connection.set("incrtest", "0");
        actual.add(connection.incr("incrtest"));
        actual.add(connection.get("incrtest"));
        actual.add(connection.decr("incrtest"));
        actual.add(connection.get("incrtest"));
        verifyResults(Arrays.asList(new Object[]{1l, "1", 0l, "0"}), actual);

        connection.del("incrtest");
    }

    @Test
    public void testStrLen() {
        connection.set("strlentest", "cat");
        actual.add(connection.strLen("strlentest"));
        verifyResults(Arrays.asList(new Object[]{3l}), actual);

        connection.del("strlentest");
    }

    // List operations

    @Test(expected = UnsupportedOperationException.class)
    public void testBLPop() {
        actual.add(connection.lPush("poplist", "foo"));
        actual.add(connection.lPush("poplist", "bar"));
        actual.add(connection.bLPop(100, "poplist", "otherlist"));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l,
                        Arrays.asList(new String[]{"poplist", "bar"})}), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBRPop() {
        actual.add(connection.rPush("rpoplist", "bar"));
        actual.add(connection.rPush("rpoplist", "foo"));
        actual.add(connection.bRPop(1, "rpoplist"));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l,
                        Arrays.asList(new String[]{"rpoplist", "foo"})}), actual);

        connection.del("rpoplist");
    }

    @Test
    public void testLInsert() {
        actual.add(connection.rPush("MyList", "hello"));
        actual.add(connection.rPush("MyList", "world"));
        actual.add(connection.lInsert("MyList", Position.AFTER, "hello", "big"));
        actual.add(connection.lRange("MyList", 0, -1));
        actual.add(connection.lInsert("MyList", Position.BEFORE, "big", "very"));
        actual.add(connection.lRange("MyList", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 3l,
                        Arrays.asList(new String[]{"hello", "big", "world"}), 4l,
                        Arrays.asList(new String[]{"hello", "very", "big", "world"})}), actual);
    }

    @Test
    public void testLPop() {
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.rPush("PopList", "world"));
        actual.add(connection.lPop("PopList"));
        verifyResults(Arrays.asList(new Object[]{1l, 2l, "hello"}), actual);
    }

    @Test
    public void testLRem() {
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.rPush("PopList", "big"));
        actual.add(connection.rPush("PopList", "world"));
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.lRem("PopList", 2, "hello"));
        actual.add(connection.lRange("PopList", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 3l, 4l, 2l,
                        Arrays.asList(new String[]{"big", "world"})}), actual);
    }

    @Test
    public void testLSet() {
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.rPush("PopList", "big"));
        actual.add(connection.rPush("PopList", "world"));
        connection.lSet("PopList", 1, "cruel");
        actual.add(connection.lRange("PopList", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 3l,
                        Arrays.asList(new String[]{"hello", "cruel", "world"})}), actual);
    }

    @Test
    public void testLTrim() {
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.rPush("PopList", "big"));
        actual.add(connection.rPush("PopList", "world"));
        connection.lTrim("PopList", 1, -1);
        actual.add(connection.lRange("PopList", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 3l,
                        Arrays.asList(new String[]{"big", "world"})}), actual);
    }

    @Test
    public void testRPop() {
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.rPush("PopList", "world"));
        actual.add(connection.rPop("PopList"));
        verifyResults(Arrays.asList(new Object[]{1l, 2l, "world"}), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRPopLPush() {
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.rPush("PopList", "world"));
        actual.add(connection.rPush("pop2", "hey"));
        actual.add(connection.rPopLPush("PopList", "pop2"));
        actual.add(connection.lRange("PopList", 0, -1));
        actual.add(connection.lRange("pop2", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 1l, "world",
                        Arrays.asList(new String[]{"hello"}),
                        Arrays.asList(new String[]{"world", "hey"})}), actual);

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBRPopLPush() {
        actual.add(connection.rPush("PopList", "hello"));
        actual.add(connection.rPush("PopList", "world"));
        actual.add(connection.rPush("pop2", "hey"));
        actual.add(connection.bRPopLPush(1, "PopList", "pop2"));
        actual.add(connection.lRange("PopList", 0, -1));
        actual.add(connection.lRange("pop2", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, 1l, "world",
                        Arrays.asList(new String[]{"hello"}),
                        Arrays.asList(new String[]{"world", "hey"})}), actual);
    }

    @Test
    public void testLPushX() {
        actual.add(connection.rPush("mylist", "hi"));
        actual.add(connection.lPushX("mylist", "foo"));
        actual.add(connection.lRange("mylist", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, Arrays.asList(new String[]{"foo", "hi"})}),
                actual);
    }

    @Test
    public void testRPushX() {
        actual.add(connection.rPush("mylist", "hi"));
        actual.add(connection.rPushX("mylist", "foo"));
        actual.add(connection.lRange("mylist", 0, -1));
        verifyResults(
                Arrays.asList(new Object[]{1l, 2l, Arrays.asList(new String[]{"hi", "foo"})}),
                actual);
    }

    @Test
    public void testLIndex() {
        actual.add(connection.lPush("testylist", "foo"));
        actual.add(connection.lIndex("testylist", 0));
        verifyResults(Arrays.asList(new Object[]{1l, "foo"}), actual);

        connection.del("testylist");
    }

    @Test
    public void testLPush() throws Exception {
        actual.add(connection.lPush("testlist", "bar"));
        actual.add(connection.lPush("testlist", "baz"));
        actual.add(connection.lRange("testlist", 0, -1));
        verifyResults(Arrays.asList(new Object[]{1l, 2l,
                Arrays.asList(new String[]{"baz", "bar"})}), actual);

        connection.del("testlist");
    }

    // Set operations

    @Test
    public void testSAdd() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        assertEquals(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar"})),
                connection.sMembers("myset"));
    }

    @Test
    public void testSCard() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        assertEquals(Long.valueOf(2), connection.sCard("myset"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSDiff() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        connection.sAdd("otherset", "bar");
        assertEquals(new HashSet<String>(Collections.singletonList("foo")),
                connection.sDiff("myset", "otherset"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSDiffStore() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        connection.sAdd("otherset", "bar");
        connection.sDiffStore("thirdset", "myset", "otherset");
        assertEquals(new HashSet<String>(Collections.singletonList("foo")),
                connection.sMembers("thirdset"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSInter() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        connection.sAdd("otherset", "bar");
        assertEquals(new HashSet<String>(Collections.singletonList("bar")),
                connection.sInter("myset", "otherset"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSInterStore() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        connection.sAdd("otherset", "bar");
        connection.sInterStore("thirdset", "myset", "otherset");
        assertEquals(new HashSet<String>(Collections.singletonList("bar")),
                connection.sMembers("thirdset"));
    }

    @Test
    public void testSIsMember() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        assertTrue(connection.sIsMember("myset", "foo"));
        assertFalse(connection.sIsMember("myset", "baz"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSMove() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        connection.sAdd("otherset", "bar");
        assertTrue(connection.sMove("myset", "otherset", "foo"));
        assertEquals(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar"})),
                connection.sMembers("otherset"));
    }

    @Ignore
    @Test
    public void testSPop() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        assertTrue(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar"}))
                .contains(connection.sPop("myset")));
    }

    @Test
    public void testSRandMember() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        assertTrue(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar"}))
                .contains(connection.sRandMember("myset")));
    }

    @Test
    public void testSRem() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        assertTrue(connection.sRem("myset", "foo") == 1);
        assertFalse(connection.sRem("myset", "baz") == 1);
        assertEquals(new HashSet<String>(Collections.singletonList("bar")),
                connection.sMembers("myset"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSUnion() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        connection.sAdd("otherset", "bar");
        connection.sAdd("otherset", "baz");
        assertEquals(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar", "baz"})),
                connection.sUnion("myset", "otherset"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSUnionStore() {
        connection.sAdd("myset", "foo");
        connection.sAdd("myset", "bar");
        connection.sAdd("otherset", "bar");
        connection.sAdd("otherset", "baz");
        connection.sUnionStore("thirdset", "myset", "otherset");
        assertEquals(new HashSet<String>(Arrays.asList(new String[]{"foo", "bar", "baz"})),
                connection.sMembers("thirdset"));
    }

    // ZSet

    @Test
    public void testZAddAndZRange() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James", "Bob"})),
                connection.zRange("myset", 0, -1));
    }

    @Test
    public void testZCard() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(Long.valueOf(2), connection.zCard("myset"));
    }

    @Test
    public void testZCount() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 4, "Joe");
        assertEquals(Long.valueOf(2), connection.zCount("myset", 1, 2));
    }

    @Test
    public void testZIncrBy() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 4, "Joe");
        connection.zIncrBy("myset", 2, "Joe");
        assertEquals(new LinkedHashSet<String>(Collections.singletonList("Joe")),
                connection.zRangeByScore("myset", 6, 6));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testZInterStore() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 4, "Joe");
        connection.zAdd("otherset", 1, "Bob");
        connection.zAdd("otherset", 4, "James");
        assertEquals(Long.valueOf(2), connection.zInterStore("thirdset", "myset", "otherset"));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob", "James"})),
                connection.zRange("thirdset", 0, -1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testZInterStoreAggWeights() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 4, "Joe");
        connection.zAdd("otherset", 1, "Bob");
        connection.zAdd("otherset", 4, "James");
        assertEquals(Long.valueOf(2), connection.zInterStore("thirdset", Aggregate.MAX, new int[]{
                2, 3}, "myset", "otherset"));
        assertEquals(
                new LinkedHashSet<StringTuple>(Arrays.asList(new StringTuple[]{
                        new DefaultStringTuple("Bob".getBytes(), "Bob", 4d),
                        new DefaultStringTuple("James".getBytes(), "James", 12d)})),
                connection.zRangeWithScores("thirdset", 0, -1));
    }

    @Test
    public void testZRangeWithScores() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(
                new LinkedHashSet<StringTuple>(Arrays.asList(new StringTuple[]{
                        new DefaultStringTuple("James".getBytes(), "James", 1d),
                        new DefaultStringTuple("Bob".getBytes(), "Bob", 2d)})),
                connection.zRangeWithScores("myset", 0, -1));
    }

    @Test
    public void testZRangeByScore() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"James"})),
                connection.zRangeByScore("myset", 1, 1));
    }

    @Test
    public void testZRangeByScoreOffsetCount() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob"})),
                connection.zRangeByScore("myset", 1d, 3d, 1, -1));
    }

    @Test
    public void testZRangeByScoreWithScores() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(
                new LinkedHashSet<StringTuple>(
                        Arrays.asList(new StringTuple[]{new DefaultStringTuple("Bob".getBytes(),
                                "Bob", 2d)})), connection.zRangeByScoreWithScores("myset", 2d, 5d));
    }

    @Test
    public void testZRangeByScoreWithScoresOffsetCount() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(
                new LinkedHashSet<StringTuple>(
                        Arrays.asList(new StringTuple[]{new DefaultStringTuple(
                                "James".getBytes(), "James", 1d)})),
                connection.zRangeByScoreWithScores("myset", 1d, 5d, 0, 1));
    }

    @Test
    public void testZRevRange() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob", "James"})),
                connection.zRevRange("myset", 0, -1));
    }

    @Test
    public void testZRevRangeWithScores() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(
                new LinkedHashSet<StringTuple>(Arrays.asList(new StringTuple[]{
                        new DefaultStringTuple("Bob".getBytes(), "Bob", 2d),
                        new DefaultStringTuple("James".getBytes(), "James", 1d)})),
                connection.zRevRangeWithScores("myset", 0, -1));
    }

    @Test
    public void testZRevRangeByScoreOffsetCount() {
        byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes());
        byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes());
        assertEquals(
                new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob",
                        "James"})), SerializationUtils.deserialize(
                        byteConnection.zRevRangeByScore("myset".getBytes(), 0d,
                                3d, 0, 5), stringSerializer));
    }

    @Test
    public void testZRevRangeByScore() {
        byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes());
        byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes());
        assertEquals(
                new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob",
                        "James"})), SerializationUtils.deserialize(
                        byteConnection.zRevRangeByScore("myset".getBytes(), 0d,
                                3d), stringSerializer));
    }

    @Test
    public void testZRevRangeByScoreWithScoresOffsetCount() {
        byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes());
        byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes());
        assertEquals(
                new LinkedHashSet<Tuple>(
                        Arrays.asList(new Tuple[]{new DefaultTuple("Bob"
                                .getBytes(), 2d)})),
                byteConnection.zRevRangeByScoreWithScores("myset".getBytes(),
                        0d, 3d, 0, 1));
    }

    @Test
    public void testZRevRangeByScoreWithScores() {
        byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes());
        byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes());
        byteConnection.zAdd("myset".getBytes(), 3, "Joe".getBytes());
        assertEquals(
                new LinkedHashSet<Tuple>(Arrays.asList(new Tuple[]{
                        new DefaultTuple("Bob".getBytes(), 2d),
                        new DefaultTuple("James".getBytes(), 1d)})),
                byteConnection.zRevRangeByScoreWithScores("myset".getBytes(),
                        0d, 2d));
    }

    @Test
    public void testZRank() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(Long.valueOf(0), connection.zRank("myset", "James"));
        assertEquals(Long.valueOf(1), connection.zRank("myset", "Bob"));
    }

    @Test
    public void testZRem() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertTrue(connection.zRem("myset", "James") == 1);
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob"})),
                connection.zRange("myset", 0l, -1l));
    }

    @Test
    public void testZRemRangeByRank() {
        connection.del("myset");
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        Long zRemRange = connection.zRemRange("myset", 0l, 3l);
        assertEquals(Long.valueOf(2), zRemRange);
        assertTrue(connection.zRange("myset", 0l, -1l).isEmpty());
    }

    @Test
    public void testZRemRangeByScore() {
        connection.del("myset");
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        assertEquals(Long.valueOf(1), connection.zRemRangeByScore("myset", 0d, 1d));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob"})),
                connection.zRange("myset", 0l, -1l));
    }

    @Test
    public void testZRevRank() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 3, "Joe");
        assertEquals(Long.valueOf(0), connection.zRevRank("myset", "Joe"));
    }

    @Test
    public void testZScore() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 3, "Joe");
        assertEquals(Double.valueOf(3d), connection.zScore("myset", "Joe"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testZUnionStore() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 5, "Joe");
        connection.zAdd("otherset", 1, "Bob");
        connection.zAdd("otherset", 4, "James");
        assertEquals(Long.valueOf(3), connection.zUnionStore("thirdset", "myset", "otherset"));
        assertEquals(
                new LinkedHashSet<String>(Arrays.asList(new String[]{"Bob", "James", "Joe"})),
                connection.zRange("thirdset", 0, -1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testZUnionStoreAggWeights() {
        connection.zAdd("myset", 2, "Bob");
        connection.zAdd("myset", 1, "James");
        connection.zAdd("myset", 4, "Joe");
        connection.zAdd("otherset", 1, "Bob");
        connection.zAdd("otherset", 4, "James");
        assertEquals(Long.valueOf(3), connection.zUnionStore("thirdset", Aggregate.MAX, new int[]{
                2, 3}, "myset", "otherset"));
        assertEquals(
                new LinkedHashSet<StringTuple>(Arrays.asList(new StringTuple[]{
                        new DefaultStringTuple("Bob".getBytes(), "Bob", 4d),
                        new DefaultStringTuple("Joe".getBytes(), "Joe", 8d),
                        new DefaultStringTuple("James".getBytes(), "James", 12d)})),
                connection.zRangeWithScores("thirdset", 0, -1));
    }

    // Hash Ops

    @Test
    public void testHSetGet() throws Exception {
        String hash = getClass() + ":hashtest";
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String value1 = "foo";
        String value2 = "bar";
        actual.add(connection.hSet(hash, key1, value1).booleanValue());
        actual.add(connection.hSet(hash, key2, value2).booleanValue());
        actual.add(connection.hGet(hash, key1));
        actual.add(connection.hGetAll(hash));
        Map<String, String> expected = new HashMap<String, String>();
        expected.put(key1, value1);
        expected.put(key2, value2);
        //verifyResults(Arrays.asList(new Object[] { true, true, value1, expected }), actual);

        connection.del(hash);
    }

    @Test
    public void testHSetNX() throws Exception {
        actual.add(connection.hSetNX("myhash", "key1", "foo"));
        actual.add(connection.hSetNX("myhash", "key1", "bar"));
        actual.add(connection.hGet("myhash", "key1"));
        verifyResults(Arrays.asList(new Object[]{true, false, "foo"}), actual);
    }

    @Test
    public void testHDel() throws Exception {
        connection.hSet("test", "key", "val");
        assertTrue(connection.hDel("test", "key") == 1);
        assertFalse(connection.hDel("test", "foo") == 1);
        assertFalse(connection.hExists("test", "key"));
    }

    @Test
    public void testHIncrBy() {
        actual.add(connection.hSet("test", "key", "2"));
        actual.add(connection.hIncrBy("test", "key", 3l));
        actual.add(connection.hGet("test", "key"));
        verifyResults(Arrays.asList(new Object[]{true, 5l, "5"}), actual);
    }

    @Test
    public void testHKeys() {
        connection.hSet("test", "key", "2");
        connection.hSet("test", "key2", "2");
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[]{"key", "key2"})),
                connection.hKeys("test"));
    }

    @Test
    public void testHLen() {
        actual.add(connection.hSet("test", "key", "2"));
        actual.add(connection.hSet("test", "key2", "2"));
        actual.add(connection.hLen("test"));
        verifyResults(Arrays.asList(new Object[]{true, true, 2l}), actual);
    }

    @Test
    public void testHMGetSet() {
        Map<String, String> tuples = new HashMap<String, String>();
        tuples.put("key", "foo");
        tuples.put("key2", "bar");
        connection.hMSet("test", tuples);
        actual.add(connection.hMGet("test", "key", "key2"));
        verifyResults(Arrays.asList(new Object[]{Arrays.asList(new String[]{"foo", "bar"})}),
                actual);
    }

    @Test
    public void testHVals() {
        actual.add(connection.hSet("test", "key", "foo"));
        actual.add(connection.hSet("test", "key2", "bar"));
        actual.add(connection.hVals("test"));
        verifyResults(
                Arrays.asList(new Object[]{true, true,
                        Arrays.asList(new String[]{"foo", "bar"})}), actual);
    }

    private static final Point POINT_ARIGENTO = new Point(13.583333, 37.316667);
    private static final Point POINT_CATANIA = new Point(15.087269, 37.502669);
    private static final Point POINT_PALERMO = new Point(13.361389, 38.115556);

    private static final GeoLocation<String> ARIGENTO = new GeoLocation<String>("arigento", POINT_ARIGENTO);
    private static final GeoLocation<String> CATANIA = new GeoLocation<String>("catania", POINT_CATANIA);
    private static final GeoLocation<String> PALERMO = new GeoLocation<String>("palermo", POINT_PALERMO);

    protected List<Object> getResults() {
        return actual;
    }

    @Test
    public void geoAddSingleGeoLocation() {
        String key = "geo";
        actual.add(connection.geoAdd(key, PALERMO));

        List<Object> result = getResults();
        assertThat((Long) result.get(0), is(1L));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void geoAddMultipleGeoLocations() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(PALERMO, ARIGENTO, CATANIA, PALERMO)));

        List<Object> result = getResults();
        assertThat((Long) result.get(0), is(3L));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void geoDist() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(PALERMO, CATANIA)));
        actual.add(connection.geoDist(key, PALERMO.getName(), CATANIA.getName()));

        List<Object> result = getResults();
        assertThat(((Distance) result.get(1)).getValue(), is(closeTo(166274.15156960033D, 0.005)));
        assertThat(((Distance) result.get(1)).getUnit(), is("m"));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void geoDistWithMetric() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(PALERMO, CATANIA)));
        actual.add(connection.geoDist(key, PALERMO.getName(), CATANIA.getName(), Metrics.KILOMETERS));

        List<Object> result = getResults();
        assertThat(((Distance) result.get(1)).getValue(), is(closeTo(166.27415156960033D, 0.005)));
        assertThat(((Distance) result.get(1)).getUnit(), is("km"));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void geoHash() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(PALERMO, CATANIA)));
        actual.add(connection.geoHash(key, PALERMO.getName(), CATANIA.getName()));

        List<Object> result = getResults();
        assertThat(((List<String>) result.get(1)).get(0), is("sqc8b49rny0"));
        assertThat(((List<String>) result.get(1)).get(1), is("sqdtr74hyu0"));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void geoHashNonExisting() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(PALERMO, CATANIA)));
        actual.add(connection.geoHash(key, PALERMO.getName(), ARIGENTO.getName(), CATANIA.getName()));

        List<Object> result = getResults();
        assertThat(((List<String>) result.get(1)).get(0), is("sqc8b49rny0"));
        assertThat(((List<String>) result.get(1)).get(1), is(nullValue()));
        assertThat(((List<String>) result.get(1)).get(2), is("sqdtr74hyu0"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void geoPosition() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(PALERMO, CATANIA)));

        actual.add(connection.geoPos(key, PALERMO.getName(), CATANIA.getName()));

        List<Object> result = getResults();
        assertThat(((List<Point>) result.get(1)).get(0).getX(), is(closeTo(POINT_PALERMO.getX(), 0.005)));
        assertThat(((List<Point>) result.get(1)).get(0).getY(), is(closeTo(POINT_PALERMO.getY(), 0.005)));

        assertThat(((List<Point>) result.get(1)).get(1).getX(), is(closeTo(POINT_CATANIA.getX(), 0.005)));
        assertThat(((List<Point>) result.get(1)).get(1).getY(), is(closeTo(POINT_CATANIA.getY(), 0.005)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void geoPositionNonExisting() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(PALERMO, CATANIA)));

        actual.add(connection.geoPos(key, PALERMO.getName(), ARIGENTO.getName(), CATANIA.getName()));

        List<Object> result = getResults();
        assertThat(((List<Point>) result.get(1)).get(0).getX(), is(closeTo(POINT_PALERMO.getX(), 0.005)));
        assertThat(((List<Point>) result.get(1)).get(0).getY(), is(closeTo(POINT_PALERMO.getY(), 0.005)));

        assertThat(((List<Point>) result.get(1)).get(1), is(nullValue()));

        assertThat(((List<Point>) result.get(1)).get(2).getX(), is(closeTo(POINT_CATANIA.getX(), 0.005)));
        assertThat(((List<Point>) result.get(1)).get(2).getY(), is(closeTo(POINT_CATANIA.getY(), 0.005)));
    }

    @SuppressWarnings("unchecked")
    @Test
    // TODO
    public void geoRadiusShouldReturnMembersCorrectly() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(ARIGENTO, CATANIA, PALERMO)));

        actual.add(connection.geoRadius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS))));
        actual.add(connection.geoRadius(key, new Circle(new Point(15D, 37D), new Distance(150D, KILOMETERS))));

        List<Object> results = getResults();
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent(), hasSize(3));
        assertThat(((GeoResults<GeoLocation<String>>) results.get(2)).getContent(), hasSize(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void geoRadiusShouldReturnDistanceCorrectly() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(ARIGENTO, CATANIA, PALERMO)));

        actual.add(connection.geoRadius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)),
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance()));

        List<Object> results = getResults();
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent(), hasSize(3));
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent().get(0).getDistance().getValue(),
                is(closeTo(130.423D, 0.005)));
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent().get(0).getDistance().getUnit(),
                is("km"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void geoRadiusShouldApplyLimit() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(ARIGENTO, CATANIA, PALERMO)));

        actual.add(connection.geoRadius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)),
                newGeoRadiusArgs().limit(2)));

        List<Object> results = getResults();
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent(), hasSize(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void geoRadiusByMemberShouldReturnMembersCorrectly() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(ARIGENTO, CATANIA, PALERMO)));

        actual.add(connection.geoRadiusByMember(key, PALERMO.getName(), new Distance(100, KILOMETERS),
                newGeoRadiusArgs().sortAscending()));

        List<Object> results = getResults();
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent().get(0).getContent().getName(),
                is(PALERMO.getName()));
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent().get(1).getContent().getName(),
                is(ARIGENTO.getName()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void geoRadiusByMemberShouldReturnDistanceCorrectly() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(ARIGENTO, CATANIA, PALERMO)));

        actual.add(connection.geoRadiusByMember(key, PALERMO.getName(), new Distance(100, KILOMETERS),
                newGeoRadiusArgs().includeDistance()));

        List<Object> results = getResults();
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent(), hasSize(2));
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent().get(0).getDistance().getValue(),
                is(closeTo(90.978D, 0.005)));
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent().get(0).getDistance().getUnit(),
                is("km"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void geoRadiusByMemberShouldApplyLimit() {
        String key = "geo";
        actual.add(connection.geoAdd(key, Arrays.asList(ARIGENTO, CATANIA, PALERMO)));

        actual.add(connection.geoRadiusByMember(key, PALERMO.getName(), new Distance(200, KILOMETERS),
                newGeoRadiusArgs().limit(2)));

        List<Object> results = getResults();
        assertThat(((GeoResults<GeoLocation<String>>) results.get(1)).getContent(), hasSize(2));
    }

    @Test
    public void pfAddShouldAddValuesCorrectly() {
        connection.pfAdd(KEY_1_BYTES, VALUE_1_BYTES, VALUE_2_BYTES, VALUE_3_BYTES);
        
        assertThat(connection.pfCount(KEY_1_BYTES), is(3L));
    }

    @Test
    public void pfCountShouldAllowCountingOnSingleKey() {
        connection.pfAdd(KEY_1, VALUE_1, VALUE_2, VALUE_3);
        
        assertThat(connection.pfCount(KEY_1_BYTES), is(3L));
    }

    @Test
    public void pfCountShouldAllowCountingOnSameSlotKeys() {
        connection.pfAdd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
        connection.pfAdd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);
        
        assertThat(connection.pfCount(SAME_SLOT_KEY_1_BYTES), is(2L));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void pfMergeShouldWorkWhenAllKeysMapToSameSlot() {
        connection.pfAdd(SAME_SLOT_KEY_1, VALUE_1, VALUE_2);
        connection.pfAdd(SAME_SLOT_KEY_2, VALUE_2, VALUE_3);

        connection.pfMerge(SAME_SLOT_KEY_3_BYTES, SAME_SLOT_KEY_1_BYTES, SAME_SLOT_KEY_2_BYTES);

        assertThat(connection.pfCount(SAME_SLOT_KEY_3), is(3L));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void pfMergeShouldThrowErrorOnDifferentSlotKeys() {
        connection.pfMerge(KEY_3_BYTES, KEY_1_BYTES, KEY_2_BYTES);
    }

    @Test
    public void zRangeByLexShouldReturnResultCorrectly() throws UnsupportedEncodingException {
        connection.zAdd(KEY_1, 0, "a");
        connection.zAdd(KEY_1, 0, "b");
        connection.zAdd(KEY_1, 0, "c");
        connection.zAdd(KEY_1, 0, "d");
        connection.zAdd(KEY_1, 0, "e");
        connection.zAdd(KEY_1, 0, "f");
        connection.zAdd(KEY_1, 0, "g");

        Set<String> values = connection.zRangeByLex(KEY_1, Range.range().lte("c"));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[] { "a", "b", "c" })), values);

        values = connection.zRangeByLex(KEY_1, Range.range().lt("c"));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[] { "a", "b" })), values);

        values = connection.zRangeByLex(KEY_1, Range.range().gte("aaa").lt("g"));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[] { "b", "c", "d", "e", "f" })), values);

        values = connection.zRangeByLex(KEY_1, Range.range().gte("e"));
        assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[] { "e", "f", "g" })), values);
    }
    
    @Test
    public void scanShouldReadEntireValueRange() {
        Set<String> keys = new HashSet<String>();
        connection.set("spring", "data");

        int itemCount = 22;
        for (int i = 0; i < itemCount; i++) {
            String k = "key_" + i;
            connection.set(k, ("foo_" + i));
            keys.add(k);
        }

        Cursor<byte[]> cursor = connection.scan(scanOptions().count(20).match("ke*").build());

        while (cursor.hasNext()) {
            byte[] value = cursor.next();
            assertThat(new String(value), not(containsString("spring")));
            keys.remove(new String(value));
        }

        assertThat(keys.size(), lessThan(itemCount));
    }

    @Test
    public void sscanShouldRetrieveAllValuesInSetCorrectly() {
        for (int i = 0; i < 30; i++) {
            connection.sAdd(KEY_1_BYTES, JedisConverters.toBytes(Integer.valueOf(i)));
        }

        int count = 0;
        Cursor<byte[]> cursor = connection.sScan(KEY_1_BYTES, ScanOptions.NONE);
        while (cursor.hasNext()) {
            count++;
            cursor.next();
        }

        assertThat(count, is(30));
    }
    
    @Test
    public void zScanShouldReadEntireValueRange() {
        int nrOfValues = 321;
        for (int i = 0; i < nrOfValues; i++) {
            connection.zAdd(KEY_1_BYTES, i, JedisConverters.toBytes("value-" + i));
        }

        Cursor<Tuple> tuples = connection.zScan(KEY_1_BYTES, ScanOptions.NONE);

        int count = 0;
        while (tuples.hasNext()) {
            tuples.next();
            count++;
        }

        assertThat(count, equalTo(nrOfValues));
    }

    @Test
    public void hScanShouldReadEntireValueRange() {
        int nrOfValues = 321;
        for (int i = 0; i < nrOfValues; i++) {
            connection.hSet(KEY_1_BYTES, JedisConverters.toBytes("key" + i), JedisConverters.toBytes("value-" + i));
        }

        Cursor<Map.Entry<byte[], byte[]>> cursor = connection.hScan(KEY_1_BYTES,
                scanOptions().match("key*").build());

        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            i++;
        }

        assertThat(i, is(nrOfValues));
    }
    
    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void shouldThrowExceptionWhenAccessingClosedCursor() throws IOException {
        Cursor<byte[]> cursor = RedisClusterConnection.createScanCursor(
                (GatewayClient) connection.getNativeConnection(), ScanOptions.NONE);

        try {
            assertFalse(cursor.isClosed());
            cursor.next();
        } finally {
            cursor.close();
        }
    }
    
    @Test
    public void shouldReadEntireValueRangeWhenAccessingOpenCursor() throws IOException {
        int numOfValues = 321;
        for (int i = 0; i < numOfValues; i++) {
            connection.hSet(KEY_1_BYTES, JedisConverters.toBytes("key" + i), JedisConverters.toBytes("value-" + i));
        }

        Cursor<Entry<byte[], byte[]>> cursor = RedisClusterConnection.createHScanCursor(
                (GatewayClient) connection.getNativeConnection(), KEY_1_BYTES, ScanOptions.NONE);
        cursor.open();
        
        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            i++;
        }

        assertThat(i, is(numOfValues));
        cursor.close();
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void repoeningCursorShouldHappenAtLastPosition() throws IOException {
        Cursor<byte[]> cursor = connection.scan(ScanOptions.NONE);
        while (cursor.hasNext()) {
            cursor.next();
        }
        cursor.close();
        assertTrue(cursor.isClosed());
        
        cursor.open();
    }
    
    @Test
    public void scanEmptyKeys() {
        String key = "not_exist_key";
        ScanOptions options = scanOptions().match(key + "*").build();
        
        connection.del(key);
        assertCursorEmpty(connection.scan(options));
        assertCursorEmpty(connection.hScan(key, options));
        assertCursorEmpty(connection.sScan(key, options));
        assertCursorEmpty(connection.zScan(key, options));
    }
    
    @Test
    public void scanWholeMatchingKeys() {
        String containerKey = "containerKey";
        String[] keys = {"test:string:scan1", "test:string:scan2", "test:string:scan3"};
        ScanOptions options = new ScanOptionsBuilder()
                .match("test:string:scan*")
                .count(2)
                .build();
        
        connection.del(containerKey);
        for (int i = 0; i < keys.length; i++) {
            connection.del(keys[i]);
        }

        // Scan
        List<String> scanResults = new ArrayList<String>();
        for (int i = 0; i < keys.length; i++) {
            connection.setRange(keys[i], keys[i], 5);
        }
        Cursor<byte[]> scanCursor = connection.scan(options);
        while (scanCursor.hasNext()) {
            scanResults.add(new String(scanCursor.next()));
        }
        for (String key : keys) {
            assertTrue(key + " isn't scaned", scanResults.contains(key));
        }
        scanResults.clear();
        
        // Sscan
        for (int i = 0; i < keys.length; i++) {
            connection.sAdd(containerKey, keys[i]);
        }
        Cursor<String> sscanCursor = connection.sScan(containerKey, options);
        while (sscanCursor.hasNext()) {
            scanResults.add(sscanCursor.next());
        }
        for (String key : keys) {
            assertTrue(key + " isn't scaned", scanResults.contains(key));
        }
        
        // Hscan
        connection.del(containerKey);
        Map<String, String> hscanResults = new HashMap<String, String>();
        for (int i = 0; i < keys.length; i++) {
            connection.hSet(containerKey, keys[i], keys[i] + "v");
        }
        Cursor<Entry<String, String>> hscanCursor = connection.hScan(containerKey, options);
        while (hscanCursor.hasNext()) {
            Entry<String, String> e = hscanCursor.next();
            hscanResults.put(e.getKey(), e.getValue());
        }
        for (String key : keys) {
            assertTrue(key + " isn't scaned", hscanResults.get(key).equals(key + "v"));
        }
        hscanResults.clear();
        
        // Zscan
        connection.del(containerKey);
        Map<String, Double> zscanResults = new HashMap<String, Double>();
        for (int i = 0; i < keys.length; i++) {
            connection.zAdd(containerKey, i, keys[i]);
        }
        Cursor<StringTuple> zscanCursor = connection.zScan(containerKey, options);
        while (zscanCursor.hasNext()) {
            StringTuple st = zscanCursor.next();
            zscanResults.put(st.getValueAsString(), st.getScore());
        }
        for (int i = 0; i < keys.length; i++) {
            assertTrue(keys[i] + " isn't scaned", zscanResults.get(keys[i]) == i);
        }
        zscanResults.clear();
    }
    
    protected void verifyResults(List<Object> expected, List<Object> actual) {
        assertEquals(expected, actual);
    }

    protected boolean exists(String key, long timeout) {
        boolean exists = true;
        for (long currentTime = System.currentTimeMillis(); System.currentTimeMillis()
                - currentTime < timeout; ) {
            if (!connection.exists(key)) {
                exists = false;
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        return exists;
    }
    
    protected <T> void assertCursorEmpty(Cursor<T> cursor) {
        List<T> results = new ArrayList<T>();
        
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        
        assertTrue(results.isEmpty());
    }
}


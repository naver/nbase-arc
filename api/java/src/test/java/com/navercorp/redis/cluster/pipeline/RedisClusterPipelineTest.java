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

package com.navercorp.redis.cluster.pipeline;

import static com.navercorp.redis.cluster.connection.RedisProtocol.Keyword.INCRBY;
import static com.navercorp.redis.cluster.connection.RedisProtocol.Keyword.OVERFLOW;
import static com.navercorp.redis.cluster.connection.RedisProtocol.Keyword.SAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;
import org.springframework.data.redis.connection.DefaultTuple;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.navercorp.redis.cluster.RedisClusterTestBase;
import com.navercorp.redis.cluster.RedisClusterTestBase.ResultChecker;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.geo.GeoRadiusParam;

/**
 * @author jaehong.kim
 */
public class RedisClusterPipelineTest extends RedisClusterTestBase {
	
    @Override
    public void clear() {
    	redis.del(K0);
    	redis.del(K1);
    	redis.del(K1);
    	redis.del(BK0);
    	redis.del(BK1);
    	redis.del(BK2);
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_KEY_1);
        redis.del(REDIS_KEY_2);
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BKEY_1);
        redis.del(REDIS_BKEY_2);
        redis.ssdel(TRIPLES_KEY_0);
        redis.ssdel(TRIPLES_BKEY_0);
    }

	void t(String methodName, Class[] paramTypes, Object[] args, Object expectedResult, ResultChecker checker) {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        pipeline.setTimeout(10000);
        
        try {
			Method m;
			try {
				m = pipeline.getClass().getDeclaredMethod(methodName, paramTypes);
				m.invoke(pipeline, args);
			} catch (Exception cause) {
				throw new RuntimeException(methodName, cause);
			}
			
			if (m.getGenericReturnType() != Void.TYPE) {
				List<Object> rs = pipeline.syncAndReturnAll();
				assertEquals(methodName, 1, rs.size());
				checker.check(methodName, expectedResult, rs.get(0));
			}
        } finally {
        	pipeline.close();
        }
	}
	
    @Test
    public void testCommands() {
    	long now = System.currentTimeMillis();
		byte[] k0Dump = { 0, 2, 86, 48, 7, 0, -45, 12, -56, -116, 91, -56, -22, -31 };
    	
    	t("setnx", p(String.class, String.class), a(K0, V1), 1L, EQ);
    	t("setex", p(String.class, int.class, String.class), a(K0, 10000, V0), OK, EQ);
    	t("mset", p(String[].class), a((Object) new String[] { K1, V1, K2, V2 }), OK, EQ);
    	t("mget", p(String[].class), a((Object) new String[] {K0, K1, K2}), Arrays.asList(V0, V1, V2), EQ);
    	t("pexpireAt", p(String.class, long.class), a(K0, now + 100000L), 1L, EQ);
    	t("expireAt", p(String.class, long.class), a(K0, now + 100L), 1L, EQ);
    	t("persist", p(String.class), a(K0), 1L, EQ);
    	t("psetex", p(String.class, long.class, String.class), a(K0, 10000L, V0), OK, EQ);
    	t("pexpire", p(String.class, long.class), a(K0, 10000L), 1L, EQ);
    	t("pttl", p(String.class), a(K0), 1000L, LT);
    	t("dump", p(String.class), a(K0), k0Dump, EQ);
		t("del", p(String[].class), a((Object) new String[] { K0 }), 1L, EQ);
    	t("restore", p(String.class, long.class, byte[].class), a(K0, 0L, k0Dump), OK, EQ);
    	t("get", p(String.class), a(K0), V0, EQ);
    	t("getSet", p(String.class, String.class), a(K0, V2), V0, EQ);
    	t("append", p(String.class, String.class), a(K0, V1), 4L, EQ);
    	// K0: V0V1
    	t("setbit", p(String.class, long.class, boolean.class), a(K0, 0L, true), false, EQ);
    	t("getbit", p(String.class, long.class), a(K0, 0L), true, EQ);
    	t("bitcount", p(String.class), a(K0), 15L, EQ);
    	t("bitcount", p(String.class, long.class, long.class), a(K0, 0, -1), 15L, EQ);
    	t("setrange", p(String.class, long.class, String.class), a(K0, 1L, "AB"), 4L, EQ);
    	// K0: VAB1
    	t("getrange", p(String.class, long.class, long.class), a(K0, 1L, 3L), "AB1", EQ);
    	t("strlen", p(String.class), a(K0), 4L, EQ);
    	t("substr", p(byte[].class, int.class, int.class), a(BK0, 1, 2), "AB".getBytes(Charsets.UTF_8), EQ);
    	t("substr", p(String.class, int.class, int.class), a(K0, 2, 3), "B1", EQ);
    	t("touch", p(byte[][].class), a((Object) new byte[][] { BK0, BK1 }), 2L, EQ);
    	t("set", p(String.class, String.class), a(K0, ZERO), OK, EQ);
    	t("decrBy", p(String.class, long.class), a(K0, 10L), -10L, EQ);
    	t("decr", p(String.class), a(K0), -11L, EQ);
    	t("incrBy", p(String.class, long.class), a(K0, 1), -10L, EQ);
    	t("incrByFloat", p(String.class, double.class), a(K0, 0.5), Double.valueOf(-9.5), EQ);
    	
    	// Hash
    	redis.del(K0);
    	redis.del(K1);
    	redis.del(K2);
    	t("hsetnx", p(String.class, String.class, String.class), a(K0, F0, V0), 1L, EQ);
    	LinkedHashMap<String, String> kv = Maps.newLinkedHashMap();
    	kv.put(F1, V1);
    	kv.put(F2, V2);
    	t("hmset", p(String.class, Map.class), a(K0, kv), OK, EQ);
    	t("hmget", p(String.class, String[].class), a(K0, new String[] {F0, F1, F2}), Arrays.asList(V0, V1, V2), EQ);
    	t("hexists", p(String.class, String.class), a(K0, F0), true, EQ);
    	t("hget", p(String.class, String.class), a(K0, F0), V0, EQ);
    	kv.put(F0, V0);
    	t("hgetAll", p(String.class), a(K0), kv, EQ);
    	t("hkeys", p(String.class), a(K0), Sets.newHashSet(F0, F1, F2), EQ);
    	t("hvals", p(String.class), a(K0), Arrays.asList(V0, V1, V2), IN);
    	t("hlen", p(String.class), a(K0), 3L, EQ);
    	t("hstrlen", p(byte[].class, byte[].class), a(BK0, F0.getBytes(Charsets.UTF_8)), 2L, EQ);
    	
		t("hdel", p(String.class, String[].class), a(K0, new String[] { F0, F1 }), 2L, EQ);
		t("hset", p(String.class, String.class, String.class), a(K0, F0, ZERO), 1L, EQ);
    	t("hincrByFloat", p(String.class, String.class, double.class), a(K0, F0, 0.5), Double.valueOf(0.5), EQ);
    	
    	// List
    	redis.del(K0);
    	redis.del(K1);
    	redis.del(K2);
    	t("lpush", p(String.class, String[].class), a(K0, (Object) new String[] {ZERO, V1}), 2L, EQ);
    	t("lpushx", p(String.class, String.class), a(K0, V0), 3L, EQ);
    	t("lset", p(String.class, long.class, String.class), a(K0, 2, V2), OK, EQ);
    	// V0 V1 V2
    	t("llen", p(String.class), a(K0), 3L, EQ);
    	t("lindex", p(String.class, long.class), a(K0, 1), V1, EQ);
    	t("linsert", p(String.class, LIST_POSITION.class, String.class, String.class), a(K0, LIST_POSITION.BEFORE, V1, ZERO), 4L, EQ);
    	// V0 ZERO V1 V2
    	t("lpop", p(String.class), a(K0), V0, EQ);
    	// ZERO V1 V2
    	t("ltrim", p(String.class, long.class, long.class), a(K0, 1L, 2L), OK, EQ);
    	// V1 V2
    	t("rpop", p(String.class), a(K0), V2, EQ);
    	// V1
    	t("rpush", p(String.class, String[].class), a(K0, (Object) new String[] { V0, ZERO }), 3L, EQ);
    	// V1 V0 ZERO
    	t("rpushx", p(String.class, String.class), a(K1, V1), 0L, EQ);
    	// V1 V0 ZERO
    	t("lrange", p(String.class, long.class, long.class), a(K0, 0L, -1L), Arrays.asList(V1, V0, ZERO), EQ);
    	t("lrem", p(String.class, long.class, String.class), a(K0, 1L, ZERO), 1L, EQ);
    	t("lrange", p(String.class, long.class, long.class), a(K0, 0L, -1L), Arrays.asList(V1, V0), EQ);

    	// Set
    	redis.del(K0);
    	redis.del(K1);
    	redis.del(K2);
    	t("sadd", p(String.class, String[].class), a(K0, (Object) new String[] { V0, V1, V2 }), 3L, EQ);
    	t("scard", p(String.class), a(K0), 3L, EQ);
    	t("smembers", p(String.class), a(K0), Sets.newHashSet(V0, V1, V2), EQ);
    	t("srandmember", p(String.class), a(K0), Sets.newHashSet(V0, V1, V2), IN);
    	t("srandmember", p(String.class, int.class), a(K0, 3), Sets.newHashSet(V0, V1, V2), EQ);
    	t("sismember", p(String.class, String.class), a(K0, V0), true, EQ);
    	t("srem", p(String.class, String[].class), a(K0, (Object) new String[] { V0, V1 }), 2L, EQ);
    	
    	// Sorted Set
    	redis.del(K0);
    	redis.del(K1);
    	redis.del(K2);
    	Map<Double, byte[]> mapDB = Maps.newHashMap();
    	mapDB.put(1.0, BV0);
    	mapDB.put(2.0, BV1);
    	mapDB.put(3.0, BV2);
    	t("zadd", p(byte[].class, Map.class), a(BK0, mapDB), 3L, EQ);
    	// 1.0 V0 2.0 V1 3.0 V2
    	Map<Double, String> mapDS = Maps.newHashMap();
    	mapDS.put(4.0, V0);
    	mapDS.put(5.0, V1);
    	mapDS.put(6.0, V2);
    	t("zadd", p(String.class, Map.class), a(K0, mapDS), 0L, EQ);	
    	t("zincrby", p(String.class, double.class, String.class), a(K0, 2.0, V2), Double.valueOf(8.0), EQ);
    	Map<byte[], Double> mapBD = Maps.newHashMap();
    	mapBD.put(BV0, 7.0);
    	mapBD.put(BV1, 8.0);
    	mapBD.put(BV2, 9.0);
    	t("zadd2", p(byte[].class, Map.class), a(BK0, mapBD), 0L, EQ);
    	Map<String, Double> mapSD = Maps.newHashMap();
    	mapSD.put(V0, 10.0);
    	mapSD.put(V1, 20.0);
    	mapSD.put(V2, 30.0);
    	t("zadd2", p(String.class, Map.class), a(K0, mapSD), 0L, EQ);
    	t("zcard", p(String.class), a(K0), 3L, EQ);
    	t("zcount", p(String.class, double.class, double.class), a(K0, -100.0, 100.0), 3L, EQ);
    	t("zcount", p(String.class, String.class, String.class), a(K0, "-100", "100"), 3L, EQ);
    	t("zrank", p(String.class, String.class), a(K0, V0), 0L, EQ);
    	t("zrevrank", p(String.class, String.class), a(K0, V0), 2L, EQ);
    	t("zscore", p(String.class, String.class), a(K0, V0), Double.valueOf(10.0), EQ);
    	t("zrange", p(String.class, long.class, long.class), a(K0, 0L, -1L), Sets.newHashSet(V0, V1, V2), EQ);
    	t("zrangeByLex", p(String.class, String.class, String.class, int.class, int.class), a(K0, "(A", "(z", 0,2), Sets.newHashSet(V0, V1), EQ);
    	t("zrangeByScore", p(String.class, double.class, double.class), a(K0, -100L, 25L), Sets.newHashSet(V0, V1), EQ);
    	t("zrangeByScore", p(String.class, double.class, double.class, int.class, int.class), a(K0, -100L, 100L, 0, 1), Sets.newHashSet(V0), EQ);
    	t("zrangeByScore", p(String.class, String.class, String.class), a(K0, "0", "100"), Sets.newHashSet(V0, V1, V2), EQ);
    	t("zrangeByScore", p(String.class, String.class, String.class, int.class, int.class), a(K0, "0", "100", 1, 1), Sets.newHashSet(V1), EQ);
    	Set<Tuple> mT = Sets.newHashSet();
    	mT.add(new Tuple(V0, 10.0));
    	mT.add(new Tuple(V1, 20.0));
    	mT.add(new Tuple(V2, 30.0));
    	t("zrangeByScoreWithScores", p(String.class, double.class, double.class), a(K0, -100L, 100L), mT, EQ);
    	t("zrangeByScoreWithScores", p(String.class, double.class, double.class, int.class, int.class), a(K0, -100L, 100L, 0, 10), mT, EQ);
    	t("zrangeByScoreWithScores", p(String.class, String.class, String.class), a(K0, "-100", "100"), mT, EQ);
    	t("zrangeByScoreWithScores", p(String.class, String.class, String.class, int.class, int.class), a(K0, "-100", "100", 0, 10), mT, EQ);
    	t("zrangeWithScores", p(String.class, long.class, long.class), a(K0, 0L, -1L), mT, EQ);
    	t("zrevrange", p(String.class, long.class, long.class), a(K0, 0L, -1L), Sets.newHashSet(V0, V1, V2), EQ);
    	t("zrevrangeByScore", p(String.class, double.class, double.class), a(K0, 25.0, 15.0), Sets.newHashSet(V1), EQ);
    	t("zrevrangeByScore", p(String.class, double.class, double.class, int.class, int.class), a(K0, 35.0, 15.0, 0, 1), Sets.newHashSet(V2), EQ);
    	t("zrevrangeByScore", p(String.class, String.class, String.class), a(K0, "25.0", "15.0"), Sets.newHashSet(V1), EQ);
    	t("zrevrangeByScore", p(String.class, String.class, String.class, int.class, int.class), a(K0, "35.0", "15.0", 0, 1), Sets.newHashSet(V2), EQ);
    	mT.clear();
    	mT.add(new Tuple(V2, 30.0));
    	mT.add(new Tuple(V1, 20.0));
    	mT.add(new Tuple(V0, 10.0));    	
    	t("zrevrangeByScoreWithScores", p(String.class, double.class, double.class), a(K0, 100.0, -100.0), mT, EQ);
    	t("zrevrangeByScoreWithScores", p(String.class, double.class, double.class, int.class, int.class), a(K0, 100.0, -100.0, 0, 10), mT, EQ);
    	t("zrevrangeByScoreWithScores", p(String.class, String.class, String.class), a(K0, "100", "-100"), mT, EQ);
    	t("zrevrangeByScoreWithScores", p(String.class, String.class, String.class, int.class, int.class), a(K0, "100", "-100", 0, 10), mT, EQ);
    	t("zrevrangeWithScores", p(String.class, long.class, long.class), a(K0, 0L, -1L), mT, EQ);
    	t("zremrangeByScore", p(String.class, String.class, String.class), a(K0, "0", "15"), 1L, EQ);
    	t("zremrangeByScore", p(String.class, double.class, double.class), a(K0, 0.0, 15.0), 0L, EQ);
    	t("zremrangeByRank", p(String.class, long.class, long.class), a(K0, 1L, 1L), 1L, EQ);
		t("zrem", p(String.class, String[].class), a(K0, (Object) new String[] { V1 }), 1L, EQ);
    	
    	// TripleS List
    	redis.del(K0);
    	t("sladdAt", p(String.class, String.class, String.class, long.class, String[].class), a(K0, F0, N0, now + 10000L, (Object) new String[] {V0}), 1L, EQ);
		t("sladd", p(String.class, String.class, String.class, long.class, String[].class), a(K0, F0, N1, 100000L, (Object) new String[] { ZERO }), 1L, EQ);
		t("sladd", p(String.class, String.class, String.class, String[].class), a(K0, F0, N2, (Object) new String[] { ZERO }), 1L, EQ);
		t("slset", p(String.class, String.class, String.class, long.class, String[].class), a(K0, F0, N1, 100000L, (Object) new String[] { V1 }), 0L, EQ);
    	t("slset", p(String.class, String.class, String.class, String[].class), a(K0, F0, N2, (Object) new String[] { V2 }), 0L, EQ);
		t("slcount", p(byte[].class), a(BK0), 1L, EQ);
    	t("slcount", p(byte[].class, byte[].class), a(BK0, F0.getBytes(Charsets.UTF_8)), 3L, EQ);
    	t("slcount", p(String.class), a(K0), 1L, EQ);
    	t("slcount", p(String.class, String.class), a(K0, F0), 3L, EQ);
    	t("slcount", p(String.class, String.class, String.class), a(K0, F0, N0), 1L, EQ);
    	t("slexists", p(String.class, String.class, String.class, String.class), a(K0, F0, N0, V0), true, EQ);
    	t("slget", p(String.class, String.class, String.class), a(K0, F0, N0), Sets.newHashSet(V0), EQ);
    	t("slkeys", p(String.class), a(K0), Sets.newHashSet(F0), EQ);
    	t("slkeys", p(String.class, String.class), a(K0, F0), Sets.newHashSet(N0, N1, N2), EQ);
    	t("slvals", p(String.class, String.class), a(K0, F0), Sets.newHashSet(V0, V1, V2), EQ);
    	Map<String, List<String>> mSLS = Maps.newHashMap();
    	mSLS.put(N0, Arrays.asList(V0));
    	mSLS.put(N1, Arrays.asList(V1));
    	mSLS.put(N2, Arrays.asList(V2));
    	t("slmget", p(String.class, String.class, String[].class), a(K0, F0, (Object) new String[] { N0, N1, N2 }), mSLS, EQ);
    	t("slexpire", p(byte[].class, byte[].class, byte[].class, byte[].class, long.class), a(BK0, F0.getBytes(Charsets.UTF_8), N0.getBytes(Charsets.UTF_8), BV0, 10000L), 1L, EQ);
    	t("slttl", p(byte[].class, byte[].class, byte[].class), a(BK0, F0.getBytes(Charsets.UTF_8), N0.getBytes(Charsets.UTF_8)), 10000L, LT);
    	t("slexpire", p(String.class, long.class), a(K0, 10000L), 1L, EQ);
    	t("slexpire", p(String.class, String.class, long.class), a(K0, F0, 10000L), 1L, EQ);
    	t("slexpire", p(String.class, String.class, String.class, long.class), a(K0, F0, N0, 10000L), 1L, EQ);
    	t("slttl", p(String.class, String.class, String.class), a(K0, F0, N0), 1000L, LT);
    	t("slexpire", p(String.class, String.class, String.class, String.class, long.class), a(K0, F0, N0, V0, 1000000L), 1L, EQ);
    	t("slttl", p(String.class, String.class, String.class, String.class), a(K0, F0, N0, V0), 100000L, LT);
    	t("slrem", p(String.class, String.class, String.class, String[].class), a(K0, F0, N0, (Object) new String[] { V0 }), 1L, EQ);
    	t("slrem", p(String.class, String.class, String.class), a(K0, F0, N1), 1L, EQ);
    	t("slrem", p(String.class, String.class), a(K0, F0), 1L, EQ);
    	t("sldel", p(String.class), a(K0), 0L, EQ);
    	
    	// TripleS Set
    	redis.del(K0);
    	t("ssaddAt", p(String.class, String.class, String.class, long.class, String[].class), a(K0, F0, N0, now + 10000L, (Object) new String[] {V0}), 1L, EQ);
		t("ssadd", p(String.class, String.class, String.class, long.class, String[].class), a(K0, F0, N1, 100000L, (Object) new String[] { ZERO }), 1L, EQ);
		t("ssadd", p(String.class, String.class, String.class, String[].class), a(K0, F0, N2, (Object) new String[] { ZERO }), 1L, EQ);
		t("ssset", p(String.class, String.class, String.class, long.class, String[].class), a(K0, F0, N1, 100000L, (Object) new String[] { V1 }), 0L, EQ);
    	t("ssset", p(String.class, String.class, String.class, String[].class), a(K0, F0, N2, (Object) new String[] { V2 }), 0L, EQ);
		t("sscount", p(byte[].class), a(BK0), 1L, EQ);
    	t("sscount", p(byte[].class, byte[].class), a(BK0, F0.getBytes(Charsets.UTF_8)), 3L, EQ);
    	t("sscount", p(String.class), a(K0), 1L, EQ);
    	t("sscount", p(String.class, String.class), a(K0, F0), 3L, EQ);
    	t("sscount", p(String.class, String.class, String.class), a(K0, F0, N0), 1L, EQ);
    	t("ssexists", p(String.class, String.class, String.class, String.class), a(K0, F0, N0, V0), true, EQ);
    	t("ssget", p(String.class, String.class, String.class), a(K0, F0, N0), Sets.newHashSet(V0), EQ);
    	t("sskeys", p(String.class), a(K0), Sets.newHashSet(F0), EQ);
    	t("sskeys", p(String.class, String.class), a(K0, F0), Sets.newHashSet(N0, N1, N2), EQ);
    	t("ssvals", p(String.class, String.class), a(K0, F0), Sets.newHashSet(V0, V1, V2), EQ);
    	t("ssmget", p(String.class, String.class, String[].class), a(K0, F0, (Object) new String[] { N0, N1, N2 }), mSLS, EQ);
    	t("ssexpire", p(byte[].class, byte[].class, byte[].class, byte[].class, long.class), a(BK0, F0.getBytes(Charsets.UTF_8), N0.getBytes(Charsets.UTF_8), BV0, 10000L), 1L, EQ);
    	t("ssttl", p(byte[].class, byte[].class, byte[].class), a(BK0, F0.getBytes(Charsets.UTF_8), N0.getBytes(Charsets.UTF_8)), 10000L, LT);
    	t("ssexpire", p(String.class, long.class), a(K0, 10000L), 1L, EQ);
    	t("ssexpire", p(String.class, String.class, long.class), a(K0, F0, 10000L), 1L, EQ);
    	t("ssexpire", p(String.class, String.class, String.class, long.class), a(K0, F0, N0, 10000L), 1L, EQ);
    	t("ssttl", p(String.class, String.class, String.class), a(K0, F0, N0), 1000L, LT);
    	t("ssexpire", p(String.class, String.class, String.class, String.class, long.class), a(K0, F0, N0, V0, 1000000L), 1L, EQ);
    	t("ssttl", p(String.class, String.class, String.class, String.class), a(K0, F0, N0, V0), 100000L, LT);
    	t("ssrem", p(String.class, String.class, String.class, String[].class), a(K0, F0, N0, (Object) new String[] { V0 }), 1L, EQ);
    	t("ssrem", p(String.class, String.class, String.class), a(K0, F0, N1), 1L, EQ);
    	t("ssrem", p(String.class, String.class), a(K0, F0), 1L, EQ);
    	t("ssdel", p(String.class), a(K0), 0L, EQ);
    }

    @Test
    public void redisBinary() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        pipeline.setTimeout(10000);

        List<Object> expect = new ArrayList<Object>();

        // set, delete, get.
        pipeline.set(REDIS_BKEY_0, REDIS_BVALUE_0);
        expect.add("OK");
        pipeline.exists(REDIS_BKEY_0);
        expect.add(Boolean.TRUE);
        pipeline.get(REDIS_BKEY_0);
        expect.add(REDIS_BVALUE_0);
        pipeline.del(REDIS_BKEY_0);
        expect.add(1L);
        pipeline.exists(REDIS_BKEY_0);
        expect.add(Boolean.FALSE);

        // set if not exist.
        pipeline.set(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BNX, REDIS_BEX, 2);
        expect.add("OK");

        pipeline.set(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BNX, REDIS_BEX, 2);
        expect.add(null);

        // set if exist.
        pipeline.set(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BXX, REDIS_BEX, 2);
        expect.add("OK");

        pipeline.del(REDIS_BKEY_0);
        expect.add(1L);
        pipeline.set(REDIS_BKEY_0, REDIS_BVALUE_0, REDIS_BXX, REDIS_BEX, 2);
        expect.add(null);

        // type, ttl
        pipeline.set(REDIS_BKEY_0, REDIS_BVALUE_0);
        expect.add("OK");
        pipeline.type(REDIS_BKEY_0);
        expect.add("string");
        pipeline.expire(REDIS_BKEY_0, 10);
        expect.add(1L);
        pipeline.ttl(REDIS_BKEY_0);
        expect.add(10L);

        pipeline.hset(REDIS_BKEY_1, REDIS_BKEY_2, REDIS_BVALUE_0);
        expect.add(1L);
        pipeline.hget(REDIS_BKEY_1, REDIS_BKEY_2);
        expect.add(REDIS_BVALUE_0);

        List<Object> result = pipeline.syncAndReturnAll();
        System.out.println(result);
        assertEquals(expect.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            if (expect.get(i) instanceof byte[]) {
                Arrays.equals((byte[]) expect.get(i), (byte[]) result.get(i));
            } else {
                assertEquals("index=" + i, expect.get(i), result.get(i));
            }
        }
        pipeline.close();
    }

    @Test
    public void redis() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        pipeline.setTimeout(10000);

        List<Object> expect = new ArrayList<Object>();

        // set, exists.
        pipeline.set(REDIS_KEY_0, REDIS_VALUE_0);
        expect.add("OK");
        pipeline.exists(REDIS_KEY_0);
        expect.add(Boolean.TRUE);
        pipeline.del(REDIS_KEY_0);
        expect.add(1L);
        pipeline.exists(REDIS_KEY_0);
        expect.add(Boolean.FALSE);

        // set if not exist.
        pipeline.set(REDIS_KEY_0, REDIS_VALUE_0, REDIS_NX, REDIS_EX, 2);
        expect.add("OK");

        pipeline.set(REDIS_KEY_0, REDIS_VALUE_0, REDIS_NX, REDIS_EX, 2);
        expect.add(null);

        // set if exist.
        pipeline.set(REDIS_KEY_0, REDIS_VALUE_0, REDIS_XX, REDIS_EX, 2);
        expect.add("OK");

        pipeline.del(REDIS_KEY_0);
        expect.add(1L);
        pipeline.set(REDIS_KEY_0, REDIS_VALUE_0, REDIS_XX, REDIS_EX, 2);
        expect.add(null);

        // set, type, expire.
        pipeline.set(REDIS_KEY_0, REDIS_VALUE_0);
        expect.add("OK");
        pipeline.type(REDIS_KEY_0);
        expect.add("string");
        pipeline.expire(REDIS_KEY_0, 10);
        expect.add(1L);
        pipeline.ttl(REDIS_KEY_0);
        expect.add(10L);

        List<Object> result = pipeline.syncAndReturnAll();
        System.out.println(result);
        assertEquals(expect.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(expect.get(i), result.get(i));
        }
        pipeline.close();
    }

    @Test
    public void hash() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        pipeline.setTimeout(10000);

        List<Object> expect = new ArrayList<Object>();

        for (int i = 0; i < 10; i++) {
            pipeline.hincrBy(REDIS_KEY_0, REDIS_FIELD_0, 1);
            expect.add(Long.valueOf(i + 1));
        }
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(expect.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(expect.get(i), result.get(i));
        }

        pipeline.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void triples() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        pipeline.setTimeout(10000);

        // add, get.
        pipeline.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        pipeline.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        // once more.
        pipeline.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        pipeline.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);

        // mget.
        pipeline.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        pipeline.ssmget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_NAME_1);

        pipeline.ssmget(TRIPLES_KEY_1, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_NAME_1);

        List<Object> result = pipeline.syncAndReturnAll();
        int index = 0;
        // check result.
        assertEquals(1L, result.get(index++));
        Set<String> values = (Set<String>) result.get(index++);
        assertEquals(1, values.size());
        assertEquals(true, values.contains(TRIPLES_VALUE_0));

        // check second result.
        assertEquals(1L, result.get(index++));
        values = (Set<String>) result.get(index++);
        assertEquals(2, values.size());
        assertEquals(true, values.contains(TRIPLES_VALUE_1));

        // check mget result.
        assertEquals(1L, result.get(index++));
        Map<String, Set<String>> hash = (Map<String, Set<String>>) result.get(index++);
        assertEquals(2, hash.get(TRIPLES_NAME_0).size());
        assertEquals(1, hash.get(TRIPLES_NAME_1).size());

        pipeline.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void triplesBinary() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        pipeline.setTimeout(10000);

        // add, get.
        pipeline.ssadd(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0, 3, TRIPLES_BVALUE_0);
        pipeline.ssget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0);
        pipeline.ssadd(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0, 3, TRIPLES_BVALUE_1);
        pipeline.ssget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0);

        pipeline.ssadd(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_1, 3, TRIPLES_BVALUE_0);
        pipeline.ssmget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0, TRIPLES_BNAME_1);

        List<Object> result = pipeline.syncAndReturnAll();
        System.out.println(result.size() + ", " + result);
        int index = 0;
        assertEquals(1L, result.get(index++));
        Set<byte[]> values = (Set<byte[]>) result.get(index++);
        assertEquals(1, values.size());

        assertEquals(1L, result.get(index++));
        values = (Set<byte[]>) result.get(index++);
        assertEquals(2, values.size());

        assertEquals(1L, result.get(index++));
        Map<byte[], Set<byte[]>> hash = (Map<byte[], Set<byte[]>>) result.get(index++);
        assertEquals(2, hash.get(TRIPLES_BNAME_0).size());
        assertEquals(1, hash.get(TRIPLES_BNAME_1).size());


        pipeline.close();
    }

    @Test
    public void bitfieldCommands() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.bitfield(REDIS_KEY_0, INCRBY.toString(), "u2", "100", "1", OVERFLOW.toString(),
                SAT.toString(), INCRBY.toString(), "u2", "102", "1");
        pipeline.bitfield(REDIS_KEY_0.getBytes(), INCRBY.raw, "u2".getBytes(), "100".getBytes(),
                "1".getBytes(), OVERFLOW.raw, SAT.raw, INCRBY.raw, "u2".getBytes(), "102".getBytes(), "1".getBytes());
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(2, result.size());

        List<Long> reply = (List<Long>) result.get(0);
        assertEquals(2, reply.size());
        assertTrue(1 == reply.get(0));
        assertTrue(1 == reply.get(1));

        reply = (List<Long>) result.get(1);
        assertEquals(2, reply.size());
        assertTrue(2 == reply.get(0));
        assertTrue(2 == reply.get(1));

        pipeline.close();
    }
    
    @Test
    public void geoCommands() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        double longitude = 1.0;
        double latitude = 2.0;
        String member = "Seoul";
        pipeline.geoadd(REDIS_KEY_0, longitude, latitude, member);

        Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<String, GeoCoordinate>();
        memberCoordinateMap.put("Daejon", new GeoCoordinate(2.0, 3.0));
        memberCoordinateMap.put("Daegu", new GeoCoordinate(30.0, 40.0));
        pipeline.geoadd(REDIS_KEY_0, memberCoordinateMap);
        pipeline.geodist(REDIS_KEY_0, "Seoul", "Daejon");
        pipeline.geodist(REDIS_KEY_0, "Seoul", "Daejon", GeoUnit.KM);
        
        pipeline.geohash(REDIS_KEY_0, "Seoul", "Daejon");
        
        pipeline.geopos(REDIS_KEY_0, "Seoul", "Daejon");
        
        pipeline.georadius(REDIS_KEY_0, 1.0, 2.0, 1000.0, GeoUnit.KM);
        
        pipeline.georadius(REDIS_KEY_0, 1.0, 2.0, 1000.0, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
        
        pipeline.georadiusByMember(REDIS_KEY_0, "Seoul", 1000.0, GeoUnit.KM);
        
        pipeline.georadiusByMember(REDIS_KEY_0, "Seoul", 1000.0, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());

        pipeline.geodist(REDIS_KEY_0.getBytes(Charsets.UTF_8), "Seoul".getBytes(Charsets.UTF_8), "Daejon".getBytes(Charsets.UTF_8));
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(11, result.size());
        
        assertEquals(Long.valueOf(1), result.get(0));
        
        assertEquals(Long.valueOf(2), result.get(1));
        assertEquals(157222, ((Double) result.get(2)).intValue());
        assertEquals(157, ((Double) result.get(3)).intValue());
        
        List<String> hashes = (List<String>) result.get(4);
        assertEquals("s02equ04ve0", hashes.get(0));
        assertEquals("s093jd0k720", hashes.get(1));
        
        List<GeoCoordinate> coords = (List<GeoCoordinate>) result.get(5);
        assertEquals(2, coords.size());
        
        List<GeoRadiusResponse> members = (List<GeoRadiusResponse>) result.get(6);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());

        members = (List<GeoRadiusResponse>) result.get(7);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
        
        members = (List<GeoRadiusResponse>) result.get(8);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
        
        members = (List<GeoRadiusResponse>) result.get(9);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());

        assertEquals(157222, ((Double) result.get(10)).intValue());
        
        pipeline.close();
    }
    
    @Test
    public void hyperLogLogCommands() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.pfadd(REDIS_KEY_0, "a", "b", "c", "d", "e", "f", "g");
        pipeline.pfcount(REDIS_KEY_0);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(2, result.size());
        
        assertEquals(Long.valueOf(1), result.get(0));
        assertEquals(Long.valueOf(7), result.get(1));

        pipeline.close();
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void hscan() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.hset(REDIS_KEY_0, "b", "b");
        pipeline.hset(REDIS_KEY_0, "a", "a");

        pipeline.hscan(REDIS_KEY_0, SCAN_POINTER_START);

        // binary
        pipeline.hset(REDIS_KEY_0.getBytes(), bbar, bcar);
        
        pipeline.hscan(REDIS_KEY_0.getBytes(), SCAN_POINTER_START_BINARY);
        
        pipeline.hscan(REDIS_KEY_0, SCAN_POINTER_START, new ScanParams().match("*"));
        pipeline.hscan(REDIS_KEY_0.getBytes(), SCAN_POINTER_START_BINARY, new ScanParams().match("*"));
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(7, result.size());

        @SuppressWarnings("unchecked")
        ScanResult<Map.Entry<String, String>> scanResult = (ScanResult<Entry<String, String>>) result.get(2);
        assertEquals(0, scanResult.getCursor());
        assertFalse(scanResult.getResult().isEmpty());

        @SuppressWarnings("unchecked")
        ScanResult<Map.Entry<byte[], byte[]>> bResult = (ScanResult<Entry<byte[], byte[]>>) result.get(4);

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
        
        scanResult = (ScanResult<Entry<String, String>>) result.get(5);
        assertEquals(0, scanResult.getCursor());
        assertFalse(scanResult.getResult().isEmpty());

        bResult = (ScanResult<Entry<byte[], byte[]>>) result.get(6);
        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        pipeline.close();
    }

    @Test
    public void scan() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.set(REDIS_KEY_0, "b");
        pipeline.set(REDIS_KEY_1, "a");

        pipeline.scan(SCAN_POINTER_START);

        pipeline.scan(SCAN_POINTER_START_BINARY);
        
        pipeline.scan(SCAN_POINTER_START_BINARY, new ScanParams().match("*"));
        pipeline.scan(SCAN_POINTER_START, new ScanParams().match("*"));
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(6, result.size());
        
        @SuppressWarnings("unchecked")
        ScanResult<String> scanResult = (ScanResult<String>) result.get(2); 

        assertNotNull(scanResult.getStringCursor());
        assertFalse(scanResult.getResult().isEmpty());

        // binary
        @SuppressWarnings("unchecked")
        ScanResult<byte[]> bResult = (ScanResult<byte[]>) result.get(3); 

        assertNotNull(bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        // binary, scan param
        bResult = (ScanResult<byte[]>) result.get(4);

        assertNotNull(bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
        
        // binary
        scanResult = (ScanResult<String>) result.get(5); 

        assertNotNull(bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        pipeline.close();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void cscan() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.cscanlen();
        pipeline.cscandigest();
        
        // String
        pipeline.set(REDIS_KEY_0, "b");
        pipeline.set(REDIS_KEY_1, "a");
        pipeline.cscan(0, SCAN_POINTER_START);
        pipeline.cscan(0, SCAN_POINTER_START, new ScanParams().match("*").count(10));

        // binary
        pipeline.cscan(0, SCAN_POINTER_START_BINARY);
        pipeline.cscan(0, SCAN_POINTER_START_BINARY, new ScanParams().match("*").count(10));

        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(8, result.size());
        
        assertTrue((Long) result.get(0) > 0);
        assertNotNull(result.get(1));
        
        ScanResult<String> sResult = (ScanResult<String>) result.get(4);
        assertNotNull(sResult.getStringCursor());
        assertFalse(sResult.getResult().isEmpty());
        
        sResult = (ScanResult<String>) result.get(5);
        assertNotNull(sResult.getStringCursor());
        assertFalse(sResult.getResult().isEmpty());
        
        ScanResult<byte[]> bResult = (ScanResult<byte[]>) result.get(6); 
        assertNotNull(bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
        
        bResult = (ScanResult<byte[]>) result.get(7); 
        assertNotNull(bResult.getStringCursor());
        assertFalse(bResult.getResult().isEmpty());
                    
        pipeline.close();
    }
    
    @Test
    public void sscan() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.sadd(REDIS_KEY_0, "a", "b");

        pipeline.sscan(REDIS_KEY_0, SCAN_POINTER_START);
        
        // binary
        pipeline.sadd(REDIS_KEY_0.getBytes(), ba, bb);

        pipeline.sscan(REDIS_KEY_0.getBytes(), SCAN_POINTER_START_BINARY);
        
        pipeline.sscan(REDIS_KEY_0, SCAN_POINTER_START, new ScanParams().match("*"));
        pipeline.sscan(REDIS_KEY_0.getBytes(), SCAN_POINTER_START_BINARY, new ScanParams().match("*"));
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(6, result.size());
        
        @SuppressWarnings("unchecked")
        ScanResult<String> scanResult = (ScanResult<String>) result.get(1);  

        assertEquals(SCAN_POINTER_START, scanResult.getStringCursor());
        assertFalse(scanResult.getResult().isEmpty());

        @SuppressWarnings("unchecked")
        ScanResult<byte[]> bResult = (ScanResult<byte[]>) result.get(3); 

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        scanResult = (ScanResult<String>) result.get(4);
        assertEquals(SCAN_POINTER_START, scanResult.getStringCursor());
        assertFalse(scanResult.getResult().isEmpty());
        
        bResult = (ScanResult<byte[]>) result.get(5); 
        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        pipeline.close();
    }

    @Test
    public void zscan() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);

        pipeline.zadd(REDIS_KEY_0, 1, "a");
        pipeline.zadd(REDIS_KEY_0, 2, "b");

        pipeline.zscan(REDIS_KEY_0, SCAN_POINTER_START);

        // binary
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, ba);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bb);

        pipeline.zscan(REDIS_KEY_0.getBytes(), SCAN_POINTER_START_BINARY);

        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(6, result.size());

        @SuppressWarnings("unchecked")
        ScanResult<Tuple> scanResult = (ScanResult<Tuple>) result.get(2);

        assertEquals(SCAN_POINTER_START, scanResult.getStringCursor());
        assertFalse(scanResult.getResult().isEmpty());

        @SuppressWarnings("unchecked")
        ScanResult<Tuple> bResult = (ScanResult<Tuple>) result.get(5);

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        pipeline.close();
    }

    @Test
    public void zscanMatch() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);

        ScanParams params = new ScanParams();
        params.match("a*");

        pipeline.zadd(REDIS_KEY_0, 2, "b");
        pipeline.zadd(REDIS_KEY_0, 1, "a");
        pipeline.zadd(REDIS_KEY_0, 11, "aa");
        pipeline.zscan(REDIS_KEY_0, SCAN_POINTER_START, params);

        // binary
        params = new ScanParams();
        params.match(bbarstar);
        
        pipeline.zadd(REDIS_KEY_0.getBytes(), 2, bbar1);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bbar2);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 11, bbar3);
        pipeline.zscan(REDIS_KEY_0.getBytes(), SCAN_POINTER_START_BINARY, params);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(8, result.size());
        
        @SuppressWarnings("unchecked")
        ScanResult<Tuple> scanResult = (ScanResult<Tuple>) result.get(3); 

        assertEquals(SCAN_POINTER_START, scanResult.getStringCursor());
        assertFalse(scanResult.getResult().isEmpty());

        @SuppressWarnings("unchecked")
        ScanResult<Tuple> bResult = (ScanResult<Tuple>) result.get(7); 

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());

        pipeline.close();
    }

    @Test
    public void zscanCount() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);

        ScanParams params = new ScanParams();
        params.count(2);

        pipeline.zadd(REDIS_KEY_0, 1, "a1");
        pipeline.zadd(REDIS_KEY_0, 2, "a2");
        pipeline.zadd(REDIS_KEY_0, 3, "a3");
        pipeline.zadd(REDIS_KEY_0, 4, "a4");
        pipeline.zadd(REDIS_KEY_0, 5, "a5");
        
        pipeline.zscan(REDIS_KEY_0, SCAN_POINTER_START, params);

        // binary
        params = new ScanParams();
        params.count(2);

        pipeline.zadd(REDIS_KEY_0.getBytes(), 2, bbar1);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bbar2);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 11, bbar3);
        
        pipeline.zscan(REDIS_KEY_0.getBytes(), SCAN_POINTER_START_BINARY, params);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(10, result.size());
        
        @SuppressWarnings("unchecked")
        ScanResult<Tuple> scanResult = (ScanResult<Tuple>) result.get(5); 

        assertFalse(scanResult.getResult().isEmpty());

        @SuppressWarnings("unchecked")
        ScanResult<Tuple> bResult = (ScanResult<Tuple>) result.get(9);

        assertFalse(bResult.getResult().isEmpty());

        pipeline.close();
    }

    @Test
    public void zlexcount() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.zadd(REDIS_KEY_0, 1, "a");
        pipeline.zadd(REDIS_KEY_0, 1, "b");
        pipeline.zadd(REDIS_KEY_0, 1, "c");
        pipeline.zadd(REDIS_KEY_0, 1, "aa");

        pipeline.zlexcount(REDIS_KEY_0, "[aa", "(c");
        pipeline.zlexcount(REDIS_KEY_0, "-", "+");
        pipeline.zlexcount(REDIS_KEY_0, "-", "(c");
        pipeline.zlexcount(REDIS_KEY_0, "[aa", "+");
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(8, result.size());
        
        long count = (Long) result.get(4); 
        assertEquals(2, count);

        count = (Long) result.get(5);
        assertEquals(4, count);

        count = (Long) result.get(6);
        assertEquals(3, count);

        count = (Long) result.get(7);
        assertEquals(3, count);

        pipeline.close();
    }

    @Test
    public void zlexcountBinary() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        // Binary
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, ba);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bc);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bb);

        pipeline.zlexcount(REDIS_KEY_0.getBytes(), bInclusiveB, bExclusiveC);
        pipeline.zlexcount(REDIS_KEY_0.getBytes(), bLexMinusInf, bLexPlusInf);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(5, result.size());
        
        long count = (Long) result.get(3);
        assertEquals(1, count);

        count = (Long) result.get(4);
        assertEquals(3, count);

        pipeline.close();
    }

    @Test
    public void zremrangeByLex() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.zadd(REDIS_KEY_0, 1, "a");
        pipeline.zadd(REDIS_KEY_0, 1, "b");
        pipeline.zadd(REDIS_KEY_0, 1, "c");
        pipeline.zadd(REDIS_KEY_0, 1, "aa");

        pipeline.zremrangeByLex(REDIS_KEY_0, "[aa", "(c");
        pipeline.zrangeByLex(REDIS_KEY_0, "-", "+");
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(6, result.size());
        
        long count = (Long) result.get(4);  

        assertEquals(2, count);

        Set<String> expected = new LinkedHashSet<String>();
        expected.add("a");
        expected.add("c");

        assertEquals(expected, result.get(5)); 

        pipeline.close();
    }

    @Test
    public void zrangeByLexBinary() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        // binary
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, ba);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bc);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bb);

        pipeline.zrangeByLex(REDIS_KEY_0.getBytes(), bInclusiveB, bExclusiveC);
        pipeline.zrangeByLex(REDIS_KEY_0.getBytes(), bLexMinusInf, bLexPlusInf, 0, 2);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(5, result.size());
        
        Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
        bExpected.add(bb);

        assertByteArraySetEquals(bExpected, (Set<byte[]>) result.get(3));

        bExpected.clear();
        bExpected.add(ba);
        bExpected.add(bb);

        // with LIMIT
        assertByteArraySetEquals(bExpected, (Set<byte[]>) result.get(4));

        pipeline.close();
    }

    @Test
    public void zremrangeByLexBinary() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, ba);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bc);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bb);

        pipeline.zremrangeByLex(REDIS_KEY_0.getBytes(), bInclusiveB, bExclusiveC);
        pipeline.zrangeByLex(REDIS_KEY_0.getBytes(), bLexMinusInf, bLexPlusInf);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(5, result.size());
        
        long bresult = (Long) result.get(3);

        assertEquals(1, bresult);

        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(ba);
        bexpected.add(bc);

        assertByteArraySetEquals(bexpected, (Set<byte[]>) result.get(4));

        pipeline.close();
    }

    @Test
    public void zrevrangeByLex() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.zadd(REDIS_KEY_0, 1, "aa");
        pipeline.zadd(REDIS_KEY_0, 1, "c");
        pipeline.zadd(REDIS_KEY_0, 1, "bb");
        pipeline.zadd(REDIS_KEY_0, 1, "d");

        pipeline.zrevrangeByLex(REDIS_KEY_0, "[c", "(aa");
        pipeline.zrevrangeByLex(REDIS_KEY_0, "+", "-", 1, 2);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(6, result.size());
        
        Set<String> expected = new LinkedHashSet<String>();
        expected.add("c");
        expected.add("bb");

        // exclusive aa ~ inclusive c
        assertEquals(expected, result.get(4));

        expected.clear();
        expected.add("c");
        expected.add("bb");

        // with LIMIT
        assertEquals(expected, result.get(5));

        pipeline.close();
    }

    @Test
    public void zrevrangeByLexBinary() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        // binary
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, ba);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bc);
        pipeline.zadd(REDIS_KEY_0.getBytes(), 1, bb);

        pipeline.zrevrangeByLex(REDIS_KEY_0.getBytes(), bExclusiveC, bInclusiveB);
        pipeline.zrevrangeByLex(REDIS_KEY_0.getBytes(), bLexPlusInf, bLexMinusInf, 0, 2);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(5, result.size());
        
        Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
        bExpected.add(bb);

        assertByteArraySetEquals(bExpected, (Set<byte[]>) result.get(3));

        bExpected.clear();
        bExpected.add(bb);
        bExpected.add(ba);

        // with LIMIT
        assertByteArraySetEquals(bExpected, (Set<byte[]>) result.get(4));

        pipeline.close();
    }

    @Test
    public void hstrlen() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.hset(REDIS_KEY_0, "myhash", "k1");
        pipeline.hstrlen("myhash", "k1");
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(2, result.size());
        
        Long response = (Long) result.get(1); 
        assertEquals(0l, response.longValue());

        pipeline.close();
    }
    
    @Test
    public void touch() {
        RedisClusterPipeline pipeline = new RedisClusterPipeline(null);
        pipeline.setServer(server);
        
        pipeline.set(REDIS_KEY_0, REDIS_VALUE_0);
        pipeline.set(REDIS_KEY_1, REDIS_VALUE_1);
        pipeline.set(REDIS_KEY_2, REDIS_VALUE_2);
        pipeline.touch(REDIS_KEY_0, REDIS_KEY_1, REDIS_KEY_2);
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(4, result.size());
        assertEquals(Long.valueOf(3), result.get(3));

        pipeline.close();
    }
}

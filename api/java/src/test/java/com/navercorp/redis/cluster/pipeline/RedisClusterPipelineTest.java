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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import com.navercorp.redis.cluster.RedisClusterTestBase;

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
        redis.del(REDIS_KEY_0);
        redis.del(REDIS_KEY_1);
        redis.del(REDIS_KEY_2);
        redis.del(REDIS_BKEY_0);
        redis.del(REDIS_BKEY_1);
        redis.del(REDIS_BKEY_2);
        redis.ssdel(TRIPLES_KEY_0);
        redis.ssdel(TRIPLES_BKEY_0);
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
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(10, result.size());
        
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
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(5, result.size());

        @SuppressWarnings("unchecked")
        ScanResult<Map.Entry<String, String>> scanResult = (ScanResult<Entry<String, String>>) result.get(2);
        assertEquals(0, scanResult.getCursor());
        assertFalse(scanResult.getResult().isEmpty());

        @SuppressWarnings("unchecked")
        ScanResult<Map.Entry<byte[], byte[]>> bResult = (ScanResult<Entry<byte[], byte[]>>) result.get(4);

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
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(4, result.size());
        
        @SuppressWarnings("unchecked")
        ScanResult<String> scanResult = (ScanResult<String>) result.get(2); 

        assertNotNull(scanResult.getStringCursor());
        assertFalse(scanResult.getResult().isEmpty());

        // binary
        @SuppressWarnings("unchecked")
        ScanResult<byte[]> bResult = (ScanResult<byte[]>) result.get(3); 

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
        
        List<Object> result = pipeline.syncAndReturnAll();
        assertEquals(4, result.size());
        
        @SuppressWarnings("unchecked")
        ScanResult<String> scanResult = (ScanResult<String>) result.get(1);  

        assertEquals(SCAN_POINTER_START, scanResult.getStringCursor());
        assertFalse(scanResult.getResult().isEmpty());

        @SuppressWarnings("unchecked")
        ScanResult<byte[]> bResult = (ScanResult<byte[]>) result.get(3); 

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

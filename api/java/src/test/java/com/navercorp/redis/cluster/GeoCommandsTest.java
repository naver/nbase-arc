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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.params.geo.GeoRadiusParam;

/**
 * @author jaehong.kim
 */
public class GeoCommandsTest extends RedisClusterTestBase {

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
    }

    @Test
    public void geoadd() {
        double longitude = 1.0;
        double latitude = 2.0;
        String member = "Seoul";
        assertEquals(Long.valueOf(1), redis.geoadd(REDIS_KEY_0, longitude, latitude, member));
    }

    @Test
    public void geodist() {
        Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<String, GeoCoordinate>();
        memberCoordinateMap.put("Seoul", new GeoCoordinate(1.0, 2.0));
        memberCoordinateMap.put("Daejon", new GeoCoordinate(10.0, 20.0));
        assertEquals(Long.valueOf(2), redis.geoadd(REDIS_KEY_0, memberCoordinateMap));
        assertEquals(2228216, redis.geodist(REDIS_KEY_0, "Seoul", "Daejon").intValue());
        assertEquals(2228, redis.geodist(REDIS_KEY_0, "Seoul", "Daejon", GeoUnit.KM).intValue());
    }
    
    @Test
    public void geohash() {
        Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<String, GeoCoordinate>();
        memberCoordinateMap.put("Seoul", new GeoCoordinate(1.0, 2.0));
        memberCoordinateMap.put("Daejon", new GeoCoordinate(10.0, 20.0));
        assertEquals(Long.valueOf(2), redis.geoadd(REDIS_KEY_0, memberCoordinateMap));
        List<String> hashes = redis.geohash(REDIS_KEY_0, "Seoul", "Daejon");
        assertEquals("s02equ04ve0", hashes.get(0));
        assertEquals("s5x1g8cu2y0", hashes.get(1));
    }
    
    @Test
    public void geopos() {
        Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<String, GeoCoordinate>();
        memberCoordinateMap.put("Seoul", new GeoCoordinate(1.0, 2.0));
        memberCoordinateMap.put("Daejon", new GeoCoordinate(10.0, 20.0));
        assertEquals(Long.valueOf(2), redis.geoadd(REDIS_KEY_0, memberCoordinateMap));
        
        List<GeoCoordinate> hashes = redis.geopos(REDIS_KEY_0, "Seoul", "Daejon");
        assertEquals(2, hashes.size());
    }
    
    @Test
    public void georadius() {
        Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<String, GeoCoordinate>();
        memberCoordinateMap.put("Seoul", new GeoCoordinate(1.0, 2.0));
        memberCoordinateMap.put("Daejon", new GeoCoordinate(2.0, 3.0));
        assertEquals(Long.valueOf(2), redis.geoadd(REDIS_KEY_0, memberCoordinateMap));
        
        List<GeoRadiusResponse> members = redis.georadius(REDIS_KEY_0, 1.0, 2.0, 1000.0, GeoUnit.KM);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());

        members = redis.georadius(REDIS_KEY_0, 1.0, 2.0, 1000.0, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
    }
    
    @Test
    public void georadiusByMember() {
        Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<String, GeoCoordinate>();
        memberCoordinateMap.put("Seoul", new GeoCoordinate(1.0, 2.0));
        memberCoordinateMap.put("Daejon", new GeoCoordinate(2.0, 3.0));
        assertEquals(Long.valueOf(2), redis.geoadd(REDIS_KEY_0, memberCoordinateMap));
        
        List<GeoRadiusResponse> members = redis.georadiusByMember(REDIS_KEY_0, "Seoul", 1000.0, GeoUnit.KM);
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
        
        members = redis.georadiusByMember(REDIS_KEY_0, "Seoul", 1000.0, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
        assertEquals("Seoul", members.get(0).getMemberByString());
        assertEquals("Daejon", members.get(1).getMemberByString());
    }
    
}

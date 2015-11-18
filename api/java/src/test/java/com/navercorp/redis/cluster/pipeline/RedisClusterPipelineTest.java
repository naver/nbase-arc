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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.navercorp.redis.cluster.RedisClusterTestBase;

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
}
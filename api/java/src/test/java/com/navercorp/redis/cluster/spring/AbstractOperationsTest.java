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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class AbstractOperationsTest {
    @Autowired
    private StringRedisClusterTemplate redis;
    private Operations<String, String> ops;

    @Before
    public void before() {
        ops = new Operations<String, String>(redis);
    }

    @After
    public void after() {

    }

    @Test
    public void serializer() {
        assertNotNull(ops.keySerializer());
        assertNotNull(ops.valueSerializer());
        assertNotNull(ops.hashKeySerializer());
        assertNotNull(ops.hashValueSerializer());
        assertNotNull(ops.stringSerializer());
    }

    @Test
    public void operations() {
        assertNotNull(ops.getOperations());
    }

    @Test
    public void raw() {
        byte[] string = ops.rawString("key");
        byte[] value = ops.rawValue("value");

        byte[] hashKey = ops.rawHashKey("key");
        byte[] hashValue = ops.rawHashValue("value");

        byte[][] keys = ops.rawKeys("key", "otherKey");
        keys = ops.rawKeys(Arrays.asList("key0", "key1"));
        keys = ops.rawKeys("key", Arrays.asList("key0", "key1"));
    }

    @Test
    public void deserializeHashMap() {
        Map<byte[], byte[]> entries = new HashMap<byte[], byte[]>();
        byte[] hashKey = ops.rawHashKey("key");
        byte[] hashValue = ops.rawHashValue("value");
        entries.put(hashKey, hashValue);
        Map<Object, Object> deserializeHashMap = ops
                .deserializeHashMap(entries);
        System.out.println("deserializeHashMap : " + deserializeHashMap);
    }

    class Operations<K, V> extends AbstractOperations<K, V> {

        Operations(RedisClusterTemplate<K, V> template) {
            super(template);
        }
    }
}
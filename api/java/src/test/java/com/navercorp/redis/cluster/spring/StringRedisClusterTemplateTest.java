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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.gateway.GatewayClient;
import com.navercorp.redis.cluster.pipeline.RedisClusterPipeline;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-StringRedisClusterTemplate.xml")
public class StringRedisClusterTemplateTest {

    @Autowired
    private StringRedisClusterTemplate redisTemplate;

    @Autowired
    private RedisClusterTemplate<String, String> redisTemplateJdkSerialization;

    @Autowired
    GatewayClient gatewayClient;

    private static final String KEY = "key1";
    private static final String KEY2 = "key2";

    private static final String VALUE = "value1";
    private static final String VALUE2 = "value2";

    @Test
    public void basic() {
        // redis command
        redisTemplate.opsForValue().set("foo", "bar");
        String value = redisTemplate.opsForValue().get("foo");
        assertEquals("bar", value);
    }

    @Test
    public void serializationDifferentRedisTemplate() {
        // redis command
        redisTemplate.delete("foo");
        redisTemplateJdkSerialization.opsForValue().set("foo", "bar"); // JDK Serialization
        String value = redisTemplate.opsForValue().get("foo"); // java.lang.String.getBytes()
        assertEquals(null, value);
    }

    @Test
    public void serializationDifferentGatewayClient() {
        // redis command
        gatewayClient.del("foo");
        redisTemplateJdkSerialization.opsForValue().set("foo", "bar"); // JDK Serialization
        String value = gatewayClient.get("foo"); // java.lang.String.getBytes()
        assertEquals(null, value);
    }

    @Test
    public void doPipeline() {
        redisTemplate.delete("test");
        GatewayClient cline = (GatewayClient) redisTemplate.getConnectionFactory().getConnection().getNativeConnection();
        RedisClusterPipeline pipeline = cline.pipeline();

        try {
            pipeline.incr("test");
            pipeline.incr("test");
            pipeline.incr("test");
            pipeline.incr("test");

            List<Object> result = pipeline.syncAndReturnAll();
            Object[] expected = {1L, 2L, 3L, 4L};
            assertArrayEquals(expected, result.toArray());
        } finally {
            pipeline.close();
        }
    }

    @Test
    public void complex() {
        // pipeline & operation
        redisTemplate.delete("test_sortedset");
        GatewayClient cline = (GatewayClient) redisTemplate.getConnectionFactory().getConnection().getNativeConnection();
        RedisClusterPipeline pipeline = cline.pipeline();

        try {
            for (int i = 0; i < 21; i++) {
                Set<String> list = redisTemplate.opsForZSet().range("test_sortedset", 0, -1);
                System.out.println("list: " + list);
                pipeline.zadd("test_sortedset", Double.parseDouble("1234567890"), "00" + i);

                //redisTemplate.opsForZSet().add("test_sortedset", "00" + i, Double.parseDouble("1234567890"));
            }

            List<Object> result = pipeline.syncAndReturnAll();
            System.out.println("result: " + result);
        } finally {
            pipeline.close();
        }
    }

    @Test
    public void multiSetMultiGetDelete() {
        Map<String, String> keysValuesMap = new HashMap<String, String>();
        keysValuesMap.put(KEY, VALUE);
        keysValuesMap.put(KEY2, VALUE2);

        // set
        redisTemplate.opsForValue().multiSet(keysValuesMap);

        List<String> keys = new ArrayList<String>();
        keys.add(KEY);
        keys.add(KEY2);

        List<String> expected = new ArrayList<String>();
        expected.add(VALUE);
        expected.add(VALUE2);

        // get
        List<String> result = redisTemplate.opsForValue().multiGet(keys);
        assertArrayEquals(expected.toArray(), result.toArray());

        // delete
        redisTemplate.delete(keys);
        assertNull(redisTemplate.opsForValue().get(KEY));
        assertNull(redisTemplate.opsForValue().get(KEY2));
    }


}

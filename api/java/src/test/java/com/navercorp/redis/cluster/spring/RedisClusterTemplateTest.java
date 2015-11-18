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

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.util.SafeEncoder;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-example1.xml")
public class RedisClusterTemplateTest {

    @Autowired
    private RedisClusterTemplate<String, String> redisTemplate;

    @Test
    public void defaultKeySerializer() {
        JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer();
        byte[] jdkKey = jdkSerializer.serialize("key_serializer_test-001");
        System.out.println(jdkKey);
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        byte[] stringKey = stringSerializer.serialize("key_serializer_test-001");
        System.out.println(stringKey);

        for (byte b : jdkKey) {
            System.out.print(b);
        }
        System.out.println("");
        for (byte b : stringKey) {
            System.out.print(b);
        }
        System.out.println("");


        redisTemplate.opsForHash().put("redisTemplate_key0", "hashKey0", "hashValue0");
        assertTrue(redisTemplate.opsForHash().hasKey("redisTemplate_key0", "hashKey0"));
        assertFalse(redisTemplate.opsForHash().hasKey("unknown_key", "hashKey0"));
        redisTemplate.delete("redisTemplate_key0");

    }

    @Test
    public void executePipelined() {
        final int batchSize = 10;

        List<Object> results = redisTemplate.executePipelined(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                for (int i = 0; i < batchSize; i++) {
                    connection.incr(SafeEncoder.encode("myqueue"));
                }
                return null;
            }
        });

        System.out.println(results);
    }


    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void exception() {
        redisTemplate.opsForHash().put("redisTemplate_key1", "foo", "bar");
        redisTemplate.opsForValue().get("redisTemplate_key1");
    }
}

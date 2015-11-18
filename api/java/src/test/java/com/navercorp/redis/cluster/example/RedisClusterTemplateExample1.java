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

package com.navercorp.redis.cluster.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.navercorp.redis.cluster.spring.BoundSessionOfHashListOperations;
import com.navercorp.redis.cluster.spring.SessionOfHashSetOperations;
import com.navercorp.redis.cluster.spring.StringRedisClusterTemplate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-example1.xml")
public class RedisClusterTemplateExample1 {

    @Autowired
    private StringRedisClusterTemplate redisTemplate;

    @Test
    public void opsForValue() {
        redisTemplate.opsForValue().set("name", "clark kent", 10, TimeUnit.SECONDS);
        String value = redisTemplate.opsForValue().get("name");

        System.out.println(value);
    }

    @Test
    public void opsForHash() {
        redisTemplate.opsForHash().put("superhero", "phantom", "comics");
        redisTemplate.opsForHash().put("superhero", "superman", "comics");
        redisTemplate.opsForHash().put("superhero", "batman", "comics");
        redisTemplate.opsForHash().put("superhero", "iron man", "marvel");

        String value = (String) redisTemplate.opsForHash().get("superhero", "superman");

        System.out.println(value);
    }


    @Test
    public void opsForHashList() {
        BoundSessionOfHashListOperations<String, String, String> ops = redisTemplate.boundSessionOfHashListOps(
                "superhero", "superhero");

        ops.add("comics", "phantom");
        ops.add("comics", "superman");
        ops.add("comics", "batman");
        ops.add("marvel", "iron man");

        List<String> value = ops.get("comics");

        System.out.println(value);
    }


    @Test
    public void opsForSessionOfHashList() {
        // triples command
        redisTemplate.opsForSessionOfHashList().add("superman", "review", "netizen", "good", 10, TimeUnit.SECONDS);
        redisTemplate.opsForSessionOfHashList().add("superman", "review", "netizen", "wonderful", 10, TimeUnit.SECONDS);
        redisTemplate.opsForSessionOfHashList().add("superman", "review", "netizen", "bad", 10, TimeUnit.SECONDS);
        redisTemplate.opsForSessionOfHashList().add("superman", "review", "netizen", "awesome", 10, TimeUnit.SECONDS);
        redisTemplate.opsForSessionOfHashList().add("superman", "review", "netizen", "good", 10, TimeUnit.SECONDS);
        redisTemplate.opsForSessionOfHashList().add("superman", "review", "netizen", "bad", 10, TimeUnit.SECONDS);

        List<Object> result = redisTemplate.opsForSessionOfHashList().get("superman", "review", "netizen");

        System.out.println(result);
    }

    @Test
    public void boundSessionofHashList() {
        // triples command
        redisTemplate.boundSessionOfHashListOps("superman", "review").add("netizen", "good", 10, TimeUnit.SECONDS);

        BoundSessionOfHashListOperations<String, String, String> ops = redisTemplate.boundSessionOfHashListOps(
                "superman", "review");
        ops.add("netizen", "wonderful", 10, TimeUnit.SECONDS);
        ops.add("netizen", "bad", 10, TimeUnit.SECONDS);
        ops.add("netizen", "awesome", 10, TimeUnit.SECONDS);
        ops.add("netizen", "good", 10, TimeUnit.SECONDS);
        ops.add("netizen", "bad", 10, TimeUnit.SECONDS);

        List<Object> result = redisTemplate.opsForSessionOfHashList().get("superman", "review", "netizen");

        System.out.println(result);
    }

    @Test
    public void opsForSessionOfHashSet() {
        List<Object> objectValues = new ArrayList<Object>();
        objectValues.add("Lois");
        objectValues.add("Perry");
        objectValues.add("Jimmy");

        redisTemplate.opsForSessionOfHashSet().multiAdd("superman", "friends", "metropolis", objectValues, 10,
                TimeUnit.SECONDS);
        Set<Object> objectResult = redisTemplate.opsForSessionOfHashSet().get("superman", "friends", "metropolis");
        System.out.println(objectResult);

        // generic
        List<String> stringValues = new ArrayList<String>();
        stringValues.add("Lois");
        stringValues.add("Perry");
        stringValues.add("Jimmy");

        SessionOfHashSetOperations<String, String, String> ops = redisTemplate.opsForSessionOfHashSet();
        ops.multiAdd("superman", "friends", "metropolis", stringValues, 10, TimeUnit.SECONDS);

        Set<String> stringResult = ops.get("superman", "friends", "metropolis");
        System.out.println(stringResult);
    }
}
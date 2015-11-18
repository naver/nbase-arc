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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.spring.RedisClusterTemplate;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-example3.xml")
public class RedisClusterTemplateExample3 {

    @Autowired
    private RedisClusterTemplate redisTemplate;

    @Test
    public void jacksonSerializer() {
        // person
        Map<String, Object> person = new HashMap<String, Object>();
        person.put("firstName", "clark");
        person.put("lastName", "kent");
        person.put("age", 34);
        // in address
        Map<String, Object> address = new HashMap<String, Object>();
        address.put("street", "daily planet");
        address.put("number", 1);

        person.put("address", address);

        // triples command
        redisTemplate.opsForSessionOfHashList().add("superman", "movie", "cast", person, 10, TimeUnit.SECONDS);
        List<Map<String, Object>> result = redisTemplate.opsForSessionOfHashList().get("superman", "movie", "cast");

        System.out.println(result);
    }
}
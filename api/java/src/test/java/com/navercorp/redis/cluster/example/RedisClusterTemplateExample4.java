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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.redis.cluster.spring.Address;
import com.navercorp.redis.cluster.spring.Person;
import com.navercorp.redis.cluster.spring.RedisClusterTemplate;

/**
 * @author jaehong.kim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-example4.xml")
public class RedisClusterTemplateExample4 {

    @Autowired
    private RedisClusterTemplate redisTemplate;

    @Test
    public void jdkSerializer() {
        // person
        Person person = new Person();
        person.setFirstName("clark");
        person.setLastName("kent");
        person.setAge(34);
        // in address
        Address address = new Address();
        address.setStreet("daily planet");
        address.setNumber(1);

        person.setAddress(address);

        // triples command
        redisTemplate.opsForSessionOfHashList().add("superman", "movie", "cast", person, 10, TimeUnit.SECONDS);
        List<Person> result = redisTemplate.opsForSessionOfHashList().get("superman", "movie", "cast");

        System.out.println(result);
    }
}
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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author seongminwoo
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class RedisCulsterConnectionFactoryTest {
    Logger log = LoggerFactory.getLogger(RedisCulsterConnectionFactoryTest.class);

    @Autowired
    RedisClusterConnectionFactory factory;

    @Autowired
    RedisTemplate<String, String> redisTemplate;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void createConnectionFactoryBean() {
        log.debug("factory : {}", factory);
        assertTrue(factory != null);
    }

    @Test
    public void createConnectionObjectFromFactory() {
        RedisClusterConnection connection = factory.getConnection();
        log.debug("connection : {}", connection);
        assertTrue(connection != null);
    }

    @Test
    public void createRedisTemplateBean() {
        log.debug("redisTemplate : {}", redisTemplate);
        assertTrue(redisTemplate != null);
    }

    @Test
    public void executeGetAfterSet() {
        String key = "foo";
        String value = "bar";
        redisTemplate.opsForValue().set(key, "bar");
        String result = redisTemplate.opsForValue().get(key);
        assertEquals(value, result);
    }

    /**
     * 전체 서버 오류시
     */
    @Ignore
    @Test(expected = JedisConnectionException.class)
    public void operationWithAllWrongGatewayAddress() {
        String configLocation = "applicationContextWrongAddress.xml";
        ApplicationContext context = new ClassPathXmlApplicationContext(configLocation);

        @SuppressWarnings("unchecked")
        RedisTemplate<String, String> redisTemplate = (RedisTemplate<String, String>) context.getBean("redisTemplate");
        redisTemplate.opsForValue().set("foo", "bar");
    }
}

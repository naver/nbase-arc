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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
public class DefaultSessionOfHashSetOperationsTest {

    @Autowired
    private StringRedisClusterTemplate redis;
    private SessionOfHashSetOperations<String, String, String> ops;
    private List<String> values = new ArrayList<String>();

    @Before
    public void before() {
        ops = redis.opsForSessionOfHashSet();

        // test data
        values.add("a");
        values.add("b");
        values.add("c");
    }

    @After
    public void after() {
        ops.del("s001");
        ops.del("s002");
    }

    @Test
    public void get() {
        Set<String> list = ops.get("s001", "f001", "k001");
        assertEquals(0, list.size());

        long result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        list = ops.get("s001", "f001", "k001");
        assertEquals(3, list.size());
    }

    @Test
    public void multiGet() {
        long result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        result = ops.multiAdd("s001", "f001", "k002", values);
        assertEquals(3, result);
        result = ops.multiAdd("s001", "f001", "k003", values);
        assertEquals(3, result);

        List<String> keys = new ArrayList<String>();
        keys.add("k001");
        keys.add("k002");
        keys.add("k003");

        Map<String, Set<String>> map = ops.mulitGet("s001", "f001", keys);
        assertEquals(3, map.get("k001").size());
    }

    @Test
    public void keys() {
        Set<String> keys = ops.keys("s001", "f001");
        assertEquals(0, keys.size());

        long result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        result = ops.multiAdd("s001", "f001", "k002", values);
        assertEquals(3, result);
        result = ops.multiAdd("s001", "f001", "k003", values);
        assertEquals(3, result);

        keys = ops.keys("s001", "f001");
        assertEquals(3, keys.size());

        keys = ops.keys("s001");
        assertEquals(1, keys.size());
    }

    @Test
    public void add() {
        long result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        Set<String> list = ops.get("s001", "f001", "k001");
        assertEquals(3, list.size());

        result = ops.add("s001", "f001", "k001", "d");
        assertEquals(1, result);
        list = ops.get("s001", "f001", "k001");
        assertEquals(4, list.size());
    }

    @Test
    public void set() {
        long result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        Set<String> list = ops.get("s001", "f001", "k001");
        assertEquals(3, list.size());

        result = ops.set("s001", "f001", "k001", "d");
        assertEquals(0, result);
        list = ops.get("s001", "f001", "k001");
        assertEquals(1, list.size());

        result = ops.multiSet("s001", "f001", "k001", values);
        assertEquals(0, result);
        list = ops.get("s001", "f001", "k001");
        assertEquals(3, list.size());
    }

    @Test
    public void del() {
        long result = ops.del("s001", "f001", "k001");
        assertEquals(0, result);

        result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        result = ops.del("s001");
        assertEquals(1, result);

        result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        result = ops.del("s001", "f001");
        assertEquals(1, result);

        result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        result = ops.del("s001", "f001", "k001");
        assertEquals(3, result);

        result = ops.add("s001", "f001", "k001", "a");
        assertEquals(1, result);
        result = ops.del("s001", "f001", "k001", "a");
        assertEquals(1, result);

        result = ops.multiAdd("s001", "f001", "k001", values);
        assertEquals(3, result);
        result = ops.multiDel("s001", "f001", "k001", values);
        assertEquals(3, result);

    }

    @Test
    public void count() {
        long count = ops.count("s001", "f001", "k001");
        assertEquals(0, count);

        long result = ops.add("s001", "f001", "k001", "one");
        assertEquals(1, result);
        count = ops.count("s001", "f001", "k001");
        assertEquals(1, count);

        result = ops.multiAdd("s001", "f001", "k002", values);
        assertEquals(3, result);

        count = ops.count("s001", "f001", "k002");
        assertEquals(3, count);
    }

    @Test
    public void expire() {
        long result = ops.expire("s001", "f001", "k001", 1, TimeUnit.SECONDS);
        assertEquals(0, result);

        result = ops.add("s001", "f001", "k001", "a");
        assertEquals(1, result);
        result = ops.expire("s001", 5, TimeUnit.SECONDS);
        assertEquals(1, result);
        result = ops.ttl("s001", "f001", "k001", "a");
        assertTrue(result > 0 && result <= 5000);

        result = ops.expire("s001", "f001", 5, TimeUnit.SECONDS);
        assertEquals(1, result);
        result = ops.ttl("s001", "f001", "k001", "a");
        assertTrue(result > 0 && result <= 5000);

        result = ops.expire("s001", "f001", "k001", 5, TimeUnit.SECONDS);
        assertEquals(1, result);
        result = ops.ttl("s001", "f001", "k001", "a");
        assertTrue(result > 0 && result <= 5000);

        result = ops.expire("s001", "f001", "k001", "a", 5, TimeUnit.SECONDS);
        assertEquals(1, result);
        result = ops.ttl("s001", "f001", "k001", "a");
        assertTrue(result > 0 && result <= 5000);
    }

    @Test
    public void ttl() {
        long result = ops.ttl("s001", "f001", "k001", "a");
        assertEquals(-1, result);

        result = ops.add("s001", "f001", "k001", "a", 5, TimeUnit.SECONDS);
        assertEquals(1, result);
        result = ops.ttl("s001", "f001", "k001", "a");
        System.out.println(result);
        assertTrue(result > 0 && result <= 5000);
    }

    @Test
    public void values() {
        ops.add("s001", "f001", "k001", "a");
        ops.add("s001", "f001", "K002", "b");
        assertEquals(2, ops.values("s001", "f001").size());

        ops.add("s001", "f001", "k001", "b");
        assertEquals(3, ops.values("s001", "f001").size());
    }
}
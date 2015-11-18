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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.redis.cluster.util.TestEnvUtils;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author jaehong.kim
 */
public class RedisClusterPoolTest {
    private final Logger log = LoggerFactory.getLogger(RedisClusterPoolTest.class);

    @Test
    public void getResource() {
        RedisClusterPool pool = new RedisClusterPool(TestEnvUtils.getHost(), TestEnvUtils.getPort());
        RedisCluster redis = pool.getResource();
        assertNotNull(redis);
        pool.returnResource(redis);
        pool.destroy();
    }

    @Test
    public void minIdle() throws InterruptedException {
        RedisClusterPoolConfig config = new RedisClusterPoolConfig();
        config.setInitialSize(0);
        config.setMaxActive(8);
        config.setMaxIdle(8);
        config.setMinIdle(2);
        config.setMinEvictableIdleTimeMillis(-1);
        config.setTimeBetweenEvictionRunsMillis(2L * 1000L);
        log.debug("initialSize : {} ", config.getInitialSize());

        RedisClusterPool pool = new RedisClusterPool(config, TestEnvUtils.getHost(), TestEnvUtils.getPort());

        // expeted ativie is 0 and idle is min idle.
        Thread.sleep(10000);
        assertEquals(0, pool.getNumActive());
        assertEquals(config.getMinIdle(), pool.getNumIdle());
        pool.destroy();
    }

    @Test
    public void maxActiveMaxIdleDifferent() throws InterruptedException {
        RedisClusterPoolConfig config = new RedisClusterPoolConfig();
        config.setInitialSize(0);
        config.setMaxActive(8);
        config.setMaxIdle(8);
        config.setMinIdle(2);
        config.setTimeBetweenEvictionRunsMillis(10L * 1000L);
        log.debug("maxActive : {} ", config.getMaxActive());
        log.debug("maxIdle : {} ", config.getMaxIdle());

        RedisClusterPool pool = new RedisClusterPool(config, TestEnvUtils.getHost(), TestEnvUtils.getPort());

        List<RedisCluster> list = new ArrayList<RedisCluster>();

        log.debug("-- get resource");

        // active is max, idle is  0.
        for (int i = 0; i < config.getMaxActive(); i++) {
            list.add(pool.getResource());
        }
        assertEquals(config.getMaxActive(), pool.getNumActive());
        assertEquals(0, pool.getNumIdle());

        for (int i = 0; i < list.size(); i++) {
            pool.returnResource(list.get(i));
        }

        // sleep 10.
        Thread.sleep(10000);
        assertEquals(0, pool.getNumActive());
        assertEquals(config.getMaxIdle(), pool.getNumIdle());

        pool.destroy();
    }

    @Test(expected = JedisConnectionException.class)
    public void overflow() throws InterruptedException {
        RedisClusterPoolConfig config = new RedisClusterPoolConfig();
        config.setMaxTotal(10);

        RedisClusterPool pool = new RedisClusterPool(config, TestEnvUtils.getHost(), TestEnvUtils.getPort());
        List<RedisCluster> list = new ArrayList<RedisCluster>();

        for (int i = 0; i < config.getMaxTotal() + 1; i++) {
            list.add(pool.getResource());
        }

        fail("pass overflow active");

        pool.destroy();
    }
}

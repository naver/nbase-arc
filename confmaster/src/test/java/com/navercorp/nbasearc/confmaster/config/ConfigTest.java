/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.config;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.config.Config;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class ConfigTest {

    @Autowired
    Config config;

    @Value("${confmaster.zookeeper.address}")
    String zookeeperAddress;
    
    @Value("${confmaster.heartbeat.timeout}")
    Long heartbeatTimeout;
    
    @Value("${confmaster.heartbeat.interval}")
    Long heartbeatInterval;
    
    @Test
    public void configCorrect() {
        assertEquals(zookeeperAddress, config.getZooKeeperAddress());
        assertEquals(zookeeperAddress, "127.0.0.1:12181");
        
        assertEquals(heartbeatTimeout, config.getHeartbeatTimeout());
        assertEquals(heartbeatTimeout, Long.valueOf(4000L));
        
        assertEquals(heartbeatInterval, config.getHeartbeatInterval());
        assertEquals(heartbeatInterval, Long.valueOf(1000L));
    }

}

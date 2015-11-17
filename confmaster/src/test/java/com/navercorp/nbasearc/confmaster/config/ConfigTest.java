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

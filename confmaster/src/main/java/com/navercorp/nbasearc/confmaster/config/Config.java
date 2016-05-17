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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Config {
    @Value("${confmaster.ip}")
    private String ip;
    @Value("${confmaster.port}")
    private Integer port;
    @Value("${confmaster.charset}")
    private String charset;
    @Value("${confmaster.delim}")
    private String delim;

    @Value("${confmaster.zookeeper.address}")
    private String zooKeeperAddress;
    
    @Value("${confmaster.cluster.pgs.timeout}")
    private Integer clusterPgsTimeout;
    @Value("${confmaster.cluster.gw.timeout}")
    private Integer clusterGwTimeout;
    
    @Value("${confmaster.server.client.max}")
    private Integer serverClientMax;
    @Value("${confmaster.server.client.buffer.size}")
    private Integer serverClientBufferSize;
    @Value("${confmaster.server.client.timeout}")
    private Long serverClientTimeout;
    
    @Value("${confmaster.server.thread.max}")
    private Integer serverJobThreadMax;
    @Value("${confmaster.server.workflow.log.max}")
    private Integer serverJobWorkflowLogMax;
    @Value("${confmaster.server.command.rolechange.logcatch.timeout}")
    private Long serverCommandRolechangeLogcatchTimeout;
    @Value("${confmaster.server.command.rolechange.lconn.timeout}")
    private Long serverCommandROlechangeLconnTimeout;
    @Value("${confmaster.server.command.slowlog}")
    private Long serverCommandSlowlog;
    @Value("${confmaster.server.command.pgsdel.maxretry}")
    private Integer serverCommandPgsdelMaxretry;
    @Value("${confmaster.server.command.mig2pc.catchup.timeout}")
    private Long serverCommandMig2pcCatchupTimeout;
    
    @Value("${confmaster.heartbeat.timeout}")
    private Long heartbeatTimeout;
    @Value("${confmaster.heartbeat.interval}")
    private Long heartbeatInterval;
    @Value("${confmaster.heartbeat.slowlog}")
    private Long heartbeatSlowlog;
    @Value("${confmaster.heartbeat.nio.session.buffer.size}")
    private Integer heartbeatNioSessionBufferSize; 
    @Value("${confmaster.heartbeat.nio.select.timeout}")
    private Long heartbeatNioSelectionTimeout;
    @Value("${confmaster.heartbeat.nio.slowloop}")
    private Long heartbeatNioSlowloop;
    
    @Value("${confmaster.statistics.interval}")
    private Long statisticsInterval;

    public String getIp() {
        return ip;
    }

    public Integer getPort() {
        return port;
    }

    public String getCharset() {
        return charset;
    }

    public String getDelim() {
        return delim;
    }

    public String getZooKeeperAddress() {
        return zooKeeperAddress;
    }

    public Integer getClusterPgsTimeout() {
        return clusterPgsTimeout;
    }

    public Integer getClusterGwTimeout() {
        return clusterGwTimeout;
    }

    public Integer getServerClientMax() {
        return serverClientMax;
    }
    
    public Integer getServerClientBufferSize() {
        return serverClientBufferSize;
    }
    
    public Long getServerClientTimeout() {
        return serverClientTimeout;
    }

    public Integer getServerJobThreadMax() {
        return serverJobThreadMax;
    }

    public Integer getServerJobWorkflowLogMax() {
        return serverJobWorkflowLogMax;
    }

    public Long getServerCommandRolechangeLogcatchTimeout() {
        return serverCommandRolechangeLogcatchTimeout;
    }

    public Long getServerCommandRolechangeLconnTimeout() {
        return serverCommandROlechangeLconnTimeout;
    }

    public Long getServerCommandSlowlog() {
        return serverCommandSlowlog;
    }

    public Integer getServerCommandPgsdelMaxretry() {
        return serverCommandPgsdelMaxretry;
    }

    public Long getServerCommandMig2pcCatchupTimeout() {
        return serverCommandMig2pcCatchupTimeout;
    }

    public Long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public Long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public Long getHeartbeatSlowlog() {
        return heartbeatSlowlog;
    }

    public Integer getHeartbeatNioSessionBufferSize() {
        return heartbeatNioSessionBufferSize;
    }
    
    public Long getHeartbeatNioSelectionTimeout() {
        return heartbeatNioSelectionTimeout;
    }

    public Long getHeartbeatNioSlowloop() {
        return heartbeatNioSlowloop;
    }
    
    public Long getStatisticsInterval() {
        return statisticsInterval;
    }

}

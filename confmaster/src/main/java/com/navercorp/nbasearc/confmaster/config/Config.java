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
    @Value("${confmaster.cluster.pg.id.max}")
    private Integer clusterPgIdMax;
    @Value("${confmaster.cluster.pgs.id.max}")
    private Integer clusterPgsIdMax;
    @Value("${confmaster.cluster.gw.id.max}")
    private Integer clusterGwIdMax;
    
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

    public Integer getClusterPgIdMax() {
        return clusterPgIdMax;
    }

    public Integer getClusterPgsIdMax() {
        return clusterPgsIdMax;
    }

    public Integer getClusterGwIdMax() {
        return clusterGwIdMax;
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

    public void setServerCommandPgsdelMaxretry(
            Integer serverCommandPgsdelMaxretry) {
        this.serverCommandPgsdelMaxretry = serverCommandPgsdelMaxretry;
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

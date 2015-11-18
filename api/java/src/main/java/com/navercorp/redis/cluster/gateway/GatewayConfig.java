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
package com.navercorp.redis.cluster.gateway;

import com.navercorp.redis.cluster.RedisClusterPoolConfig;

/**
 * The Class GatewayConfig.
 *
 * @author jaehong.kim
 */
public class GatewayConfig {

    /**
     * The Constant DEFAULT_HEALTH_CHECK_USED.
     */
    public static final boolean DEFAULT_HEALTH_CHECK_USED = true;

    /**
     * The Constant DEFAULT_HEALTH_CHECK_PERIOD_SECOND.
     */
    public static final int DEFAULT_HEALTH_CHECK_PERIOD_SECOND = 10;

    /**
     * The Constant DEFAULT_HEALTH_CHECK_THREAD_SIZE.
     */
    public static final int DEFAULT_HEALTH_CHECK_THREAD_SIZE = 3;

    /**
     * The Constant DEFAULT_TIMEOUT_MILLISEC.
     */
    public static final int DEFAULT_TIMEOUT_MILLISEC = 1000;

    /**
     * The Constant DEFAULT_GATEWAY_SELECTOR_METHOD.
     */
    public static final String DEFAULT_GATEWAY_SELECTOR_METHOD = GatewayServerSelector.METHOD_ROUND_ROBIN;

    public static final int DEFAULT_MAX_RETRY = 0;

    public static final int DEFAULT_BACKGROUND_POOL_SIZE = 64;

    public static final long DEFAULT_CLIENT_SYNC_TIMEUNIT_MILLIS = 0;

    public static final long DEFAULT_CONNECT_DELAY_MILLIS = 10;

    public static final boolean DEFAULT_AFFINITY_USED = true;

    public static final int DEFAULT_ZK_SESSION_TIMEOUT = 60 * 1000;

    public static final int DEFAULT_ZK_CONNECT_TIMEOUT = 1000;

    /**
     * The ip address.
     */
    private String ipAddress;

    /**
     * The domain address.
     */
    private String domainAddress;

    /**
     * The timeout millisec.
     */
    private int timeoutMillisec = DEFAULT_TIMEOUT_MILLISEC;

    /**
     * The health check period seconds.
     */
    private int healthCheckPeriodSeconds = DEFAULT_HEALTH_CHECK_PERIOD_SECOND;

    /**
     * The health check thread size.
     */
    private int healthCheckThreadSize = DEFAULT_HEALTH_CHECK_THREAD_SIZE;

    /**
     * The health check used.
     */
    private boolean healthCheckUsed = DEFAULT_HEALTH_CHECK_USED;

    /**
     * The pool config.
     */
    private RedisClusterPoolConfig poolConfig = new RedisClusterPoolConfig();

    /**
     * The gateway selector method.
     */
    private String gatewaySelectorMethod = DEFAULT_GATEWAY_SELECTOR_METHOD;

    private String keyspace = null;

    /**
     * The zookeeper address.
     */
    private String zkAddress;

    /**
     * the zookeeper cluster name
     */
    private String clusterName;

    private int maxRetry = DEFAULT_MAX_RETRY;

    private int backgroundPoolSize = DEFAULT_BACKGROUND_POOL_SIZE;

    private long clientSyncTimeUnitMillis = DEFAULT_CLIENT_SYNC_TIMEUNIT_MILLIS;

    private long connectPerDelayMillis = DEFAULT_CONNECT_DELAY_MILLIS;

    private boolean affinityUsed = DEFAULT_AFFINITY_USED;

    private int zkSessionTimeout = DEFAULT_ZK_SESSION_TIMEOUT;

    private int zkConnectTimeout = DEFAULT_ZK_CONNECT_TIMEOUT;

    /**
     * Gets the ip address.
     *
     * @return the ip address
     */
    public String getIpAddress() {
        return ipAddress;
    }

    /**
     * Gets the domain address.
     *
     * @return the domain address
     */
    public String getDomainAddress() {
        return domainAddress;
    }

    /**
     * Sets the ip address.
     *
     * @param ipAddress [ip:port,...]
     */
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    /**
     * Sets the domain address.
     *
     * @param domainAddress [domain:port]
     */
    public void setDomainAddress(String domainAddress) {
        this.domainAddress = domainAddress;
    }

    /**
     * Gets the timeout millisec.
     *
     * @return the timeout millisec
     */
    public int getTimeoutMillisec() {
        return timeoutMillisec;
    }

    /**
     * Sets the timeout millisec.
     *
     * @param timeoutMillisec the new timeout millisec
     */
    public void setTimeoutMillisec(int timeoutMillisec) {
        this.timeoutMillisec = timeoutMillisec;
    }

    /**
     * Gets the health check period seconds.
     *
     * @return the health check period seconds
     */
    public int getHealthCheckPeriodSeconds() {
        return healthCheckPeriodSeconds;
    }

    /**
     * Sets the health check period seconds.
     *
     * @param healthCheckPeriodSeconds the new health check period seconds
     */
    public void setHealthCheckPeriodSeconds(int healthCheckPeriodSeconds) {
        this.healthCheckPeriodSeconds = healthCheckPeriodSeconds;
    }

    /**
     * Gets the health check thread size.
     *
     * @return the health check thread size
     */
    public int getHealthCheckThreadSize() {
        return healthCheckThreadSize;
    }

    /**
     * Sets the health check thread size.
     *
     * @param healthCheckThreadSize the new health check thread size
     */
    public void setHealthCheckThreadSize(int healthCheckThreadSize) {
        this.healthCheckThreadSize = healthCheckThreadSize;
    }

    /**
     * Gets the pool config.
     *
     * @return the pool config
     */
    public RedisClusterPoolConfig getPoolConfig() {
        return poolConfig;
    }

    /**
     * Sets the pool config.
     *
     * @param poolConfig the new pool config
     */
    public void setPoolConfig(RedisClusterPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    /**
     * Checks if is health check used.
     *
     * @return true, if is health check used
     */
    public boolean isHealthCheckUsed() {
        return healthCheckUsed;
    }

    /**
     * Sets the health check used.
     *
     * @param healthCheckUsed the new health check used
     */
    public void setHealthCheckUsed(boolean healthCheckUsed) {
        this.healthCheckUsed = healthCheckUsed;
    }

    /**
     * Gets the gateway selector method.
     *
     * @return the gateway selector method
     */
    public String getGatewaySelectorMethod() {
        return gatewaySelectorMethod;
    }

    /**
     * Sets the gateway selector method.
     *
     * @param gatewaySelectorMethod the new gateway selector method
     */
    public void setGatewaySelectorMethod(String gatewaySelectorMethod) {
        this.gatewaySelectorMethod = gatewaySelectorMethod;
    }

    @Deprecated
    public String getKeyspace() {
        return keyspace;
    }

    @Deprecated
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        if (maxRetry >= 0) {
            this.maxRetry = maxRetry;
        }
    }

    public int getBackgroundPoolSize() {
        return backgroundPoolSize;
    }

    public void setBackgroundPoolSize(int backgroundPoolSize) {
        this.backgroundPoolSize = backgroundPoolSize;
    }

    public long getClientSyncTimeUnitMillis() {
        return clientSyncTimeUnitMillis;
    }

    public void setClientSyncTimeUnitMillis(long clientSyncTimeUnitMillis) {
        this.clientSyncTimeUnitMillis = clientSyncTimeUnitMillis;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    /**
     * Sets the zookeeper address.
     *
     * @param zkAddress [ip:port,...] comma separated host:port pairs, each
     *                  corresponding to a zk server. e.g.
     *                  "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     */
    public void setZkAddress(String zkAddress) {
        this.zkAddress = trimAllWhitespace(zkAddress);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = trimAllWhitespace(clusterName);
        ;
    }

    public boolean isZkUsed() {
        return getZkAddress() != null && getClusterName() != null;
    }

    public long getConnectPerDelayMillis() {
        return connectPerDelayMillis;
    }

    public void setConnectPerDelayMillis(long connectPerDelayMillis) {
        this.connectPerDelayMillis = connectPerDelayMillis;
    }

    public boolean isAffinityUsed() {
        return affinityUsed;
    }

    public void setAffinityUsed(boolean affinityUsed) {
        this.affinityUsed = affinityUsed;
    }

    public int getZkSessionTimeout() {
        return zkSessionTimeout;
    }

    public void setZkSessionTimeout(int zkSessionTimeout) {
        this.zkSessionTimeout = zkSessionTimeout;
    }

    public int getZkConnectTimeout() {
        return zkConnectTimeout;
    }

    public void setZkConnectTimeout(int zkConnectTimeout) {
        this.zkConnectTimeout = zkConnectTimeout;
    }

    /**
     * Trim <i>all</i> whitespace from the given String: leading, trailing, and
     * inbetween characters.
     *
     * @param str the String to check
     * @return the trimmed String
     * @see java.lang.Character#isWhitespace
     */
    private static String trimAllWhitespace(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }

        StringBuilder sb = new StringBuilder(str);
        int index = 0;
        while (sb.length() > index) {
            if (Character.isWhitespace(sb.charAt(index))) {
                sb.deleteCharAt(index);
            } else {
                index++;
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{ipAddress=");
        builder.append(ipAddress);
        builder.append(", domainAddress=");
        builder.append(domainAddress);
        builder.append(", timeoutMillisec=");
        builder.append(timeoutMillisec);
        builder.append(", healthCheckPeriodSeconds=");
        builder.append(healthCheckPeriodSeconds);
        builder.append(", healthCheckThreadSize=");
        builder.append(healthCheckThreadSize);
        builder.append(", healthCheckUsed=");
        builder.append(healthCheckUsed);
        builder.append(", poolConfig=");
        builder.append(poolConfig);
        builder.append(", gatewaySelectorMethod=");
        builder.append(gatewaySelectorMethod);
        builder.append(", keyspace=");
        builder.append(keyspace);
        builder.append(", zkAddress=");
        builder.append(zkAddress);
        builder.append(", clusterName=");
        builder.append(clusterName);
        builder.append(", maxRetry=");
        builder.append(maxRetry);
        builder.append(", backgroundPoolSize=");
        builder.append(backgroundPoolSize);
        builder.append(", clientSyncTimeUnitMillis=");
        builder.append(clientSyncTimeUnitMillis);
        builder.append(", connectPerDelayMillis=");
        builder.append(connectPerDelayMillis);
        builder.append(", affinityUsed=");
        builder.append(affinityUsed);
        builder.append(", zkSessionTimeout=");
        builder.append(zkSessionTimeout);
        builder.append(", zkConnectTimeout=");
        builder.append(zkConnectTimeout);
        builder.append("}");
        return builder.toString();
    }
}
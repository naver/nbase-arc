
/*
 *    Copyright 2010-2015 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.navercorp.redis.cluster.spring;

import com.navercorp.redis.cluster.RedisClusterPoolConfig;
import com.navercorp.redis.cluster.gateway.GatewayConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties
 * {@link NBaseArcAutoConfiguration}
 *
 * @author Junhwan Oh
 */
@ConfigurationProperties(prefix = "nbase.arc")
public class NBaseArcSpringbootProperties {
    private Gateway gateway = new Gateway();
    private Pool pool = new Pool();

    public Pool getPool() {
        return pool;
    }

    public void setPool(Pool pool) {
        this.pool = pool;
    }

    public Gateway getGateway() {
        return gateway;
    }

    public void setGateway(Gateway gateway) {
        this.gateway = gateway;
    }

    public class Pool {

        private boolean lifo = RedisClusterPoolConfig.DEFAULT_LIFO;
        private boolean fairness = RedisClusterPoolConfig.DEFAULT_FAIRNESS;
        private long maxWaitMillis = RedisClusterPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
        private long minEvictableIdleTimeMillis = RedisClusterPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
        private long softMinEvictableIdleTimeMillis = RedisClusterPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
        private int numTestsPerEvictionRun = RedisClusterPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
        private String evictionPolicyClassName = RedisClusterPoolConfig.DEFAULT_EVICTION_POLICY_CLASS_NAME;
        private boolean testOnCreate = RedisClusterPoolConfig.DEFAULT_TEST_ON_CREATE;
        private boolean testOnBorrow = RedisClusterPoolConfig.DEFAULT_TEST_ON_BORROW;
        private boolean testOnReturn = RedisClusterPoolConfig.DEFAULT_TEST_ON_RETURN;
        private boolean testWhileIdle = RedisClusterPoolConfig.DEFAULT_TEST_WHILE_IDLE;
        private long timeBetweenEvictionRunsMillis = RedisClusterPoolConfig.DEFAULT_INITIAL_SIZE;
        private boolean blockWhenExhausted = RedisClusterPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED;
        private boolean jmxEnabled = RedisClusterPoolConfig.DEFAULT_JMX_ENABLE;
        private String jmxNamePrefix = RedisClusterPoolConfig.DEFAULT_JMX_NAME_PREFIX;
        private String jmxNameBase = RedisClusterPoolConfig.DEFAULT_JMX_NAME_BASE;

        public boolean isLifo() {
            return lifo;
        }

        public void setLifo(boolean lifo) {
            this.lifo = lifo;
        }

        public boolean isFairness() {
            return fairness;
        }

        public void setFairness(boolean fairness) {
            this.fairness = fairness;
        }

        public long getMaxWaitMillis() {
            return maxWaitMillis;
        }

        public void setMaxWaitMillis(long maxWaitMillis) {
            this.maxWaitMillis = maxWaitMillis;
        }

        public long getMinEvictableIdleTimeMillis() {
            return minEvictableIdleTimeMillis;
        }

        public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
            this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
        }

        public long getSoftMinEvictableIdleTimeMillis() {
            return softMinEvictableIdleTimeMillis;
        }

        public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
            this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
        }

        public int getNumTestsPerEvictionRun() {
            return numTestsPerEvictionRun;
        }

        public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
            this.numTestsPerEvictionRun = numTestsPerEvictionRun;
        }

        public String getEvictionPolicyClassName() {
            return evictionPolicyClassName;
        }

        public void setEvictionPolicyClassName(String evictionPolicyClassName) {
            this.evictionPolicyClassName = evictionPolicyClassName;
        }

        public boolean isTestOnCreate() {
            return testOnCreate;
        }

        public void setTestOnCreate(boolean testOnCreate) {
            this.testOnCreate = testOnCreate;
        }

        public boolean isTestOnBorrow() {
            return testOnBorrow;
        }

        public void setTestOnBorrow(boolean testOnBorrow) {
            this.testOnBorrow = testOnBorrow;
        }

        public boolean isTestOnReturn() {
            return testOnReturn;
        }

        public void setTestOnReturn(boolean testOnReturn) {
            this.testOnReturn = testOnReturn;
        }

        public boolean isTestWhileIdle() {
            return testWhileIdle;
        }

        public void setTestWhileIdle(boolean testWhileIdle) {
            this.testWhileIdle = testWhileIdle;
        }

        public long getTimeBetweenEvictionRunsMillis() {
            return timeBetweenEvictionRunsMillis;
        }

        public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
            this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        }

        public boolean isBlockWhenExhausted() {
            return blockWhenExhausted;
        }

        public void setBlockWhenExhausted(boolean blockWhenExhausted) {
            this.blockWhenExhausted = blockWhenExhausted;
        }

        public boolean isJmxEnabled() {
            return jmxEnabled;
        }

        public void setJmxEnabled(boolean jmxEnabled) {
            this.jmxEnabled = jmxEnabled;
        }

        public String getJmxNamePrefix() {
            return jmxNamePrefix;
        }

        public void setJmxNamePrefix(String jmxNamePrefix) {
            this.jmxNamePrefix = jmxNamePrefix;
        }

        public String getJmxNameBase() {
            return jmxNameBase;
        }

        public void setJmxNameBase(String jmxNameBase) {
            this.jmxNameBase = jmxNameBase;
        }
    }
    public class Gateway {
        private String ipAddress;
        private String domainAddress;
        private int timeoutMillisec = GatewayConfig.DEFAULT_TIMEOUT_MILLISEC;
        private int healthCheckPeriodSeconds = GatewayConfig.DEFAULT_HEALTH_CHECK_PERIOD_SECOND;
        private int healthCheckThreadSize = GatewayConfig.DEFAULT_HEALTH_CHECK_THREAD_SIZE;
        private boolean healthCheckUsed = GatewayConfig.DEFAULT_HEALTH_CHECK_USED;
        private String gatewaySelectorMethod = GatewayConfig.DEFAULT_GATEWAY_SELECTOR_METHOD;
        private String zkAddress;
        private String clusterName;
        private int maxRetry = GatewayConfig.DEFAULT_MAX_RETRY;
        private int backgroundPoolSize = GatewayConfig.DEFAULT_BACKGROUND_POOL_SIZE;
        private long clientSyncTimeUnitMillis = GatewayConfig.DEFAULT_CLIENT_SYNC_TIMEUNIT_MILLIS;
        private long connectPerDelayMillis = GatewayConfig.DEFAULT_CONNECT_DELAY_MILLIS;
        private boolean affinityUsed = GatewayConfig.DEFAULT_AFFINITY_USED;
        private int zkSessionTimeout = GatewayConfig.DEFAULT_ZK_SESSION_TIMEOUT;
        private int zkConnectTimeout = GatewayConfig.DEFAULT_ZK_CONNECT_TIMEOUT;

        public String getIpAddress() {
            return ipAddress;
        }

        public void setIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
        }

        public String getDomainAddress() {
            return domainAddress;
        }

        public void setDomainAddress(String domainAddress) {
            this.domainAddress = domainAddress;
        }

        public int getTimeoutMillisec() {
            return timeoutMillisec;
        }

        public void setTimeoutMillisec(int timeoutMillisec) {
            this.timeoutMillisec = timeoutMillisec;
        }

        public int getHealthCheckPeriodSeconds() {
            return healthCheckPeriodSeconds;
        }

        public void setHealthCheckPeriodSeconds(int healthCheckPeriodSeconds) {
            this.healthCheckPeriodSeconds = healthCheckPeriodSeconds;
        }

        public int getHealthCheckThreadSize() {
            return healthCheckThreadSize;
        }

        public void setHealthCheckThreadSize(int healthCheckThreadSize) {
            this.healthCheckThreadSize = healthCheckThreadSize;
        }

        public boolean isHealthCheckUsed() {
            return healthCheckUsed;
        }

        public void setHealthCheckUsed(boolean healthCheckUsed) {
            this.healthCheckUsed = healthCheckUsed;
        }

        public String getGatewaySelectorMethod() {
            return gatewaySelectorMethod;
        }

        public void setGatewaySelectorMethod(String gatewaySelectorMethod) {
            this.gatewaySelectorMethod = gatewaySelectorMethod;
        }

        public String getZkAddress() {
            return zkAddress;
        }

        public void setZkAddress(String zkAddress) {
            this.zkAddress = zkAddress;
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public int getMaxRetry() {
            return maxRetry;
        }

        public void setMaxRetry(int maxRetry) {
            this.maxRetry = maxRetry;
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
    }
}

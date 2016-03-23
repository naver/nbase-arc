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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@link EnableAutoConfiguration Auto-Configuration} for nBase-Arc RedisTemplate
 * {@link GatewayConfig}, {@link RedisClusterConnectionFactory} and {@link StringRedisClusterTemplate}
 *
 * @author Junhwan Oh
 */
@Configuration
@ConditionalOnProperty("nbase.arc.gateway.zkAddress")
@ConditionalOnClass({GatewayConfig.class, RedisClusterConnectionFactory.class, StringRedisClusterTemplate.class, RedisClusterPoolConfig.class})
@EnableConfigurationProperties(NBaseArcSpringbootProperties.class)
public class NBaseArcAutoConfiguration {

    private Logger logger = LoggerFactory.getLogger(NBaseArcAutoConfiguration.class);

    @Autowired
    private NBaseArcSpringbootProperties properties;


    @Bean
    @ConditionalOnMissingBean
    public RedisClusterPoolConfig poolConfig() {
        RedisClusterPoolConfig poolConfig = new RedisClusterPoolConfig();

        poolConfig.setLifo(properties.getPool().isLifo());
        poolConfig.setFairness(properties.getPool().isFairness());
        poolConfig.setMaxWaitMillis(properties.getPool().getMaxWaitMillis());
        poolConfig.setMinEvictableIdleTimeMillis(properties.getPool().getMinEvictableIdleTimeMillis());
        poolConfig.setSoftMinEvictableIdleTimeMillis(properties.getPool().getSoftMinEvictableIdleTimeMillis());
        poolConfig.setNumTestsPerEvictionRun(properties.getPool().getNumTestsPerEvictionRun());
        poolConfig.setEvictionPolicyClassName(properties.getPool().getEvictionPolicyClassName());
        poolConfig.setTestOnCreate(properties.getPool().isTestOnCreate());
        poolConfig.setTestOnBorrow(properties.getPool().isTestOnBorrow());
        poolConfig.setTestOnReturn(properties.getPool().isTestOnReturn());
        poolConfig.setTestWhileIdle(properties.getPool().isTestWhileIdle());
        poolConfig.setTimeBetweenEvictionRunsMillis(properties.getPool().getTimeBetweenEvictionRunsMillis());
        poolConfig.setBlockWhenExhausted(properties.getPool().isBlockWhenExhausted());
        poolConfig.setJmxEnabled(properties.getPool().isJmxEnabled());
        poolConfig.setJmxNamePrefix(properties.getPool().getJmxNamePrefix());
        poolConfig.setJmxNameBase(properties.getPool().getJmxNameBase());

        logger.debug(poolConfig.toString());
        return poolConfig;
    }

    @Bean
    @ConditionalOnMissingBean
    public GatewayConfig gatewayConfig(RedisClusterPoolConfig poolConfig) {
        GatewayConfig gatewayConfig = new GatewayConfig();

        gatewayConfig.setPoolConfig(poolConfig);
        gatewayConfig.setIpAddress(properties.getGateway().getIpAddress());
        gatewayConfig.setDomainAddress(properties.getGateway().getDomainAddress());
        gatewayConfig.setTimeoutMillisec(properties.getGateway().getTimeoutMillisec());
        gatewayConfig.setHealthCheckPeriodSeconds(properties.getGateway().getHealthCheckPeriodSeconds());
        gatewayConfig.setHealthCheckThreadSize(properties.getGateway().getHealthCheckThreadSize());
        gatewayConfig.setHealthCheckUsed(properties.getGateway().isHealthCheckUsed());
        gatewayConfig.setGatewaySelectorMethod(properties.getGateway().getGatewaySelectorMethod());
        gatewayConfig.setZkAddress(properties.getGateway().getZkAddress());
        gatewayConfig.setClusterName(properties.getGateway().getClusterName());
        gatewayConfig.setMaxRetry(properties.getGateway().getMaxRetry());
        gatewayConfig.setBackgroundPoolSize(properties.getGateway().getBackgroundPoolSize());
        gatewayConfig.setClientSyncTimeUnitMillis(properties.getGateway().getClientSyncTimeUnitMillis());
        gatewayConfig.setConnectPerDelayMillis(properties.getGateway().getConnectPerDelayMillis());
        gatewayConfig.setAffinityUsed(properties.getGateway().isAffinityUsed());
        gatewayConfig.setZkSessionTimeout(properties.getGateway().getZkSessionTimeout());
        gatewayConfig.setZkConnectTimeout(properties.getGateway().getZkConnectTimeout());
        logger.debug(gatewayConfig.toString());

        return gatewayConfig;
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnMissingBean
    public RedisClusterConnectionFactory redisClusterConnectionFactory(GatewayConfig gatewayConfig) {
        logger.debug("Initialled redisClusterConnectionFactory zkAddress=" + gatewayConfig.getZkAddress() + ";clusterName=" + gatewayConfig.getClusterName());
        RedisClusterConnectionFactory redisClusterConnectionFactory = new RedisClusterConnectionFactory();
        redisClusterConnectionFactory.setConfig(gatewayConfig);
        return redisClusterConnectionFactory;
    }

    @Bean
    @ConditionalOnMissingBean
    public StringRedisClusterTemplate redisTemplate(RedisClusterConnectionFactory redisClusterConnectionFactory) {
        logger.debug("Init StringRedisClusterTemplate");
        StringRedisClusterTemplate redisClusterTemplate = new StringRedisClusterTemplate();
        redisClusterTemplate.setConnectionFactory(redisClusterConnectionFactory);
        return redisClusterTemplate;
    }


}

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

import com.navercorp.redis.cluster.gateway.GatewayConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * {@link EnableAutoConfiguration Auto-Configuration} for nBase-Arc RedisTemplate
 * {@link GatewayConfig}, {@link RedisClusterConnectionFactory} and {@link StringRedisClusterTemplate}
 *
 * @author Junhwan Oh
 */
@Configuration
@ConditionalOnClass({ GatewayConfig.class, RedisClusterConnectionFactory.class , StringRedisClusterTemplate.class })
@EnableConfigurationProperties(NBaseArcSpringbootProperties.class)
public class NBaseArcAutoConfiguration implements DisposableBean{

    private static Log log = LogFactory.getLog(NBaseArcAutoConfiguration.class);

    private RedisClusterConnectionFactory redisClusterConnectionFactory;

    @Autowired
    private NBaseArcSpringbootProperties properties;

    @Bean
    @ConditionalOnMissingBean
    public GatewayConfig gatewayConfig(){
        return properties;
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisClusterConnectionFactory redisClusterConnectionFactory(){
        redisClusterConnectionFactory = new RedisClusterConnectionFactory();
        redisClusterConnectionFactory.setConfig(gatewayConfig());
        return redisClusterConnectionFactory;
    }

    @Bean
    @ConditionalOnMissingBean
    public StringRedisClusterTemplate redisTemplate(){
        StringRedisClusterTemplate redisClusterTemplate = new StringRedisClusterTemplate();
        redisClusterTemplate.setConnectionFactory(redisClusterConnectionFactory);
        return redisClusterTemplate;
    }
    public void destroy() throws Exception {
        if(redisClusterConnectionFactory != null) {
            redisClusterConnectionFactory.destroy();
        }
    }
}

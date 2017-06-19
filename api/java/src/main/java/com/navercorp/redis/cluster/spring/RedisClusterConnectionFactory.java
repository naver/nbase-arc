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

import com.navercorp.redis.cluster.RedisCluster;
import com.navercorp.redis.cluster.gateway.GatewayClient;
import com.navercorp.redis.cluster.gateway.GatewayConfig;
import com.navercorp.redis.cluster.gateway.GatewayException;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnection;

/**
 * Connection factory creating {@link RedisCluster} based connections.
 *
 * @author jaehong.kim
 */
public class RedisClusterConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

    private final RedisClusterExceptionConverter exceptionConverter = new RedisClusterExceptionConverter();

    /**
     * The client.
     */
    private GatewayClient client;

    /**
     * The config.
     */
    private GatewayConfig config = new GatewayConfig();

    private boolean convertPipelineAndTxResults = true;

    /**
     * Post process a newly retrieved connection. Useful for decorating or executing
     * initialization commands on a new connection.
     * This implementation simply returns the connection.
     *
     * @param connection the connection
     * @return processed connection
     */
    protected RedisClusterConnection postProcessConnection(RedisClusterConnection connection) {
        return connection;
    }

    /*
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    public void afterPropertiesSet() {
        if (client == null) {
            client = new GatewayClient(config);
        }
    }

    /*
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    public void destroy() {
        client.destroy();
    }

    /*
     * @see org.springframework.data.redis.connection.RedisConnectionFactory#getConnection()
     */
    public RedisClusterConnection getConnection() {
        final RedisClusterConnection connection = new RedisClusterConnection(this.client);
        connection.setConvertPipelineAndTxResults(this.convertPipelineAndTxResults);
        return postProcessConnection(connection);
    }

    /*
     * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
     */
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
        if(ex instanceof GatewayException) {
            return exceptionConverter.convert(ex);
        }

        return null;
    }

    /**
     * Gets the config.
     *
     * @return the config
     */
    public GatewayConfig getConfig() {
        return config;
    }

    /**
     * Sets the config.
     *
     * @param config the new config
     */
    public void setConfig(GatewayConfig config) {
        this.config = config;
    }

    /**
     * Specifies if pipelined results should be converted to the expected data type. If false, results of
     * {@link JedisConnection#closePipeline()} and {@link JedisConnection#exec()} will be of the type returned by the
     * Jedis driver
     *
     * @return Whether or not to convert pipeline and tx results
     */
    public boolean getConvertPipelineAndTxResults() {
        return convertPipelineAndTxResults;
    }

    /**
     * Specifies if pipelined results should be converted to the expected data type. If false, results of
     * {@link JedisConnection#closePipeline()} and {@link JedisConnection#exec()} will be of the type returned by the
     * Jedis driver
     *
     * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
     */
    public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
        this.convertPipelineAndTxResults = convertPipelineAndTxResults;
    }
}

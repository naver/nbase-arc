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

import java.io.IOException;
import java.net.UnknownHostException;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;

import com.navercorp.redis.cluster.gateway.GatewayException;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author jaehong.kim
 */
public class RedisClusterExceptionConverter implements Converter<Exception, DataAccessException> {

    public DataAccessException convert(Exception ex) {
        if (ex instanceof DataAccessException) {
            return (DataAccessException) ex;
        }

        if (ex instanceof GatewayException) {
            final Throwable t = ((GatewayException) ex).getCause();
            if (t != null) {
                if (t instanceof JedisDataException) {
                    return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
                }
                if (t instanceof JedisConnectionException) {
                    return new RedisConnectionFailureException(ex.getMessage(), ex);
                }
                if (t instanceof JedisException) {
                    return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
                }
                if (t instanceof UnknownHostException) {
                    return new RedisConnectionFailureException(ex.getMessage(), ex);
                }
                if (t instanceof IOException) {
                    return new RedisConnectionFailureException(ex.getMessage(), ex);
                }
            }
        }

        return new RedisSystemException(ex.getMessage(), ex);

    }
}

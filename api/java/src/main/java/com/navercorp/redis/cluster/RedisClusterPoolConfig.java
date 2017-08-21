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
package com.navercorp.redis.cluster;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Subclass of org.apache.commons.pool.impl.GenericObjectPool.Config that
 * includes getters/setters so it can be more easily configured by Spring and
 * other IoC frameworks.
 * <br> 
 * <br> 
 * Spring example:<br>
 * <br>
 * &lt;bean id="redisClusterPoolConfig" class="RedisClusterPoolConfig"&gt; &lt;property
 * name="testWhileIdle" value="true"/&gt; &lt;/bean&gt;
 * <br> 
 * <pre>
 *  maxActive is 8
 *  testWhileIdle is true
 *  minEvictableIdleTimeMillis is 60000
 *  timeBetweenEvictionRunsMillis is 30000
 *  numTestsPerEvictionRun is -1
 *  ...
 *  </pre>
 *
 * For information on parameters refer to:
 *
 * http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/
 * GenericObjectPool.html
 *
 * @author jaehong.kim
 */
public class RedisClusterPoolConfig extends GenericObjectPoolConfig {
    public static final int DEFAULT_INITIAL_SIZE = 8;
    public static final int DEFAULT_MAX_ACTIVE = DEFAULT_INITIAL_SIZE;
    public static final int DEFAULT_MAX_IDLE = DEFAULT_MAX_ACTIVE;
    public static final int DEFAULT_MIN_IDLE = 0;
    public static final int DEFAULT_MAX_WAIT_MILLIS = 1000;
    public static final int DEFAULT_NUM_TEST_PER_EVICTION_RUN = 2;
    public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 5L * 60L * 1000L; //  5 minutes

    /**
     * The initial number of connections that are created when the pool
     * is started
     */
    private int initialSize = DEFAULT_INITIAL_SIZE;

    public RedisClusterPoolConfig() {
        // RedisClusterPool defaults setting different with GenericObjectPool.Config
        setMaxWaitMillis(DEFAULT_MAX_WAIT_MILLIS);
        setMaxTotal(DEFAULT_MAX_ACTIVE);
        setMaxIdle(DEFAULT_MAX_IDLE);
        setMinIdle(DEFAULT_MIN_IDLE);
        setTestWhileIdle(true);
        setLifo(false);
        setMinEvictableIdleTimeMillis(-1);
        setTimeBetweenEvictionRunsMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        setNumTestsPerEvictionRun(DEFAULT_NUM_TEST_PER_EVICTION_RUN);
    }

    public void setMaxWait(long maxWaitMillis) {
        setMaxWaitMillis(maxWaitMillis);
    }

    public long getMaxWait() {
        return getMaxWaitMillis();
    }

    public int getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public int getMaxActive() {
        return getMaxTotal();
    }

    public void setMaxActive(int maxActive) {
        setMaxTotal(maxActive);
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("initialSize=").append(initialSize).append(", ");
        builder.append("maxIdle=").append(getMaxIdle()).append(", ");
        builder.append("minIdle=").append(getMinIdle()).append(", ");
        builder.append("maxActive=").append(getMaxActive()).append(", ");
        builder.append("maxWait=").append(getMaxWait()).append(", ");
        builder.append("whenExhaustedAction=").append(getBlockWhenExhausted()).append(", ");
        builder.append("testOnBorrow=").append(getTestOnBorrow()).append(", ");
        builder.append("testOnReturn=").append(getTestOnReturn()).append(", ");
        builder.append("testWhileIdle=").append(getTestWhileIdle()).append(", ");
        builder.append("timeBetweenEvictionRunsMillis=").append(getTimeBetweenEvictionRunsMillis()).append(", ");
        builder.append("numTestsPerEvictionRun=").append(getNumTestsPerEvictionRun()).append(", ");
        builder.append("minEvictableIdleTimeMillis=").append(getMinEvictableIdleTimeMillis()).append(", ");
        builder.append("softMinEvictableIdleTimeMillis=").append(getSoftMinEvictableIdleTimeMillis());
        builder.append("}");

        return builder.toString();
    }

}

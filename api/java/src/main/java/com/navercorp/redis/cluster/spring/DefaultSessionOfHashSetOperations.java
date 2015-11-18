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

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

/**
 * The Class DefaultSessionOfHashSetOperations.
 *
 * @param <H>  the generic type
 * @param <HK> the generic type
 * @param <HV> the generic type
 * @author jaehong.kim
 */
public class DefaultSessionOfHashSetOperations<H, HK, HV> extends AbstractOperations<H, Object> implements
        SessionOfHashSetOperations<H, HK, HV> {

    /**
     * Instantiates a new default session of hash set operations.
     *
     * @param template the template
     */
    @SuppressWarnings("unchecked")
    DefaultSessionOfHashSetOperations(RedisClusterTemplate<H, ?> template) {
        super((RedisClusterTemplate<H, Object>) template);
    }

    /*
     * @see SessionOfHashSetOperations#get(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Set<HV> get(H key, HK field, HK name) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);

        Set<byte[]> rawElementValues = execute(new RedisCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedis(RedisConnection connection) {
                return ((RedisClusterConnection) connection).ssGet(rawKey, rawHashKey, rawElementName);
            }
        }, true);

        return (Set<HV>) deserializeValues(rawElementValues);
    }

    /*
     * @see SessionOfHashSetOperations#mulitGet(java.lang.Object, java.lang.Object, java.util.Collection)
     */
    public Map<HK, Set<HV>> mulitGet(H key, HK field, Collection<HK> names) {
        if (names.isEmpty()) {
            return new HashMap<HK, Set<HV>>();
        }

        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[][] rawElementNames = new byte[names.size()][];
        int count = 0;
        for (HK name : names) {
            rawElementNames[count++] = rawHashKey(name);
        }

        Map<byte[], Set<byte[]>> rawElementValues = execute(new RedisCallback<Map<byte[], Set<byte[]>>>() {
            public Map<byte[], Set<byte[]>> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssMGet(rawKey, rawHashKey, rawElementNames);
            }
        }, true);

        return deserializeHashSet(rawElementValues);
    }

    /*
     * @see SessionOfHashSetOperations#keys(java.lang.Object)
     */
    public Set<HK> keys(H key) {
        final byte[] rawKey = rawKey(key);

        Set<byte[]> rawHashKeys = execute(new RedisCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssKeys(rawKey);
            }
        }, true);

        return deserializeHashKeys(rawHashKeys);
    }

    /*
     * @see SessionOfHashSetOperations#keys(java.lang.Object, java.lang.Object)
     */
    public Set<HK> keys(H key, HK field) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);

        Set<byte[]> rawElementNames = execute(new RedisCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssKeys(rawKey, rawHashKey);
            }
        }, true);

        return deserializeHashKeys(rawElementNames);
    }

    /*
     * @see SessionOfHashSetOperations#add(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long add(H key, HK field, HK name, HV value) {
        return add(key, field, name, value, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashSetOperations#add(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long add(H key, HK field, HK name, HV value, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssAdd(rawKey, rawHashKey, rawElementName, expireSeconds,
                        rawElementValue);
            }
        }, true);
    }

    public Long addAt(H key, HK field, HK name, HV value, final long millisecondsTimestamp) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssAddAt(rawKey, rawHashKey, rawElementName,
                        millisecondsTimestamp, rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#multiAdd(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection)
     */
    public Long multiAdd(H key, HK field, HK name, Collection<HV> values) {
        return multiAdd(key, field, name, values, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashSetOperations#multiAdd(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection, long, java.util.concurrent.TimeUnit)
     */
    public Long multiAdd(H key, HK field, HK name, Collection<HV> values, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[][] rawElementValues = new byte[values.size()][];
        int count = 0;
        for (HV value : values) {
            rawElementValues[count++] = rawHashValue(value);
        }
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssAdd(rawKey, rawHashKey, rawElementName, expireSeconds,
                        rawElementValues);
            }
        }, true);
    }

    public Long multiAddAt(H key, HK field, HK name, Collection<HV> values, final long millisecondsTimestamp) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[][] rawElementValues = new byte[values.size()][];
        int count = 0;
        for (HV value : values) {
            rawElementValues[count++] = rawHashValue(value);
        }

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssAddAt(rawKey, rawHashKey, rawElementName,
                        millisecondsTimestamp, rawElementValues);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#set(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long set(H key, HK field, HK name, HV value) {
        return set(key, field, name, value, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashSetOperations#set(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long set(H key, HK field, HK name, HV value, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssSet(rawKey, rawHashKey, rawElementName, expireSeconds,
                        rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#multiSet(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection)
     */
    public Long multiSet(H key, HK field, HK name, Collection<HV> values) {
        return multiSet(key, field, name, values, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashSetOperations#multiSet(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection, long, java.util.concurrent.TimeUnit)
     */
    public Long multiSet(H key, HK field, HK name, Collection<HV> values, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[][] rawElementValues = new byte[values.size()][];
        int count = 0;
        for (HV value : values) {
            rawElementValues[count++] = rawHashValue(value);
        }
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssSet(rawKey, rawHashKey, rawElementName, expireSeconds,
                        rawElementValues);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#del(java.lang.Object)
     */
    public Long del(H key) {
        final byte[] rawKey = rawKey(key);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssDel(rawKey);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#del(java.lang.Object, java.lang.Object)
     */
    public Long del(H key, HK field) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssRem(rawKey, rawHashKey);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#del(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long del(H key, HK field, HK name) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssRem(rawKey, rawHashKey, rawElementName);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#del(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long del(H key, HK field, HK name, HV value) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssRem(rawKey, rawHashKey, rawElementName, rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#multiDel(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection)
     */
    public Long multiDel(H key, HK field, HK name, Collection<HV> values) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[][] rawElementValues = new byte[values.size()][];
        int count = 0;
        for (HV value : values) {
            rawElementValues[count++] = rawHashValue(value);
        }

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssRem(rawKey, rawHashKey, rawElementName, rawElementValues);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#count(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long count(H key, HK field, HK name) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssCount(rawKey, rawHashKey, rawElementName);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#exists(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Boolean exists(H key, HK field, HK name, HV value) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);

        return execute(new RedisCallback<Boolean>() {
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssExists(rawKey, rawHashKey, rawElementName,
                        rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#expire(java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssExpire(rawKey, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#expire(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, HK field, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssExpire(rawKey, rawHashKey, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#expire(java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, HK field, HK name, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssExpire(rawKey, rawHashKey, rawElementName, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#expire(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, HK field, HK name, HV value, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssExpire(rawKey, rawHashKey, rawElementName,
                        rawElementValue, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashSetOperations#ttl(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long ttl(H key, HK field, HK name, HV value) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssTTL(rawKey, rawHashKey, rawElementName, rawElementValue);
            }
        }, true);
    }

    public List<HV> values(H key, HK field) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);

        List<byte[]> rawElementValues = execute(new RedisCallback<List<byte[]>>() {
            public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).ssVals(rawKey, rawHashKey);
            }
        }, true);

        return deserializeHashValues(rawElementValues);
    }
}
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

/**
 * The Class DefaultSessionOfHashListOperations.
 *
 * @param <H>  the generic type
 * @param <HK> the generic type
 * @param <HV> the generic type
 * @author jaehong.kim
 */
public class DefaultSessionOfHashListOperations<H, HK, HV> extends AbstractOperations<H, Object> implements
        SessionOfHashListOperations<H, HK, HV> {

    /**
     * Instantiates a new default session of hash list operations.
     *
     * @param template the template
     */
    @SuppressWarnings("unchecked")
    DefaultSessionOfHashListOperations(RedisClusterTemplate<H, ?> template) {
        super((RedisClusterTemplate<H, Object>) template);
    }

    /*
     * @see SessionOfHashListOperations#get(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public List<HV> get(H key, HK field, HK name) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);

        List<byte[]> rawElementValues = execute(new RedisCallback<List<byte[]>>() {
            public List<byte[]> doInRedis(RedisConnection connection) {
                return ((RedisClusterConnection) connection).slGet(rawKey, rawHashKey, rawElementName);
            }
        }, true);

        return deserializeHashValues(rawElementValues);
    }

    /*
     * @see SessionOfHashListOperations#multiGet(java.lang.Object, java.lang.Object, java.util.Collection)
     */
    public Map<HK, List<HV>> multiGet(H key, HK field, Collection<HK> names) {
        if (names.isEmpty()) {
            return new HashMap<HK, List<HV>>();
        }

        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[][] rawElementNames = new byte[names.size()][];
        int count = 0;
        for (HK name : names) {
            rawElementNames[count++] = rawHashKey(name);
        }

        Map<byte[], List<byte[]>> rawElementValues = execute(new RedisCallback<Map<byte[], List<byte[]>>>() {
            public Map<byte[], List<byte[]>> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slMGet(rawKey, rawHashKey, rawElementNames);
            }
        }, true);

        return deserializeHashList(rawElementValues);
    }

    /*
     * @see SessionOfHashListOperations#keys(java.lang.Object)
     */
    public Set<HK> keys(H key) {
        final byte[] rawKey = rawKey(key);

        Set<byte[]> rawHashKeys = execute(new RedisCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slKeys(rawKey);
            }
        }, true);

        return deserializeHashKeys(rawHashKeys);
    }

    /*
     * @see SessionOfHashListOperations#keys(java.lang.Object, java.lang.Object)
     */
    public Set<HK> keys(H key, HK field) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);

        Set<byte[]> rawElementNames = execute(new RedisCallback<Set<byte[]>>() {
            public Set<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slKeys(rawKey, rawHashKey);
            }
        }, true);

        return deserializeHashKeys(rawElementNames);
    }

    /*
     * @see SessionOfHashListOperations#add(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long add(H key, HK field, HK name, HV value) {
        return add(key, field, name, value, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashListOperations#add(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long add(H key, HK field, HK name, HV value, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slAdd(rawKey, rawHashKey, rawElementName, expireSeconds,
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
                return ((RedisClusterConnection) connection).slAddAt(rawKey, rawHashKey, rawElementName,
                        millisecondsTimestamp, rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#multiAdd(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection)
     */
    public Long multiAdd(H key, HK field, HK name, Collection<HV> values) {
        return multiAdd(key, field, name, values, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashListOperations#multiAdd(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection, long, java.util.concurrent.TimeUnit)
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
                return ((RedisClusterConnection) connection).slAdd(rawKey, rawHashKey, rawElementName, expireSeconds,
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
                return ((RedisClusterConnection) connection).slAddAt(rawKey, rawHashKey, rawElementName,
                        millisecondsTimestamp, rawElementValues);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#set(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long set(H key, HK field, HK name, HV value) {
        return set(key, field, name, value, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashListOperations#set(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long set(H key, HK field, HK name, HV value, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slSet(rawKey, rawHashKey, rawElementName, expireSeconds,
                        rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#multiSet(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection)
     */
    public Long multiSet(H key, HK field, HK name, Collection<HV> values) {
        return multiSet(key, field, name, values, 0, TimeUnit.SECONDS);
    }

    /*
     * @see SessionOfHashListOperations#multiSet(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection, long, java.util.concurrent.TimeUnit)
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
                return ((RedisClusterConnection) connection).slSet(rawKey, rawHashKey, rawElementName, expireSeconds,
                        rawElementValues);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#del(java.lang.Object)
     */
    public Long del(H key) {
        final byte[] rawKey = rawKey(key);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slDel(rawKey);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#del(java.lang.Object, java.lang.Object)
     */
    public Long del(H key, HK field) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slRem(rawKey, rawHashKey);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#del(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long del(H key, HK field, HK name) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slRem(rawKey, rawHashKey, rawElementName);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#del(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long del(H key, HK field, HK name, HV value) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slRem(rawKey, rawHashKey, rawElementName, rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#multiDel(java.lang.Object, java.lang.Object, java.lang.Object, java.util.Collection)
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
                return ((RedisClusterConnection) connection).slRem(rawKey, rawHashKey, rawElementName, rawElementValues);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#count(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long count(H key, HK field, HK name) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slCount(rawKey, rawHashKey, rawElementName);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#exists(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Boolean exists(H key, HK field, HK name, HV value) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);

        return execute(new RedisCallback<Boolean>() {
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slExists(rawKey, rawHashKey, rawElementName,
                        rawElementValue);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#expire(java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slExpire(rawKey, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#expire(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, HK field, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slExpire(rawKey, rawHashKey, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#expire(java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, HK field, HK name, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slExpire(rawKey, rawHashKey, rawElementName, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#expire(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(H key, HK field, HK name, HV value, long timeout, TimeUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);
        final long expireSeconds = unit.toSeconds(timeout);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slExpire(rawKey, rawHashKey, rawElementName,
                        rawElementValue, expireSeconds);
            }
        }, true);
    }

    /*
     * @see SessionOfHashListOperations#ttl(java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public Long ttl(H key, HK field, HK name, HV value) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);
        final byte[] rawElementName = rawHashKey(name);
        final byte[] rawElementValue = rawHashValue(value);

        return execute(new RedisCallback<Long>() {
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slTTL(rawKey, rawHashKey, rawElementName, rawElementValue);
            }
        }, true);
    }

    public List<HV> values(H key, HK field) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawHashKey = rawHashKey(field);

        List<byte[]> rawElementValues = execute(new RedisCallback<List<byte[]>>() {
            public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
                return ((RedisClusterConnection) connection).slVals(rawKey, rawHashKey);
            }
        }, true);

        return deserializeHashValues(rawElementValues);
    }
}
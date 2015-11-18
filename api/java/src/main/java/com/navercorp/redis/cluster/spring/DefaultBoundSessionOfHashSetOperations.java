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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The Class DefaultBoundSessionOfHashSetOperations.
 *
 * @param <H>  the generic type
 * @param <HK> the generic type
 * @param <HV> the generic type
 * @author jaehong.kim
 */
public class DefaultBoundSessionOfHashSetOperations<H, HK, HV> implements BoundSessionOfHashSetOperations<H, HK, HV> {

    /**
     * The ops.
     */
    private final SessionOfHashSetOperations<H, HK, HV> ops;

    /**
     * The key.
     */
    private H key;

    /**
     * The field.
     */
    private HK field;

    /**
     * Instantiates a new default bound session of hash set operations.
     *
     * @param key        the key
     * @param field      the field
     * @param operations the operations
     */
    public DefaultBoundSessionOfHashSetOperations(H key, HK field, RedisClusterOperations<H, ?> operations) {
        this.key = key;
        this.field = field;
        this.ops = operations.opsForSessionOfHashSet();
    }

    /**
     * Gets the key.
     *
     * @return the key
     */
    public H getKey() {
        return this.key;
    }

    /**
     * Gets the field.
     *
     * @return the field
     */
    public HK getField() {
        return this.field;
    }

    /*
     * @see BoundSessionOfHashSetOperations#get(java.lang.Object)
     */
    public Set<HV> get(HK key) {
        return ops.get(getKey(), getField(), key);
    }

    /*
     * @see BoundSessionOfHashSetOperations#multiGet(java.util.Collection)
     */
    public Map<HK, Set<HV>> multiGet(Collection<HK> keys) {
        return ops.mulitGet(getKey(), getField(), keys);
    }

    /*
     * @see BoundSessionOfHashSetOperations#keys()
     */
    public Set<HK> keys() {
        return ops.keys(getKey(), getField());
    }

    /*
     * @see BoundSessionOfHashSetOperations#add(java.lang.Object, java.lang.Object)
     */
    public Long add(HK key, HV value) {
        return ops.add(getKey(), getField(), key, value);
    }

    /*
     * @see BoundSessionOfHashSetOperations#add(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long add(HK key, HV value, long timeout, TimeUnit unit) {
        return ops.add(getKey(), getField(), key, value, timeout, unit);
    }

    public Long addAt(HK key, HV value, long millisecondsTimestamp) {
        return ops.addAt(getKey(), getField(), key, value, millisecondsTimestamp);
    }

    /*
     * @see BoundSessionOfHashSetOperations#add(java.lang.Object, java.util.Collection)
     */
    public Long add(HK key, Collection<HV> values) {
        return ops.multiAdd(getKey(), getField(), key, values);
    }

    /*
     * @see BoundSessionOfHashSetOperations#add(java.lang.Object, java.util.Collection, long, java.util.concurrent.TimeUnit)
     */
    public Long add(HK key, Collection<HV> values, long timeout, TimeUnit unit) {
        return ops.multiAdd(getKey(), getField(), key, values, timeout, unit);
    }

    public Long addAt(HK key, Collection<HV> values, long millisecondsTimestamp) {
        return ops.multiAddAt(getKey(), getField(), key, values, millisecondsTimestamp);
    }

    /*
     * @see BoundSessionOfHashSetOperations#set(java.lang.Object, java.lang.Object)
     */
    public Long set(HK key, HV value) {
        return ops.set(getKey(), getField(), key, value);
    }

    /*
     * @see BoundSessionOfHashSetOperations#set(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long set(HK key, HV value, long timeout, TimeUnit unit) {
        return ops.set(getKey(), getField(), key, value, timeout, unit);
    }

    /*
     * @see BoundSessionOfHashSetOperations#set(java.lang.Object, java.util.Collection)
     */
    public Long set(HK key, Collection<HV> values) {
        return ops.multiSet(getKey(), getField(), key, values);
    }

    /*
     * @see BoundSessionOfHashSetOperations#set(java.lang.Object, java.util.Collection, long, java.util.concurrent.TimeUnit)
     */
    public Long set(HK key, Collection<HV> values, long timeout, TimeUnit unit) {
        return ops.multiSet(getKey(), getField(), key, values, timeout, unit);
    }

    /*
     * @see BoundSessionOfHashSetOperations#del(java.lang.Object)
     */
    public Long del(HK key) {
        return ops.del(getKey(), getField(), key);
    }

    /*
     * @see BoundSessionOfHashSetOperations#del(java.lang.Object, java.lang.Object)
     */
    public Long del(HK key, HV value) {
        return ops.del(getKey(), getField(), key, value);
    }

    /*
     * @see BoundSessionOfHashSetOperations#del(java.lang.Object, java.util.Collection)
     */
    public Long del(HK key, Collection<HV> values) {
        return ops.multiDel(getKey(), getField(), key, values);
    }

    /*
     * @see BoundSessionOfHashSetOperations#count(java.lang.Object)
     */
    public Long count(HK key) {
        return ops.count(getKey(), getField(), key);
    }

    /*
     * @see BoundSessionOfHashSetOperations#expire(java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(HK key, long timeout, TimeUnit unit) {
        return ops.expire(getKey(), getField(), key, timeout, unit);
    }

    /*
     * @see BoundSessionOfHashSetOperations#expire(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
     */
    public Long expire(HK key, HV value, long timeout, TimeUnit unit) {
        return ops.expire(getKey(), getField(), key, value, timeout, unit);
    }

    /*
     * @see BoundSessionOfHashSetOperations#ttl(java.lang.Object, java.lang.Object)
     */
    public Long ttl(HK key, HV value) {
        return ops.ttl(getKey(), getField(), key, value);
    }

    public List<HV> values() {
        return ops.values(getKey(), getField());
    }
}
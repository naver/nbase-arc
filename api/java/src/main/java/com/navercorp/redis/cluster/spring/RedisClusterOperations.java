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

/**
 * The Interface RedisClusterOperations.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author jaehong.kim
 */
public interface RedisClusterOperations<K, V> {

    /**
     * Ops for session of hash list.
     *
     * @param <HK> the generic type
     * @param <HV> the generic type
     * @return the session of hash list operations
     */
    <HK, HV> SessionOfHashListOperations<K, HK, HV> opsForSessionOfHashList();

    /**
     * Ops for session of hash set.
     *
     * @param <HK> the generic type
     * @param <HV> the generic type
     * @return the session of hash set operations
     */
    <HK, HV> SessionOfHashSetOperations<K, HK, HV> opsForSessionOfHashSet();

    /**
     * Bound session of hash list ops.
     *
     * @param <HK>  the generic type
     * @param <HV>  the generic type
     * @param key   the key
     * @param field the field
     * @return the bound session of hash list operations
     */
    <HK, HV> BoundSessionOfHashListOperations<K, HK, HV> boundSessionOfHashListOps(K key, HK field);

    /**
     * Bound session of hash set ops.
     *
     * @param <HK>  the generic type
     * @param <HV>  the generic type
     * @param key   the key
     * @param field the field
     * @return the bound session of hash set operations
     */
    <HK, HV> BoundSessionOfHashSetOperations<K, HK, HV> boundSessionOfHashSetOps(K key, HK field);
}
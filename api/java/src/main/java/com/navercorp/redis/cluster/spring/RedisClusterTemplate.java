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

import org.springframework.data.redis.core.RedisTemplate;

/**
 * The Class RedisClusterTemplate using JdkSerializationRedisSerializer.
 * You must annotate @Qualifier to use this class with <String, String> type of generic, 
 * otherwise StringRedisClusterTemplate using StringRedisSerializer will be selected.  
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author jaehong.kim
 */
public class RedisClusterTemplate<K, V> extends RedisTemplate<K, V> implements RedisClusterOperations<K, V> {
    
    /*
     * @see RedisClusterOperations#opsForSessionOfHashList()
     */
    public <HK, HV> SessionOfHashListOperations<K, HK, HV> opsForSessionOfHashList() {
        return new DefaultSessionOfHashListOperations<K, HK, HV>(this);
    }

    /*
     * @see RedisClusterOperations#opsForSessionOfHashSet()
     */
    public <HK, HV> SessionOfHashSetOperations<K, HK, HV> opsForSessionOfHashSet() {
        return new DefaultSessionOfHashSetOperations<K, HK, HV>(this);
    }

    /*
     * @see RedisClusterOperations#boundSessionOfHashListOps(java.lang.Object, java.lang.Object)
     */
    public <HK, HV> BoundSessionOfHashListOperations<K, HK, HV> boundSessionOfHashListOps(K key, HK field) {
        return new DefaultBoundSessionOfHashListOperations<K, HK, HV>(key, field, this);
    }

    /*
     * @see RedisClusterOperations#boundSessionOfHashSetOps(java.lang.Object, java.lang.Object)
     */
    public <HK, HV> BoundSessionOfHashSetOperations<K, HK, HV> boundSessionOfHashSetOps(K key, HK field) {
        return new DefaultBoundSessionOfHashSetOperations<K, HK, HV>(key, field, this);
    }
}
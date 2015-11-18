/*
 * @(#)RedisClusterOperations.java 2013. 6. 21
 * 
 * Copyright 2013 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
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
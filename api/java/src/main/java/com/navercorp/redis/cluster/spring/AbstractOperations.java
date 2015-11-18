/*
 * Copyright 2011-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.navercorp.redis.cluster.spring;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.util.Assert;

/**
 * Internal base class used by various RedisTemplate XXXOperations implementations.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author David Liu
 */
public abstract class AbstractOperations<K, V> {
    // utility methods for the template internal methods

    /**
     * The Class ValueDeserializingRedisCallback.
     */
    abstract class ValueDeserializingRedisCallback implements RedisCallback<V> {

        /**
         * The key.
         */
        private Object key;

        /**
         * Instantiates a new value deserializing redis callback.
         *
         * @param key the key
         */
        public ValueDeserializingRedisCallback(Object key) {
            this.key = key;
        }

        /**
         * Do in redis.
         *
         * @param connection the connection
         * @return the v
         */
        public final V doInRedis(RedisClusterConnection connection) {
            byte[] result = inRedis(rawKey(key), connection);
            return deserializeValue(result);
        }

        /**
         * In redis.
         *
         * @param rawKey     the raw key
         * @param connection the connection
         * @return the byte[]
         */
        protected abstract byte[] inRedis(byte[] rawKey, RedisClusterConnection connection);
    }

    /**
     * The template.
     */
    RedisClusterTemplate<K, V> template;

    /**
     * Instantiates a new abstract operations.
     *
     * @param template the template
     */
    AbstractOperations(RedisClusterTemplate<K, V> template) {
        this.template = template;
    }

    /**
     * Key serializer.
     *
     * @return the redis serializer
     */
    RedisSerializer keySerializer() {
        return template.getKeySerializer();
    }

    /**
     * Value serializer.
     *
     * @return the redis serializer
     */
    RedisSerializer valueSerializer() {
        return template.getValueSerializer();
    }

    /**
     * Hash key serializer.
     *
     * @return the redis serializer
     */
    RedisSerializer hashKeySerializer() {
        return template.getHashKeySerializer();
    }

    /**
     * Hash value serializer.
     *
     * @return the redis serializer
     */
    RedisSerializer hashValueSerializer() {
        return template.getHashValueSerializer();
    }

    /**
     * String serializer.
     *
     * @return the redis serializer
     */
    RedisSerializer stringSerializer() {
        return template.getStringSerializer();
    }

    /**
     * Execute.
     *
     * @param <T>      the generic type
     * @param callback the callback
     * @param b        the b
     * @return the t
     */
    <T> T execute(RedisCallback<T> callback, boolean b) {
        return template.execute(callback, b);
    }

    /**
     * Gets the operations.
     *
     * @return the operations
     */
    public RedisOperations<K, V> getOperations() {
        return template;
    }

    /**
     * Raw key.
     *
     * @param key the key
     * @return the byte[]
     */
    @SuppressWarnings("unchecked")
    byte[] rawKey(Object key) {
        Assert.notNull(key, "non null key required");
        return keySerializer().serialize(key);
    }

    /**
     * Raw string.
     *
     * @param key the key
     * @return the byte[]
     */
    byte[] rawString(String key) {
        return stringSerializer().serialize(key);
    }

    /**
     * Raw value.
     *
     * @param value the value
     * @return the byte[]
     */
    @SuppressWarnings("unchecked")
    byte[] rawValue(Object value) {
        return valueSerializer().serialize(value);
    }

    /**
     * Raw hash key.
     *
     * @param <HK>    the generic type
     * @param hashKey the hash key
     * @return the byte[]
     */
    @SuppressWarnings("unchecked")
    <HK> byte[] rawHashKey(HK hashKey) {
        Assert.notNull(hashKey, "non null hash key required");
        return hashKeySerializer().serialize(hashKey);
    }

    /**
     * Raw hash value.
     *
     * @param <HV>  the generic type
     * @param value the value
     * @return the byte[]
     */
    @SuppressWarnings("unchecked")
    <HV> byte[] rawHashValue(HV value) {
        return hashValueSerializer().serialize(value);
    }

    /**
     * Raw keys.
     *
     * @param key      the key
     * @param otherKey the other key
     * @return the byte[][]
     */
    byte[][] rawKeys(K key, K otherKey) {
        final byte[][] rawKeys = new byte[2][];

        rawKeys[0] = rawKey(key);
        rawKeys[1] = rawKey(key);
        return rawKeys;
    }

    /**
     * Raw keys.
     *
     * @param keys the keys
     * @return the byte[][]
     */
    byte[][] rawKeys(Collection<K> keys) {
        return rawKeys(null, keys);
    }

    /**
     * Raw keys.
     *
     * @param key  the key
     * @param keys the keys
     * @return the byte[][]
     */
    byte[][] rawKeys(K key, Collection<K> keys) {
        final byte[][] rawKeys = new byte[keys.size() + (key != null ? 1 : 0)][];

        int i = 0;

        if (key != null) {
            rawKeys[i++] = rawKey(key);
        }

        for (K k : keys) {
            rawKeys[i++] = rawKey(k);
        }

        return rawKeys;
    }

    /**
     * Deserialize values.
     *
     * @param rawValues the raw values
     * @return the sets the
     */
    @SuppressWarnings("unchecked")
    Set<V> deserializeValues(Set<byte[]> rawValues) {
        return SerializationUtils.deserialize(rawValues, valueSerializer());
    }

    /**
     * Deserialize tuple values.
     *
     * @param rawValues the raw values
     * @return the sets the
     */
    @SuppressWarnings("unchecked")
    Set<TypedTuple<V>> deserializeTupleValues(Set<Tuple> rawValues) {
        Set<TypedTuple<V>> set = new LinkedHashSet<TypedTuple<V>>(rawValues.size());
        for (Tuple rawValue : rawValues) {
            set.add(new DefaultTypedTuple(valueSerializer().deserialize(rawValue.getValue()), rawValue.getScore()));
        }
        return set;
    }

    /**
     * Deserialize values.
     *
     * @param rawValues the raw values
     * @return the list
     */
    @SuppressWarnings("unchecked")
    List<V> deserializeValues(List<byte[]> rawValues) {
        return SerializationUtils.deserialize(rawValues, valueSerializer());
    }

    /**
     * Deserialize hash keys.
     *
     * @param <T>     the generic type
     * @param rawKeys the raw keys
     * @return the sets the
     */
    @SuppressWarnings("unchecked")
    <T> Set<T> deserializeHashKeys(Set<byte[]> rawKeys) {
        return SerializationUtils.deserialize(rawKeys, hashKeySerializer());
    }

    /**
     * Deserialize hash values.
     *
     * @param <T>       the generic type
     * @param rawValues the raw values
     * @return the list
     */
    @SuppressWarnings("unchecked")
    <T> List<T> deserializeHashValues(List<byte[]> rawValues) {
        return SerializationUtils.deserialize(rawValues, hashValueSerializer());
    }

    /**
     * Deserialize hash map.
     *
     * @param <HK>    the generic type
     * @param <HV>    the generic type
     * @param entries the entries
     * @return the map
     */
    @SuppressWarnings("unchecked")
    <HK, HV> Map<HK, HV> deserializeHashMap(Map<byte[], byte[]> entries) {
        // connection in pipeline/multi mode
        if (entries == null) {
            return null;
        }

        Map<HK, HV> map = new LinkedHashMap<HK, HV>(entries.size());

        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            map.put((HK) deserializeHashKey(entry.getKey()), (HV) deserializeHashValue(entry.getValue()));
        }

        return map;
    }

    /**
     * Deserialize hash list.
     *
     * @param <HK>    the generic type
     * @param <HV>    the generic type
     * @param entries the entries
     * @return the map
     */
    <HK, HV> Map<HK, List<HV>> deserializeHashList(Map<byte[], List<byte[]>> entries) {
        // connection in pipeline/multi mode
        if (entries == null) {
            return null;
        }

        Map<HK, List<HV>> map = new LinkedHashMap<HK, List<HV>>(entries.size());

        for (Map.Entry<byte[], List<byte[]>> entry : entries.entrySet()) {
            map.put((HK) deserializeHashKey(entry.getKey()), (List<HV>) deserializeHashValues(entry.getValue()));
        }

        return map;
    }

    /**
     * Deserialize hash set.
     *
     * @param <HK>    the generic type
     * @param <HV>    the generic type
     * @param entries the entries
     * @return the map
     */
    <HK, HV> Map<HK, Set<HV>> deserializeHashSet(Map<byte[], Set<byte[]>> entries) {
        // connection in pipeline/multi mode
        if (entries == null) {
            return null;
        }

        Map<HK, Set<HV>> map = new LinkedHashMap<HK, Set<HV>>(entries.size());

        for (Map.Entry<byte[], Set<byte[]>> entry : entries.entrySet()) {
            map.put((HK) deserializeHashKey(entry.getKey()), (Set<HV>) deserializeValues(entry.getValue()));
        }

        return map;
    }

    /**
     * Deserialize key.
     *
     * @param value the value
     * @return the k
     */
    @SuppressWarnings("unchecked")
    K deserializeKey(byte[] value) {
        return (K) keySerializer().deserialize(value);
    }

    /**
     * Deserialize value.
     *
     * @param value the value
     * @return the v
     */
    @SuppressWarnings("unchecked")
    V deserializeValue(byte[] value) {
        return (V) valueSerializer().deserialize(value);
    }

    /**
     * Deserialize string.
     *
     * @param value the value
     * @return the string
     */
    String deserializeString(byte[] value) {
        return (String) stringSerializer().deserialize(value);
    }

    /**
     * Deserialize hash key.
     *
     * @param <HK>  the generic type
     * @param value the value
     * @return the hK
     */
    @SuppressWarnings({"unchecked"})
    <HK> HK deserializeHashKey(byte[] value) {
        return (HK) hashKeySerializer().deserialize(value);
    }

    /**
     * Deserialize hash value.
     *
     * @param <HV>  the generic type
     * @param value the value
     * @return the hV
     */
    @SuppressWarnings("unchecked")
    <HV> HV deserializeHashValue(byte[] value) {
        return (HV) hashValueSerializer().deserialize(value);
    }
}
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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Interface RedisSessionOfHashSetCommands.
 *
 * @author jaehong.kim
 */
public interface RedisSessionOfHashSetCommands {

    /**
     * Ss get.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the sets the
     */
    Set<byte[]> ssGet(byte[] key, byte[] field, byte[] name);

    /**
     * Ss m get.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the map
     */
    Map<byte[], Set<byte[]>> ssMGet(byte[] key, byte[] field, byte[]... name);

    /**
     * Ss keys.
     *
     * @param key the key
     * @return the sets the
     */
    Set<byte[]> ssKeys(byte[] key);

    /**
     * Ss keys.
     *
     * @param key   the key
     * @param field the field
     * @return the sets the
     */
    Set<byte[]> ssKeys(byte[] key, byte[] field);

    /**
     * Ss add.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return the long
     */
    Long ssAdd(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Ss add.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return the long
     */
    Long ssAdd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values);


    Long ssAddAt(byte[] key, byte[] field, byte[] name, long millisecondsTimestamp, byte[]... values);


    /**
     * Ss set.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return the long
     */
    Long ssSet(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Ss set.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return the long
     */
    Long ssSet(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values);

    /**
     * Ss del.
     *
     * @param key the key
     * @return the long
     */
    Long ssDel(byte[] key);

    /**
     * Ss rem.
     *
     * @param key   the key
     * @param field the field
     * @return the long
     */
    Long ssRem(byte[] key, byte[] field);

    /**
     * Ss rem.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the long
     */
    Long ssRem(byte[] key, byte[] field, byte[] name);

    /**
     * Ss rem.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return the long
     */
    Long ssRem(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Ss count.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the long
     */
    Long ssCount(byte[] key, byte[] field, byte[] name);

    /**
     * Ss exists.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the boolean
     */
    Boolean ssExists(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Ss expire.
     *
     * @param key           the key
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long ssExpire(byte[] key, long expireSeconds);

    /**
     * Ss expire.
     *
     * @param key           the key
     * @param field         the field
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long ssExpire(byte[] key, byte[] field, long expireSeconds);

    /**
     * Ss expire.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long ssExpire(byte[] key, byte[] field, byte[] name, long expireSeconds);

    /**
     * Ss expire.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param value         the value
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long ssExpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds);

    /**
     * Ss ttl.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long ssTTL(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Ss vals.
     *
     * @param key   the key
     * @param field the field
     * @return the list
     */
    List<byte[]> ssVals(byte[] key, byte[] field);
}
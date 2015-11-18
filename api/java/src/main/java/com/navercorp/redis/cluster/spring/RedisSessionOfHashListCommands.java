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
 * The Interface RedisSessionOfHashListCommands.
 *
 * @author jaehong.kim
 */
public interface RedisSessionOfHashListCommands {

    /**
     * Sl get.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the list
     */
    List<byte[]> slGet(byte[] key, byte[] field, byte[] name);

    /**
     * Sl m get.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the map
     */
    Map<byte[], List<byte[]>> slMGet(byte[] key, byte[] field, byte[]... name);

    /**
     * Sl keys.
     *
     * @param key the key
     * @return the sets the
     */
    Set<byte[]> slKeys(byte[] key);

    /**
     * Sl keys.
     *
     * @param key   the key
     * @param field the field
     * @return the sets the
     */
    Set<byte[]> slKeys(byte[] key, byte[] field);

    /**
     * Sl add.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long slAdd(byte[] key, byte[] field, byte[] name, byte[]... value);

    /**
     * Sl add.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param value         the value
     * @return the long
     */
    Long slAdd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... value);


    Long slAddAt(byte[] key, byte[] field, byte[] name, long millisecondsTimestamp, byte[]... value);


    /**
     * Sl set.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long slSet(byte[] key, byte[] field, byte[] name, byte[]... value);

    /**
     * Sl set.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param value         the value
     * @return the long
     */
    Long slSet(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... value);

    /**
     * Sl del.
     *
     * @param key the key
     * @return the long
     */
    Long slDel(byte[] key);

    /**
     * Sl rem.
     *
     * @param key   the key
     * @param field the field
     * @return the long
     */
    Long slRem(byte[] key, byte[] field);

    /**
     * Sl rem.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the long
     */
    Long slRem(byte[] key, byte[] field, byte[] name);

    /**
     * Sl rem.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return the long
     */
    Long slRem(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Sl count.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the long
     */
    Long slCount(byte[] key, byte[] field, byte[] name);

    /**
     * Sl exists.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the boolean
     */
    Boolean slExists(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Sl expire.
     *
     * @param key           the key
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long slExpire(byte[] key, long expireSeconds);

    /**
     * Sl expire.
     *
     * @param key           the key
     * @param field         the field
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long slExpire(byte[] key, byte[] field, long expireSeconds);

    /**
     * Sl expire.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long slExpire(byte[] key, byte[] field, byte[] name, long expireSeconds);

    /**
     * Sl expire.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param value         the value
     * @param expireSeconds the expire seconds
     * @return the long
     */
    Long slExpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds);

    /**
     * Sl ttl.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long slTTL(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Sl vals.
     *
     * @param key   the key
     * @param field the field
     * @return the list
     */
    List<byte[]> slVals(byte[] key, byte[] field);
}
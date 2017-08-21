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
package com.navercorp.redis.cluster.triples;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Interface BinaryTriplesRedisClusterCommands.
 *
 * @author jaehong.kim
 */
public interface BinaryTriplesRedisClusterCommands {

    /**
     * Get the value of the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return if not exist the empty list returned.
     */
    List<byte[]> slget(byte[] key, byte[] field, byte[] name);

    /**
     * Get the value of the specified key, field, names.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return if not exists the empty list returned.
     */
    Map<byte[], List<byte[]>> slmget(byte[] key, byte[] field, byte[]... name);

    /**
     * Get the field of the specified key.
     *
     * @param key the key
     * @return if not exists the empty list returned.
     */
    Set<byte[]> slkeys(byte[] key);

    /**
     * Get the name of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return if not exists the empty list returned.
     */
    Set<byte[]> slkeys(byte[] key, byte[] field);

    /**
     * Add the values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long sladd(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Add the values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long sladd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values);

    /**
     * Add the values.
     *
     * @param key      the key
     * @param field    the field
     * @param name     the name
     * @param unixTime the unixTime
     * @param values   the values
     * @return Integer reply, the number of saved elements.
     */
    Long sladdAt(byte[] key, byte[] field, byte[] name, long unixTime, byte[]... values);

    /**
     * Set the values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long slset(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Set the values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long slset(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values);

    /**
     * Remove the specified key.
     *
     * @param key the key
     * @return Integer reply, the number of removed elements.
     */
    Long sldel(byte[] key);

    /**
     * Remove the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of removed elements.
     */
    Long slrem(byte[] key, byte[] field);

    /**
     * Remove the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of removed elements.
     */
    Long slrem(byte[] key, byte[] field, byte[] name);

    /**
     * Remove the specified key, field, name, values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of removed elements.
     */
    Long slrem(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Count the number of values.
     *
     * @param key the key
     * @return Integer reply, the number of elements.
     */
    Long slcount(byte[] key);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of elements.
     */
    Long slcount(byte[] key, byte[] field);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of elements.
     */
    Long slcount(byte[] key, byte[] field, byte[] name);

    /**
     * Test if the specified key, field, name, value exists.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Boolean reply, true if the exists, otherwise false.
     */
    Boolean slexists(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Set a timeout on the specified key.
     *
     * @param key           the key
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long slexpire(byte[] key, long expireSeconds);

    /**
     * Set a timeout on the specified key, field.
     *
     * @param key           the key
     * @param field         the field
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long slexpire(byte[] key, byte[] field, long expireSeconds);

    /**
     * Set a timeout on the specified key, field, name.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long slexpire(byte[] key, byte[] field, byte[] name, long expireSeconds);

    /**
     * Set a timeout on the specified key, field, name, value.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param value         the value
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long slexpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long slttl(byte[] key, byte[] field, byte[] name);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name, value.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long slttl(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Get the value of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return List. if not exists the empty list returned.
     */
    List<byte[]> slvals(byte[] key, byte[] field);

    // Hashes of Sets

    /**
     * Get the value of the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return if not exist the empty list returned.
     */
    Set<byte[]> ssget(byte[] key, byte[] field, byte[] name);

    /**
     * Get the value of the specified key, field, names.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return if not exists the empty list returned.
     */
    Map<byte[], Set<byte[]>> ssmget(byte[] key, byte[] field, byte[]... name);

    /**
     * Get the field of the specified key.
     *
     * @param key the key
     * @return if not exists the empty list returned.
     */
    Set<byte[]> sskeys(byte[] key);

    /**
     * Get the name of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return if not exists the empty list returned.
     */
    Set<byte[]> sskeys(byte[] key, byte[] field);

    /**
     * Add the values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssadd(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Add the values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssadd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values);

    /**
     * Add the values.
     *
     * @param key      the key
     * @param field    the field
     * @param name     the name
     * @param unixTime the unixTime
     * @param values   the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssaddAt(byte[] key, byte[] field, byte[] name, long unixTime, byte[]... values);

    /**
     * Set the values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssset(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Set the values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssset(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values);

    /**
     * Remove the specified key.
     *
     * @param key the key
     * @return Integer reply, the number of removed elements.
     */
    Long ssdel(byte[] key);

    /**
     * Remove the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of removed elements.
     */
    Long ssrem(byte[] key, byte[] field);

    /**
     * Remove the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of removed elements.
     */
    Long ssrem(byte[] key, byte[] field, byte[] name);

    /**
     * Remove the specified key, field, name, values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of removed elements.
     */
    Long ssrem(byte[] key, byte[] field, byte[] name, byte[]... values);

    /**
     * Count the number of values.
     *
     * @param key the key
     * @return Integer reply, the number of elements.
     */
    Long sscount(byte[] key);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of elements.
     */
    Long sscount(byte[] key, byte[] field);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of elements.
     */
    Long sscount(byte[] key, byte[] field, byte[] name);

    /**
     * Test if the specified key, field, name, value exists.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Boolean reply, true if the exists, otherwise false.
     */
    Boolean ssexists(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Set a timeout on the specified key.
     *
     * @param key           the key
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long ssexpire(byte[] key, long expireSeconds);

    /**
     * Set a timeout on the specified key, field.
     *
     * @param key           the key
     * @param field         the field
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long ssexpire(byte[] key, byte[] field, long expireSeconds);

    /**
     * Set a timeout on the specified key, field, name.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long ssexpire(byte[] key, byte[] field, byte[] name, long expireSeconds);

    /**
     * Set a timeout on the specified key, field, name.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param value         the value
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long ssexpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long ssttl(byte[] key, byte[] field, byte[] name);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name, value.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long ssttl(byte[] key, byte[] field, byte[] name, byte[] value);

    /**
     * Get the value of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return List. if not exists the empty list returned.
     */
    List<byte[]> ssvals(byte[] key, byte[] field);

}
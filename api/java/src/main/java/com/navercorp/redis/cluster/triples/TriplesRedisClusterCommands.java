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
 * The Interface ClusterTriplesCommands.
 *
 * @author jaehong.kim
 */
public interface TriplesRedisClusterCommands {
    // Sessions of Lists
    // //////////////////////////////////////////////////////////////////////

    /**
     * Get the value of the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return if not exist the empty list returned.
     */
    List<String> slget(String key, String field, String name);

    /**
     * Get the value of the specified key, field, names.
     *
     * @param key   the key
     * @param field the field
     * @param names the names
     * @return if not exists the empty list returned.
     */
    Map<String, List<String>> slmget(String key, String field, String... names);

    /**
     * Get the field of the specified key.
     *
     * @param key the key
     * @return if not exists the empty list returned.
     */
    Set<String> slkeys(String key);

    /**
     * Get the name of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return if not exists the empty list returned.
     */
    Set<String> slkeys(String key, String field);

    /**
     * Add the string values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long sladd(String key, String field, String name, String... values);

    /**
     * Add the string values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long sladd(String key, String field, String name, long expireSeconds, String... values);

    /**
     * Add the string values.
     *
     * @param key
     * @param field
     * @param name
     * @param unixTime
     * @param values
     * @return Integer reply, the number of saved elements.
     */
    Long sladdAt(String key, String field, String name, long unixTime, String... values);

    /**
     * Set the string values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long slset(String key, String field, String name, String... values);

    /**
     * Set the string values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long slset(String key, String field, String name, long expireSeconds, String... values);

    /**
     * Remove the specified key.
     *
     * @param key the key
     * @return Integer reply, the number of removed elements.
     */
    Long sldel(String key);

    /**
     * Remove the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of removed elements.
     */
    Long slrem(String key, String field);

    /**
     * Remove the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of removed elements.
     */
    Long slrem(String key, String field, String name);

    /**
     * Remove the specified key, field, name, values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of removed elements.
     */
    Long slrem(String key, String field, String name, String... values);

    /**
     * Count the number of values.
     *
     * @param key the key
     * @return Integer reply, the number of elements.
     */
    Long slcount(String key);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of elements.
     */
    Long slcount(String key, String field);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of elements.
     */
    Long slcount(String key, String field, String name);

    /**
     * Test if the specified key, field, name, value exists.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Boolean reply, true if the exists, otherwise false.
     */
    Boolean slexists(String key, String field, String name, String value);

    /**
     * Set a timeout on the specified key.
     *
     * @param key           the key
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0..
     */
    Long slexpire(String key, long expireSeconds);

    /**
     * Set a timeout on the specified key, field.
     *
     * @param key           the key
     * @param field         the field
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long slexpire(String key, String field, long expireSeconds);

    /**
     * Set a timeout on the specified key, field, name.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long slexpire(String key, String field, String name, long expireSeconds);

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
    Long slexpire(String key, String field, String name, String value, long expireSeconds);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long slttl(String key, String field, String name);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name, value.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long slttl(String key, String field, String name, String value);

    /**
     * Get the value of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return List. if not exists the empty list returned.
     */
    List<String> slvals(String key, String field);

    // Sessions of Sets
    // //////////////////////////////////////////////////////////////////////

    /**
     * Get the value of the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return if not exist the empty list returned.
     */
    Set<String> ssget(String key, String field, String name);

    /**
     * Get the value of the specified key, field, names.
     *
     * @param key   the key
     * @param field the field
     * @param names the names
     * @return if not exists the empty list returned.
     */
    Map<String, Set<String>> ssmget(String key, String field, String... names);

    /**
     * Get the field of the specified key.
     *
     * @param key the key
     * @return if not exists the empty list returned.
     */
    Set<String> sskeys(String key);

    /**
     * Get the name of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return if not exists the empty list returned.
     */
    Set<String> sskeys(String key, String field);

    /**
     * Add the string values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssadd(String key, String field, String name, String... values);

    /**
     * Add the string values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssadd(String key, String field, String name, long expireSeconds, String... values);

    /**
     * Add the string values.
     *
     * @param key
     * @param field
     * @param name
     * @param unixTime
     * @param values
     * @return Integer reply, the number of saved elements.
     */
    Long ssaddAt(String key, String field, String name, long unixTime, String... values);

    /**
     * Set the string values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssset(String key, String field, String name, String... values);

    /**
     * Set the string values.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @param values        the values
     * @return Integer reply, the number of saved elements.
     */
    Long ssset(String key, String field, String name, long expireSeconds, String... values);

    /**
     * Remove the specified key.
     *
     * @param key the key
     * @return Integer reply, the number of removed elements.
     */
    Long ssdel(String key);

    /**
     * Remove the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of removed elements.
     */
    Long ssrem(String key, String field);

    /**
     * Remove the specified key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of removed elements.
     */
    Long ssrem(String key, String field, String name);

    /**
     * Remove the specified key, field, name, values.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return Integer reply, the number of removed elements.
     */
    Long ssrem(String key, String field, String name, String... values);

    /**
     * Count the number of values.
     *
     * @param key the key
     * @return Integer reply, the number of elements.
     */
    Long sscount(String key);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @return Integer reply, the number of elements.
     */
    Long sscount(String key, String field);

    /**
     * Count the number of values.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Integer reply, the number of elements.
     */
    Long sscount(String key, String field, String name);

    /**
     * Test if the specified key, field, name, value exists.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Boolean reply, true if the exists, otherwise false.
     */
    Boolean ssexists(String key, String field, String name, String value);

    /**
     * Set a timeout on the specified key.
     *
     * @param key           the key
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long ssexpire(String key, long expireSeconds);

    /**
     * Set a timeout on the specified key, field.
     *
     * @param key           the key
     * @param field         the field
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long ssexpire(String key, String field, long expireSeconds);

    /**
     * Set a timeout on the specified key, field, name.
     *
     * @param key           the key
     * @param field         the field
     * @param name          the name
     * @param expireSeconds the expire seconds
     * @return Integer reply, 1 if the success, otherwise 0.
     */
    Long ssexpire(String key, String field, String name, long expireSeconds);

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
    Long ssexpire(String key, String field, String name, String value, long expireSeconds);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long ssttl(String key, String field, String name);

    /**
     * Returns the remaining time to live in milliseconds of a key, field, name, value.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return Long reply, if not exists or does not have an associated expire, -1 is returned.
     */
    Long ssttl(String key, String field, String name, String value);

    /**
     * Get the value of the specified key, field.
     *
     * @param key   the key
     * @param field the field
     * @return List. if not exists the empty list returned.
     */
    List<String> ssvals(String key, String field);
}
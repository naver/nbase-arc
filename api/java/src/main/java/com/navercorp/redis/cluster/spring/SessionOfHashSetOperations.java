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
 * The Interface SessionOfHashSetOperations.
 *
 * @param <H>  the generic type
 * @param <HK> the generic type
 * @param <HV> the generic type
 * @author jaehong.kim
 */
public interface SessionOfHashSetOperations<H, HK, HV> {

    /**
     * Gets the.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the sets the
     */
    Set<HV> get(H key, HK field, HK name);

    /**
     * Mulit get.
     *
     * @param key   the key
     * @param field the field
     * @param names the names
     * @return the map
     */
    Map<HK, Set<HV>> mulitGet(H key, HK field, Collection<HK> names);

    /**
     * Keys.
     *
     * @param key the key
     * @return the sets the
     */
    Set<HK> keys(H key);

    /**
     * Keys.
     *
     * @param key   the key
     * @param field the field
     * @return the sets the
     */
    Set<HK> keys(H key, HK field);

    /**
     * Adds the.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long add(H key, HK field, HK name, HV value);

    /**
     * Adds the.
     *
     * @param key     the key
     * @param field   the field
     * @param name    the name
     * @param value   the value
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long add(H key, HK field, HK name, HV value, long timeout, TimeUnit unit);

    /**
     * Adds the at.
     *
     * @param key                   the key
     * @param field                 the field
     * @param name                  the name
     * @param value                 the value
     * @param millisecondsTimestamp the milliseconds timestamp
     * @return the long
     */
    Long addAt(H key, HK field, HK name, HV value, long millisecondsTimestamp);

    /**
     * Multi add.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return the long
     */
    Long multiAdd(H key, HK field, HK name, Collection<HV> values);

    /**
     * Multi add.
     *
     * @param key     the key
     * @param field   the field
     * @param name    the name
     * @param values  the values
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long multiAdd(H key, HK field, HK name, Collection<HV> values, long timeout, TimeUnit unit);


    /**
     * Multi add at.
     *
     * @param key                   the key
     * @param field                 the field
     * @param name                  the name
     * @param values                the values
     * @param millisecondsTimestamp the milliseconds timestamp
     * @return the long
     */
    Long multiAddAt(H key, HK field, HK name, Collection<HV> values, long millisecondsTimestamp);


    /**
     * Sets the.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long set(H key, HK field, HK name, HV value);

    /**
     * Sets the.
     *
     * @param key     the key
     * @param field   the field
     * @param name    the name
     * @param value   the value
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long set(H key, HK field, HK name, HV value, long timeout, TimeUnit unit);

    /**
     * Multi set.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return the long
     */
    Long multiSet(H key, HK field, HK name, Collection<HV> values);

    /**
     * Multi set.
     *
     * @param key     the key
     * @param field   the field
     * @param name    the name
     * @param values  the values
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long multiSet(H key, HK field, HK name, Collection<HV> values, long timeout, TimeUnit unit);

    /**
     * Del.
     *
     * @param key the key
     * @return the long
     */
    Long del(H key);

    /**
     * Del.
     *
     * @param key   the key
     * @param field the field
     * @return the long
     */
    Long del(H key, HK field);

    /**
     * Del.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the long
     */
    Long del(H key, HK field, HK name);

    /**
     * Del.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long del(H key, HK field, HK name, HV value);

    /**
     * Multi del.
     *
     * @param key    the key
     * @param field  the field
     * @param name   the name
     * @param values the values
     * @return the long
     */
    Long multiDel(H key, HK field, HK name, Collection<HV> values);

    /**
     * Count.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @return the long
     */
    Long count(H key, HK field, HK name);

    /**
     * Exists.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the boolean
     */
    Boolean exists(H key, HK field, HK name, HV value);

    /**
     * Expire.
     *
     * @param key     the key
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long expire(H key, long timeout, TimeUnit unit);

    /**
     * Expire.
     *
     * @param key     the key
     * @param field   the field
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long expire(H key, HK field, long timeout, TimeUnit unit);

    /**
     * Expire.
     *
     * @param key     the key
     * @param field   the field
     * @param name    the name
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long expire(H key, HK field, HK name, long timeout, TimeUnit unit);

    /**
     * Expire.
     *
     * @param key     the key
     * @param field   the field
     * @param name    the name
     * @param value   the value
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long expire(H key, HK field, HK name, HV value, long timeout, TimeUnit unit);

    /**
     * Ttl.
     *
     * @param key   the key
     * @param field the field
     * @param name  the name
     * @param value the value
     * @return the long
     */
    Long ttl(H key, HK field, HK name, HV value);

    /**
     * Values.
     *
     * @param key
     * @param field
     * @return the list
     */
    List<HV> values(H key, HK field);
}
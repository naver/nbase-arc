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
 * The Interface BoundSessionOfHashListOperations.
 *
 * @param <H>  the generic type
 * @param <HK> the generic type
 * @param <HV> the generic type
 * @author jaehong.kim
 */
public interface BoundSessionOfHashListOperations<H, HK, HV> {

    /**
     * Gets the.
     *
     * @param key the key
     * @return the list
     */
    List<HV> get(HK key);

    /**
     * Multi get.
     *
     * @param keys the keys
     * @return the map
     */
    Map<HK, List<HV>> multiGet(Collection<HK> keys);

    /**
     * Keys.
     *
     * @return the sets the
     */
    Set<HK> keys();

    /**
     * Adds the.
     *
     * @param key   the key
     * @param value the value
     * @return the long
     */
    Long add(HK key, HV value);

    /**
     * Adds the.
     *
     * @param key     the key
     * @param value   the value
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long add(HK key, HV value, long timeout, TimeUnit unit);

    /**
     * Adds the at.
     *
     * @param key                   the key
     * @param value                 the value
     * @param millisecondsTimestamp the milliseconds timestamp
     * @return the long
     */
    Long addAt(HK key, HV value, long millisecondsTimestamp);

    /**
     * Adds the.
     *
     * @param key    the key
     * @param values the values
     * @return the long
     */
    Long add(HK key, Collection<HV> values);

    /**
     * Adds the.
     *
     * @param key     the key
     * @param values  the values
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long add(HK key, Collection<HV> values, long timeout, TimeUnit unit);

    /**
     * Adds the at.
     *
     * @param key                   the key
     * @param values                the values
     * @param millisecondsTimestamp the milliseconds timestamp
     * @return the long
     */
    Long addAt(HK key, Collection<HV> values, long millisecondsTimestamp);

    /**
     * Sets the.
     *
     * @param key   the key
     * @param value the value
     * @return the long
     */
    Long set(HK key, HV value);

    /**
     * Sets the.
     *
     * @param key     the key
     * @param value   the value
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long set(HK key, HV value, long timeout, TimeUnit unit);

    /**
     * Sets the.
     *
     * @param key    the key
     * @param values the values
     * @return the long
     */
    Long set(HK key, Collection<HV> values);

    /**
     * Sets the.
     *
     * @param key     the key
     * @param values  the values
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long set(HK key, Collection<HV> values, long timeout, TimeUnit unit);

    /**
     * Del.
     *
     * @param key the key
     * @return the long
     */
    Long del(HK key);

    /**
     * Del.
     *
     * @param key   the key
     * @param value the value
     * @return the long
     */
    Long del(HK key, HV value);

    /**
     * Del.
     *
     * @param key    the key
     * @param values the values
     * @return the long
     */
    Long del(HK key, Collection<HV> values);

    /**
     * Count.
     *
     * @param key the key
     * @return the long
     */
    Long count(HK key);

    /**
     * Expire.
     *
     * @param key     the key
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long expire(HK key, long timeout, TimeUnit unit);

    /**
     * Expire.
     *
     * @param key     the key
     * @param value   the value
     * @param timeout the timeout
     * @param unit    the unit
     * @return the long
     */
    Long expire(HK key, HV value, long timeout, TimeUnit unit);

    /**
     * Ttl.
     *
     * @param key   the key
     * @param value the value
     * @return the long
     */
    Long ttl(HK key, HV value);

    /**
     * Values.
     *
     * @return the list
     */
    List<HV> values();
}
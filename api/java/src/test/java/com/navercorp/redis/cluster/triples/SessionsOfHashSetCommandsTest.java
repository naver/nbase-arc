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

import org.junit.Test;

import com.navercorp.redis.cluster.RedisClusterTestBase;

/**
 * @author jaehong.kim
 */
public class SessionsOfHashSetCommandsTest extends RedisClusterTestBase {
    public void clear() {
        redis.ssdel(TRIPLES_KEY_0);
        redis.ssdel(TRIPLES_KEY_1);
    }

    @Test
    public void hsget() {
        Set<String> values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(0, values.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(2, values.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        Map<String, Set<String>> hashs = redis.ssmget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_NAME_1);
        assertEquals(2, hashs.get(TRIPLES_NAME_0).size());
        assertEquals(1, hashs.get(TRIPLES_NAME_1).size());

        hashs = redis.ssmget(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        System.out.println("Results:" + hashs);
    }

    @Test
    public void hsgetBinary() {
        Set<byte[]> values = redis.ssget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0);
        assertEquals(0, values.size());

        redis.ssadd(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0, 3, TRIPLES_BVALUE_0);
        values = redis.ssget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0);
        assertEquals(1, values.size());

        redis.ssadd(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0, 3, TRIPLES_BVALUE_1);
        values = redis.ssget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0);
        assertEquals(2, values.size());

        redis.ssadd(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_1, 3, TRIPLES_BVALUE_0);
        Map<byte[], Set<byte[]>> hashs = redis.ssmget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0, TRIPLES_BNAME_0, TRIPLES_BNAME_1);
        assertEquals(2, hashs.get(TRIPLES_BNAME_0).size());
        assertEquals(1, hashs.get(TRIPLES_BNAME_1).size());

        hashs = redis.ssmget(TRIPLES_BKEY_0, TRIPLES_BFIELD_0);
        System.out.println("Results:" + hashs);
    }

    @Test
    public void hskeys() {
        Set<String> keys = redis.sskeys(TRIPLES_KEY_0);
        assertEquals(0, keys.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        keys = redis.sskeys(TRIPLES_KEY_0);
        assertEquals(1, keys.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        keys = redis.sskeys(TRIPLES_KEY_0);
        assertEquals(2, keys.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        keys = redis.sskeys(TRIPLES_KEY_0, TRIPLES_FIELD_1);
        assertEquals(2, keys.size());
    }

    @Test
    public void hsadd() {
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        Set<String> values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(2, values.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0, TRIPLES_VALUE_1,
                TRIPLES_VALUE_2);
        values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1);
        assertEquals(3, values.size());
    }

    @Test
    public void hsaddAt() {
        long millisecondsTimestamp = System.currentTimeMillis() + 10000;
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, millisecondsTimestamp, TRIPLES_VALUE_0);
        Set<String> values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());
    }

    @Test
    public void hsset() {
        long result = redis.ssset(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(1, result);
        Set<String> values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        redis.ssset(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, TRIPLES_VALUE_1);
        values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(2, values.size());

        Set<String> keys = redis.sskeys(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, keys.size());
        assertTrue(keys.contains(TRIPLES_NAME_0));
    }

    @Test
    public void hsdel() {
        long result = redis.ssdel(TRIPLES_KEY_0);
        assertEquals(0, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.ssdel(TRIPLES_KEY_0);
        assertEquals(1, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, 3, TRIPLES_VALUE_1);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        result = redis.ssdel(TRIPLES_KEY_0);
        assertEquals(1, result);
    }

    @Test
    public void hsrem() {
        long result = redis.ssrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(0, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.ssrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(1, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.ssrem(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_2);
        result = redis.ssrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        Set<String> values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(0, values.size());
        assertEquals(values.toString(), 3, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_2);
        result = redis.ssrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, TRIPLES_VALUE_1);
        assertEquals(2, result);
        values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());
    }

    @Test
    public void hscount() {
        long count = redis.sscount(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(0, count);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        count = redis.sscount(TRIPLES_KEY_0);
        assertEquals(1, count);
        count = redis.sscount(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, count);
        count = redis.sscount(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, count);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_2);
        Set<String> values = redis.ssget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(3, values.size());
        count = redis.sscount(TRIPLES_KEY_0);
        assertEquals(1, count);
        count = redis.sscount(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, count);
        count = redis.sscount(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(3, count);
    }

    @Test
    public void hsexists() {
        boolean result = redis.ssexists(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(false, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.ssexists(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(true, result);
    }

    @Test
    public void hsexpire() {
        long result = redis.ssexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, 3);
        assertEquals(0, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.ssexpire(TRIPLES_KEY_0, 5);
        assertEquals(1, result);
        result = redis.ssexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, 5);
        assertEquals(1, result);
        result = redis.ssexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 5);
        assertEquals(1, result);
        result = redis.ssexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, 5);
        assertEquals(1, result);
    }

    @Test
    public void hsttl() {
        long result = redis.ssttl(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(-1, result);

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.ssttl(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(true, result > 0);

        result = redis.ssttl(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(true, result > 0);
    }

    @Test
    public void hsvals() {
        // check list
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, TRIPLES_VALUE_1);
        List<String> values = redis.ssvals(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(2, values.size());

        redis.ssadd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_1);
        values = redis.ssvals(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(3, values.size());
    }
}
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.navercorp.redis.cluster.RedisClusterTestBase;

/**
 * @author jaehong.kim
 */
public class SessionOfHashListCommandsTest extends RedisClusterTestBase {

    public void clear() {
        redis.sldel(TRIPLES_KEY_0);
        redis.sldel(TRIPLES_KEY_1);
        redis.sldel(TRIPLES_KEY_2);
    }

    @Test
    public void slget() {
        List<String> values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(0, values.size());

        long result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        assertEquals(1, result);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        assertEquals(1, result);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(2, values.size());

        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        assertEquals(1, result);
        Map<String, List<String>> hashs = redis.slmget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_NAME_1);
        assertEquals(2, hashs.get(TRIPLES_NAME_0).size());
        assertEquals(1, hashs.get(TRIPLES_NAME_1).size());

        hashs = redis.slmget(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(2, hashs.get(TRIPLES_NAME_0).size());
        assertEquals(1, hashs.get(TRIPLES_NAME_1).size());

        result = redis.ssadd(TRIPLES_KEY_2, TRIPLES_FIELD_1, TRIPLES_NAME_1, 3, TRIPLES_VALUE_1);
        assertEquals(1, result);
        try {
            redis.slget(TRIPLES_KEY_2, TRIPLES_FIELD_1, TRIPLES_NAME_1);
            fail("passed invalid type");
        } catch (Exception e) {
        }
    }

    @Test
    public void slset() {
        long result = redis.slset(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(1, result);
        List<String> values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        List<String> valueList = new ArrayList<String>();
        valueList.add("value1");
        valueList.add("value2");
        redis.slset(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, TRIPLES_VALUE_1);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(2, values.size());

        Set<String> keys = redis.slkeys(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, keys.size());
        assertTrue(keys.contains(TRIPLES_NAME_0));
    }

    @Test
    public void hlkeys() {
        Set<String> keys = redis.slkeys(TRIPLES_KEY_0);
        assertEquals(0, keys.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        keys = redis.slkeys(TRIPLES_KEY_0);
        assertEquals(1, keys.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        keys = redis.slkeys(TRIPLES_KEY_0);
        assertEquals(2, keys.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        keys = redis.slkeys(TRIPLES_KEY_0, TRIPLES_FIELD_1);
        assertEquals(2, keys.size());

        redis.sladd(TRIPLES_KEY_2, TRIPLES_FIELD_1, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        redis.ssadd(TRIPLES_KEY_2, TRIPLES_FIELD_2, TRIPLES_NAME_1, 3, TRIPLES_VALUE_1);
        keys = redis.slkeys(TRIPLES_KEY_2);
        assertEquals(2, keys.size());
    }

    @Test
    public void hladd() {
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        List<String> values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(2, values.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(3, values.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0, TRIPLES_VALUE_1,
                TRIPLES_VALUE_2);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1);
        assertEquals(3, values.size());
    }

    @Test
    public void hladdAt() {
        long millisecondsTimestamp = System.currentTimeMillis() + 10000;
        redis.sladdAt(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, millisecondsTimestamp, TRIPLES_VALUE_0);
        List<String> values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());
    }

    @Test
    public void hldel() {
        long result = redis.sldel(TRIPLES_KEY_0);
        assertEquals(0, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.sldel(TRIPLES_KEY_0);
        assertEquals(1, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_1, TRIPLES_NAME_1, 3, TRIPLES_VALUE_0);
        result = redis.sldel(TRIPLES_KEY_0);
        assertEquals(1, result);
    }

    @Test
    public void hlrem() {
        long result = redis.slrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(0, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.slrem(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, result);

        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        assertEquals(1, result);
        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        assertEquals(1, result);
        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_2);
        assertEquals(1, result);
        List<String> values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(values.toString(), 3, values.size());
        result = redis.slrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(values.toString(), 0, values.size());
        assertEquals(values.toString(), 3, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.slrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(1, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_2);
        result = redis.slrem(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, TRIPLES_VALUE_1);
        assertEquals(2, result);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

    }

    @Test
    public void hlcount() {
        long count = redis.slcount(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(0, count);

        long result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        assertEquals(1, result);
        count = redis.slcount(TRIPLES_KEY_0);
        assertEquals(1, count);
        count = redis.slcount(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, count);
        count = redis.slcount(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, count);

        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        assertEquals(1, result);
        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_1);
        assertEquals(1, result);
        result = redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_2);
        assertEquals(1, result);
        List<String> values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(4, values.size());
        count = redis.slcount(TRIPLES_KEY_0);
        assertEquals(1, count);
        count = redis.slcount(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(1, count);
        count = redis.slcount(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(4, count);
    }

    @Test
    public void hlexists() {
        boolean result = redis.slexists(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(false, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.slexists(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(true, result);
    }

    @Test
    public void hlexpire() throws Exception {
        long result = redis.slexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, 3);
        assertEquals(0, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.slexpire(TRIPLES_KEY_0, 5);
        assertEquals(1, result);
        result = redis.slexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, 5);
        assertEquals(1, result);
        result = redis.slexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 5);
        assertEquals(1, result);
        result = redis.slexpire(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0, 5);
        assertEquals(1, result);

        redis.sladd(TRIPLES_KEY_1, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        result = redis.slexpire(TRIPLES_KEY_1, 1);
        assertEquals(1, result);
        TimeUnit.SECONDS.sleep(2);
        List<String> values = redis.slget(TRIPLES_KEY_1, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(0, values.size());
    }

    @Test
    public void hlttl() {
        long result = redis.slttl(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(-1, result);

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        result = redis.slttl(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(true, result > 0);
        result = redis.slttl(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        assertEquals(true, result > 0);

    }

    @Test
    public void hladdAndDel() {
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        List<String> values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(1, values.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, 3, TRIPLES_VALUE_0);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(2, values.size());

        redis.del(TRIPLES_KEY_0);
        values = redis.slget(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0);
        assertEquals(0, values.size());
    }

    @Test
    public void hlvals() {
        // check list
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_0);
        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_1, TRIPLES_VALUE_1);
        List<String> values = redis.slvals(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(2, values.size());

        redis.sladd(TRIPLES_KEY_0, TRIPLES_FIELD_0, TRIPLES_NAME_0, TRIPLES_VALUE_1);
        values = redis.slvals(TRIPLES_KEY_0, TRIPLES_FIELD_0);
        assertEquals(3, values.size());
    }
}
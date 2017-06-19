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

package com.navercorp.redis.cluster;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.navercorp.redis.cluster.gateway.GatewayAddress;
import com.navercorp.redis.cluster.gateway.GatewayServer;
import com.navercorp.redis.cluster.util.TestEnvUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ComparisonFailure;

/**
 * @author jaehong.kim
 */
public abstract class RedisClusterTestBase extends Assert {
    public final String REDIS_KEY_0 = "redis_key_0";
    public final String REDIS_KEY_1 = "redis_key_1";
    public final String REDIS_KEY_2 = "redis_key_2";
    public final String REDIS_KEY_3 = "redis_key_3";
    public final String REDIS_FIELD_0 = "redis_field_0";
    public final String REDIS_FIELD_1 = "redis_field_1";
    public final String REDIS_FIELD_2 = "redis_field_2";
    public final String REDIS_FIELD_3 = "redis_field_3";
    public final String REDIS_VALUE_0 = "redis_value_0";
    public final String REDIS_VALUE_1 = "redis_value_1";
    public final String REDIS_VALUE_2 = "redis_value_2";
    public final String REDIS_VALUE_3 = "redis_value_3";
    public final String REDIS_XX = "XX";
    public final String REDIS_NX = "NX";
    public final String REDIS_EX = "EX";
    public final String REDIS_PX = "PX";
    
    public final byte[] REDIS_BKEY_0 = {0x01, 0x02, 0x03, 0x04};
    public final byte[] REDIS_BKEY_1 = {0x01, 0x02, 0x03, 0x04, 0x0A};
    public final byte[] REDIS_BKEY_2 = {0x01, 0x02, 0x03, 0x04, 0x0B};
    public final byte[] REDIS_BKEY_3 = {0x01, 0x02, 0x03, 0x04, 0x0C};
    public final byte[] REDIS_BFIELD_0 = {0x02, 0x02, 0x03, 0x04};
    public final byte[] REDIS_BFIELD_1 = {0x02, 0x02, 0x03, 0x04, 0x0A};
    public final byte[] REDIS_BFIELD_2 = {0x02, 0x02, 0x03, 0x04, 0x0B};
    public final byte[] REDIS_BFIELD_3 = {0x02, 0x02, 0x03, 0x04, 0x0C};
    public final byte[] REDIS_BVALUE_0 = {0x05, 0x06, 0x07, 0x08};
    public final byte[] REDIS_BVALUE_1 = {0x05, 0x06, 0x07, 0x08, 0x0A};
    public final byte[] REDIS_BVALUE_2 = {0x05, 0x06, 0x07, 0x08, 0x0B};
    public final byte[] REDIS_BVALUE_3 = {0x05, 0x06, 0x07, 0x08, 0x0C};
    public final byte[] REDIS_BXX = {0x78, 0x78};
    public final byte[] REDIS_BNX = {0x6E, 0x78};
    public final byte[] REDIS_BEX = {0x65, 0x78};
    public final byte[] REDIS_BPX = {0x70, 0x78};
    
    public final byte[] REDIS_BKEY_VALUE_0 = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    public final byte[] REDIS_BKEY_STAR = {0x01, 0x02, 0x03, 0x04, '*'};
    public final byte[] REDIS_BVALUE_STAR = {0x05, 0x06, 0x07, 0x08, '*'};

    public static final String TRIPLES_KEY_0 = "TRIPLES_KEY_0";
    public static final String TRIPLES_KEY_1 = "TRIPLES_KEY_1";
    public static final String TRIPLES_KEY_2 = "TRIPLES_KEY_2";
    public static final String TRIPLES_FIELD_0 = "TRIPLES_FIELD_0";
    public static final String TRIPLES_FIELD_1 = "TRIPLES_FIELD_1";
    public static final String TRIPLES_FIELD_2 = "TRIPLES_FIELD_2";
    public static final String TRIPLES_NAME_0 = "TRIPLES_NAME_0";
    public static final String TRIPLES_NAME_1 = "TRIPLES_NAME_1";
    public static final String TRIPLES_VALUE_0 = "TRIPLES_VALUE_0";
    public static final String TRIPLES_VALUE_1 = "TRIPLES_VALUE_1";
    public static final String TRIPLES_VALUE_2 = "TRIPLES_VALUE_2";

    public static final byte[] TRIPLES_BKEY_0 = {0x04, 0x03, 0x02, 0x01};
    public static final byte[] TRIPLES_BKEY_1 = {0x04, 0x03, 0x02, 0x01, 0x0A};
    public static final byte[] TRIPLES_BKEY_2 = {0x04, 0x03, 0x02, 0x01, 0x0B};
    public static final byte[] TRIPLES_BFIELD_0 = {0x05, 0x04, 0x03, 0x02, 0x0A};
    public static final byte[] TRIPLES_BFIELD_1 = {0x05, 0x04, 0x03, 0x02, 0x0B};
    public static final byte[] TRIPLES_BFIELD_2 = {0x05, 0x04, 0x03, 0x02, 0x0C};
    public static final byte[] TRIPLES_BNAME_0 = {0x06, 0x05, 0x04, 0x03};
    public static final byte[] TRIPLES_BNAME_1 = {0x06, 0x05, 0x04, 0x03, 0x0A};
    public static final byte[] TRIPLES_BVALUE_0 = {0x07, 0x06, 0x05, 0x04};
    public static final byte[] TRIPLES_BVALUE_1 = {0x07, 0x06, 0x05, 0x04, 0x0A};
    public static final byte[] TRIPLES_BVALUE_2 = {0x07, 0x06, 0x05, 0x04, 0x0B};

    public static RedisCluster redis;
    public static GatewayServer server;


    @Before
    public void before() {
        redis = new RedisCluster(TestEnvUtils.getHost(), TestEnvUtils.getPort(), 1000);
        server = new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()));
        clear();
    }

    @After
    public void after() {
        redis.disconnect();
    }

    public abstract void clear();

    protected void assertEquals(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int n = 0; n < expected.size(); n++) {
            assertArrayEquals(expected.get(n), actual.get(n));
        }
    }

    protected void assertEquals(Set<byte[]> expected, Set<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<byte[]> e = expected.iterator();
        while (e.hasNext()) {
            byte[] next = e.next();
            boolean contained = false;
            for (byte[] element : expected) {
                if (Arrays.equals(next, element)) {
                    contained = true;
                }
            }
            if (!contained) {
                throw new ComparisonFailure("element is missing", Arrays.toString(next), actual.toString());
            }
        }
    }

    protected boolean arrayContains(List<byte[]> array, byte[] expected) {
        for (byte[] a : array) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }

    protected boolean setContains(Set<byte[]> set, byte[] expected) {
        for (byte[] a : set) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }
}

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

package com.navercorp.redis.cluster.gateway;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class GatewayAffinityTest {

    @Test
    public void put() {
        GatewayAffinity affinity = new GatewayAffinity();

        affinity.put(1, 1, AffinityState.READ);
        affinity.put(2, 2, AffinityState.WRITE);


        List<Integer> result = affinity.get(1, AffinityState.READ);
        assertEquals(1, result.size());

        result = affinity.get(2, AffinityState.READ);
        assertEquals(1, result.size());

        result = affinity.get(999, AffinityState.WRITE);
        assertEquals(0, result.size());
    }


}

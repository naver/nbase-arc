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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class GatewayServerSelectorTest {

    List<GatewayServer> list = new ArrayList<GatewayServer>();

    @Before
    public void before() {
        list.add(new GatewayServer(new GatewayAddress(1, "111.111.111.111:111")));
        list.add(new GatewayServer(new GatewayAddress(2, "222.222.222.222:222")));
        list.add(new GatewayServer(new GatewayAddress(3, "333.333.333.333:333")));
    }

    @Test
    public void random() {
        GatewayServerSelector selector = new GatewayServerSelector(GatewayServerSelector.METHOD_RANDOM);

        for (int i = 0; i < 1000; i++) {
            GatewayServer server = selector.get(list);
            assertEquals(true, list.contains(server));
            System.out.println(server);
        }
    }

    @Test
    public void roundrobin() {
        GatewayServerSelector selector = new GatewayServerSelector(GatewayServerSelector.METHOD_ROUND_ROBIN);

        Map<String, AtomicInteger> result = new HashMap<String, AtomicInteger>();

        System.out.println(Integer.MAX_VALUE);

        for (int i = 0; i < 1000000; i++) {
            GatewayServer server = selector.get(list);
            assertEquals(true, list.contains(server));

            // counting
            AtomicInteger count = result.get(server.getAddress().getName());
            if (count == null) {
                count = new AtomicInteger(1);
                result.put(server.getAddress().getName(), count);
            } else {
                count.incrementAndGet();
            }
        }

        for (AtomicInteger count : result.values()) {
            System.out.println(count.get());
        }
    }

}

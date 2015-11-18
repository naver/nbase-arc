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

import com.navercorp.redis.cluster.util.TestEnvUtils;
import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class GatewayTest {
    Gateway manager;

    @Test
    public void init() {
        GatewayConfig config = new GatewayConfig();

        // empty address.
        config.setIpAddress("");
        try {
            manager = new Gateway(config);
            fail("passed 'not found address'");
        } catch (Exception e) {
        } finally {
            if (manager != null) {
                manager.destroy();
            }
        }

        // valid address.
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());
        manager = new Gateway(config);
        assertEquals(1, manager.getServers().size());
        manager.destroy();
    }

    @Test
    public void getServer() {
        GatewayConfig config = new GatewayConfig();
        config.setIpAddress(TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort());
        manager = new Gateway(config);

        // check random selected.
        for (int i = 0; i < 100; i++) {
            GatewayServer server = manager.getServer(-1, AffinityState.READ);
            assertNotNull(server);
        }

        // check invalid state.
        GatewayServer server01 = manager.getServer(-1, AffinityState.READ);
        server01.setValid(false);

        GatewayServer server02 = manager.getServer(-1, AffinityState.READ);
        for (int i = 0; i < 100; i++) {
            GatewayServer server = manager.getServer(-1, AffinityState.READ);
            assertEquals(server02.getAddress().getName(), server.getAddress().getName());
        }

        // all of invalid state.
        server02.setValid(false);
        GatewayServer server03 = manager.getServer(-1, AffinityState.READ);
        if (server03 != server01 && server03 != server02) {
            fail("invalid return");
        }

        manager.destroy();
    }
}
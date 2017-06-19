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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.navercorp.redis.cluster.RedisClusterPoolConfig;
import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author seongminwoo
 */
public class HealthCheckManagerTest {
    /**
     * Test method for {@link HealthCheckManager#start()}.
     */
    @Test
    public void run() throws Exception {
        final List<GatewayServer> servers = new ArrayList<GatewayServer>();

        RedisClusterPoolConfig config = new RedisClusterPoolConfig();
        config.setInitialSize(8);

        final GatewayServer valid = new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort()), config,
                1000, null);
        valid.setValid(true);
        final GatewayServer invalid = new GatewayServer(new GatewayAddress(2, "10.96.250.209:6000"), config, 1000, null);
        invalid.setValid(false);
        servers.add(valid);
        servers.add(invalid);

        GatewayServerData data = new GatewayServerData() {
            public Collection<GatewayServer> getServers() {
                return servers;
            }

            public void reload(List<GatewayAddress> addresses) {
            }

            @Override
            public void reload(GatewayAffinity affinity) {
            }
        };

        HealthCheckManager manager = new HealthCheckManager(data, 1, 2);
        TimeUnit.SECONDS.sleep(3);
        manager.shutdown();
    }
}


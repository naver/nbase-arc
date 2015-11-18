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

import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author jaehong.kim
 */
public class HealthCheckManagerTestMain {

    public void running() throws Exception {
        final List<GatewayServer> servers = new ArrayList<GatewayServer>();
        servers.add(new GatewayServer(new GatewayAddress(1, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort())));
        servers.add(new GatewayServer(new GatewayAddress(2, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort())));
        servers.add(new GatewayServer(new GatewayAddress(3, TestEnvUtils.getHost() + ":" + TestEnvUtils.getPort())));

        GatewayServerData data = new GatewayServerData() {
            public Collection<GatewayServer> getServers() {
                return servers;
            }

            public void changeGatewayValid(String address, boolean valid) {
            }

            public void reload(List<GatewayAddress> addresses) {
            }

            @Override
            public void reload(GatewayAffinity affinity) {
            }
        };

        HealthCheckManager manager = new HealthCheckManager(data, 1, 2);
        while (true) {
            System.out.println("Servers state=" + servers);
            TimeUnit.SECONDS.sleep(30);
        }
    }

    public static void main(String[] args) throws Exception {
        HealthCheckManagerTestMain main = new HealthCheckManagerTestMain();
        main.running();
    }
}

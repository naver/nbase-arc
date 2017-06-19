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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author jaehong.kim
 */
public class NodeWatcherTest {

    @Test
    public void getGatewayAddress() throws Exception {
        NodeWatcher watcher = new NodeWatcher(new GatewayServerData() {
            private List<GatewayAddress> addresses;

            @Override
            public void reload(List<GatewayAddress> addresses) {
                this.addresses = addresses;
            }

            @Override
            public Collection<GatewayServer> getServers() {
                return null;
            }

            @Override
            public void reload(GatewayAffinity affinity) {
            }
        });

        GatewayConfig config = new GatewayConfig();
        config.setZkAddress(TestEnvUtils.getZkAddress());
        config.setClusterName(TestEnvUtils.getClusterName());

        watcher.setConfig(config);
        watcher.start();
        List<GatewayAddress> result = watcher.getGatewayAddress();

        TimeUnit.SECONDS.sleep(1);

        watcher.stop();
    }

    @Test
    public void getGatewayAffinity() throws Exception {
        NodeWatcher watcher = new NodeWatcher(new GatewayServerData() {
            private List<GatewayAddress> addresses;

            @Override
            public void reload(List<GatewayAddress> addresses) {
                this.addresses = addresses;
            }

            @Override
            public Collection<GatewayServer> getServers() {
                return null;
            }

            @Override
            public void reload(GatewayAffinity affinity) {
            }
        });

        GatewayConfig config = new GatewayConfig();
        config.setZkAddress(TestEnvUtils.getZkAddress());
        config.setClusterName(TestEnvUtils.getClusterName());

        watcher.setConfig(config);
        watcher.start();
        GatewayAffinity affinity = watcher.getGatewayAffinity();

        TimeUnit.SECONDS.sleep(1);

        watcher.stop();
    }


}


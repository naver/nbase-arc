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
public class GatewayClientZookeeperUse {

    @Test
    public void usage() {
        final GatewayConfig config = new GatewayConfig();
        config.setZkAddress(TestEnvUtils.getZkAddress());
        config.setClusterName(TestEnvUtils.getClusterName());

        final GatewayClient client = new GatewayClient(config);
        client.set("name", "clark kent");
        String name = client.get("name");

        System.out.println(name);
        client.destroy();
    }

    @Test
    public void usageIP() {
        final GatewayConfig config = new GatewayConfig();
        config.setZkAddress(TestEnvUtils.getZkAddress());
        config.setClusterName(TestEnvUtils.getClusterName());

        final GatewayClient client = new GatewayClient(config);
        client.set("name", "clark kent");
        String result = client.get("name");

        assertEquals("clark kent", result);
        client.destroy();
    }

    @Test
    public void usageDNS() {
        String host = TestEnvUtils.getZkAddress();
        final GatewayConfig config = new GatewayConfig();
        config.setZkAddress(host);
        config.setClusterName(TestEnvUtils.getClusterName());

        final GatewayClient client = new GatewayClient(config);
        client.set("name", "clark kent");
        String result = client.get("name");

        assertEquals("clark kent", result);
        client.destroy();
    }

    @Test(expected = IllegalArgumentException.class)
    public void usageWrongDNS() {
        String host = "wrongdev.xnbasearc.navercorp.com ";
        final GatewayConfig config = new GatewayConfig();
        config.setZkAddress(host);
        config.setClusterName("rtcs_test");

        final GatewayClient client = new GatewayClient(config);
        client.set("name", "clark kent");
        String result = client.get("name");

        assertEquals("clark kent", result);
        client.destroy();
    }

    @Test
    public void usageLoop() throws InterruptedException {
        final GatewayConfig config = new GatewayConfig();
        config.setZkAddress(TestEnvUtils.getZkAddress());
        config.setClusterName(TestEnvUtils.getClusterName());

        final GatewayClient client = new GatewayClient(config);
        client.set("name", "clark kent");

        int loop = 100;

        for (int i = 0; i < loop; i++) {
            String name = client.get("name");
            System.out.println(name);
            Thread.sleep(100);
        }

        client.destroy();
    }


    @Test
    public void unknown() {
        final GatewayConfig config = new GatewayConfig();
        config.setZkAddress("111.111.111.111:2181,222.222.222.222:2181,333.333.333.333:2181");
        config.setClusterName("testCluster");

        final GatewayClient client = new GatewayClient(config);
        client.set("name", "clark kent");
        String name = client.get("name");

        System.out.println(name);
        client.destroy();
    }
}

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

import java.util.UUID;

import com.navercorp.redis.cluster.RedisClusterPoolConfig;
import com.navercorp.redis.cluster.util.TestEnvUtils;

/**
 * @author jaehong.kim
 */
public class GatewayClientAffinityTestMain {

    public void start() throws Exception {
        GatewayConfig config = new GatewayConfig();
        config.setZkAddress(TestEnvUtils.getZkAddress());
        config.setClusterName(TestEnvUtils.getClusterName());
        config.setHealthCheckUsed(false);
        config.setTimeoutMillisec(1000);

        RedisClusterPoolConfig poolConfig = new RedisClusterPoolConfig();
        poolConfig.setMaxActive(1);
        //		poolConfig.setMaxIdle(8);
        //		poolConfig.setMinIdle(8);

        config.setPoolConfig(poolConfig);

        final GatewayClient client = new GatewayClient(config);

        final int max = 100;
        for (int i = 0; i < max; i++) {
            final String key = UUID.randomUUID().toString();
            client.setex(key, 1, "foo");
//			final String value = client.get(key);
//			if (!value.equals("foo")) {
//				throw new Exception("");
//			}
        }

        for (GatewayServer server : client.getGateway().getServers()) {
            System.out.println(server.getResource().info("gateway"));
        }

        client.destroy();
    }

    public static void main(String[] args) throws Exception {
        GatewayClientAffinityTestMain main = new GatewayClientAffinityTestMain();
        main.start();
    }
}
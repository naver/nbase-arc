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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author seongminwoo
 */
public class GatewayConfigTest {
    GatewayConfig config;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        config = new GatewayConfig();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void timeoutDefault() {
        assertEquals(GatewayConfig.DEFAULT_TIMEOUT_MILLISEC, config.getTimeoutMillisec());
    }

    @Test
    public void setTimeout() {
        int timeout = 1000;
        config.setTimeoutMillisec(timeout);
        assertEquals(timeout, config.getTimeoutMillisec());
    }

    @Test
    public void setAddress() {
        GatewayClient client = null;

        try {
            config.setIpAddress(null);
            client = new GatewayClient(config);
            fail("illegal argument");
        } catch (Exception e) {
        }

        try {
            config.setIpAddress("");
            client = new GatewayClient(config);
            fail("illegal argument");
        } catch (Exception e) {
        }


        try {
            config.setIpAddress("foo");
            client = new GatewayClient(config);
            fail("illegal argument");
        } catch (Exception e) {
        }

        try {
            config.setIpAddress("111.111.111.111");
            client = new GatewayClient(config);
            fail("illegal argument");
        } catch (Exception e) {
        }

        config.setIpAddress("111.111.111.111:9009");
        client = new GatewayClient(config);
        config.setIpAddress("111.111.111.111:9009,222.222.222.222:9009");
        client = new GatewayClient(config);

        // white space possible
        config.setIpAddress("111.111.111.111:9009, 222.222.222.222:9009");
        client = new GatewayClient(config);
        assertEquals("111.111.111.111:9009, 222.222.222.222:9009", config.getIpAddress());

        // white space possible
        config.setIpAddress(null);
        config.setDomainAddress("dev.com:9009  ");

        assertEquals("dev.com:9009  ", config.getDomainAddress());

        try {
            config.setDomainAddress("bad.domain:9009,foo.bar:9009");
            client = new GatewayClient(config);
            fail("illegal argument");
        } catch (Exception e) {
        } finally {
            client.destroy();
        }

    }

}

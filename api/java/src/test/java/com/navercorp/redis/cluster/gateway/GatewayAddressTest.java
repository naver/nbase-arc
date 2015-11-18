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
public class GatewayAddressTest {

    @Test
    public void operation() {
        GatewayAddress address = new GatewayAddress(1, "1.1.1.1:6379");
        assertEquals("1.1.1.1", address.getHost());
        assertEquals(6379, address.getPort());
        assertEquals("1.1.1.1:6379", address.getName());

        // not found port
        try {
            new GatewayAddress(1, "1.1.1.1");
            fail("passed not found port");
        } catch (Exception e) {
        }

        // invalid port
        try {
            new GatewayAddress(1, "1.1.1.1:ABC");
            fail("passed invalid port");
        } catch (Exception e) {
        }

        // space
        new GatewayAddress(1, "1.1.1.1: 6379");
        new GatewayAddress(1, "1.1.1.1 :6379");
    }

    @Test
    public void asList() {
        List<GatewayAddress> list = GatewayAddress.asList("1.1.1.1:6379,1.1.1.2:6379,1.1.1.3:6379");
        assertEquals(3, list.size());

        // space
        list = GatewayAddress.asList("1.1.1.1:6379, 1.1.1.2:6379, 1.1.1.3:6379");
        assertEquals(3, list.size());
    }

    @Test
    public void asListFromDomain() {
        // check domain
        List<GatewayAddress> list = GatewayAddress.asListFromDomain("naver.com:80");
        assertEquals(true, list.size() > 0);
    }
}
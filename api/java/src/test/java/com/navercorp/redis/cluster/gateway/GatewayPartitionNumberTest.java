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

import org.junit.Test;

/**
 * @author jaehong.kim
 */
public class GatewayPartitionNumberTest {

    @Test
    public void crc16() {
        assertEquals(44950, GatewayPartitionNumber.crc16("foo".getBytes()));
        assertEquals(37829, GatewayPartitionNumber.crc16("bar".getBytes()));
        assertEquals(31879, GatewayPartitionNumber.crc16("nBase-ARC".getBytes()));


        assertEquals(55177, GatewayPartitionNumber.crc16("1234".getBytes()));
        assertEquals(36885, GatewayPartitionNumber.crc16("12345678".getBytes()));
        assertEquals(15101, GatewayPartitionNumber.crc16("abcdef".getBytes()));
        assertEquals(47139, GatewayPartitionNumber.crc16("abdef".getBytes()));
        assertEquals(690, GatewayPartitionNumber.crc16("qwerty".getBytes()));
        assertEquals(44733, GatewayPartitionNumber.crc16("line_rangers".getBytes()));
        assertEquals(12739, GatewayPartitionNumber.crc16("123456789".getBytes()));

    }

    @Test
    public void getPartitionNumber() throws Exception {
        assertEquals(3990, GatewayPartitionNumber.get("foo"));
        assertEquals(3990, GatewayPartitionNumber.get("foo".getBytes()));

        assertEquals(5061, GatewayPartitionNumber.get("bar"));
        assertEquals(5061, GatewayPartitionNumber.get("bar".getBytes()));

        assertEquals(7303, GatewayPartitionNumber.get("nBase-ARC"));
        assertEquals(7303, GatewayPartitionNumber.get("nBase-ARC".getBytes()));

        assertEquals(2263, GatewayPartitionNumber.get("한글"));
        assertEquals(2263, GatewayPartitionNumber.get("한글".getBytes("UTF-8")));


    }


}

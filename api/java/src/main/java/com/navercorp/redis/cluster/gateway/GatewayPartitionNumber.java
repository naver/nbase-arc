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

/**
 * @author jaehong.kim
 */
public class GatewayPartitionNumber {

    public static final int NOT_MATCHED = -1;

    public final static int POLYNOMIAL = 0x1021;

    public static int get(final String str) {
        return get(toBytes(str));
    }

    public static int get(final byte[] bytes) {
        if (bytes == null) {
            return NOT_MATCHED;
        }

        return crc16(bytes) % 8192;
    }

    public static int crc16(byte[] bytes) {
        int crc = 0;
        for (byte b : bytes) {
            for (int i = 0; i < 8; i++) {
                boolean bit = ((b >> (7 - i) & 1) == 1);
                boolean c15 = ((crc >> 15 & 1) == 1);
                crc <<= 1;

                if (c15 ^ bit)
                    crc ^= POLYNOMIAL;
            }
        }

        return crc &= 0xffff;
    }

    private static byte[] toBytes(final String key) {
        try {
            return key.getBytes("UTF-8");
        } catch (Exception e) {
            return null;
        }
    }
}

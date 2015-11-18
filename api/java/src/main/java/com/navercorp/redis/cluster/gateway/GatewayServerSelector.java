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

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Class GatewayServerSelector.
 *
 * @author jaehong.kim
 */
public class GatewayServerSelector {

    /**
     * The Constant METHOD_RANDOM.
     */
    public static final String METHOD_RANDOM = "random";

    /**
     * The Constant METHOD_ROUND_ROBIN.
     */
    public static final String METHOD_ROUND_ROBIN = "round-robin";

    /**
     * The Constant MAX_SEQ.
     */
    private static final int MAX_SEQ = Integer.MAX_VALUE;

    /**
     * The Enum METHOD.
     */
    private enum METHOD {

        /**
         * The RANDOM.
         */
        RANDOM,
        /**
         * The ROUN d_ robin.
         */
        ROUND_ROBIN
    }

    /**
     * The method.
     */
    private final METHOD method;

    /**
     * The seq.
     */
    private final AtomicInteger seq = new AtomicInteger(0);

    /**
     * Instantiates a new gateway server selector.
     *
     * @param method the method
     */
    public GatewayServerSelector(String method) {
        if (method != null && METHOD_RANDOM.equals(method.toLowerCase())) {
            this.method = METHOD.RANDOM;
        } else {
            // default
            this.method = METHOD.ROUND_ROBIN;
        }
    }

    /**
     * Gets the.
     *
     * @param list the list
     * @return the gateway server
     */
    public GatewayServer get(List<GatewayServer> list) {
        if (list.size() == 0) {
            throw new IllegalArgumentException("list must not be zero size");
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        if (this.method == METHOD.RANDOM) {
            return random(list);
        }

        return roundrobin(list);
    }

    /**
     * Random.
     *
     * @param list the list
     * @return the gateway server
     */
    GatewayServer random(List<GatewayServer> list) {
        return list.get(new Random().nextInt(list.size()));
    }

    /**
     * Roundrobin.
     *
     * @param list the list
     * @return the gateway server
     */
    GatewayServer roundrobin(List<GatewayServer> list) {
        this.seq.compareAndSet(MAX_SEQ, 0);
        int index = seq.getAndIncrement();
        final int size = list.size();
        if (index >= size) {
            index = index % size;
        }

        return list.get(index);
    }
}
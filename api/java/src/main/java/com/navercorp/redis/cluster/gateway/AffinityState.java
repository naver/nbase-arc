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
 * The Enum AffinityState.
 *
 * @author jaehong.kim
 */
public enum AffinityState {

    /**
     * The all.
     */
    ALL,
    /**
     * The read.
     */
    READ,
    /**
     * The write.
     */
    WRITE,
    /**
     * The none.
     */
    NONE;

    /**
     * Gets the.
     *
     * @param state the state
     * @return the affinity state
     */
    public static AffinityState get(final char state) {
        if (state == 'A') {
            return ALL;
        } else if (state == 'R') {
            return READ;
        } else if (state == 'W') {
            return WRITE;
        }

        return NONE;
    }
}
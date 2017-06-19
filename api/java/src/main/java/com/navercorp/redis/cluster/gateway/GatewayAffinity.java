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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.navercorp.redis.cluster.gateway.GatewayAffinity.AffinityInfo;

/**
 * @author jaehong.kim
 */
public class GatewayAffinity {

    // gateway id : affinity(partition number & state)
    private ConcurrentHashMap<Integer, AffinityInfo> table = new ConcurrentHashMap<Integer, AffinityInfo>();

    public List<Integer> get(final int partitionNumber, final AffinityState state) {
        final AffinityInfo info = table.get(partitionNumber);
        if (info == null) {
            // not found affinity partition
            return new ArrayList<Integer>();
        }

        return info.get(state);
    }

    public void put(final int id, final String affinityCode) {
        // RLE decode
        final String affinity = RunLengthHelper.decode(affinityCode);
        for (int i = 0; i < affinity.length(); i++) {
            final AffinityState state = AffinityState.get(affinity.charAt(i));
            if (state != AffinityState.NONE) {
                put(i, id, state);
            }
        }
    }

    public void put(final int partitionNumber, final int gatewayId, final AffinityState state) {
        AffinityInfo info = table.get(partitionNumber);
        if (info == null) {
            info = new AffinityInfo();
        }
        // concurrent
        AffinityInfo prev = table.putIfAbsent(partitionNumber, info);
        if (prev != null) {
            info = prev;
        }
        info.put(gatewayId, state);
    }

    ConcurrentHashMap<Integer, AffinityInfo> getTable() {
        return table;
    }

    public String toString() {
        return table.toString();
    }

    static public class AffinityInfo {
        // for performance
        private final List<Integer> writeIds = new ArrayList<Integer>();
        private final List<Integer> otherIds = new ArrayList<Integer>();

        public void put(final int id, final AffinityState state) {
            if (state == AffinityState.ALL || state == AffinityState.WRITE) {
                // write = all or write
                writeIds.add(id);
            }
            otherIds.add(id);
        }

        public List<Integer> get(final AffinityState state) {
            if (state == AffinityState.WRITE) {
                return writeIds;
            }

            return otherIds;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{writeIds=");
            builder.append(writeIds);
            builder.append(", otherIds=");
            builder.append(otherIds);
            builder.append("}");
            return builder.toString();
        }
    }
}

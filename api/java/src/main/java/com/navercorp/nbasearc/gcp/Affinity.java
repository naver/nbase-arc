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

package com.navercorp.nbasearc.gcp;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.navercorp.redis.cluster.gateway.AffinityState;
import com.navercorp.redis.cluster.gateway.GatewayAffinity;

class Affinity {
    private AtomicReference<ConcurrentHashMap<Integer /*gwid*/, GatewayAffinity.AffinityInfo>> affinityInfo;

    Affinity() {
        affinityInfo = new AtomicReference<ConcurrentHashMap<Integer, GatewayAffinity.AffinityInfo>>();
    }

    void reload(ConcurrentHashMap<Integer, GatewayAffinity.AffinityInfo> affinityInfos) {
        affinityInfo.set(affinityInfos);
    }
    
    List<Integer> affinityGateway(int hash, AffinityState state) {
        if (affinityInfo.get() == null) {
            return null;
        }
        
        GatewayAffinity.AffinityInfo affinityGateways = affinityInfo.get().get(hash);
        if (affinityGateways == null) {
            return null;
        }
        
        return affinityGateways.get(state);
    }

    int affinityCost(int hash, Integer gwId, AffinityState state) {
        ConcurrentHashMap<Integer, GatewayAffinity.AffinityInfo> affinity = affinityInfo.get();
        if (affinity == null) {
            return Integer.MAX_VALUE;
        }

        GatewayAffinity.AffinityInfo affinityInfo = affinity.get(hash);
        if (affinityInfo == null) {
            return Integer.MAX_VALUE;
        }
        
        for (Integer id : affinityInfo.get(state)) {
            if (id == gwId) {
                return 0;
            }
        }
        return Integer.MAX_VALUE;
    }
}

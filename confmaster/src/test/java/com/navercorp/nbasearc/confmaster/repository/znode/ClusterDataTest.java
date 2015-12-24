/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.repository.znode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;

public class ClusterDataTest {

    public ClusterData build() {
        ClusterData d = new ClusterData();
        
        d.setKeySpaceSize(Constant.KEY_SPACE_SIZE);
        d.setPhase(Constant.CLUSTER_PHASE_INIT);
        d.setPnPgMap(buildPnPgMap(0));
        d.setQuorumPolicy(Arrays.asList(0, 1));
        
        return d;
    }
    
    public List<Integer> buildPnPgMap(int pg) {
        List<Integer> pnPgMap = new ArrayList<Integer>();
        for (int i = 0; i < Constant.KEY_SPACE_SIZE; i++) {
            pnPgMap.add(pg);
        }
        return pnPgMap;
    }
    
    @Test
    public void equals() {
        ClusterData d1 = build();
        assertEquals(d1, d1);

        ClusterData d2 = build();
        assertEquals(d1, d2);

        d2 = build();
        d2.setKeySpaceSize(8000);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setPhase(Constant.CLUSTER_PHASE_RUNNING);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setPnPgMap(buildPnPgMap(1));
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setQuorumPolicy(Arrays.asList(0, 1, 2));
        assertNotEquals(d1, d2);

        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }

}

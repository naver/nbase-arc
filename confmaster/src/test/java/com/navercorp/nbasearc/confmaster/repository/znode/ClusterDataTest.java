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

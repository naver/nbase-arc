package com.navercorp.nbasearc.confmaster.repository.znode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.repository.znode.FailureDetectorData;

public class FailureDetectorDataTest {

    @Test
    public void equals() {
        FailureDetectorData d1 = new FailureDetectorData();
        assertEquals(d1, d1);
        d1.setQuorumPolicy(Arrays.asList(0, 1));
        
        FailureDetectorData d2 = new FailureDetectorData();
        d2.setQuorumPolicy(Arrays.asList(0, 1));
        assertEquals(d1, d2);
        
        d2.setQuorumPolicy(Arrays.asList(0, 1, 2));
        assertNotEquals(d1, d2);
        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }

}

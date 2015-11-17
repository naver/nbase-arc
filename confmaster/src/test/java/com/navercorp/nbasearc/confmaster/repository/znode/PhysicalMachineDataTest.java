package com.navercorp.nbasearc.confmaster.repository.znode;

import static org.junit.Assert.*;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;

public class PhysicalMachineDataTest {

    @Test
    public void equals() {
        PhysicalMachineData d1 = new PhysicalMachineData();
        assertEquals(d1, d1);
        d1.setIp("192.168.0.10");
        
        PhysicalMachineData d2 = new PhysicalMachineData();
        d2.setIp("192.168.0.10");
        assertEquals(d1, d2);
        
        d2.setIp("192.168.0.20");
        assertNotEquals(d1, d2);
        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }

}

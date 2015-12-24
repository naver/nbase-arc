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

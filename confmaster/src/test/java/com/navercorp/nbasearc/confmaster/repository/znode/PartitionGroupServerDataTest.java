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
import static com.navercorp.nbasearc.confmaster.Constant.*;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;


public class PartitionGroupServerDataTest {

    public PartitionGroupServerData build() {
        PartitionGroupServerData d = new PartitionGroupServerData();
        d.initialize(0, "test01.pm", "192.168.0.10", 5009, 5000, 5003,
                SERVER_STATE_NORMAL, PGS_ROLE_SLAVE, 0, HB_MONITOR_YES);
        d.setStateTimestamp(1000L);
        return d;
    }
    
    @Test
    public void equals() {
        PartitionGroupServerData d1 = build();
        assertEquals(d1, d1);
        
        PartitionGroupServerData d2 = build();
        assertEquals(d1, d2);

        d2.setHb(HB_MONITOR_NO);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setMasterGen(1);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setPgId(1);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setPmIp("192.168.0.20");
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setPmName("test02.pm");
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setRedisPort(6009);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setRole(PGS_ROLE_MASTER);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setSmrBasePort(6000);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setSmrMgmtPort(6003);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setState(SERVER_STATE_FAILURE);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setStateTimestamp(2000L);
        assertNotEquals(d1, d2);

        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }

}

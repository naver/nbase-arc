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

package com.navercorp.nbasearc.confmaster.server.cluster;

import static org.junit.Assert.*;
import static com.navercorp.nbasearc.confmaster.Constant.*;

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.PartitionGroupServerData;


public class PartitionGroupServerDataTest {

    public PartitionGroupServerData build() {
    	PartitionGroupServerData d = new PartitionGroupServerData("0", "test01.pm", "192.168.0.10", 5000, 5009);
        d.state = SERVER_STATE_NORMAL;
        d.setRole(PGS_ROLE_SLAVE);
        d.hb = HB_MONITOR_YES;
        d.masterGen = 0;
        d.stateTimestamp = 1000L;
        return d;
    }
    
    @Test
    public void equals() {
        PartitionGroupServerData d1 = build();
        assertEquals(d1, d1);
        
        PartitionGroupServerData d2 = build();
        assertEquals(d1, d2);

        d2.hb = HB_MONITOR_NO;
        assertNotEquals(d1, d2);

        d2 = build();
        d2.masterGen = 1;
        assertNotEquals(d1, d2);

        d2 = build();
        d2.pgId = 1;
        assertNotEquals(d1, d2);

        d2 = build();
        d2.pmIp = "192.168.0.20";
        assertNotEquals(d1, d2);

        d2 = build();
        d2.pmName = "test02.pm";
        assertNotEquals(d1, d2);

        d2 = build();
        d2.redisPort = 6009;
        assertNotEquals(d1, d2);

        d2 = build();
        d2.setRole(PGS_ROLE_MASTER);
        assertNotEquals(d1, d2);

        d2 = build();
        d2.smrBasePort = 6000;
        assertNotEquals(d1, d2);

        d2 = build();
        d2.smrMgmtPort = 6003;
        assertNotEquals(d1, d2);

        d2 = build();
        d2.state = SERVER_STATE_FAILURE;
        assertNotEquals(d1, d2);

        d2 = build();
        d2.stateTimestamp = 2000L;
        assertNotEquals(d1, d2);

        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }
    
    @Test
    public void partitionGroupServerData() throws Exception {
        PartitionGroupServerData data = build();
        MemoryObjectMapper mapper = new MemoryObjectMapper();
        mapper.writeValueAsString(data);
    }

}

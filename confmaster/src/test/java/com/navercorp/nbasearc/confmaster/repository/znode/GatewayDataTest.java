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

import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;


public class GatewayDataTest {
    
    public GatewayData build() {
        GatewayData d = new GatewayData();
        d.initialize("test01.pm", "192.168.0.10", 10000, SERVER_STATE_NORMAL, HB_MONITOR_YES);
        return d;
    }

    @Test
    public void equals() {
        GatewayData d1 = build();
        assertEquals(d1, d1);
        
        GatewayData d2 = build();
        assertEquals(d1, d2);

        d2.setState(SERVER_STATE_FAILURE);
        assertNotEquals(d1, d2);

        d2.initialize("test01.pm", "192.168.0.10", 10000, SERVER_STATE_NORMAL, HB_MONITOR_YES);
        d2.setPmName("test02.pm");
        assertNotEquals(d1, d2);

        d2.initialize("test01.pm", "192.168.0.10", 10000, SERVER_STATE_NORMAL, HB_MONITOR_YES);
        d2.setPmName("192.168.0.20");
        assertNotEquals(d1, d2);

        d2.initialize("test01.pm", "192.168.0.10", 10000, SERVER_STATE_NORMAL, HB_MONITOR_YES);
        d2.setPort(20000);
        assertNotEquals(d1, d2);

        d2.initialize("test01.pm", "192.168.0.10", 10000, SERVER_STATE_NORMAL, HB_MONITOR_YES);
        d2.setHB(HB_MONITOR_NO);
        assertNotEquals(d1, d2);
        
        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }

}

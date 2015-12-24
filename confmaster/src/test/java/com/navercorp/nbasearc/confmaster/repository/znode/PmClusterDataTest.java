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

import org.junit.Test;

import com.navercorp.nbasearc.confmaster.repository.znode.PmClusterData;

public class PmClusterDataTest {

    @Test
    public void equals() {
        PmClusterData d1 = new PmClusterData();
        assertEquals(d1, d1);
        
        PmClusterData d2 = new PmClusterData();
        assertEquals(d1, d2);

        d1.addPgsId(0);
        d1.addPgsId(1);
        d1.addGwId(0);
        d1.addGwId(1);
        d2.addPgsId(0);
        d2.addPgsId(1);
        d2.addGwId(0);
        d2.addGwId(1);
        assertEquals(d1, d2);

        d2.addGwId(2);
        assertNotEquals(d1, d2);
        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }

}

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

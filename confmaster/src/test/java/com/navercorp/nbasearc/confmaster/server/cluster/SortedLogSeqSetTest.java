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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class SortedLogSeqSetTest extends BasicSetting {
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        LeaderState.setLeader();
        BasicSetting.beforeClass();
    }
    
    @Before
    public void before() throws Exception {
        super.before();
        MockitoAnnotations.initMocks(this);
    }
    
    @After
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void sortLogSequence() throws Exception {
        // Initialize
        createCluster();
        createPm();
        createPg();

        // Create master
        for (int i = 0; i < MAX_PGS; i++) {
            createPgs(i);
            mockPgs(i);
            joinPgs(i);
        }
        
        // Sort descending order
        SortedLogSeqSet slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX_PGS; i++) {
            LogSequence ls = new LogSequence(getPgs(i-1));
            ls.min = i * 100;
            ls.logCommit = i * 200;
            ls.max = i * 300;
            ls.beCommit = i * 200;
            slss.add(ls);
        }
        
        assertEquals(0, slss.index(getPgs(2)));
        assertEquals(1, slss.index(getPgs(1)));
        assertEquals(2, slss.index(getPgs(0)));
        
        assertLogSeq(slss.get(getPgs(0)), 100, 200, 300, 200);
        assertLogSeq(slss.get(getPgs(1)), 200, 400, 600, 400);
        assertLogSeq(slss.get(getPgs(2)), 300, 600, 900, 600);

        // Equal LogSequences
        slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX_PGS; i++) {
            LogSequence ls = new LogSequence(getPgs(0));
            ls.min = 100;
            ls.logCommit = 200;
            ls.max = 300;
            ls.beCommit = 200;
            slss.add(ls);
        }
        
        assertEquals(1, slss.size());

        // Different sequences, but equal PGS 
        slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX_PGS; i++) {
            LogSequence ls = new LogSequence(getPgs(0));
            ls.min = i * 100;
            ls.logCommit = i * 200;
            ls.max = i * 300;
            ls.beCommit = i * 200;
            slss.add(ls);
        }

        assertEquals(3, slss.size());

        // Different PGSes, but equal sequence
        slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX_PGS; i++) {
            LogSequence ls = new LogSequence(getPgs(i - 1));
            ls.min = 100;
            ls.logCommit = 200;
            ls.max = 300;
            ls.beCommit = 200;
            slss.add(ls);
        }
        
        assertEquals(3, slss.size());
    }
    
    private void assertLogSeq(LogSequence ls, long min, long commit, long max, long be) {
        assertEquals(min, ls.getMin());
        assertEquals(commit, ls.getLogCommit());
        assertEquals(max, ls.getMax());
        assertEquals(be, ls.getBeCommit());
    }

}

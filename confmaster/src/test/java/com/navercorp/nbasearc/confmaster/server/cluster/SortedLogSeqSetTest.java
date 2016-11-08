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

import static org.junit.Assert.assertEquals;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class SortedLogSeqSetTest extends BasicSetting {
    
    @Autowired
    ConfMaster confMaster;
    
    @Autowired
    ApplicationContext context;

    final ObjectMapper mapper = new ObjectMapper();
    final PartitionGroupServerData data = new PartitionGroupServerData(String.valueOf(pgName), pmName, pmData.ip, 10000, 10009);

    @BeforeClass
    public static void beforeClass() throws Exception {
        LeaderState.setLeader();
        BasicSetting.beforeClass();
    }
    
    @Before
    public void before() throws Exception {
        super.before();
        MockitoAnnotations.initMocks(this);
        confMaster.setState(ConfMaster.RUNNING);
    }
    
    @After
    public void after() throws Exception {
        super.after();
    }
    
    @Test
    public void sortLogSequence() throws Exception {
        createCluster();
        createPm();
        createPg();
        
        final int MAX = 3;
        final byte[] raw = mapper.writeValueAsBytes(data);
        PartitionGroupServer []objs = new PartitionGroupServer[MAX];
        for (int i = 0; i < MAX; i++) {
            objs[i] = new PartitionGroupServer(context, raw, clusterName, String.valueOf(i), 0);
        }
        
        // Sort descending order
        SortedLogSeqSet slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX; i++) {
            LogSequence ls = new LogSequence(objs[i-1]);
            ls.min = i * 100;
            ls.logCommit = i * 200;
            ls.max = i * 300;
            ls.beCommit = i * 200;
            slss.add(ls);
        }
        
        assertEquals(1, slss.stdCompetitionRank(objs[2]));
        assertEquals(2, slss.stdCompetitionRank(objs[1]));
        assertEquals(3, slss.stdCompetitionRank(objs[0]));
        
        assertLogSeq(slss.get(objs[0]), 100, 200, 300, 200);
        assertLogSeq(slss.get(objs[1]), 200, 400, 600, 400);
        assertLogSeq(slss.get(objs[2]), 300, 600, 900, 600);

        // Equal LogSequences
        slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX; i++) {
            LogSequence ls = new LogSequence(objs[0]);
            ls.min = 100;
            ls.logCommit = 200;
            ls.max = 300;
            ls.beCommit = 200;
            slss.add(ls);
        }
        
        assertEquals(1, slss.size());

        // Different sequences, but equal PGS 
        slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX; i++) {
            LogSequence ls = new LogSequence(objs[0]);
            ls.min = i * 100;
            ls.logCommit = i * 200;
            ls.max = i * 300;
            ls.beCommit = i * 200;
            slss.add(ls);
        }

        assertEquals(3, slss.size());

        // Different PGSes, but equal sequence
        slss = new SortedLogSeqSet();
        for (int i = 1; i <= MAX; i++) {
            LogSequence ls = new LogSequence(objs[i - 1]);
            ls.min = 100;
            ls.logCommit = 200;
            ls.max = 300;
            ls.beCommit = 200;
            slss.add(ls);
        }
        
        assertEquals(3, slss.size());
    }

    private void _rankLogSequence(int arySize, PartitionGroupServer[] objs,
            int[] seqMaxs, int[] expectedRanks) throws Exception {

        // Sort descending order
        SortedLogSeqSet slss = new SortedLogSeqSet();
        for (int i = 0; i < arySize; i++) {
            LogSequence ls = new LogSequence(objs[i]);
            ls.min = seqMaxs[i] - 10;
            ls.logCommit = seqMaxs[i];
            ls.max = seqMaxs[i];
            ls.beCommit = seqMaxs[i];
            slss.add(ls);
        }

        assertEquals(arySize, slss.size());
        for (int i = 0; i < arySize; i++) {
            assertEquals(expectedRanks[i], slss.stdCompetitionRank(objs[i]));
        }
    }
    
    @Test
    public void rankLogSequence() throws Exception {
        createCluster();
        createPm();
        createPg();
        
        final int MAX = 100;

        PartitionGroupServer []objs = new PartitionGroupServer[MAX];
        for (int i = 0; i < MAX; i++) {
            objs[i] = new PartitionGroupServer(context, clusterName, String.valueOf(i),
                    String.valueOf(i), "test01.arc", "192.168.0.1", 10000, 10009, 0);
        }
        
        _rankLogSequence(10, objs, 
                new int[]{ 10, 20, 20, 40, 50, 60, 70, 70, 70, 100 }, 
                new int[]{ 10, 8, 8, 7, 6, 5, 2, 2, 2, 1 });

        _rankLogSequence(10, objs, 
                new int[]{ 100, 70, 70, 70, 60, 50, 40, 20, 20, 10 }, 
                new int[]{ 1, 2, 2, 2, 5, 6, 7, 8, 8, 10 });

        _rankLogSequence(10, objs, 
                new int[]{ 10, 10, 70, 70, 70, 100, 100, 100, 100, 10 }, 
                new int[]{ 8, 8, 5, 5, 5, 1, 1, 1, 1, 8 });

        _rankLogSequence(10, objs, 
                new int[]{ 100, 100, 70, 70, 70, 10, 10, 10, 10, 100 }, 
                new int[]{ 1, 1, 4, 4, 4, 7, 7, 7, 7, 1 });
    }
    
    private void assertLogSeq(LogSequence ls, long min, long commit, long max, long be) {
        assertEquals(min, ls.getMin());
        assertEquals(commit, ls.getLogCommit());
        assertEquals(max, ls.getMax());
        assertEquals(be, ls.getBeCommit());
    }

}


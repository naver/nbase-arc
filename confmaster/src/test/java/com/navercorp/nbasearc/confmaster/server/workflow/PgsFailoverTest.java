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

package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.jayway.awaitility.Awaitility.await;
import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mimic.MimicSMR;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class PgsFailoverTest extends BasicSetting {
    
    @Autowired
    ConfMaster confMaster;
    
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
    public void restartRecovery() throws Exception {
        MimicSMR[] mimics = new MimicSMR[MAX_PGS];
        
        // Initialize
        createCluster();
        createPm();
        createPg();
        PartitionGroup pg = getPg();
        
        // Create master
        createPgs(0);
        mockPgs(0);
        joinPgs(0);
        PartitionGroupServer p1 = getPgs(0);
        mimics[0] = mimic(p1);
        mimics[0].init();
        mockHeartbeatResult(p1, PGS_ROLE_LCONN, mimics[0].execute(PGS_PING));

        MasterFinder masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        assertEquals(GREEN, p1.getData().getColor());
        assertEquals(PGS_ROLE_MASTER, p1.getData().getRole());
        assertEquals(SERVER_STATE_NORMAL, p1.getData().getState());
        assertEquals(HB_MONITOR_YES, p1.getData().getHb());
        assertEquals(1, p1.getData().getMasterGen());
        assertEquals(0, pg.getData().currentGen());
        QuorumValidator quorumValidator = new QuorumValidator(mimics[0], 0); 
        await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);
        
        // Create slave
        createPgs(1);
        mockPgs(1);
        joinPgs(1);
        PartitionGroupServer p2 = getPgs(1);
        mimics[1] = mimic(p2);
        mimics[1].init();
        mockHeartbeatResult(p2, PGS_ROLE_LCONN, mimics[1].execute(PGS_PING));

        SlaveFinder slaveCond = new SlaveFinder(getPgsList());
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveCond);
        assertTrue(slaveCond.isSlave(p2));
        assertEquals(GREEN, p2.getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, p2.getData().getRole());
        assertEquals(SERVER_STATE_NORMAL, p2.getData().getState());
        assertEquals(HB_MONITOR_YES, p2.getData().getHb());
        assertEquals(1, p2.getData().getMasterGen());
        assertEquals(0, pg.getData().currentGen());
        quorumValidator = new QuorumValidator(mimics[0], 1); 
        await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);
        
        // Master must not be changed
        assertEquals(p1, masterCond.getMaster());
        assertEquals(GREEN, p1.getData().getColor());
        assertEquals(PGS_ROLE_MASTER, p1.getData().getRole());
        assertEquals(SERVER_STATE_NORMAL, p1.getData().getState());
        assertEquals(HB_MONITOR_YES, p1.getData().getHb());
        assertEquals(1, p1.getData().getMasterGen());
        assertEquals(0, pg.getData().currentGen());
        
        // Mimic p1 failure
        mimics[0].execute("role none");
        mimics[1].execute("role lconn");
        mockHeartbeatResult(p1, PGS_ROLE_NONE, mimics[0].execute(PGS_PING));
        
        await("reconfiguration for master.").atMost(assertionTimeout, SECONDS)
                .until(new RoleColorValidator(p2, PGS_ROLE_MASTER, GREEN));
        assertEquals(GREEN, p2.getData().getColor());
        assertEquals(PGS_ROLE_MASTER, p2.getData().getRole());
        assertEquals(SERVER_STATE_NORMAL, p2.getData().getState());
        assertEquals(HB_MONITOR_YES, p2.getData().getHb());
        assertEquals(2, p2.getData().getMasterGen());
        assertEquals(1, pg.getData().currentGen());
        quorumValidator = new QuorumValidator(mimics[1], 0); 
        await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);

        assertEquals(p1, masterCond.getMaster());
        assertEquals(RED, p1.getData().getColor());
        assertEquals(PGS_ROLE_NONE, p1.getData().getRole());
        assertEquals(SERVER_STATE_FAILURE, p1.getData().getState());
        assertEquals(HB_MONITOR_YES, p1.getData().getHb());
        assertEquals(1, p1.getData().getMasterGen());
        
        // Recover p1 
        mimics[0].init();
        mockHeartbeatResult(p1, PGS_ROLE_LCONN, mimics[0].execute(PGS_PING));
        
        await("reconfiguration for slave.").atMost(assertionTimeout, SECONDS)
                .until(new RoleColorValidator(p1, PGS_ROLE_SLAVE, GREEN));
        assertEquals(GREEN, p1.getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, p1.getData().getRole());
        assertEquals(SERVER_STATE_NORMAL, p1.getData().getState());
        assertEquals(HB_MONITOR_YES, p1.getData().getHb());
        assertEquals(2, p1.getData().getMasterGen());
        assertEquals(1, pg.getData().currentGen());
        
        assertEquals(GREEN, p2.getData().getColor());
        assertEquals(PGS_ROLE_MASTER, p2.getData().getRole());
        assertEquals(SERVER_STATE_NORMAL, p2.getData().getState());
        assertEquals(HB_MONITOR_YES, p2.getData().getHb());
        assertEquals(2, p2.getData().getMasterGen());
        assertEquals(1, pg.getData().currentGen());
        quorumValidator = new QuorumValidator(mimics[1], 1); 
        await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);
    }
    
    public class TransitState {
        private final int c;
        private final int q;
        
        public TransitState(int c, int q) {
            this.c = c;
            this.q = q;
        }
        
        @Override
        public String toString() {
            return "TransitState(c: " + getC() + ", q:" + getQ() + ")";
        }

        public int getC() {
            return c;
        }

        public int getQ() {
            return q;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (o == this) {
                return true;
            }
            if (!(o instanceof TransitState)) {
                return false;
            }

            TransitState rhs = (TransitState) o;
            if (c != rhs.c) {
                return false;
            }
            if (q != rhs.q) {
                return false;
            }
            return true;
        }
        
        @Override
        public int hashCode() {
            return c << 16 + q;
        }
    }
    
    public List<TransitState> getValidTransitStates(int copy, int c, int q) {
        List<TransitState> transitStates = new ArrayList<TransitState>();
        
        for (int c1 = 1; c1 <= copy; c1++) {
            for (int q1 = 0; q1 < copy; q1++) {
                if (c1 >= c - q && ((c1 > 0 && c1 <= copy) && (q1 >= 0 && q1 < copy) && (q1 < c1))) {
                    transitStates.add(new TransitState(c1, q1));
                }
            }
        }
        
        return transitStates;
    }
    
    public void reset(int c, int q) {
        getPg().getData().setCopy(c);
        getPg().getData().setQuorum(q);
    }
    
    public void transit(int COPY, int c, int q, boolean isok) {
        TransitState asIs = new TransitState(
                getPg().getData().getCopy(), getPg().getData().getQuorum());
        TransitState toBe = new TransitState(c, q);

        if (isok) {
            assertTrue("Couldn't allow. COPY: " + COPY + ", " + asIs + " -> " + toBe, 
                    checkCopyQuorum(getPg().getData().getCopy(), getPg()
                            .getData().getQuorum(), toBe.getC()));
            getPg().getData().setCopy(toBe.getC());
            getPg().getData().setQuorum(toBe.getQ());
            System.out.println("allow transit " + asIs + " -> " + toBe);
        } else {
            assertFalse("Couldn't block. COPY: " + COPY + ", " + asIs + " -> " + toBe, 
                    checkCopyQuorum(getPg().getData().getCopy(), getPg()
                            .getData().getQuorum(), toBe.getC()));
            System.out.println("block transit " + asIs + " -> " + toBe);
        }
        
    }

    /*
     * @return Returns true if successful or false otherwise.
     */
    public boolean checkCopyQuorum(final int copy, final int quorum, final int alive) {
        final boolean success = alive >= (copy - quorum);
        if (success) {
            Logger.info(
                    "Check copy-quorum success. available: {}, pg(copy: {}, quorum: {})",
                    new Object[] { alive, copy, quorum });
        } else {
            Logger.error(
                    "Check copy-quorum fail. available: {}, pg(copy: {}, quorum: {})",
                    new Object[] { alive, copy, quorum });
        }

        return success;
    }
    
    @Test
    public void checkCopyQuorumSuccess() throws Exception {
        // Initialize
        createCluster();
        createPm();
        createPg();
        for (int id = 0; id < MAX_PGS; id++) {
            createPgs(id);
            mockPgs(id);
            joinPgs(id);
        }
        
        final int COPY = MAX_PGS;
        assertTrue(checkCopyQuorum(getPg().getData().getCopy(), getPg()
                .getData().getQuorum(), COPY));
        
        for (int c = 1; c <= COPY; c++) {
            for (int q = 0; q <= c - 1; q++) {
                List<TransitState> validStates = getValidTransitStates(COPY, c, q);
                for (TransitState state : validStates) {
                    // Transit -> (C, Q)
                    reset(c, q);

                    // Transit -> (C1, Q1)
                    transit(COPY, state.getC(), state.getQ(), true);

                    if (c < state.getC()) {
                        // Transit -> (C1, C1 - C)
                        transit(COPY, state.getC(), state.getC() - c, true);
                    }

                    // Transit -> (C, Q)
                    transit(COPY, c, q, true);
                }
            }
        }
    }
    
    @Test
    public void checkCopyFail() throws Exception {
        // Initialize
        createCluster();
        createPm();
        createPg();
        for (int id = 0; id < MAX_PGS; id++) {
            createPgs(id);
            mockPgs(id);
            joinPgs(id);
        }
        
        final int COPY = MAX_PGS;
        assertTrue(checkCopyQuorum(getPg().getData().getCopy(),
                getPg().getData().getQuorum(), COPY));
        
        for (int c = 1; c <= COPY; c++) {
            for (int q = 0; q <= c - 1; q++) {
                Set<TransitState> validStates = 
                    new HashSet<TransitState>(getValidTransitStates(COPY, c, q));
                
                for (int c1 = 1; c1 <= c; c1++) {
                    for (int q1 = 0; q1 < c - 1; q1++) {
                        TransitState state = new TransitState(c1, q1);
                        if (!validStates.contains(state) && state.getQ() < state.getC()) {
                            reset(c, q);
                            transit(COPY, c1, q1, false);
                        }
                    }
                }
            }
        }
    }
    
}

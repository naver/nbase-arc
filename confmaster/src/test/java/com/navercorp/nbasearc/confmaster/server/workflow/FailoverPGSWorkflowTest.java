package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.jayway.awaitility.Awaitility.await;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_MASTER;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_NONE;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_LCONN;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_NORMAL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class FailoverPGSWorkflowTest extends BasicSetting {

    final int MAX_PGS = 3;
    
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
    public void validMasterGen() throws Exception {
        createPm();
        createCluster();
        createPg();
        createPgs(0);
        
        SortedMap<Integer, Long> masterGenMap = new TreeMap<Integer, Long>();
        PartitionGroupServer pgs = getPgs(0);
        PartitionGroup pg = getPg();
        
        pgs.setData((PartitionGroupServerData)(pgsDatas[0].clone()));
        pg.setData(pgData);
        
        /*
         * Success scenario : normal master failover
          MGEN 3        4
          PGS0 M ----> (S)
          PGS1 S ---->  M
         */
        PartitionGroupServerData pgsModified = 
            PartitionGroupServerData.builder().from(pgs.getData())
                .withMasterGen(4).build();
        pgs.setData(pgsModified);
        
        masterGenMap.put(0, 0L);
        masterGenMap.put(1, 0L);
        masterGenMap.put(2, 50L);
        masterGenMap.put(3, 100L);
        
        PartitionGroupData pgModifed = 
            PartitionGroupData.builder().from(pg.getData())
                .withMasterGenMap(masterGenMap).build();
        pg.setData(pgModifed);
        
        LogSequence logSeq = new LogSequence();
        Field logCommit = LogSequence.class.getDeclaredField("logCommit");
        logCommit.setAccessible(true);
        logCommit.set(logSeq, 100L);
        assertTrue(pg.checkJoinConstraintSlave(pgs, logSeq, 0L, workflowLogDao).isSuccess());
        
        /*
         * Success scenario : PGS's master generation number is less than PG's
          MGEN 2       3         4         5
          PGS0 M ----> X - - - - - - - - - - - - > O (S)
          PGS1 S ----> M ----> X S ----> O M ---->    M
          PGS2 S ----> S ---->   M ----> X S ----> O  S
         */
        pgsModified = 
            PartitionGroupServerData.builder().from(pgs.getData())
                .withMasterGen(3).build();
        pgs.setData(pgsModified);
        
        masterGenMap.clear();
        masterGenMap.put(0, 0L);
        masterGenMap.put(1, 0L);
        masterGenMap.put(2, 50L);
        masterGenMap.put(3, 100L);
        masterGenMap.put(4, 200L);
        
        pgModifed = 
            PartitionGroupData.builder().from(pg.getData())
                .withMasterGenMap(masterGenMap).build();
            pg.setData(pgModifed);

        assertTrue(pg.checkJoinConstraintSlave(pgs, logSeq, 0L, workflowLogDao).isSuccess());
        
        /*
         * Failure scenario : PGS's commit sequence is greater than PG's
          MGEN 2                                3
          PGS0 M   -----------------------> X - - -> O (F)
          PGS1 S X - - - - - - - - - - - - -> O M ------->
          PGS2 S X - - - - - - - - - - - - -> O S ------->
         */
        pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withMasterGen(2).build();
            pgs.setData(pgsModified);
            
        masterGenMap.clear();
        masterGenMap.put(0, 0L);
        masterGenMap.put(1, 0L);
        masterGenMap.put(2, 50L);

        pgModifed = 
            PartitionGroupData.builder().from(pg.getData())
                .withMasterGenMap(masterGenMap).build();
        pg.setData(pgModifed);
            
        logCommit.set(logSeq, 70L);
        assertFalse(pg.checkJoinConstraintSlave(pgs, logSeq, 0L, workflowLogDao).isSuccess());
        
        /*
         * Failure scenario : PGS's master generation number is greater than PG's
         */
        pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withMasterGen(4).build();
            pgs.setData(pgsModified);
        
        masterGenMap.clear();
        masterGenMap.put(0, 0L);
        masterGenMap.put(1, 0L);
        masterGenMap.put(2, 50L);

        pgModifed = 
            PartitionGroupData.builder().from(pg.getData())
                .withMasterGenMap(masterGenMap).build();
        pg.setData(pgModifed);
        
        assertFalse(pg.checkJoinConstraintSlave(pgs, logSeq, 0L, workflowLogDao).isSuccess());
    }
    
    @Test
    public void masterElection() throws Exception {
        // Initialize
        createCluster();
        createPm();
        createPg();
        for (int id = 0; id < MAX_PGS; id++) {
            createPgs(id);
            mockPgs(id);
        }

        for (int id = 0; id < MAX_PGS; id++) {
            mockPgsLconn(getPgs(id));
        }
        
        PartitionGroupServer master = waitRoleMaster(getPgsList());
        for (int id = 0; id < MAX_PGS; id++) {
            if (getPgs(id).equals(master)) {
                continue;
            }
            waitRoleSlave(getPgs(id));
        }
        
        // Check master
        assertEquals("Check the role of the master.", 
                PGS_ROLE_MASTER, master.getData().getRole());
        assertEquals("Check the state of master PGS",
                SERVER_STATE_NORMAL, master.getState());

        // Check slave
        for (int id = 0; id < MAX_PGS; id++) {
            if (getPgs(id).equals(master)) {
                continue;
            }
            assertEquals("Check role of the slave.",
                    PGS_ROLE_SLAVE, getPgs(id).getData().getRole());
            assertEquals("Check the state of a slave.", 
                    SERVER_STATE_NORMAL, getPgs(id).getState());
        }
        
        // Set quorum
        getPg().getData().setQuorum(1);
        
        // Failover
        testRoleNone(master);
        
        // Check states and roles
        for (int id = 0; id < MAX_PGS; id++) {
            if (getPgs(id).equals(master)) {
                continue;
            }
            await("After master crashes, failover with a slave.").atMost(
                    5, SECONDS).until(new StateChecker(getPgs(id), SERVER_STATE_NORMAL));
        }
        assertEquals("Check the role of the crashed PGS.", 
                PGS_ROLE_NONE, master.getData().getRole());
        assertEquals("CHeck ther state of the crashed PGS."
                , SERVER_STATE_FAILURE, master.getState());
        assertEquals("Check that there must be a master."
                , 1, count(PGS_ROLE_MASTER));
        assertEquals("Check that another PGS must be a slave."
                , 1, count(PGS_ROLE_SLAVE));
    }

    @Test
    public void masterElectionCopyQuorumFail() throws Exception {
        // Initialize
        createCluster();
        createPm();
        createPg();
        for (int id = 0; id < MAX_PGS; id++) {
            createPgs(id);
            mockPgs(id);
        }

        for (int id = 0; id < MAX_PGS; id++) {
            mockPgsLconn(getPgs(id));
        }
        
        PartitionGroupServer master = waitRoleMaster(getPgsList());
        for (int id = 0; id < MAX_PGS; id++) {
            if (getPgs(id).equals(master)) {
                continue;
            }
            waitRoleSlave(getPgs(id));
        }
        
        // Check roles
        assertEquals("Check the role of the master.", 
                PGS_ROLE_MASTER, master.getData().getRole());
        assertEquals("Check the state of master PGS",
                SERVER_STATE_NORMAL, master.getState());

        for (int id = 0; id < MAX_PGS; id++) {
            if (getPgs(id).equals(master)) {
                continue;
            }
            assertEquals("Check role of the slave.",
                    PGS_ROLE_SLAVE, getPgs(id).getData().getRole());
            assertEquals("Check the state of a slave.", 
                    SERVER_STATE_NORMAL, getPgs(id).getState());
        }
        
        // Failover
        testRoleNone(master);
        Thread.sleep(5000);

        for (int id = 0; id < MAX_PGS; id++) {
            if (getPgs(id).equals(master)) {
                continue;
            }
            assertEquals(SERVER_STATE_LCONN, getPgs(id).getState());
        }
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
                    getPg().checkCopyQuorum(toBe.getC()));
            getPg().getData().setCopy(toBe.getC());
            getPg().getData().setQuorum(toBe.getQ());
            System.out.println("allow transit " + asIs + " -> " + toBe);
        } else {
            assertFalse("Couldn't block. COPY: " + COPY + ", " + asIs + " -> " + toBe, 
                    getPg().checkCopyQuorum(toBe.getC()));
            System.out.println("block transit " + asIs + " -> " + toBe);
        }
        
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
        }
        
        final int COPY = MAX_PGS;
        assertTrue(getPg().checkCopyQuorum(COPY));
        
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
        }
        
        final int COPY = MAX_PGS;
        assertTrue(getPg().checkCopyQuorum(COPY));
        
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
    
    private int count(String role) {
        int count = 0;
        for (int id = 0; id < MAX_PGS; id++) {
            if (getPgs(id).getData().getRole().equals(role)) {
                count++;
            }
        }
        return count;
    }
    
    public static class MasterChecker implements Callable<Boolean> {
        private final List<PartitionGroupServer> pgsList;
        private PartitionGroupServer master;
        
        public MasterChecker(List<PartitionGroupServer> pgsList) {
            this.pgsList = pgsList;
        }
        
        public Boolean call() throws Exception {
            for (PartitionGroupServer pgs : pgsList) {
                if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
                    setMaster(pgs);
                    return true;
                }
            }
            return false;
        }

        public PartitionGroupServer getMaster() {
            return master;
        }

        private void setMaster(PartitionGroupServer master) {
            this.master = master;
        }
    }

    public static class RoleChecker implements Callable<Boolean> {
        private final PartitionGroupServer pgs;
        private final String role;
        
        public RoleChecker(PartitionGroupServer pgs, String role) {
            this.pgs = pgs;
            this.role = role;
        }
        
        public Boolean call() throws Exception {
            return pgs.getData().getRole().equals(role);
        }
    }
    
    class StateChecker implements Callable<Boolean> {
        private final PartitionGroupServer pgs;
        private final String state;
        
        public StateChecker(PartitionGroupServer pgs, String state) {
            this.pgs = pgs;
            this.state = state;
        }
        
        public Boolean call() throws Exception {
            System.out.println("m(" + pgs.getData().getState() + "), e(" + state + ")");
            return pgs.getData().getState().equals(state);
        }
    }
}

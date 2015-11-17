package com.navercorp.nbasearc.confmaster.server.cluster;

import static org.junit.Assert.*;

import java.lang.reflect.Field;

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
import com.navercorp.nbasearc.confmaster.repository.znode.PgDataBuilder;
import com.navercorp.nbasearc.confmaster.repository.znode.PgsDataBuilder;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup.SlaveJoinInfo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class PartitionGroupTest extends BasicSetting {

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
    public void checkJoinConstraintMaster() throws Exception {
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
        
        LogSequence masterLogSeq = new LogSequence();
        Field logCommit = LogSequence.class.getDeclaredField("logCommit");
        logCommit.setAccessible(true);

        // Master has the same commit sequence of current-mgen-sequence
        logCommit.set(masterLogSeq, getPg().getData().currentSeq());
        assertTrue(
            "Check join constraint for master fail. (Master has the same commit sequence)",
            getPg().checkJoinConstraintMaster(master, masterLogSeq, 1L, workflowLogDao));

        // Master has the larger commit sequence of current-mgen-sequence        
        logCommit.set(masterLogSeq, getPg().getData().currentSeq() + 1L);
        assertTrue(
            "Check join constraint for master fail. (Master has larger commit sequence)",
            getPg().checkJoinConstraintMaster(master, masterLogSeq, 1L, workflowLogDao));

        // Master has the less commit sequence of current-mgen-sequence
        logCommit.set(masterLogSeq, getPg().getData().currentSeq() - 1L);
        assertFalse(
            "Check join constraint for master fail. (Master has larger commit sequence)",
            getPg().checkJoinConstraintMaster(master, masterLogSeq, 1L, workflowLogDao));
    }

    @Test
    public void checkJoinConstraintSlave() throws Exception {
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

        // Add master generation
        getPg().setData(new PgDataBuilder().from(getPg().getData())
                .addMasterGen(1000).addMasterGen(2000).addMasterGen(3000).build());
        /*
         *  Master generation number of pg is 4
         *  pgs-mgen | pg-mgen-map | seq
         *      1    |      0      |  0
         *      2    |      1      | 1000
         *      3    |      2      | 2000
         *      4    |      3      | 3000
         */
        System.out.println(getPg().getData().getMasterGenMap());
        assertEquals(4, getPg().getData().currentGen());
        assertEquals(new Long(3000), getPg().getData().currentSeq());

        PartitionGroupServer slave = getPgs(1);
        
        // Slave log sequence
        LogSequence slaveLogSeq = new LogSequence();
        Field logCommit = LogSequence.class.getDeclaredField("logCommit");
        logCommit.setAccessible(true);

        // Slave max sequence
        Field logMax = LogSequence.class.getDeclaredField("max");
        logMax.setAccessible(true);

        // PGS.MGEN < PG.MGEN
        slave.setData(new PgsDataBuilder().from(slave.getData())
                .withMasterGen(getPg().getData().currentGen()-1).build());
        {
            // && PGS.LOG(commit) < PG.HIST[PGS.MGEN + 1]
            logCommit.set(slaveLogSeq, 2500);
            logMax.set(slaveLogSeq, 2700);
            SlaveJoinInfo joinInfo = getPg().checkJoinConstraintSlave(
                    slave, slaveLogSeq, 1L, workflowLogDao);
            assertTrue(joinInfo.isSuccess());
            assertEquals(joinInfo.getJoinSeq(), logMax.get(slaveLogSeq));
            
            // && PGS.LOG(commit) == PG.HIST[PGS.MGEN + 1]
            logCommit.set(slaveLogSeq, 3000);
            logMax.set(slaveLogSeq, 3000);
            joinInfo = getPg().checkJoinConstraintSlave(
                    slave, slaveLogSeq, 1L, workflowLogDao);
            assertTrue(joinInfo.isSuccess());
            assertEquals(joinInfo.getJoinSeq(), logMax.get(slaveLogSeq));
            
            // && PGS.LOG(commit) > PG.HIST[PGS.MGEN + 1]
            logCommit.set(slaveLogSeq, 3500);
            logMax.set(slaveLogSeq, 3500);
            joinInfo = getPg().checkJoinConstraintSlave(
                    slave, slaveLogSeq, 1L, workflowLogDao);
            assertFalse(joinInfo.isSuccess());
        }
        
        // PGS.MGEN == PG.MGEN
        slave.setData(new PgsDataBuilder().from(slave.getData())
                .withMasterGen(getPg().getData().currentGen()).build());
        {
            // && PGS.LOG(commit) < PG.HIST[PGS.MGEN + 1]
            logCommit.set(slaveLogSeq, 2500);
            logMax.set(slaveLogSeq, 2700);
            SlaveJoinInfo joinInfo = getPg().checkJoinConstraintSlave(
                    slave, slaveLogSeq, 1L, workflowLogDao);
            assertTrue(joinInfo.isSuccess());
            assertEquals(joinInfo.getJoinSeq(), logMax.get(slaveLogSeq));
            
            // && PGS.LOG(commit) == PG.HIST[PGS.MGEN + 1]
            logCommit.set(slaveLogSeq, 3000);
            logMax.set(slaveLogSeq, 3000);
            joinInfo = getPg().checkJoinConstraintSlave(
                    slave, slaveLogSeq, 1L, workflowLogDao);
            assertTrue(joinInfo.isSuccess());
            assertEquals(joinInfo.getJoinSeq(), logMax.get(slaveLogSeq));
            
            // && PGS.LOG(commit) > PG.HIST[PGS.MGEN + 1]
            logCommit.set(slaveLogSeq, 3500);
            logMax.set(slaveLogSeq, 3500);
            joinInfo = getPg().checkJoinConstraintSlave(
                    slave, slaveLogSeq, 1L, workflowLogDao);
            assertTrue(joinInfo.isSuccess());
            assertEquals(joinInfo.getJoinSeq(), logMax.get(slaveLogSeq));
        }
        
        // PGS.MGEN > PG.MGEN
        slave.setData(new PgsDataBuilder().from(slave.getData())
                .withMasterGen(getPg().getData().currentGen()+1).build());
        logCommit.set(slaveLogSeq, 2500);
        logMax.set(slaveLogSeq, 2700);
        SlaveJoinInfo joinInfo = getPg().checkJoinConstraintSlave(
                slave, slaveLogSeq, 1L, workflowLogDao);
        assertFalse(joinInfo.isSuccess());
    }

}

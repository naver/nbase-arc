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
import static org.junit.Assert.*;

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
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.mimic.MimicPGS;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class RoleAdjustmentWorkflowTest extends BasicSetting {

    MimicPGS mimics[] = new MimicPGS[MAX_PGS];
    
    @Autowired
    ConfMaster confMaster;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }
    
    @Before
    public void before() throws Exception {
        super.before();
        confMaster.setState(ConfMaster.RUNNING);
        
        MockitoAnnotations.initMocks(this);
        
        mimics[0] = new MimicPGS(8103, 8109);
        mimics[1] = new MimicPGS(9103, 9109);
        mimics[2] = new MimicPGS(10103, 10109);
        
        mimics[0].start();
        mimics[1].start();
        mimics[2].start();
    }
    
    @After
    public void after() throws Exception {
        // Clear cluster
        Cluster cluster = getCluster();
        for (PartitionGroupServer pgs : container.getPgsList(cluster.getName())) {
            deletePgs(Integer.valueOf(pgs.getName()),
                    container.getPgsList(cluster.getName()).size() == 1);
        }

        if (!container.getPgList(cluster.getName()).isEmpty()) {
            deletePg();
        }

        if (!container.getGwList(cluster.getName()).isEmpty()) {
            deleteGw();
        }
        
        mimics[0].stop();
        mimics[1].stop();
        mimics[2].stop();
        
        super.after();
    }
    
    public void runWorkflows() throws Exception {        
        QuorumAdjustmentWorkflow qa = new QuorumAdjustmentWorkflow(getPg(),
                false, context);
        qa.execute();

        MasterElectionWorkflow me = new MasterElectionWorkflow(getPg(), null,
                false, context);
        me.execute();

        YellowJoinWorkflow yj = new YellowJoinWorkflow(getPg(), false, context);
        yj.execute();

        BlueJoinWorkflow bj = new BlueJoinWorkflow(getPg(), false, context);
        bj.execute();

        MembershipGrantWorkflow mg = new MembershipGrantWorkflow(getPg(),
                false, context);
        mg.execute();
    }
    
    @Test
    public void roleAdjustment() throws Exception {        
        // Initialize
        createCluster();
        createPm();
        createPg();
        PartitionGroup pg = getPg();
        
        // Create master
        // {} -> M
        createPgs(0);
        doCommand("pgs_join " + getPgs(0).getClusterName() + " " + getPgs(0).getName());
        PartitionGroupServer p1 = getPgs(0);

        RoleAdjustmentWorkflow ra = new RoleAdjustmentWorkflow(getPg(), false,
                context);
        ra.execute();
        assertEquals(PGS_ROLE_NONE, p1.getRole());
        assertEquals(BLUE, p1.getColor());
        
        runWorkflows();
        
        MasterFinder masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        
        // Create slave for 2copy
        // M -> M S
        createPgs(1);
        doCommand("pgs_join " + getPgs(1).getClusterName() + " " + getPgs(1).getName());
        PartitionGroupServer p2 = getPgs(1);
        
        PartitionGroupServerData d2 = p2.clonePersistentData();
        d2.setRole(PGS_ROLE_LCONN);
        p2.setPersistentData(d2);
        ra = new RoleAdjustmentWorkflow(getPg(), false, context);
        ra.execute();
        assertEquals(PGS_ROLE_LCONN, p2.getRole());
        assertEquals(YELLOW, p2.getColor());
        assertEquals(PGS_ROLE_MASTER, p1.getRole());
        assertEquals(GREEN, p1.getColor());
        
        runWorkflows();
        
        SlaveFinder slaveFinder = new SlaveFinder(getPgsList());
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveFinder);
        assertTrue(slaveFinder.isSlave(p2));
        validateNormal(p2, pg, PGS_ROLE_SLAVE, 0);
        
        // Master must not be changed
        // M S
        masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);

        // Create slave for 3copy
        // M S -> M S S
        createPgs(2);
        doCommand("pgs_join " + getPgs(2).getClusterName() + " " + getPgs(2).getName());
        PartitionGroupServer p3 = getPgs(2);
        
        PartitionGroupServerData d3 = p3.clonePersistentData();
        d3.setRole(PGS_ROLE_LCONN);
        p3.setPersistentData(d3);        		
        ra = new RoleAdjustmentWorkflow(getPg(), false, context);
        ra.execute();
        assertEquals(PGS_ROLE_LCONN, p3.getRole());
        assertEquals(YELLOW, p3.getColor());
        assertEquals(PGS_ROLE_SLAVE, p2.getRole());
        assertEquals(GREEN, p2.getColor());
        assertEquals(PGS_ROLE_MASTER, p1.getRole());
        assertEquals(GREEN, p1.getColor());
        
        runWorkflows();
        
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(
                new RoleColorValidator(p3, PGS_ROLE_SLAVE, GREEN));
        validateNormal(p3, pg, PGS_ROLE_SLAVE, 0);
        
        // Master must not be changed
        masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        
        // Another slave must not be changed
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(
                new RoleColorValidator(p2, PGS_ROLE_SLAVE, GREEN));
        validateNormal(p2, pg, PGS_ROLE_SLAVE, 0);
        
        // Slave2 fail and RA
        mimics[2].mSmr.execute("role none");
        d3 = p3.clonePersistentData();
        d3.setRole(PGS_ROLE_NONE);
        p3.setPersistentData(d3);
        
        ra = new RoleAdjustmentWorkflow(getPg(), false, context);
        ra.execute();

        // RA/toRed make master lconn.
        assertEquals(BLUE, p1.getColor());
        assertEquals(PGS_ROLE_LCONN, p1.getRole());
        assertEquals(SERVER_STATE_LCONN, p1.getState());
        assertEquals(HB_MONITOR_YES, p1.getHeartbeat());
        assertEquals(1, p1.getMasterGen());
        assertEquals(0, pg.currentGen());
    }
    
}

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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static com.jayway.awaitility.Awaitility.await;
import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;

import java.util.HashMap;
import java.util.Map;

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
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mimic.MimicSMR;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class Workflow201Test extends BasicSetting {

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
        confMaster.setState(ConfMaster.RUNNING);
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @Autowired
    ZooKeeperHolder zookeeper;

    public String getConfiguration() {
        StringBuilder sb = new StringBuilder();
        for (PartitionGroupServer pgs : getPgsList()) {
            sb.append(pgs).append("\n").append(pgs.getData()).append("\n");
        }

        return sb.toString();
    }
    
    public boolean checkConfiguration() {
        int masterCnt = 0;
        Map<Color, Integer> colorMap = new HashMap<Color, Integer>();
        int quorum = -1;
        
        for (Color c : Color.values()) {
            colorMap.put(c,  0);
        }

        for (PartitionGroupServer pgs : getPgsList()) {
            String r = pgs.getData().getRole();
            Color c = pgs.getData().getColor();
            
            if (r.equals(PGS_ROLE_MASTER)) {
                if (c != GREEN) {
                    return false;
                }
                masterCnt++;
            } else if (r.equals(PGS_ROLE_SLAVE)) {
                if (c != YELLOW && c != BLUE && c == GREEN) {
                    return false;
                }
            }
            
            colorMap.put(c, colorMap.get(c) + 1);
        }
        
        quorum = getPg().getData().getQuorum();
        
        if (masterCnt != 1) {
            return false;
        }
        if (quorum != colorMap.get(BLUE)) {
            return false;
        }
        
        return true;
    }

    @Test
    public void initialReconfiguration() throws Exception {
        MimicSMR[] mimics = new MimicSMR[MAX_PGS];
        
        createPm();
        createCluster();

        // Add PG
        createPg();

        // Check initial value of PG
        assertEquals(-1, getPg().getData().currentGen());
        assertEquals(Integer.valueOf(0), getPg().getData().getCopy());
        assertEquals(Integer.valueOf(0), getPg().getData().getQuorum());
        assertEquals(0, getPg().getData().getPgsIdList().size());

        // Add first PGS
        createPgs(0);

        // SE
        StartOfTheEpochWorkflow se = new StartOfTheEpochWorkflow(getPg(), getPgs(0), context);
        se.execute();

        // ME
        PartitionGroupServer master = getPgs(0);
        mimics[0] = mimic(master);
        mimics[0].init();
        
        MasterElectionWorkflow me = new MasterElectionWorkflow(getPg(), null,
                false, context);
        me.execute();
        System.out.println("ME\n" + getConfiguration());

        // Check Master
        assertEquals(PGS_ROLE_MASTER, master.getData().getRole());
        assertEquals(GREEN, master.getData().getColor());
        assertEquals(0, getPg().getData().currentGen());

        // Add Slave
        for (int slaveIdx = 1; slaveIdx < 3; slaveIdx++) {
            createPgs(slaveIdx);
            mimics[slaveIdx] = mimic(getPgs(slaveIdx));
            mimics[slaveIdx].init();

            // IC
            IncreaseCopyWorkflow ic = new IncreaseCopyWorkflow(getPgs(slaveIdx), getPg(), context);
            ic.execute();

            // to LCONN
            PartitionGroupServer slave = getPgs(slaveIdx);
            PartitionGroupServerData slave1M = PartitionGroupServerData
                    .builder().from(slave.getData()).withRole(PGS_ROLE_LCONN)
                    .build();
            slave.setData(slave1M);

            // RA
            RoleAdjustmentWorkflow ra = new RoleAdjustmentWorkflow(getPg(),
                    false, context);
            ra.execute();
            System.out.println("RA\n" + getConfiguration());

            // Check lconn
            assertEquals(PGS_ROLE_LCONN, slave.getData().getRole());
            assertEquals(YELLOW, slave.getData().getColor());
        }
        
        // YJ
        YellowJoinWorkflow yj = new YellowJoinWorkflow(getPg(), false, context);
        yj.execute();
        System.out.println("YJ\n" + getConfiguration());

        for (int slaveIdx = 1; slaveIdx < 3; slaveIdx++) {
            PartitionGroupServer slave = getPgs(slaveIdx);

            // Check whether pgs became a slave
            assertEquals(PGS_ROLE_SLAVE, slave.getData().getRole());
            assertEquals(YELLOW, slave.getData().getColor());
            assertEquals(getPg().getData().currentGen() + 1, slave.getData()
                    .getMasterGen());
        }

        // MG
        MembershipGrantWorkflow mg = new MembershipGrantWorkflow(getPg(),
                false, context);
        mg.execute();
        System.out.println("MG\n" + getConfiguration());

        // Check quorum of master
        QuorumValidator quorumValidator = new QuorumValidator(mimics[0], 2);
        await("test for quorum.").atMost(assertionTimeout, SECONDS).until(
                quorumValidator);

        // Kill master
        BlockingSocketImpl msockDead = mock(BlockingSocketImpl.class);
        master.setServerConnection(msockDead);
        when(msockDead.execute(anyString())).thenReturn("");
        
        PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                .from(master.getData()).withRole(PGS_ROLE_NONE).build();
        master.setData(pgsM);

        // RA
        final int mgen = getPg().getData().currentGen();
        RoleAdjustmentWorkflow ra = new RoleAdjustmentWorkflow(getPg(), false,
                context);
        ra.execute();

        for (PartitionGroupServer pgs : getPgsList()) {
            if (!pgs.getData().getRole().equals(PGS_ROLE_SLAVE)) {
                continue;
            }

            pgsM = PartitionGroupServerData.builder().from(pgs.getData())
                    .withRole(PGS_ROLE_LCONN).build();
            pgs.setData(pgsM);
            ra = new RoleAdjustmentWorkflow(getPg(), false, context);
            ra.execute();

            assertEquals(PGS_ROLE_LCONN, pgs.getData().getRole());
            assertEquals(BLUE, pgs.getData().getColor());
        }

        // Check
        assertEquals(PGS_ROLE_NONE, master.getData().getRole());
        assertEquals(RED, master.getData().getColor());

        // ME
        me = new MasterElectionWorkflow(getPg(), null, false, context);
        me.execute();

        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
                master = pgs;
            }
        }
        assertNotNull(master);
        assertEquals(GREEN, master.getData().getColor());
        assertEquals(mgen + 1, getPg().getData().currentGen());

        // BJ
        int bsCnt = 0;
        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs.getData().getRole().equals(PGS_ROLE_LCONN)) {
                bsCnt++;
            }
        }

        BlueJoinWorkflow bj = new BlueJoinWorkflow(getPg(), false, context);
        bj.execute();

        int gsCnt = 0;
        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs.getData().getRole().equals(PGS_ROLE_SLAVE)) {
                assertEquals(GREEN, pgs.getData().getColor());
                gsCnt++;
            }
        }
        assertEquals(1, bsCnt);
        assertEquals(1, gsCnt);

        System.out.println(getConfiguration());
    }

    @Test
    public void opWorkflowCommands() throws Exception {
        MimicSMR[] mimics = new MimicSMR[MAX_PGS];
        
        createPm();
        createCluster();

        // Add PG
        createPg();

        // Check initial value of PG
        assertEquals(-1, getPg().getData().currentGen());
        assertEquals(Integer.valueOf(0), getPg().getData().getCopy());
        assertEquals(Integer.valueOf(0), getPg().getData().getQuorum());
        assertEquals(0, getPg().getData().getPgsIdList().size());

        // Add first PGS
        createPgs(0);

        // SE
        StartOfTheEpochWorkflow se = new StartOfTheEpochWorkflow(getPg(), getPgs(0), context);
        se.execute();

        // ME
        PartitionGroupServer master = getPgs(0);
        mimics[0] = mimic(master);
        mimics[0].init();
        
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " QA false forced");
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " ME false forced");

        // Check Master
        assertEquals(PGS_ROLE_MASTER, master.getData().getRole());
        assertEquals(GREEN, master.getData().getColor());
        assertEquals(0, getPg().getData().currentGen());

        // Add Slave
        for (int slaveIdx = 1; slaveIdx < 3; slaveIdx++) {
            createPgs(slaveIdx);
            mimics[slaveIdx] = mimic(getPgs(slaveIdx));
            mimics[slaveIdx].init();

            // IC
            IncreaseCopyWorkflow ic = new IncreaseCopyWorkflow(getPgs(slaveIdx), getPg(), context);
            ic.execute();

            // to LCONN
            PartitionGroupServer slave = getPgs(slaveIdx);
            PartitionGroupServerData slave1M = PartitionGroupServerData
                    .builder().from(slave.getData()).withRole(PGS_ROLE_LCONN)
                    .build();
            slave.setData(slave1M);

            // RA
            doCommand("op_wf " + clusterName + " " + getPg().getName() + " RA false forced");

            // Check lconn
            assertEquals(PGS_ROLE_LCONN, slave.getData().getRole());
            assertEquals(YELLOW, slave.getData().getColor());
        }
        
        // YJ
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " YJ false forced");

        for (int slaveIdx = 1; slaveIdx < 3; slaveIdx++) {
            PartitionGroupServer slave = getPgs(slaveIdx);

            // Check whether pgs became a slave
            assertEquals(PGS_ROLE_SLAVE, slave.getData().getRole());
            assertEquals(YELLOW, slave.getData().getColor());
            assertEquals(getPg().getData().currentGen() + 1, slave.getData()
                    .getMasterGen());
        }

        // MG
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " MG false forced");
        System.out.println("MG\n" + getConfiguration());

        // Check quorum of master
        QuorumValidator quorumValidator = new QuorumValidator(mimics[0], 2);
        await("test for quorum.").atMost(assertionTimeout, SECONDS).until(
                quorumValidator);
        
        // Kill master
        BlockingSocketImpl msockDead = mock(BlockingSocketImpl.class);
        master.setServerConnection(msockDead);
        when(msockDead.execute(anyString())).thenReturn("");
        
        PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                .from(master.getData()).withRole(PGS_ROLE_NONE).build();
        master.setData(pgsM);

        // RA
        final int mgen = getPg().getData().currentGen();
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " RA false forced");

        for (PartitionGroupServer pgs : getPgsList()) {
            if (!pgs.getData().getRole().equals(PGS_ROLE_SLAVE)) {
                continue;
            }

            pgsM = PartitionGroupServerData.builder().from(pgs.getData())
                    .withRole(PGS_ROLE_LCONN).build();
            pgs.setData(pgsM);
            doCommand("op_wf " + clusterName + " " + getPg().getName() + " RA false forced");

            assertEquals(PGS_ROLE_LCONN, pgs.getData().getRole());
            assertEquals(BLUE, pgs.getData().getColor());
        }

        // Check
        assertEquals(PGS_ROLE_NONE, master.getData().getRole());
        assertEquals(RED, master.getData().getColor());

        // ME
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " ME false forced");

        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
                master = pgs;
            }
        }
        assertNotNull(master);
        assertEquals(GREEN, master.getData().getColor());
        assertEquals(mgen + 1, getPg().getData().currentGen());

        // BJ
        int bsCnt = 0;
        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs.getData().getRole().equals(PGS_ROLE_LCONN)) {
                bsCnt++;
            }
        }

        doCommand("op_wf " + clusterName + " " + getPg().getName() + " BJ false forced");

        int gsCnt = 0;
        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs.getData().getRole().equals(PGS_ROLE_SLAVE)) {
                assertEquals(GREEN, pgs.getData().getColor());
                gsCnt++;
            }
        }
        assertEquals(1, bsCnt);
        assertEquals(1, gsCnt);

        System.out.println(getConfiguration());
    }
    
}

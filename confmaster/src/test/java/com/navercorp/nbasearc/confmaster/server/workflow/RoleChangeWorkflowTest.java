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

import java.util.ArrayList;
import java.util.List;

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
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ClientSessionHandler.ReplyFormatter;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.mimic.MimicPGS;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class RoleChangeWorkflowTest extends BasicSetting {

    MimicPGS mimics[] = new MimicPGS[MAX_PGS];
    ReplyFormatter replyFormatter = new ReplyFormatter();
    
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
        if (cluster != null) {
            for (PartitionGroupServer pgs : pgsImo.getList(cluster.getName())) {
                deletePgs(Integer.valueOf(pgs.getName()),
                        pgsImo.getList(cluster.getName()).size() == 1);
            }
    
            if (!pgImo.getList(cluster.getName()).isEmpty()) {
                deletePg();
            }
    
            if (!gwImo.getList(cluster.getName()).isEmpty()) {
                deleteGw();
            }
        }
        
        mimics[0].stop();
        mimics[1].stop();
        mimics[2].stop();
        
        super.after();
    }
    
    public void runWorkflows() throws Exception {
        RoleAdjustmentWorkflow ra = new RoleAdjustmentWorkflow(getPg(), false,
                context);
        ra.execute();

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
    
    // slaveCnt must be 2 or 3.
    private void init(final int slaveCnt) throws Exception {        
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
        
        p2.setData(PartitionGroupServerData.builder().from(p2.getData())
                .withRole(PGS_ROLE_LCONN).build());
        
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

        if (slaveCnt == 3) {
            // Create slave for 3copy
            // M S -> M S S
            createPgs(2);
            doCommand("pgs_join " + getPgs(2).getClusterName() + " " + getPgs(2).getName());
            PartitionGroupServer p3 = getPgs(2);
            
            p3.setData(PartitionGroupServerData.builder().from(p3.getData())
                    .withRole(PGS_ROLE_LCONN).build());
            
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
        }
    }
    
    @Test
    public void checkMasterHint3Copy() throws Exception { 
        init(3);

        doCommand("pg_dq " + getPg().getClusterName() + " " + getPg().getName());
        assertEquals(Integer.valueOf(3), getPg().getData().getCopy());
        assertEquals(Integer.valueOf(1), getPg().getData().getQuorum());
        
        mimics[0].mSmr.setSeqLog(100, 100, 200, 100);
        mimics[1].mSmr.setSeqLog(100, 100, 100, 100);
        mimics[2].mSmr.setSeqLog(0, 0, 0, 0);
        
        List<PartitionGroupServer> masterHints = new ArrayList<PartitionGroupServer>();
        masterHints.add(getPgs(2));
        RoleChangeWorkflow rc = new RoleChangeWorkflow(getPg(), masterHints, context);
        rc.execute();
        assertEquals(
                "-ERR role change fail. masterHint: [test_cluster/pg:0/pgs:2], "
                        + "exception: com.navercorp.nbasearc.confmaster.ConfMasterException$MgmtSmrCommandException. "
                        + "[test_cluster/pg:0/pgs:2] has no recent logs",
                rc.getResultString());
        
        // Recovery
        MasterFinder mf = new MasterFinder(getPgsList()); 
        await("test for master recovery.").atMost(assertionTimeout, SECONDS).until(mf);
        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs == mf.getMaster()) {
                continue;
            }
            RoleColorValidator rv = new RoleColorValidator(getPgs(2), PGS_ROLE_SLAVE, GREEN); 
            await("test for slave recovery.").atMost(assertionTimeout, SECONDS).until(rv);
        }
        
        masterHints.clear();
        masterHints.add(getPgs(2));
        masterHints.add(getPgs(1));
        rc = new RoleChangeWorkflow(getPg(), masterHints, context);
        rc.execute();
        assertEquals("{\"master\":1,\"role_slave_error\":[]}", rc.getResultString());
    }
    
    @Test
    public void checkMasterHint2Copy() throws Exception { 
        init(2);

        doCommand("pg_dq " + getPg().getClusterName() + " " + getPg().getName());
        assertEquals(Integer.valueOf(2), getPg().getData().getCopy());
        assertEquals(Integer.valueOf(0), getPg().getData().getQuorum());
        
        mimics[0].mSmr.setSeqLog(100, 100, 200, 100);
        mimics[1].mSmr.setSeqLog(0, 0, 0, 0);

        List<PartitionGroupServer> masterHints = new ArrayList<PartitionGroupServer>();
        masterHints.add(getPgs(1));
        RoleChangeWorkflow rc = new RoleChangeWorkflow(getPg(), masterHints, context);
        rc.execute();
        assertEquals(
                "-ERR role change fail. masterHint: [test_cluster/pg:0/pgs:1], "
                        + "exception: com.navercorp.nbasearc.confmaster.ConfMasterException$MgmtSmrCommandException. "
                        + "[test_cluster/pg:0/pgs:1] has no recent logs",
                rc.getResultString());
        
        // Recovery
        MasterFinder mf = new MasterFinder(getPgsList()); 
        await("test for master recovery.").atMost(assertionTimeout, SECONDS).until(mf);
        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs == mf.getMaster()) {
                continue;
            }
            RoleColorValidator rv = new RoleColorValidator(getPgs(1), PGS_ROLE_SLAVE, GREEN); 
            await("test for slave recovery.").atMost(assertionTimeout, SECONDS).until(rv);
        }

        doCommand("pg_iq " + getPg().getClusterName() + " " + getPg().getName());
        assertEquals(Integer.valueOf(2), getPg().getData().getCopy());
        assertEquals(Integer.valueOf(1), getPg().getData().getQuorum());
        
        mimics[0].mSmr.setSeqLog(100, 100, 200, 100);
        mimics[1].mSmr.setSeqLog(100, 100, 200, 100);
        
        masterHints.add(getPgs(1));
        rc = new RoleChangeWorkflow(getPg(), masterHints, context);
        rc.execute();
        assertEquals("{\"master\":1,\"role_slave_error\":[]}", rc.getResultString());
    }

    @Test
    public void modifyPgQuorum() throws Exception {
        init(2);
        
        // pg_iq fail
        JobResult result = doCommand("pg_iq " + clusterName + " " + pgName);
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR not enough available pgs.\"}\r\n",
                formatReply(result, null));

        result = doCommand("pg_iq xx" + clusterName + " " + pgName);
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. xxtest_cluster\"}\r\n",
                formatReply(result, null));

        result = doCommand("pg_iq " + clusterName + " 99");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pg does not exist. test_cluster/pg:99\"}\r\n",
                formatReply(result, null));

        // pg_dq success
        result = doCommand("pg_dq " + clusterName + " " + pgName);
        assertEquals("{\"state\":\"success\",\"msg\":\"+OK\"}\r\n",
                formatReply(result, null));

        // pg_dq fail
        result = doCommand("pg_dq " + clusterName + " " + pgName);
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR invalid quorum.\"}\r\n",
                formatReply(result, null));

        result = doCommand("pg_dq xx" + clusterName + " " + pgName);
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. xxtest_cluster\"}\r\n",
                formatReply(result, null));

        result = doCommand("pg_dq " + clusterName + " 99");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pg does not exist. test_cluster/pg:99\"}\r\n",
                formatReply(result, null));
        
        // pg_iq success
        result = doCommand("pg_iq " + clusterName + " " + pgName);
        assertEquals("{\"state\":\"success\",\"msg\":\"+OK\"}\r\n",
                formatReply(result, null));

        // Usage pg_iq
        result = doCommand("pg_iq");
        assertEquals("pg_iq <cluster_name> <pg_id>\r\n", 
                formatReply(result, null));

        // Usage pg_dq
        result = doCommand("pg_dq");
        assertEquals("pg_dq <cluster_name> <pg_id>\r\n", 
                formatReply(result, null));
    }
    
    
}

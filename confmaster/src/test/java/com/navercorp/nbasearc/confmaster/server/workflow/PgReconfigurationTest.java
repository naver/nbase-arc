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
import static com.navercorp.nbasearc.confmaster.Constant.Color.GREEN;
import static com.navercorp.nbasearc.confmaster.Constant.Color.RED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.log4j.Level.DEBUG;
import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

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
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mimic.IInjector;
import com.navercorp.nbasearc.confmaster.server.mimic.MimicPGS;

/*
 * Integrated Test For Confmaster
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class PgReconfigurationTest extends BasicSetting {

    ConfMaster cc = null;
    Thread cmThread;
    CountDownLatch latch = new CountDownLatch(1); 
    MimicPGS mimics[] = new MimicPGS[MAX_PGS];
    
    @Autowired
    Config config;
    
    @Autowired
    ConfMaster confMaster;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }
    
    @Before
    public void before() throws Exception {
        zookeeper.initialize();
        zookeeper.deleteAllZNodeRecursive();
        
        cc = context.getBean(ConfMaster.class);
        
        cmThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cc.initialize();
                    latch.countDown();
                    cc.run();
                } catch (Exception e) {
                    Logger.error("Exception propagated to the main.", e);
                    Logger.flush(DEBUG);
                }
            }
        });
        cmThread.start();
        latch.await();

        confMaster.setState(ConfMaster.RUNNING);
        
        joinLeave.initialize(context);
        
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
        
        mimics[0].stop();
        mimics[1].stop();
        mimics[2].stop();
        
        cc.terminate();
        cmThread.join();
        
        if (cc != null) {
            cc.release();
        }

        executor.release();
        
        confmasterService.release();
        
        LeaderState.init();
    }
    /*
     * copy: 1 ~ 3.
     */
    private void init(final int copy) throws Exception {
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
        MasterFinder masterCond = new MasterFinder(getPgsList());
        boolean success = false;
        for (int i = 0; i < assertionTimeout; i++) {
            if (masterCond.call()) {
                success = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (success == false) {
            PartitionGroupServerData d = p1.getData();
            System.out.println(p1 + " " + d);
        }
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        
        if (copy > 1) {
            // Create slave for 2copy
            // M -> M S
            createPgs(1);
            doCommand("pgs_join " + getPgs(1).getClusterName() + " "
                    + getPgs(1).getName());

            PartitionGroupServer p2 = getPgs(1);
            SlaveFinder slaveFinder = new SlaveFinder(getPgsList());
            await("test for role slave.").atMost(assertionTimeout, SECONDS)
                    .until(slaveFinder);
            assertTrue(slaveFinder.isSlave(p2));
            validateNormal(p2, pg, PGS_ROLE_SLAVE, 0);

            QuorumValidator quorumValidator = new QuorumValidator(
                    mimics[0].mSmr, 1);
            await("test for quorum.").atMost(assertionTimeout, SECONDS).until(
                    quorumValidator);

            // Master must not be changed
            // M S
            masterCond = new MasterFinder(getPgsList());
            await("test for role master.").atMost(assertionTimeout, SECONDS)
                    .until(masterCond);
            assertEquals(p1, masterCond.getMaster());
            validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        }
        
        if (copy > 2) {
            // Create slave for 2copy
            // M -> M S
            createPgs(2);
            doCommand("pgs_join " + getPgs(2).getClusterName() + " "
                    + getPgs(2).getName());

            PartitionGroupServer p3 = getPgs(2);
            RoleColorValidator slaveValidator = new RoleColorValidator(p3,
                    PGS_ROLE_SLAVE, GREEN);
            await("test for role slave.").atMost(assertionTimeout, SECONDS)
                    .until(slaveValidator);

            QuorumValidator quorumValidator = new QuorumValidator(
                    mimics[0].mSmr, 2);
            await("test for quorum.").atMost(assertionTimeout, SECONDS).until(
                    quorumValidator);

            // Master must not be changed
            // M S
            masterCond = new MasterFinder(getPgsList());
            await("test for role master.").atMost(assertionTimeout, SECONDS)
                    .until(masterCond);
            assertEquals(p1, masterCond.getMaster());
            validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        }
    }
    
    private void printPgsState() {
        for (int i = 0; i < 2; i++) {
            PartitionGroupServer pgs = getPgs(i);
            PartitionGroupServerData d = pgs.getData();
            System.out.println(pgs + ", " + d.getRole() + ", " + d.getColor());
        }
    }
    
    @Test
    public void masterElection() throws Exception {
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
        MasterFinder masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        
        // Create slave for 2copy
        // M -> M S
        createPgs(1);
        doCommand("pgs_join " + getPgs(1).getClusterName() + " " + getPgs(1).getName());
        
        PartitionGroupServer p2 = getPgs(1);
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

        // Mimic p1 failure
        // N L -> N M
        mimics[0].mSmr.execute("role none");
        mimics[1].mSmr.execute("role lconn");
        
        await("reconfiguration for master.").atMost(assertionTimeout, SECONDS).until(
                new RoleColorValidator(p2, PGS_ROLE_MASTER, GREEN));
        validateNormal(p2, pg, PGS_ROLE_MASTER, 1);
        await("quorum must 0").atMost(assertionTimeout, SECONDS).until(
                new QuorumValidator(mimics[1].mSmr, 0));

        assertEquals(RED, p1.getData().getColor());
        assertEquals(PGS_ROLE_NONE, p1.getData().getRole());
        assertEquals(SERVER_STATE_FAILURE, p1.getData().getState());
        assertEquals(HB_MONITOR_YES, p1.getData().getHb());
        assertEquals(1, p1.getData().getMasterGen());
        
        // Recover p1
        // N M -> S M
        mimics[0].mSmr.init();
        
        await("reconfiguration for slave.").atMost(assertionTimeout, SECONDS)
                .until(new RoleColorValidator(p1, PGS_ROLE_SLAVE, GREEN));
        validateNormal(p1, pg, PGS_ROLE_SLAVE, 1);
        validateNormal(p2, pg, PGS_ROLE_MASTER, 1);
        await("quorum must 1").atMost(assertionTimeout, SECONDS).until(
                new QuorumValidator(mimics[1].mSmr, 1));

        // Create slave for 3copy
        // S M -> S M S
        createPgs(2);
        doCommand("pgs_join " + getPgs(2).getClusterName() + " " + getPgs(2).getName());
        
        PartitionGroupServer p3 = getPgs(2);
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(
                new RoleColorValidator(p3, PGS_ROLE_SLAVE, GREEN));
        validateNormal(p3, pg, PGS_ROLE_SLAVE, 1);
        
        // Master must not be changed
        masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p2, masterCond.getMaster());
        validateNormal(p2, pg, PGS_ROLE_MASTER, 1);
        
        // Another slave must not be changed
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(
                new RoleColorValidator(p1, PGS_ROLE_SLAVE, GREEN));
        validateNormal(p1, pg, PGS_ROLE_SLAVE, 1);

        // Mimic p2 failure
        // S M S -> L N L
        mimics[0].mSmr.execute("role lconn");
        mimics[1].mSmr.execute("role none");
        mimics[2].mSmr.execute("role lconn");
        
        await("failure detection").atMost(assertionTimeout, SECONDS).until(
                new RoleColorValidator(p2, PGS_ROLE_NONE, RED));
        assertEquals(RED, p2.getData().getColor());
        assertEquals(PGS_ROLE_NONE, p2.getData().getRole());
        assertEquals(SERVER_STATE_FAILURE, p2.getData().getState());
        assertEquals(HB_MONITOR_YES, p2.getData().getHb());
        assertEquals(2, p2.getData().getMasterGen());
        
        MasterFinder masterFinder = new MasterFinder(getPgsList());
        await("reconfiguration for master.").atMost(assertionTimeout, SECONDS).until(masterFinder);
        validateNormal(masterFinder.getMaster(), pg, PGS_ROLE_MASTER, 2);
        await("quorum must 1").atMost(assertionTimeout, SECONDS).until(
                new QuorumValidator(mimics[Integer.valueOf(masterFinder
                        .getMaster().getName())].mSmr, 1));
        
        slaveFinder = new SlaveFinder(getPgsList());
        await("reconfiguration for slave.").atMost(assertionTimeout, SECONDS).until(slaveFinder);
        assertEquals(1, slaveFinder.getSlaves().size());
        for (PartitionGroupServer slv : slaveFinder.getSlaves()) {
            validateNormal(slv, pg, PGS_ROLE_SLAVE, 2);
        }
        
        // Recover p2
        // M N S -> M S S
        mimics[1].mSmr.init();
        
        await("reconfiguration for slave.").atMost(assertionTimeout, SECONDS)
                .until(new RoleColorValidator(p2, PGS_ROLE_SLAVE, GREEN));
        validateNormal(p2, pg, PGS_ROLE_SLAVE, 2);
        
        // Del master
        masterFinder = new MasterFinder(getPgsList());
        await("reconfiguration for master.").atMost(assertionTimeout, SECONDS).until(masterFinder);
        validateNormal(masterFinder.getMaster(), pg, PGS_ROLE_MASTER, 2);

        PartitionGroupServer leftPgs = masterFinder.getMaster();
        doCommand("pgs_leave " + leftPgs.getClusterName() + " "
                + leftPgs.getName());
        doCommand("pgs_lconn " + leftPgs.getClusterName() + " "
                + leftPgs.getName());

        // Check mGen
        for (PartitionGroupServer pgs : getPgsList()) {
            // Skip left PGS
            if (pgs == leftPgs) {
                continue;
            }
            await("validate mGen.").atMost(assertionTimeout, SECONDS).until(
                    new MasterGenValidator(pgs, 4));
        }

        mimics[0].mSmr.execute("role lconn");
        mimics[1].mSmr.execute("role lconn");
        mimics[2].mSmr.execute("role lconn");

        // Check mGen
        for (PartitionGroupServer pgs : getPgsList()) {
            // Skip left PGS
            if (pgs == leftPgs) {
                continue;
            }
            await("validate mGen.").atMost(assertionTimeout, SECONDS).until(
                    new MasterGenValidator(pgs, 5));
        }
        
        // Check new master
        masterFinder = new MasterFinder(getPgsList());
        await("reconfiguration for master.").atMost(assertionTimeout, SECONDS).until(masterFinder);
        validateNormal(masterFinder.getMaster(), pg, PGS_ROLE_MASTER, 4);

        slaveFinder = new SlaveFinder(getPgsList());
        await("reconfiguration for slave.").atMost(assertionTimeout, SECONDS).until(slaveFinder);
        assertEquals(1, slaveFinder.getSlaves().size());
        for (PartitionGroupServer slv : slaveFinder.getSlaves()) {
            validateNormal(slv, pg, PGS_ROLE_SLAVE, 4);
        }
    }

    @Test
    public void roleChange() throws Exception {        
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
        MasterFinder masterCond = new MasterFinder(getPgsList());
        boolean success = false;
        for (int i = 0; i < assertionTimeout; i++) {
            if (masterCond.call()) {
                success = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (success == false) {
            PartitionGroupServerData d = p1.getData();
            System.out.println(p1 + " " + d);
        }
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        
        // Create slave for 2copy
        // M -> M S
        createPgs(1);
        doCommand("pgs_join " + getPgs(1).getClusterName() + " " + getPgs(1).getName());
        
        PartitionGroupServer p2 = getPgs(1);
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
    
        // Role change
        // M S -> S M
        JobResult jr = doCommand("role_change " + getPgs(0).getClusterName() + " " + getPgs(1).getName());
        System.out.println(jr.getMessages());
        assertEquals(PGS_ROLE_MASTER, p2.getData().getRole());
        assertEquals(GREEN, p2.getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, p1.getData().getRole());
        assertEquals(GREEN, p1.getData().getColor());
    }

    @Test
    public void changeQuorum() throws Exception {        
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
        MasterFinder masterCond = new MasterFinder(getPgsList());
        boolean success = false;
        for (int i = 0; i < assertionTimeout; i++) {
            if (masterCond.call()) {
                success = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (success == false) {
            PartitionGroupServerData d = p1.getData();
            System.out.println(p1 + " " + d);
        }
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        
        // Create slave for 2copy
        // M -> M S
        createPgs(1);
        doCommand("pgs_join " + getPgs(1).getClusterName() + " " + getPgs(1).getName());
        
        PartitionGroupServer p2 = getPgs(1);
        SlaveFinder slaveFinder = new SlaveFinder(getPgsList());
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveFinder);
        assertTrue(slaveFinder.isSlave(p2));
        validateNormal(p2, pg, PGS_ROLE_SLAVE, 0);

        QuorumValidator quorumValidator = new QuorumValidator(mimics[0].mSmr, 1);
        await("test for quorum.").atMost(assertionTimeout, SECONDS).until(
                quorumValidator);
        
        // Master must not be changed
        // M S
        masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
    
        // Decrease quorum
        doCommand("pg_dq " + getPg().getClusterName() + " " + getPg().getName());
        quorumValidator = new QuorumValidator(mimics[0].mSmr, 0);
        await("quorum validation after pg_dq.").atMost(assertionTimeout,
                SECONDS).until(quorumValidator);

        // Decrease quorum
        doCommand("pg_iq " + getPg().getClusterName() + " " + getPg().getName());
        quorumValidator = new QuorumValidator(mimics[0].mSmr, 1);
        await("quorum validation after pg_iq.").atMost(assertionTimeout,
                SECONDS).until(quorumValidator);
    }

    @Test
    public void unstableNetwork() throws Exception {
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
        MasterFinder masterCond = new MasterFinder(getPgsList());
        boolean success = false;
        for (int i = 0; i < assertionTimeout; i++) {
            if (masterCond.call()) {
                success = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (success == false) {
            PartitionGroupServerData d = p1.getData();
            System.out.println(p1 + " " + d);
        }
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
        
        // Create slave for 2copy
        // M -> M S
        createPgs(1);
        doCommand("pgs_join " + getPgs(1).getClusterName() + " " + getPgs(1).getName());
        
        PartitionGroupServer p2 = getPgs(1);
        SlaveFinder slaveFinder = new SlaveFinder(getPgsList());
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveFinder);
        assertTrue(slaveFinder.isSlave(p2));
        validateNormal(p2, pg, PGS_ROLE_SLAVE, 0);

        QuorumValidator quorumValidator = new QuorumValidator(mimics[0].mSmr, 1);
        await("test for quorum.").atMost(assertionTimeout, SECONDS).until(
                quorumValidator);
        
        // Master must not be changed
        // M S
        masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p1, masterCond.getMaster());
        validateNormal(p1, pg, PGS_ROLE_MASTER, 0);
    
        // Unstable network
        for (MimicPGS m : mimics) {
            m.mSmr.setUnstable(true);
        }
        
        for (int i = 0; i < 10; i++) {
            printPgsState();
            Thread.sleep(1000);
        }
        
        // Stable network
        for (MimicPGS m : mimics) {
            m.mSmr.setUnstable(false);
        }

        for (int i = 0; i < 5; i++) {
            printPgsState();
            Thread.sleep(1000);
        }

        masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(GREEN, masterCond.getMaster().getData().getColor());
        quorumValidator = new QuorumValidator(mimics[Integer.valueOf(masterCond
                .getMaster().getName())].mSmr, 1);
        await("test for quorum.").atMost(assertionTimeout, SECONDS).until(
                quorumValidator);

        slaveFinder = new SlaveFinder(getPgsList());
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveFinder);
        for (PartitionGroupServer pgs : slaveFinder.getSlaves()) {
            assertEquals(GREEN, pgs.getData().getColor());
        }
    }
    
    @Test
    public void pgsLeaveFailAtRa() throws Exception {
        init(3);
        
        mimics[0].mSmr.setInjector(new IInjector<String, String>() {
            @Override
            public String inject(String rqst) {
                if (rqst.contains("setquorum")) {
                    return "";
                } else {
                    return null;
                }
            }
        });
        
        JobResult jr = tryCommand("pgs_leave " + getPgs(1).getClusterName() + " " + getPgs(1).getName());
        List<Throwable> el = jr.getExceptions();
        assertEquals(1, el.size());
        assertTrue(el.get(0) instanceof MgmtSetquorumException);
        assertEquals("2", mimics[0].mSmr.execute("getquorum"));
        
        mimics[0].mSmr.setInjector(null);
        QuorumValidator qv = new QuorumValidator(mimics[0].mSmr, 1);
        await("test for quorum.").atMost(assertionTimeout, SECONDS).until(qv);
        
        assertEquals(PGS_ROLE_MASTER, getPgs(0).getData().getRole());
        assertEquals(GREEN, getPgs(0).getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, getPgs(2).getData().getRole());
        assertEquals(GREEN, getPgs(2).getData().getColor());
    }
    
    @Test
    public void pgQuorumFailAtRa() throws Exception {
        init(3);
        
        mimics[0].mSmr.setInjector(new IInjector<String, String>() {
            @Override
            public String inject(String rqst) {
                if (rqst.contains("setquorum")) {
                    return "";
                } else {
                    return null;
                }
            }
        });
        
        // Decrease Quorum
        JobResult jr = tryCommand("pg_dq " + getPg().getClusterName() + " " + getPg().getName());
        List<Throwable> el = jr.getExceptions();
        assertEquals(1, el.size());
        assertTrue(el.get(0) instanceof MgmtSetquorumException);
        assertEquals("2", mimics[0].mSmr.execute("getquorum"));
        
        mimics[0].mSmr.setInjector(null);
        QuorumValidator qv = new QuorumValidator(mimics[0].mSmr, 1);
        await("test for quorum.").atMost(assertionTimeout, SECONDS).until(qv);
        
        assertEquals(PGS_ROLE_MASTER, getPgs(0).getData().getRole());
        assertEquals(GREEN, getPgs(0).getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, getPgs(1).getData().getRole());
        assertEquals(GREEN, getPgs(1).getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, getPgs(2).getData().getRole());
        assertEquals(GREEN, getPgs(2).getData().getColor());

        mimics[0].mSmr.setInjector(new IInjector<String, String>() {
            @Override
            public String inject(String rqst) {
                if (rqst.contains("setquorum")) {
                    return "";
                } else {
                    return null;
                }
            }
        });
        
        // Increase Quorum
        jr = tryCommand("pg_iq " + getPg().getClusterName() + " " + getPg().getName());
        el = jr.getExceptions();
        assertEquals(1, el.size());
        assertTrue(el.get(0) instanceof MgmtSetquorumException);
        assertEquals("1", mimics[0].mSmr.execute("getquorum"));
        
        mimics[0].mSmr.setInjector(null);

        jr = tryCommand("pg_iq " + getPg().getClusterName() + " " + getPg().getName());
        el = jr.getExceptions();
        assertEquals(0, el.size());
        assertEquals("2", mimics[0].mSmr.execute("getquorum"));
        
        assertEquals(PGS_ROLE_MASTER, getPgs(0).getData().getRole());
        assertEquals(GREEN, getPgs(0).getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, getPgs(1).getData().getRole());
        assertEquals(GREEN, getPgs(1).getData().getColor());
        assertEquals(PGS_ROLE_SLAVE, getPgs(2).getData().getRole());
        assertEquals(GREEN, getPgs(2).getData().getColor());
    }

    public class FaultInjector implements IInjector<String, String> {
        int doMasterCnt;
        int doSlaveCnt;
        int doLconnCnt;
        final int masterLimit;
        final int slaveLimit;
        final int lconnLimit;

        public FaultInjector(final int masterLimit, final int slaveLimit, final int lconnLimit) {
            doMasterCnt = 0;
            doSlaveCnt = 0;
            doLconnCnt = 0;
            this.masterLimit = masterLimit;
            this.slaveLimit = slaveLimit;
            this.lconnLimit = lconnLimit;
        }

        @Override
        public String inject(String rqst) {
            if (rqst.contains("role master")) {
                doMasterCnt++;
                if (doMasterCnt > masterLimit) {
                    return null;
                } else {
                    return "";
                }
            } else if (rqst.contains("role slave")) {
                doSlaveCnt++;
                if (doSlaveCnt > slaveLimit) {
                    return null;
                } else {
                    return "";
                }
            } else if (rqst.contains("role lconn")) {
                doLconnCnt++;
                if (doLconnCnt > lconnLimit) {
                    return null;
                } else {
                    return "";
                }
            } else {
                return null;
            }
        }
    }
    
    private void doRoleFailTest(FaultInjector fi) throws InterruptedException {
        MasterFinder mf = new MasterFinder(getPgsList());
        await("check master.").atMost(assertionTimeout, SECONDS).until(mf);
        
        mimics[Integer.valueOf(mf.getMaster().getName())].mSmr.execute("role lconn");
        
        for (int i = 0; i < MAX_PGS; i++) {
            mimics[i].mSmr.setInjector(fi);
        }
        assertTrue(fi.doMasterCnt == 0);
        assertTrue(fi.doSlaveCnt == 0);
        for (int i = 0; i < 5 + (fi.masterLimit + fi.slaveLimit + fi.lconnLimit) * 3; i++) {
            Thread.sleep(1000);
        }
        
        for (int i = 0; i < MAX_PGS; i++) {
            mimics[i].mSmr.setInjector(null);
        }

        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
        }
        
        mf = new MasterFinder(getPgsList());
        await("check master.").atMost(assertionTimeout, SECONDS).until(mf);
        for (PartitionGroupServer pgs : getPgsList()) {
            if (pgs == mf.getMaster()) {
                continue;
            }
            
            RoleColorValidator rv = new RoleColorValidator(pgs, PGS_ROLE_SLAVE, GREEN); 
            await("check slave.").atMost(assertionTimeout, SECONDS).until(rv);
        }
        
        QuorumValidator qv = new QuorumValidator(mimics[Integer.valueOf(mf
                .getMaster().getName())].mSmr, 2);
        await("check quorum.").atMost(assertionTimeout, SECONDS).until(qv);
    }
    
    public void logPgs() {
        for (PartitionGroupServer pgs : getPgsList()) {
            System.out.println(pgs + " " + pgs.getData().getRole() + " "
                    + pgs.getData().getColor() + " "
                    + pgs.getData().getMasterGen());
        }
    }
    
    @Test
    public void unstableNetworkDoRoleFail() throws Exception {
        init(3);
        
        for (int role = 0; role < 3; role++) {
            for (int c = 1; c <= 3; c++) {
                FaultInjector fi = new FaultInjector(
                        c * (role == 1 ? 1 : 0), 
                        c * (role == 2 ? 1 : 0), 
                        c * (role == 3 ? 1 : 0));
                doRoleFailTest(fi);
                assertTrue(fi.doMasterCnt + fi.doSlaveCnt + fi.doLconnCnt > 0);
                logPgs();
            }
        }
        
        for (int c = 1; c <= 2; c++) {
            FaultInjector fi = new FaultInjector(c, c, c);
            doRoleFailTest(fi);
            assertTrue(fi.doMasterCnt > 0);
            assertTrue(fi.doSlaveCnt > 0);
            assertTrue(fi.doLconnCnt > 0);
            logPgs();
        }
    }
    
}

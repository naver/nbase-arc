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

package com.navercorp.nbasearc.confmaster;

import static com.jayway.awaitility.Awaitility.await;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_NO;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_YES;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_NONE;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_NORMAL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.log4j.Level.DEBUG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResultProcessor;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSessionHandler;
import com.navercorp.nbasearc.confmaster.heartbeat.HeartbeatChecker;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.ClusterBackupScheduleDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkNotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PGSComponentMock;
import com.navercorp.nbasearc.confmaster.server.cluster.PGSJoinLeaveSetting;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.command.CommandExecutor;
import com.navercorp.nbasearc.confmaster.server.command.ConfmasterService;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionHandler;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.server.workflow.FailoverPGSWorkflowTest.MasterChecker;
import com.navercorp.nbasearc.confmaster.server.workflow.FailoverPGSWorkflowTest.RoleChecker;

public class BasicSetting {

    @Autowired
    protected ApplicationContext context;
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected LeaderElectionHandler leaderElectoin;
    
    @Autowired
    protected HeartbeatChecker heartbeatChecker;
    
    @Autowired 
    protected ThreadPool executor;
    @Autowired
    protected CommandExecutor commandExecutor;
    @Autowired
    protected WorkflowExecutor workflowExecutor;
    
    @Autowired
    protected PhysicalMachineClusterImo pmClusterImo;
    @Autowired
    protected ClusterImo clusterImo;
    @Autowired
    protected PhysicalMachineImo pmImo;
    @Autowired
    protected PartitionGroupImo pgImo;
    @Autowired
    protected PartitionGroupServerImo pgsImo;
    @Autowired
    protected RedisServerImo rsImo;
    @Autowired
    protected GatewayImo gwImo;    

    @Autowired
    protected ZkNotificationDao notificationDao;
    @Autowired
    protected ZkWorkflowLogDao workflowLogDao;
    @Autowired
    protected ClusterBackupScheduleDao clusterBackupScheduleDao;
    
    @Autowired
    protected ConfmasterService confmasterService;

    @Autowired 
    private HBResultProcessor hbcProc;
    
    final protected String ok = S2C_OK;
    final protected long timeout = 10000000L;
    final protected int MAX_PGS = 3;
    protected PGSJoinLeaveSetting joinLeave = new PGSJoinLeaveSetting();

    // Physical machine
    protected static String pmName = "test01.arc";
    protected static PhysicalMachineData pmData = new PhysicalMachineData("127.0.0.100");
    
    // Cluster
    protected static String clusterName = "test_cluster";
    protected static ClusterData clusterData;
    static {
        List<Integer> quorumPolicy = new ArrayList<Integer>();
        List<Integer> slot = new ArrayList<Integer>();
        Integer keyspaceSize = 8192;
        
        quorumPolicy.add(0);
        quorumPolicy.add(1);
        for (int i = 0; i < keyspaceSize; i++) {
            slot.add(0);
        }
        
        clusterData = new ClusterData();
        clusterData.setPhase(Constant.CLUSTER_PHASE_INIT);
        clusterData.setPnPgMap(slot);
        clusterData.setQuorumPolicy(quorumPolicy);
        clusterData.setKeySpaceSize(keyspaceSize);
    }
    
    // Partition Group
    protected static String pgName = "0";
    protected static PartitionGroupData pgData = new PartitionGroupData();
    static {
        pgData = PartitionGroupData.builder().from(new PartitionGroupData())
                .addMasterGen(0L).build();
    }
    
    // Partition Group Server 1
    protected static String[] pgsNames = new String[]{"0", "1", "2"};
    protected static PartitionGroupServerData[] pgsDatas = new PartitionGroupServerData[3];
    static {
        pgsDatas[0] = new PartitionGroupServerData();
        pgsDatas[0].initialize(Integer.valueOf(pgName), pmName, pmData.getIp(),
                8109, 8100, 8103, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                pgData.currentGen(), HB_MONITOR_NO);
        pgsDatas[1] = new PartitionGroupServerData();
        pgsDatas[1].initialize(Integer.valueOf(pgName), pmName, pmData.getIp(),
                8109, 8100, 8103, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                pgData.currentGen(), HB_MONITOR_NO);
        pgsDatas[2] = new PartitionGroupServerData();
        pgsDatas[2].initialize(Integer.valueOf(pgName), pmName, pmData.getIp(),
                8109, 8100, 8103, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                pgData.currentGen(), HB_MONITOR_NO);
    }
    
    // Gateway
    protected static String gwName = "1";
    protected static GatewayData gwData;
    static {
        gwData = new GatewayData();
        gwData.initialize(pmName, pmData.getIp(), 6000, SERVER_STATE_FAILURE,
                HB_MONITOR_YES);
    }
    
    public static void beforeClass() throws Exception {
        try {
            EmbeddedZooKeeper.setConfigPath("./target/test-classes/zoo.cfg");
            EmbeddedZooKeeper.start();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public void before() throws Exception {
        zookeeper.initialize();
        heartbeatChecker.initialize();
        
        commandExecutor.initialize();
        workflowExecutor.initialize();
        
        zookeeper.deleteAllZNodeRecursive();
        
        confmasterService.initialize();
        executor.initialize();
        
        confmasterService.loadAll();

        leaderElectoin.becomeLeader();

        joinLeave.initialize(context);
    }
    
    public void after() throws Exception {
        try {
            executor.release();
            
            confmasterService.release();
            
            LeaderState.init();
            
            zookeeper.deleteAllZNodeRecursive();
            
            zookeeper.release();
        } finally {
            Logger.flush(DEBUG);
        }
    }

    public JobResult doCommand(String line) throws Exception {
        Future<JobResult> future = commandExecutor.perform(line, null);
        try {
            JobResult result = future.get(timeout, TimeUnit.MILLISECONDS);
            assertNotNull(result);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public void pmAdd(String pmName, String pmIp) throws Exception {
        JobResult result = doCommand("pm_add " + pmName + " " + pmIp);
        assertEquals("perform pm_add.", ok, result.getMessages().get(0));
        
        PhysicalMachine pm = pmImo.get(pmName);
        assertNotNull("check pm after pm_add.", pm);
        assertEquals("check pm data.", pmIp, pm.getData().getIp());
    }
    
    public void pmDel(String pmName) throws Exception {
        JobResult result = doCommand("pm_del " + pmName);
        assertEquals("perform pm_del.", ok, result.getMessages().get(0));
        
        PhysicalMachine pm = pmImo.get(pmName);
        assertNull("check pm after pm_del.", pm);
    }
    
    public void pgAdd(String pgId, String clusterName) throws Exception{
        String command = "";
        for (String s : new String[]{"pg_add", clusterName, pgId}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of pg_add", ok, result.getMessages().get(0));
        
        PartitionGroup pg = pgImo.get(pgId, clusterName);
        assertNotNull("check pg after pg_add", pg);
    }
    
    public void pgDel(String pgId, String clusterName) throws Exception {
        JobResult result = doCommand("pg_del " + clusterName + " " + pgId);
        assertEquals("check result of pg_del", ok, result.getMessages().get(0));
        assertNull("check pg deleted", pgImo.get(pgId, clusterName));
    }
    
    public void pgsAdd(String pgsId, String pgId, String clusterName, int port) throws Exception {
        String command = "";
        for (String s : new String[]{"pgs_add", clusterName, pgsId, pgId, pmName, pmData.getIp(), String.valueOf(port), String.valueOf(port + 9)}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of pgs_add", ok, result.getMessages().get(0));
        
        PartitionGroupServer pgs = pgsImo.get(pgsId, clusterName);
        assertNotNull(pgs);
        
        PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName, pmName);
        assertTrue("Check pm-cluster contains this pgs.", 
                pmCluster.getData().getPgsIdList().contains(Integer.valueOf(pgsId)));
    }

    public void pgsDel(String pgsId, boolean isLastPgs) throws Exception {
        JobResult result = doCommand("pgs_del " + clusterName + " " + pgsId);
        assertEquals("check result of pgs_del", ok, result.getMessages().get(0));
        assertNull("check pgs deleted", pgsImo.get(pgsId, clusterName));
        
        if (!isLastPgs) {
            PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName, pmName);
            assertFalse("Check pm-cluster does not contains this pgs.", 
                    pmCluster.getData().getPgsIdList().contains(Integer.valueOf(pgsId)));
        }
    }
    
    public void gwAdd(String gwId, String clusterName) throws Exception {
        String command = "";
        for (String s : new String[]{"gw_add", clusterName, gwId, pmName, pmData.getIp(), "6000"}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of gw_add", ok, result.getMessages().get(0));
        
        Gateway gateway = gwImo.get(gwId, clusterName);
        assertNotNull(gateway);
        
        PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName, pmName);
        assertTrue("Check pm-cluster contains this gw.", 
                pmCluster.getData().getGwIdList().contains(Integer.valueOf(gwId)));
    }

    public void gwDel(String gwId, String clusterName) throws Exception {
        JobResult result = doCommand("gw_del " + clusterName + " " + gwId);
        assertEquals("check result of gw_del", ok, result.getMessages().get(0));
        assertNull("check gateway deleted", gwImo.get(gwId, clusterName));
        
        // TODO : test for ClusterInPhysicalMachine, whether it is deleted, when it contains no gw and no pgs. 
        PhysicalMachineCluster pmCluster = pmClusterImo.get(clusterName, pmName);
        if (pmCluster != null) {
            assertFalse("Check pm-cluster does not contains this gw.", 
                    pmCluster.getData().getGwIdList().contains(Integer.valueOf(gwId)));
        }
    }
    
    public void clusterAdd(String clusterName, String quorumPolicy) throws Exception {
        JobResult result = doCommand("cluster_add " + clusterName + " " + quorumPolicy);
        assertEquals("perform cluster_add.", ok, result.getMessages().get(0));
        
        Cluster cluster = clusterImo.get(clusterName);
        assertNotNull("check cluster after cluster_add.", cluster);
        List<Integer> qpExpected = new ArrayList<Integer>();
        for (String quorum : quorumPolicy.split(":")) {
            qpExpected.add(Integer.valueOf(quorum));
        }
        assertEquals("check cluster data.", qpExpected, 
                cluster.getData().getQuorumPolicy());
    }
    
    public void clusterDel(String clusterName) throws Exception {
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("check result of cluster_del", ok, result.getMessages().get(0));
        assertNull("check cluster deleted.", clusterImo.get(clusterName));
    }
    
    public void slotSetPg(String clusterName, String pgName) throws Exception {
        String command = "";
        for (String s : new String[]{"slot_set_pg", clusterName, "0:8191", pgName}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of slot_set_pg", ok, result.getMessages().get(0));
        
        Cluster cluster = clusterImo.get(clusterName);
        List<Integer> slot = new ArrayList<Integer>();
        for (int i = 0; i < 8192; i++) {
            slot.add(0);
        }
        assertEquals("check slot of pg after slot_set_pg", slot, cluster.getData().getPbPgMap());
    }
    
    public void createPm() throws Exception {
        pmAdd(pmName, pmData.getIp());
    }
    
    public void deletePm() throws Exception {
        pmDel(pmName);
    }

    public void createCluster() throws Exception {
        clusterAdd(clusterName, "0:1");
    }
    
    public void deleteCluster() throws Exception {
        clusterDel(clusterName);
    }
    
    public Cluster getCluster() {
        return clusterImo.get(clusterName);
    }
    
    public void createPg() throws Exception {
        pgAdd(pgName, clusterName);
        slotSetPg(clusterName, pgName);
    }
    
    public void deletePg() throws Exception {
        pgDel(pgName, clusterName);
    }
    
    public void createPgs(int index) throws Exception {
        pgsAdd(pgsNames[index], 
                String.valueOf(pgsDatas[index].getPgId()), 
                clusterName, 
                pgsDatas[index].getSmrBasePort());
    }
    
    public void deletePgs(int index, boolean isLastPgs) throws Exception {
        pgsDel(pgsNames[index], isLastPgs);
    }
    
    public PartitionGroup getPg() {
        return pgImo.get(pgName, clusterName);
    }
    
    public PartitionGroupServer getPgs(int index) {
        return pgsImo.get(pgsNames[index], clusterName);
    }
    
    public List<PartitionGroupServer> getPgsList() {
        return pgsImo.getList(clusterName);
    }
    
    public void setPgs(PartitionGroupServer pgs) throws SecurityException,
            NoSuchFieldException, IllegalArgumentException,
            IllegalAccessException {
        Field containerField = PartitionGroupServerImo.class.getDeclaredField("container");
        containerField.setAccessible(true);
        Map<String, PartitionGroupServer> container = 
                (Map<String, PartitionGroupServer>) containerField.get(pgsImo);
        container.put(pgs.getPath(), pgs);
    }
    
    public RedisServer getRs(int index) {
        return rsImo.get(pgsNames[index], clusterName);
    }
    
    public Gateway getGw() {
        return gwImo.get(gwName, clusterName);
    }

    public void createGw() throws Exception {
        gwAdd(gwName, clusterName);
    }

    public void deleteGw() throws Exception {
        gwDel(gwName, clusterName);
    }

    public void mockPgs(int id) throws Exception {
        setPgs(spy(getPgs(id)));
        
        // Create mocks for PGS
        PGSComponentMock mockup = new PGSComponentMock(getPgs(id), getRs(id)); 

        // Mock Heartbeat Session Handler
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        getPgs(id).getHbc().setHandler(handler);
        
        // PGS Join
        joinLeave.pgsJoin(getPgs(id), getRs(id), mockup);
    }
    
    public void testRoleNone(PartitionGroupServer pgs) throws Exception {
        // Mock up PGS '0' to be in LCONN state
        final String pingResp = "";
        mockSocket(pgs, pingResp);
        mockHeartbeatResult(pgs, SERVER_STATE_FAILURE, pingResp);
        
        // It expects that PGS '0' becomes a master.
        await("failure detected.").atMost(5, SECONDS).until(new RoleChecker(pgs, PGS_ROLE_NONE));

        // Mock up PGS '0' to be a MASTER
        mockSocket(pgs, "");
    }
    
    public void mockPgsLconn(PartitionGroupServer pgs) throws Exception {
        // Mock up PGS '0' to be in LCONN state
        final String pingResp = "+OK 1 1400000000";
        mockSocket(pgs, pingResp);
        mockHeartbeatResult(pgs, SERVER_STATE_NORMAL, pingResp);    
    }
    
    public PartitionGroupServer waitRoleMaster(List<PartitionGroupServer> pgsList) throws Exception {
        MasterChecker condition = new MasterChecker(pgsList);
        
        // It expects that PGS '0' becomes a master.
        await("test for role master.").atMost(5, SECONDS).until(condition);

        // Mock up PGS '0' to be a MASTER
        mockSocket(condition.getMaster(), "+OK 2 1400000000");
        return condition.getMaster();
    }
    
    public void waitRoleSlave(PartitionGroupServer pgs) throws Exception {
        // It expects that PGS becomes a slave.
        await("test for role slave.").atMost(5, SECONDS).until(
                new RoleChecker(pgs, PGS_ROLE_SLAVE));

        // Mock up PGS to be a SLAVE
        mockSocket(pgs, "+OK 3 1400000000");
    }
    
    public void mockSocket(PartitionGroupServer pgs, String pingResp) throws IOException {
        BlockingSocket msock = mock(BlockingSocket.class);
        pgs.setServerConnection(msock);
        when(msock.execute(anyString())).thenReturn(S2C_OK);
        when(msock.execute("getseq log")).thenReturn(
                "+OK log min:0 commit:0 max:0 be_sent:0");
        when(msock.execute("ping")).thenReturn(pingResp);
    }
    
    private void mockHeartbeatResult(PartitionGroupServer pgs, String state, String pingResp) throws Exception {
        HBResult result = new HBResult(
                state, 
                pgs, 
                pingResp, 
                pgs.getData().getPmIp(),
                pgs.getData().getSmrMgmtPort(), 
                pgs.getHbc().getID());
        hbcProc.proc(result, false);
    }
    
}

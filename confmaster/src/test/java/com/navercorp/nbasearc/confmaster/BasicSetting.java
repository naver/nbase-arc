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
import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static java.util.concurrent.TimeUnit.SECONDS;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.Constant.Color;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResultProcessor;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSessionHandler;
import com.navercorp.nbasearc.confmaster.heartbeat.HeartbeatChecker;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.ClusterBackupScheduleDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkNotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.ClientSessionHandler.ReplyFormatter;
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
import com.navercorp.nbasearc.confmaster.server.mimic.MimicSMR;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

public class BasicSetting {

    @Autowired
    protected ApplicationContext context;
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected LeaderElectionHandler leaderElection;
    
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
    protected PartitionGroupDao pgDao;
    @Autowired
    protected PartitionGroupServerDao pgsDao;
    
    @Autowired
    protected ConfmasterService confmasterService;

    @Autowired 
    private HBResultProcessor hbcProc;

    ReplyFormatter replyFormatter = new ReplyFormatter();
    ObjectMapper objectMapper = new ObjectMapper();
    
    final protected long assertionTimeout = 5L;  
    
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
                Color.RED, pgData.currentGen(), HB_MONITOR_NO);
        pgsDatas[1] = new PartitionGroupServerData();
        pgsDatas[1].initialize(Integer.valueOf(pgName), pmName, pmData.getIp(),
                9109, 9100, 9103, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                Color.RED, pgData.currentGen(), HB_MONITOR_NO);
        pgsDatas[2] = new PartitionGroupServerData();
        pgsDatas[2].initialize(Integer.valueOf(pgName), pmName, pmData.getIp(),
                10109, 10100, 10103, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                Color.RED, pgData.currentGen(), HB_MONITOR_NO);
    }
    protected static PGSComponentMock[] pgsMockups = new PGSComponentMock[3];
    
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

        leaderElection.becomeLeader();

        joinLeave.initialize(context);
    }
    
    public void after() throws Exception {
        executor.release();
        
        confmasterService.release();
        
        LeaderState.init();
        
        zookeeper.deleteAllZNodeRecursive();
        
        zookeeper.release();
    }

    public JobResult tryCommand(String line) throws Exception {
        Future<JobResult> future = commandExecutor.perform(line, null);
        JobResult result = future.get(timeout, TimeUnit.MILLISECONDS);
        return result;
    }
    
    public JobResult doCommand(String line) throws Exception {
        JobResult result = tryCommand(line);
        assertNotNull(result);
        return result;
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
        if (getPgs(index).getData().getHb().equals(HB_MONITOR_YES)) {
            joinLeave.pgsLeave(getPgs(index), getRs(index), pgsMockups[index], FORCED);
        }
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
        pgsMockups[id] = new PGSComponentMock(getPgs(id), getRs(id)); 

        // Mock Heartbeat Session Handler
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        getPgs(id).getHbc().setHandler(handler);
    }

    public void joinPgs(int id) throws Exception {
        joinLeave.pgsJoin(getPgs(id), getRs(id), pgsMockups[id]);
    }
    
    public void testRoleNone(PartitionGroupServer pgs) throws Exception {
        // Mock up PGS '0' to be in LCONN state
        final String pingResp = "";
        mockSocket(pgs, pingResp);
        mockHeartbeatResult(pgs, PGS_ROLE_NONE, pingResp);
        
        // It expects that PGS '0' becomes a master.
        await("failure detected - check role").atMost(assertionTimeout, SECONDS).until(new RoleValidator(pgs, PGS_ROLE_NONE));
        await("failure detected - check color.").atMost(assertionTimeout, SECONDS).until(new ColorValidator(pgs, RED));

        // Mock up PGS '0' to be a MASTER
        mockSocket(pgs, "");
    }

    public void mockPgsNone(PartitionGroupServer pgs) throws Exception {
        // Mock up PGS '0' to be in LCONN state
        final String pingResp = "";
        mockSocket(pgs, pingResp);
        mockHeartbeatResult(pgs, PGS_ROLE_NONE, pingResp);    
    }
    
    public void mockPgsLconn(PartitionGroupServer pgs) throws Exception {
        // Mock up PGS '0' to be in LCONN state
        final String pingResp = "+OK 1 1400000000";
        mockSocket(pgs, pingResp);
        mockHeartbeatResult(pgs, PGS_ROLE_LCONN, pingResp);    
    }
    
    public PartitionGroupServer waitRoleMaster(List<PartitionGroupServer> pgsList) throws Exception {
        MasterFinder condition = new MasterFinder(pgsList);
        
        // It expects that PGS '0' becomes a master.
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(condition);

        // Mock up PGS '0' to be a MASTER
        mockSocket(condition.getMaster(), "+OK 2 1400000000");
        return condition.getMaster();
    }
    
    public void waitRoleSlave(PartitionGroupServer pgs) throws Exception {
        // It expects that PGS becomes a slave.
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(
                new RoleValidator(pgs, PGS_ROLE_SLAVE));

        // Mock up PGS to be a SLAVE
        mockSocket(pgs, "+OK 3 1400000000");
    }
    
    public void mockSocket(PartitionGroupServer pgs, final String pingResp) throws IOException {
        BlockingSocketImpl msock = mock(BlockingSocketImpl.class);
        pgs.setServerConnection(msock);
        when(msock.execute(anyString())).thenAnswer(new Answer<String>() {
            Integer quorum = new Integer(0);

            public String answer(InvocationOnMock invocation) throws Throwable {
                String cmd = (String) invocation.getArguments()[0];
                if (cmd.equals("getquorum")) {
                    return String.valueOf(quorum);
                } else if (cmd.split(" ")[0].equals("setquorum")) {
                    quorum = Integer.parseInt(cmd.split(" ")[1]);
                    return S2C_OK;
                } else if (cmd.equals("getseq log")) {
                    return "+OK log min:0 commit:0 max:0 be_sent:0";
                } else if (cmd.equals(PGS_PING)) {
                    return pingResp;
                } else if (cmd.equals("smrversion")) {
                    return "+OK 201";
                }
                return S2C_OK;
            }
        });
        
        BlockingSocketImpl msockRs = mock(BlockingSocketImpl.class);
        RedisServer rs = getRs(Integer.valueOf(pgs.getName()));
        rs.setServerConnection(msockRs);
        when(msockRs.execute(REDIS_PING)).thenReturn(REDIS_PONG);
        when(msockRs.execute(REDIS_REP_PING)).thenReturn(REDIS_PONG);
    }
    
    protected MimicSMR mimic(PartitionGroupServer pgs) {
        MimicSMR m = new MimicSMR();
        BlockingSocketImpl msock = mock(BlockingSocketImpl.class);
        try {
            when(msock.execute(anyString())).thenAnswer(m);
        } catch (IOException e) {
        }
        pgs.setServerConnection(msock);
        
        try {
            BlockingSocketImpl msockRs = mock(BlockingSocketImpl.class);
            RedisServer rs = getRs(Integer.valueOf(pgs.getName()));
            rs.setServerConnection(msockRs);
            when(msockRs.execute(REDIS_PING)).thenReturn(REDIS_PONG);
            when(msockRs.execute(REDIS_REP_PING)).thenReturn(REDIS_PONG);
        } catch (IOException e) {
        }
        
        return m;
    }
    
    protected void mockHeartbeatResult(PartitionGroupServer pgs, String role, String pingResp) throws Exception {
        HBResult result = new HBResult(
                role, 
                pgs, 
                pingResp, 
                pgs.getData().getPmIp(),
                pgs.getData().getSmrMgmtPort(), 
                pgs.getHbc().getID());
        hbcProc.proc(result, false);
    }
    
    public String formatReply(JobResult result, String leader)
            throws JsonProcessingException, IOException {
        String json = replyFormatter.get(result, leader);
        if (json.startsWith("{")) {
            objectMapper.readTree(json);
        }
        return json;
    };

    protected void validateNormal(PartitionGroupServer pgs, PartitionGroup pg, String role, int mgen) {
        assertEquals(GREEN, pgs.getData().getColor());
        assertEquals(role, pgs.getData().getRole());
        assertEquals(SERVER_STATE_NORMAL, pgs.getData().getState());
        assertEquals(HB_MONITOR_YES, pgs.getData().getHb());
        assertEquals(mgen, pgs.getData().getMasterGen());
        assertEquals(mgen, pg.getData().currentGen());
    }
    
    public static class RoleColorValidator implements Callable<Boolean> {
        private final PartitionGroupServer pgs;
        private final String role;
        private final Color color;
        
        public RoleColorValidator(PartitionGroupServer pgs, String role, Color color) {
            this.pgs = pgs;
            this.role = role;
            this.color = color;
        }
        
        public Boolean call() throws Exception {
            if (pgs.getData().getRole().equals(role)
                    && pgs.getData().getColor() == color) {
                return true;
            }
            return false;
        }
    } 
    
    public static class MasterFinder implements Callable<Boolean> {
        private final List<PartitionGroupServer> pgsList;
        private PartitionGroupServer master;
        
        public MasterFinder(List<PartitionGroupServer> pgsList) {
            this.pgsList = pgsList;
        }
        
        public Boolean call() throws Exception {
            for (PartitionGroupServer pgs : pgsList) {
                if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)
                        && pgs.getData().getColor() == GREEN) {
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
    
    public static class SlaveFinder implements Callable<Boolean> {
        private final List<PartitionGroupServer> pgsList;
        private final Set<PartitionGroupServer> slaves;
        
        public SlaveFinder(List<PartitionGroupServer> pgsList) {
            this.pgsList = pgsList;
            this.slaves = new HashSet<PartitionGroupServer>();
        }
        
        public Boolean call() throws Exception {
            for (PartitionGroupServer pgs : pgsList) {
                if (pgs.getData().getRole().equals(PGS_ROLE_SLAVE)
                        && pgs.getData().getColor() == GREEN) {
                    addSlave(pgs);
                }
            }
            return slaves.size() > 0;
        }
        
        public boolean isSlave(PartitionGroupServer hint) {
            if (slaves.contains(hint)) {
                return true;
            }
            return false;
        }
        
        public Set<PartitionGroupServer> getSlaves() {
            return slaves;
        }

        private void addSlave(PartitionGroupServer slave) {
            slaves.add(slave);
        }
    }
    
    public static class RoleValidator implements Callable<Boolean> {
        private final PartitionGroupServer pgs;
        private final String role;
        
        public RoleValidator(PartitionGroupServer pgs, String role) {
            this.pgs = pgs;
            this.role = role;
        }
        
        public Boolean call() throws Exception {
            return pgs.getData().getRole().equals(role);
        }
    }

    public static class ColorValidator implements Callable<Boolean> {
        private final PartitionGroupServer pgs;
        private final Color color;
        
        public ColorValidator(PartitionGroupServer pgs, Color color) {
            this.pgs = pgs;
            this.color = color;
        }
        
        public Boolean call() throws Exception {
            return pgs.getData().getColor().equals(color);
        }
    }
    
    public static class StateValidator implements Callable<Boolean> {
        private final PartitionGroupServer pgs;
        private final String state;
        
        public StateValidator(PartitionGroupServer pgs, String state) {
            this.pgs = pgs;
            this.state = state;
        }
        
        public Boolean call() throws Exception {
            System.out.println("m(" + pgs.getData().getState() + "), e(" + state + ")");
            return pgs.getData().getState().equals(state);
        }
    }
    
    public static class QuorumValidator implements Callable<Boolean> {
        private final MimicSMR master;
        private final int quorum;
        
        public QuorumValidator(MimicSMR master, int quorum) {
            this.master = master;
            this.quorum = quorum;
        }
        
        public Boolean call() throws Exception {
            return quorum == Integer.valueOf(master.execute("getquorum"));
        }
    }
    
}

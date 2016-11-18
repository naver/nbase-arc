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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant.Color;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResultProcessor;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSessionHandler;
import com.navercorp.nbasearc.confmaster.heartbeat.HeartbeatChecker;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.ClientSessionHandler.ReplyFormatter;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster.ClusterData;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponent;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway.GatewayData;
import com.navercorp.nbasearc.confmaster.server.cluster.GatewayLookup;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentMock;
import com.navercorp.nbasearc.confmaster.server.cluster.PGSJoinLeaveSetting;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine.PhysicalMachineData;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.command.CommandExecutor;
import com.navercorp.nbasearc.confmaster.server.command.ConfmasterService;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionHandler;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mimic.MimicSMR;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;

public class BasicSetting {

    @Autowired
    protected ApplicationContext context;
    @Autowired
    protected ZooKeeperHolder zk;
    
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
    protected ClusterComponentContainer container;    

    @Autowired
    protected GatewayLookup gwInfoNotifier;
    @Autowired
    protected WorkflowLogger workflowLogger;
    
    @Autowired
    protected ConfmasterService confmasterService;

    @Autowired 
    private HBResultProcessor hbcProc;

    ReplyFormatter replyFormatter = new ReplyFormatter();
    ObjectMapper objectMapper = new ObjectMapper();
    
    final protected long assertionTimeout = 10L;  
    
    final protected String ok = S2C_OK;
    final protected long timeout = 10000000L;
    final protected int MAX_PGS = 3;
    protected PGSJoinLeaveSetting joinLeave = new PGSJoinLeaveSetting();

    // Physical machine
    protected static String pmName = "test01.arc";
    protected static PhysicalMachineData pmData = new PhysicalMachineData("127.0.0.1");
    
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
        
        clusterData = new ClusterData(quorumPolicy, slot, CLUSTER_ON);
    }
    
    // Partition Group
    protected static String pgName = "0";
    protected static PartitionGroupData pgData = new PartitionGroupData();
    static {
        pgData = new PartitionGroupData();
    }
    
    // Partition Group Server 1
    protected static String[] pgsNames = new String[]{"0", "1", "2"};
    protected static PartitionGroupServerData[] pgsDatas = new PartitionGroupServerData[3];
    static {
    	/*
		String pgId, String pmName, String pmIp, int backendPort, int basePort
    	 */
        pgsDatas[0] = new PartitionGroupServerData(pgName, pmName, pmData.ip, 8100, 8109);
        pgsDatas[1] = new PartitionGroupServerData(pgName, pmName, pmData.ip, 9100, 9109);
        pgsDatas[2] = new PartitionGroupServerData(pgName, pmName, pmData.ip, 10100, 10109);
    }
    protected static ClusterComponentMock[] pgsMockups = new ClusterComponentMock[3];
    
    // Gateway
    protected static String gwName = "1";
    protected static GatewayData gwData;
    static {
        gwData = new GatewayData(pmName, pmData.ip, 6000);
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
        zk.initialize();
        heartbeatChecker.initialize();
        
        commandExecutor.initialize();
        workflowExecutor.initialize();
        
        zk.deleteAllZNodeRecursive();
        
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
        
        zk.deleteAllZNodeRecursive();
        
        zk.release();
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
        
        PhysicalMachine pm = container.getPm(pmName);
        assertNotNull("check pm after pm_add.", pm);
        assertEquals("check pm data.", pmIp, pm.getIp());
    }
    
    public void pmDel(String pmName) throws Exception {
        JobResult result = doCommand("pm_del " + pmName);
        assertEquals("perform pm_del.", ok, result.getMessages().get(0));
        
        PhysicalMachine pm = container.getPm(pmName);
        assertNull("check pm after pm_del.", pm);
    }
    
    public void pgAdd(String pgId, String clusterName) throws Exception{
        String command = "";
        for (String s : new String[]{"pg_add", clusterName, pgId}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of pg_add", ok, result.getMessages().get(0));
        
        PartitionGroup pg = container.getPg(clusterName, pgId);
        assertNotNull("check pg after pg_add", pg);
    }
    
    public void pgDel(String pgId, String clusterName) throws Exception {
        JobResult result = doCommand("pg_del " + clusterName + " " + pgId);
        assertEquals("check result of pg_del", ok, result.getMessages().get(0));
        assertNull("check pg deleted", container.getPg(clusterName, pgId));
    }
    
    public void pgsAdd(String pgsId, String pgId, String clusterName, int port) throws Exception {
        String command = "";
        for (String s : new String[]{"pgs_add", clusterName, pgsId, pgId, pmName, pmData.ip, String.valueOf(port), String.valueOf(port + 9)}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of pgs_add", ok, result.getMessages().get(0));
        
        PartitionGroupServer pgs = container.getPgs(clusterName, pgsId);
        assertNotNull(pgs);
        
        PhysicalMachineCluster pmCluster = container.getPmc(pmName, clusterName);
        assertTrue("Check pm-cluster contains this pgs.", 
                pmCluster.getPgsIdList().contains(Integer.valueOf(pgsId)));
    }

    public void pgsDel(String pgsId, boolean isLastPgs) throws Exception {
        JobResult result = doCommand("pgs_del " + clusterName + " " + pgsId);
        assertEquals("check result of pgs_del", ok, result.getMessages().get(0));
        assertNull("check pgs deleted", container.getPgs(clusterName, pgsId));
        
        if (!isLastPgs) {
            PhysicalMachineCluster pmCluster = container.getPmc(pmName, clusterName);
            assertFalse("Check pm-cluster does not contains this pgs.", 
                    pmCluster.getPgsIdList().contains(Integer.valueOf(pgsId)));
        }
    }
    
    public void gwAdd(String gwId, String clusterName) throws Exception {
        String command = "";
        for (String s : new String[]{"gw_add", clusterName, gwId, pmName, pmData.ip, "6000"}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of gw_add", ok, result.getMessages().get(0));
        
        Gateway gateway = container.getGw(clusterName, gwId);
        assertNotNull(gateway);
        
        PhysicalMachineCluster pmCluster = container.getPmc(pmName, clusterName);
        assertTrue("Check pm-cluster contains this gw.", 
                pmCluster.getGwIdList().contains(Integer.valueOf(gwId)));
    }

    public void gwDel(String gwId, String clusterName) throws Exception {
        JobResult result = doCommand("gw_del " + clusterName + " " + gwId);
        assertEquals("check result of gw_del", ok, result.getMessages().get(0));
        assertNull("check gateway deleted", container.getGw(clusterName, gwId));
        
        // TODO : test for ClusterInPhysicalMachine, whether it is deleted, when it contains no gw and no pgs. 
        PhysicalMachineCluster pmCluster = container.getPmc(pmName, clusterName);
        if (pmCluster != null) {
            assertFalse("Check pm-cluster does not contains this gw.", 
                    pmCluster.getGwIdList().contains(Integer.valueOf(gwId)));
        }
    }
    
    public void clusterAdd(String clusterName, String quorumPolicy) throws Exception {
        JobResult result = doCommand("cluster_add " + clusterName + " " + quorumPolicy);
        assertEquals("perform cluster_add.", ok, result.getMessages().get(0));
        
        Cluster cluster = container.getCluster(clusterName);
        assertNotNull("check cluster after cluster_add.", cluster);
        List<Integer> qpExpected = new ArrayList<Integer>();
        for (String quorum : quorumPolicy.split(":")) {
            qpExpected.add(Integer.valueOf(quorum));
        }
        assertEquals("check cluster data.", qpExpected, 
                cluster.getQuorumPolicy());
    }
    
    public void clusterDel(String clusterName) throws Exception {
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("check result of cluster_del", ok, result.getMessages().get(0));
        assertNull("check cluster deleted.", container.getCluster(clusterName));
    }
    
    public void slotSetPg(String clusterName, String pgName) throws Exception {
        String command = "";
        for (String s : new String[]{"slot_set_pg", clusterName, "0:8191", pgName}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of slot_set_pg", ok, result.getMessages().get(0));
        
        Cluster cluster = container.getCluster(clusterName);
        List<Integer> slot = new ArrayList<Integer>();
        for (int i = 0; i < 8192; i++) {
            slot.add(0);
        }
        assertEquals("check slot of pg after slot_set_pg", slot, cluster.getPnPgMap());
    }
    
    public void createPm() throws Exception {
        pmAdd(pmName, pmData.ip);
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
        return container.getCluster(clusterName);
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
                String.valueOf(pgsDatas[index].pgId), 
                clusterName, 
                pgsDatas[index].smrBasePort);
    }
    
    public void deletePgs(int index, boolean isLastPgs) throws Exception {
        if (getPgs(index).getHeartbeat().equals(HB_MONITOR_YES)) {
            joinLeave.pgsLeave(getPgs(index), getRs(index), pgsMockups[index], FORCED);
        }
        pgsDel(pgsNames[index], isLastPgs);
    }
    
    public PartitionGroup getPg() {
        return container.getPg(clusterName, pgName);
    }
    
    public PartitionGroupServer getPgs(int index) {
        return container.getPgs(clusterName, pgsNames[index]);
    }
    
    public List<PartitionGroupServer> getPgsList() {
        return container.getPgsList(clusterName);
    }
    
    public void setPgs(PartitionGroupServer pgs) throws SecurityException,
            NoSuchFieldException, IllegalArgumentException,
            IllegalAccessException {
        Field containerField = ClusterComponentContainer.class.getDeclaredField("map");
        containerField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ClusterComponent> m = 
                (Map<String, ClusterComponent>) containerField.get(container);
        m.put(pgs.getPath(), pgs);
    }
    
    public RedisServer getRs(int index) {
        return container.getRs(clusterName, pgsNames[index]);
    }
    
    public Gateway getGw() {
        return container.getGw(clusterName, gwName);
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
        pgsMockups[id] = new ClusterComponentMock(getPgs(id), getRs(id)); 

        // Mock Heartbeat Session Handler
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        ClusterComponentMock.getHeartbeatSession(getPgs(id)).setHandler(handler);
    }

    public void joinPgs(int id) throws Exception {
        joinLeave.pgsJoin(getPgs(id), getRs(id), pgsMockups[id]);
    }
    
    protected MimicSMR mimic(PartitionGroupServer pgs) {
        MimicSMR m = new MimicSMR();
        BlockingSocketImpl msock = mock(BlockingSocketImpl.class);
        try {
            when(msock.execute(anyString())).thenAnswer(m);
        } catch (IOException e) {
        }
        ClusterComponentMock.setConnectionForCommand(pgs, msock);
        
        try {
            BlockingSocketImpl msockRs = mock(BlockingSocketImpl.class);
            RedisServer rs = getRs(Integer.valueOf(pgs.getName()));
            ClusterComponentMock.setConnectionForCommand(rs, msockRs);
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
                pgs.getPmIp(),
                pgs.getSmrMgmtPort(), 
                ClusterComponentMock.getHeartbeatSession(pgs).getID());
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
        assertEquals(GREEN, pgs.getColor());
        assertEquals(role, pgs.getRole());
        assertEquals(SERVER_STATE_NORMAL, pgs.getState());
        assertEquals(HB_MONITOR_YES, pgs.getHeartbeat());
        assertEquals(mgen + 1, pgs.getMasterGen());
        assertEquals(mgen, pg.currentGen());
    }
    
    /*
     * Make cluster with 1 PG and 3 PGS
     */
    protected MimicSMR[] setupCluster() throws Exception {
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
        assertEquals(GREEN, p1.getColor());
        assertEquals(PGS_ROLE_MASTER, p1.getRole());
        assertEquals(SERVER_STATE_NORMAL, p1.getState());
        assertEquals(HB_MONITOR_YES, p1.getHeartbeat());
        assertEquals(1, p1.getMasterGen());
        assertEquals(0, pg.currentGen());
        QuorumValidator quorumValidator = new QuorumValidator(mimics[0], 0); 
        await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);
        
        // Create slaves
        for (int pgsIdx = 1; pgsIdx < 3; pgsIdx++) {
            createPgs(pgsIdx);
            mockPgs(pgsIdx);
            joinPgs(pgsIdx);
            PartitionGroupServer p2 = getPgs(pgsIdx);
            mimics[pgsIdx] = mimic(p2);
            mimics[pgsIdx].init();
            mockHeartbeatResult(p2, PGS_ROLE_LCONN, mimics[pgsIdx].execute(PGS_PING));
    
            RoleColorValidator slaveCond = new RoleColorValidator(p2, PGS_ROLE_SLAVE, GREEN);
            await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveCond);
            assertEquals(GREEN, p2.getColor());
            assertEquals(PGS_ROLE_SLAVE, p2.getRole());
            assertEquals(SERVER_STATE_NORMAL, p2.getState());
            assertEquals(HB_MONITOR_YES, p2.getHeartbeat());
            assertEquals(1, p2.getMasterGen());
            assertEquals(0, pg.currentGen());
            quorumValidator = new QuorumValidator(mimics[0], pgsIdx); 
            await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);
            
            // Master must not be changed
            assertEquals(p1, masterCond.getMaster());
            assertEquals(GREEN, p1.getColor());
            assertEquals(PGS_ROLE_MASTER, p1.getRole());
            assertEquals(SERVER_STATE_NORMAL, p1.getState());
            assertEquals(HB_MONITOR_YES, p1.getHeartbeat());
            assertEquals(1, p1.getMasterGen());
            assertEquals(0, pg.currentGen());
        }
        
        return mimics;
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
            if (pgs.getRole().equals(role)
                    && pgs.getColor() == color) {
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
                if (pgs.getRole().equals(PGS_ROLE_MASTER)
                        && pgs.getColor() == GREEN) {
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
                if (pgs.getRole().equals(PGS_ROLE_SLAVE)
                        && pgs.getColor() == GREEN) {
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
            return pgs.getRole().equals(role);
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
            return pgs.getColor().equals(color);
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
            System.out.println("m(" + pgs.getState() + "), e(" + state + ")");
            return pgs.getState().equals(state);
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

    public static class MasterGenValidator implements Callable<Boolean> {
        final PartitionGroupServer pgs;
        final int mGen; 

        public MasterGenValidator(PartitionGroupServer pgs, int mGen) {
            this.pgs = pgs;
            this.mGen = mGen;
        }

        public Boolean call() throws Exception {
            if (pgs.getMasterGen() == mGen) {
                return true;
            }
            return false;
        }
    }
    
}

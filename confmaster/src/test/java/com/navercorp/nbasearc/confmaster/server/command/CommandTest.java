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

package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.Constant.GW_PING;
import static com.navercorp.nbasearc.confmaster.Constant.GW_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_NO;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_YES;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_MASTER;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_NONE;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.REDIS_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.REDIS_REP_PING;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_NORMAL;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_UNKNOWN;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_BLOCKER;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_CRITICAL;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MAJOR;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MINOR;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MODERATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
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
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandWrongArgumentException;
import com.navercorp.nbasearc.confmaster.heartbeat.HBRefData;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSessionHandler;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterBackupSchedule;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class CommandTest extends BasicSetting {
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }
    
    @Override
    @Before
    public void before() throws Exception {
        super.before();
        createPm();
        createCluster();
        createPg();
        for (int i = 0; i < MAX_PGS; i++) {
            createPgs(i);
        }
        MockitoAnnotations.initMocks(this);
    }

    @Override
    @After
    public void after() throws Exception {
        super.after();
    }
    
    @Test
    public void appData() throws Exception {
        // Set
        int backupScheduleId = 1;
        JobResult result = doCommand("appdata_set " + clusterName + " backup " + backupScheduleId + 
                " 1 0 2 * * * * 02:00:00 3 70 base32hex rsync -az {FILE_PATH} 192.168.0.1::TEST/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.json");
        assertEquals("check appdata_set result.", ok, result.getMessages().get(0));
        ClusterBackupSchedule backupSchedule = 
                clusterBackupScheduleDao.loadClusterBackupSchedule(clusterName);
        assertFalse("check appdata after appdata_set", backupSchedule.getBackupSchedules().isEmpty());
        
        // Get
        result = doCommand("appdata_get " + clusterName + " backup all");
        String expected = backupSchedule.toString();
        assertEquals("check appdata_get result.", expected, result.getMessages().get(0));
        
        // Del
        result = doCommand("appdata_del " + clusterName + " backup " + backupScheduleId);
        assertEquals("check appdata_del resutl.", ok, result.getMessages().get(0));
        backupSchedule = clusterBackupScheduleDao.loadClusterBackupSchedule(clusterName);
        assertTrue("check appdata after appdata_del", backupSchedule.getBackupSchedules().isEmpty());
    }
    
    @Test
    public void pmAdd() throws Exception {
        pmAdd("pm_add_01.test", "127.0.0.121");
        pmAdd("pm_add_02.test", "127.0.0.122");
    }
    
    @Test
    public void pmDel() throws Exception {
        pmAdd("pm_add_01.test", "127.0.0.121");
        pmAdd("pm_add_02.test", "127.0.0.122");
        pmDel("pm_add_01.test");
        PhysicalMachine pm = pmImo.get("pm_add_02.test");
        assertNotNull("check pm after deleting another pm.", pm);
        pmDel("pm_add_02.test");
    }
    
    @Test
    public void clusterAdd() throws Exception {
        clusterAdd("c1", "0:1");
        clusterAdd("c2", "0:1:2");
        clusterAdd("c3", "0:1:2:3");
        clusterAdd("c4", "0:1:2:3:4");
        clusterAdd("c5", "0:1:5");
    }
    
    @Test
    public void clusterAddWrongQuorum() throws Exception {
        JobResult result = doCommand("cluster_add c1 0:1:0");
        assertEquals("perform cluster_add with illegal artuments.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof MgmtCommandWrongArgumentException);
    }
    
    @Test
    public void clusterDel() throws Exception {
        clusterAdd("c1", "0:1");
        clusterDel("c1");
    }
    
    @Test
    public void clusterDelNotExisting() throws Exception {
        JobResult result = doCommand("cluster_del c1");
        assertEquals("perform cluster_del not existing.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);
    }

    @Test
    public void clusterDelHasPg() throws Exception {
        for (int i = 0; i < MAX_PGS; i++) {
            deletePgs(i, (i == MAX_PGS - 1) ? true : false);
        }
        
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("perform cluster_del not empty.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);
    }

    @Test
    public void clusterDelHasPgs() throws Exception {
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("perform cluster_del not empty.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);
    }

    @Test
    public void clusterDelHasGw() throws Exception {
        gwAdd("10", clusterName);
        
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("perform cluster_del not empty.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);
    }
    
    @Test
    public void clusterInfo() throws Exception {
        Cluster cluster = clusterImo.get(clusterName);
        assertNotNull(cluster);
        
        JobResult result = doCommand("cluster_info " + clusterName);
        
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, Object> info = mapper.readValue(
                result.getMessages().get(0), 
                new TypeReference<HashMap<String,Object>>() {});
        HashMap<String, Object> clusterInfo = (HashMap<String, Object>)info.get("cluster_info"); 
        List<HashMap<String, Object>> pgInfo = (List<HashMap<String, Object>>)info.get("pg_list");
        List<HashMap<String, Object>> gwInfo = (List<HashMap<String, Object>>)info.get("gw_id_list");
        
        assertEquals(8192, clusterInfo.get("Key_Space_Size"));
        assertEquals("[0, 1]", clusterInfo.get("Quorum_Policy"));
        assertEquals("0 8192", clusterInfo.get("PN_PG_Map"));
        assertEquals(1, pgInfo.size());
        List<Integer> pgsIdList = new ArrayList<Integer>();
        pgsIdList.add(0);
        pgsIdList.add(1);
        pgsIdList.add(2);
        assertEquals(pgsIdList, ((HashMap<String, Object>)pgInfo.get(0).get("pg_data")).get("pgs_ID_List"));
        assertEquals(0, gwInfo.size());
    }
    
    @Test
    public void clusterList() throws Exception {
        JobResult result = doCommand("cluster_ls");
        assertEquals("check result of cluster_ls", "{\"list\":[\"test_cluster\"]}", result.getMessages().get(0));
    }
    
    @Test
    public void getClusterInfo() throws Exception {
        JobResult result = doCommand("get_cluster_info " + clusterName);
        String clusterInfo = "8192\r\n" + "0 8192\r\n" + "0";
        assertEquals("check result of get_cluster_info", 
                clusterInfo.trim(), 
                result.getMessages().get(0).trim());
    }
    
    @Test
    public void pgsAdd() throws Exception {
        pgsAdd("10", pgName, clusterName, 7010);
        pgsAdd("11", pgName, clusterName, 7020);
    }
    
    @Test
    public void pgsDel() throws Exception {
        pgsAdd("10", pgName, clusterName, 7010);
        pgsAdd("11", pgName, clusterName, 7020);
        pgsDel("10", false);
        pgsDel("11", true);
    }
    
    @Test
    public void gwAdd() throws Exception {
        gwAdd("10", clusterName);
        gwAdd("11", clusterName);
    }

    @Test
    public void gwAffinitySync() throws Exception {
        JobResult result = doCommand("gw_affinity_sync " + clusterName);
        assertFalse("check result of gw_affinity_sync", 
                result.getMessages().get(0).startsWith("-ERR"));
        assertTrue("check result of gw_affinity_sync", result.getExceptions().isEmpty());
    }
    
    @Test
    public void gwDel() throws Exception {
        gwAdd("10", clusterName);
        gwAdd("11", clusterName);
        gwDel("10", clusterName);
        gwDel("11", clusterName);
    }
    
    @Test
    public void gwInfo() throws Exception {
        String command = "";
        for (String s : new String[]{"gw_add", clusterName, "10", pmName, pmData.getIp(), "6000"}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of gw_add", ok, result.getMessages().get(0));
        
        result = doCommand("gw_info " + clusterName + " 10");
        GatewayData gwData = new GatewayData();
        gwData.initialize(pmName, pmData.getIp(), 6000, SERVER_STATE_FAILURE, HB_MONITOR_YES);
        assertEquals("check result of gw_info", gwData.toString(), result.getMessages().get(0));
    }
    
    @Test
    public void gwList() throws Exception {
        String command = "";
        for (String s : new String[]{"gw_add", clusterName, "10", pmName, pmData.getIp(), "6000"}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of gw_add", ok, result.getMessages().get(0));
        
        result = doCommand("gw_ls " + clusterName);
        String gwList = "{\"list\":[\"10\"]}";
        assertEquals("check result of gw_info", gwList, result.getMessages().get(0));
    }

    @Test
    public void mig2PC() throws Exception {
        // TODO : elaboration

        slotSetPg();
        
        // Create PG
        JobResult result = doCommand("pg_add " + clusterName + " 1");
        assertEquals("check result of pg_add", ok, result.getMessages().get(0));
        
        // Create GW
        String command = "";
        for (String s : new String[]{"gw_add", clusterName, "10", pmName, pmData.getIp(), "6000"}) {
            command += s + " ";
        }
        result = doCommand(command.trim());
        assertEquals("check result of gw_add", ok, result.getMessages().get(0));

        // Mock GW
        final Gateway gateway = gwImo.get("10", clusterName);
        assertNotNull(gateway);
        gateway.getData().setState(SERVER_STATE_NORMAL);
        
        BlockingSocket sock = mock(BlockingSocket.class);
        gateway.setServerConnection(sock);
        when(sock.execute(anyString())).thenReturn(S2C_OK);
        when(sock.execute(GW_PING)).thenReturn(GW_PONG);
        
        // Mock PGS
        final int id = 0;
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        getPgs(id).getHbc().setHandler(handler);

        PartitionGroupServerData pgsModified = 
            PartitionGroupServerData.builder().from(getPgs(id).getData())
                .withRole(PGS_ROLE_MASTER)
                .withState(SERVER_STATE_NORMAL).build();
        getPgs(id).setData(pgsModified);

        sock = mock(BlockingSocket.class);
        getPgs(id).setServerConnection(sock);
        when(sock.execute(anyString())).thenReturn(S2C_OK);
        when(sock.execute("ping")).thenReturn("+OK 1 1400000000");
        when(sock.execute("migrate info")).thenReturn("+OK log:1000 mig:1000 buf:1000 sent:1000 acked:1000");

        // Perform mig2pc
        result = doCommand("mig2pc " + clusterName + " 0 1 4095 8191");
        assertEquals("check result of mig2pc", ok, result.getMessages().get(0));
    }

    @Test
    public void pgAdd() throws Exception {
        String pgId = "10";
        pgAdd(pgId, clusterName);
        
        // Check master gen map
        PartitionGroup pg = pgImo.get(pgId, clusterName);
        assertTrue(pg.getData().getMasterGenMap().isEmpty());
        assertEquals(new Integer(0), pg.getData().getCopy());
        assertEquals(new Integer(0), pg.getData().getCopy());
    }

    @Test
    public void pgDel() throws Exception {
        String pgId = "10";

        pgAdd(pgId, clusterName);
        
        pgsAdd("10", pgId, clusterName, 7010);
        pgsAdd("11", pgId, clusterName, 7020);
        pgsDel("10", false);
        pgsDel("11", true);
        
        pgDel(pgId, clusterName);
    }

    @Test
    public void pgInfo() throws Exception {
        JobResult result = doCommand("pg_add " + clusterName + " 10");
        assertEquals("check result of pg_add", ok, result.getMessages().get(0));

        result = doCommand("pg_info " + clusterName + " 10");
        
        PartitionGroupData pgInfo = 
            PartitionGroupData.builder().from(new PartitionGroupData()).build();
        
        assertEquals("check result of pg_info", pgInfo.toString(), result.getMessages().get(0));
    }

    @Test
    public void pgList() throws Exception {
        JobResult result = doCommand("pg_ls " + clusterName);
        String pgList = "{\"list\":[\"0\"]}";
        assertEquals("check result of pg_ls", pgList, result.getMessages().get(0));
    }

    @Test
    public void pgsInfoAll() throws Exception {
        JobResult result = doCommand("pgs_info_all " + clusterName + " 0");
        
        PartitionGroupServerData pgsData = new PartitionGroupServerData();
        pgsData.initialize(0, pmName, pmData.getIp(), 8109, 8100, 8103,
                SERVER_STATE_FAILURE, PGS_ROLE_NONE, 0, HB_MONITOR_NO);
        HBRefData hbcRefData = new HBRefData();
        hbcRefData.setLastState(SERVER_STATE_UNKNOWN);
        hbcRefData.setLastStateTimestamp(0);
        hbcRefData.setSubmitMyOpinion(false);
        hbcRefData.setZkData(SERVER_STATE_FAILURE, 0, 0);
        String pgsInfoAll = "{\"MGMT\":" + pgsData + ",\"HBC\":" + hbcRefData + "}";
        assertEquals("check result of pgs_info_all", pgsInfoAll, result.getMessages().get(0));
    }

    @Test
    public void pgsInfo() throws Exception {
        JobResult result = doCommand("pgs_info " + clusterName + " 0");
        PartitionGroupServerData pgsData = new PartitionGroupServerData();
        pgsData.initialize(0, pmName, pmData.getIp(), 8109, 8100, 8103,
                SERVER_STATE_FAILURE, PGS_ROLE_NONE, 0, HB_MONITOR_NO);
        assertEquals("check result of pgs_info", pgsData.toString(), result.getMessages().get(0));
    }

    @Test
    public void pgsLconn() throws Exception {
        // TODO : elaboration
        int id = 0;
        setPgs(spy(getPgs(id)));
        
        // Mock Heartbeat Session Handler
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        getPgs(id).getHbc().setHandler(handler);

        BlockingSocket msock = mock(BlockingSocket.class);
        getPgs(id).setServerConnection(msock);
        when(msock.execute(anyString())).thenReturn(S2C_OK);
        
        JobResult result = doCommand("pgs_lconn " + clusterName + " " + id);
        assertEquals("check result of pgs_lconn", ok, result.getMessages().get(0));
    }

    @Test
    public void pgsList() throws Exception {
        JobResult result = doCommand("pgs_ls " + clusterName);
        String pgsList = "{\"list\":[\"0\", \"1\", \"2\"]}";
        assertEquals("check result of pgs_ls", pgsList, result.getMessages().get(0));
    }

    @Test
    public void pgsSync() throws Exception {
        // TODO : elaboration
        int id = 0;
        setPgs(spy(getPgs(id)));
        
        // Mock Heartbeat Session Handler
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        getPgs(id).getHbc().setHandler(handler);

        BlockingSocket msock = mock(BlockingSocket.class);
        getPgs(id).setServerConnection(msock);
        when(msock.execute(anyString())).thenReturn(S2C_OK);
        when(msock.execute("getseq log")).thenReturn(
                "+OK log min:0 commit:0 max:0 be_sent:0");
        when(msock.execute("ping")).thenReturn("+OK 1 1400000000");
        
        PartitionGroupServer pgs = getPgs(id);
        when(pgs.getRealState()).thenReturn(
                new PartitionGroupServer.RealState(false, 
                        SERVER_STATE_FAILURE, PGS_ROLE_NONE, 0L));
        
        JobResult result = doCommand("pgs_sync " + clusterName + " " + id);
        assertEquals("check result of pgs_sync", ok, result.getMessages().get(0));
    }

    @Test
    public void pmInfo() throws Exception {
        JobResult result = doCommand("pm_info " + pmName);
        PhysicalMachine pm = pmImo.get(pmName);
        String pmInfo = "{\"pm_info\":" + pmData + ", \"cluster_list\":" + pm.getClusterListString(pmClusterImo) + "}";
        assertEquals("check result of pm_info", pmInfo, result.getMessages().get(0));
    }
    
    @Test
    public void pmList() throws Exception {
        JobResult result = doCommand("pm_ls");
        String pmInfo = "{\"list\":[\"test01.arc\"]}";
        assertEquals("check result of pm_ls", pmInfo, result.getMessages().get(0));
    }

    @Test
    public void roleChange() throws Exception {
        // Mock PGS
        for (int id = 0; id < MAX_PGS; id++) {
            final HBSessionHandler handler = mock(HBSessionHandler.class);
            final PartitionGroupServer pgs = getPgs(id);
            doNothing().when(handler).handleResult(anyString(), anyString());
            pgs.getHbc().setHandler(handler);
            
            PartitionGroupServerData pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withRole(id == 0 ? PGS_ROLE_MASTER : PGS_ROLE_SLAVE)
                    .withState(SERVER_STATE_NORMAL)
                    .withHb(HB_MONITOR_YES).build();
            pgs.setData(pgsModified);
            
            BlockingSocket sock = mock(BlockingSocket.class);
            pgs.setServerConnection(sock);
            when(sock.execute(anyString())).thenReturn(S2C_OK);
            when(sock.execute("ping")).thenReturn("+OK 1 1400000000");
            when(sock.execute("getquorum")).thenReturn("1");
            when(sock.execute("getseq log")).thenReturn("+OK log min:0 commit:0 max:0 be_sent:0");
            
            final RedisServer rs = getRs(id);
            sock = mock(BlockingSocket.class);
            rs.setServerConnection(sock);
            when(sock.execute(REDIS_REP_PING)).thenReturn(REDIS_PONG);
        }
        
        // Perform role_change
        JobResult result = doCommand("role_change " + clusterName + " 1");
        String expected = "{\"master\":1,\"role_slave_error\":[]}";
        assertEquals("check result of role_change", expected, result.getMessages().get(0));
    }
    
    @Test
    public void slotSetPg() throws Exception {
        slotSetPg(clusterName, pgName);
    }

    @Test
    public void worklogDel() throws Exception {
        JobResult result = doCommand("worklog_del 10");
        assertEquals("check result of worklog_del", ok, result.getMessages().get(0));
    }

    @Test
    public void worklogGet() throws Exception {
        JobResult result = doCommand("worklog_get 1 1");
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, Object> worklog = mapper.readValue(
                result.getMessages().get(0), 
                new TypeReference<HashMap<String,Object>>() {});
        worklog = (HashMap<String, Object>)((ArrayList<Object>)worklog.get("list")).get(0);
        assertEquals(worklog.get("logID"), 1);
        assertTrue(worklog.get("severity").equals(SEVERITY_BLOCKER) || 
                worklog.get("severity").equals(SEVERITY_CRITICAL) || 
                worklog.get("severity").equals(SEVERITY_MAJOR) || 
                worklog.get("severity").equals(SEVERITY_MODERATE) || 
                worklog.get("severity").equals(SEVERITY_MINOR));
        assertNotNull(worklog.get("msg"));
    }

    @Test
    public void worklogHead() throws Exception {
        // TODO : elaboration
        JobResult result = doCommand("worklog_head 1");
        assertNotNull("check result of worklog_head", result.getMessages().get(0));
        assertTrue("check result of worklog_head", result.getExceptions().isEmpty());
    }

    @Test
    public void worklogInfo() throws Exception {
        // TODO : elaboration
        JobResult result = doCommand("worklog_info");
        assertNotNull("check result of worklog_info", result.getMessages().get(0));
        assertTrue("check result of worklog_info", result.getExceptions().isEmpty());
    }
    
    @Test
    public void clusterLifeCycle() throws Exception {
        final int MAX_THREAD = 3;
        final Thread[] threads = new Thread[MAX_THREAD];
        for (int threadId = 0; threadId < MAX_THREAD; threadId++) {
            
            Thread thread = new ClusterLifeCycleCommander(this);
            thread.start();
            threads[threadId] = thread;
        }
        
        for (Thread thread : threads) {
            thread.join();
        }
    }

    class Checker<T> implements Callable<Boolean> {
        T expected;
        JobResult result;
        
        public Checker(T expected, JobResult result) {
            this.expected = expected;
            this.result = result;
        }
        
        @Override
        public Boolean call() {
            return expected.equals(result.getMessages().get(0));
        }
    }
}

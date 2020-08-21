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

import static com.jayway.awaitility.Awaitility.await;
import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.GREEN;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.BasicSetting.MasterFinder;
import com.navercorp.nbasearc.confmaster.BasicSetting.QuorumValidator;
import com.navercorp.nbasearc.confmaster.BasicSetting.RoleColorValidator;
import com.navercorp.nbasearc.confmaster.BasicSetting.SlaveFinder;
import com.navercorp.nbasearc.confmaster.heartbeat.HBState;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSessionHandler;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterBackupSchedule;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway.GatewayData;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentMock;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.mimic.IInjector;
import com.navercorp.nbasearc.confmaster.server.mimic.MimicSMR;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class CommandTest extends BasicSetting {
    
    @Autowired
    ConfMaster confMaster;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }
    
    @Override
    @Before
    public void before() throws Exception {
        super.before();
        confMaster.setState(ConfMaster.RUNNING);
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
    	MemoryObjectMapper mapper = new MemoryObjectMapper(); 
    	
        // Set
        int backupScheduleId = 1;
        JobResult result = doCommand("appdata_set " + clusterName + " backup " + backupScheduleId + 
                " 1 0 2 * * * * 02:00:00 3 70 base32hex rsync -az {FILE_PATH} 192.168.0.1::TEST/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.json");
        assertEquals("check appdata_set result.", ok, result.getMessages().get(0));
        
        final String path = PathUtil.clusterBackupSchedulePath(clusterName);
		ClusterBackupSchedule backupSchedule = mapper.readValue(
				zk.getData(path, null), ClusterBackupSchedule.class); 
        assertFalse("check appdata after appdata_set", backupSchedule.getBackupSchedules().isEmpty());
        
        // Usage
        result = doCommand("appdata_set");
        assertEquals(
                "appdata_set <cluster_name> backup <backup_id> <daemon_id> <period> <base_time> <holding period(day)> <net_limit(MB/s)> <output_format> [<service url>]\r\n"
                        + "Ex) appdata_set c1 backup 1 1 0 2 * * * * 02:00:00 3 70 base32hex rsync -az {FILE_PATH} 192.168.0.1::TEST/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.json\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("appdata_set xx" + clusterName + "xx backup " + backupScheduleId + 
                " 1 0 2 * * * * 02:00:00 3 70 base32hex rsync -az {FILE_PATH} 192.168.0.1::TEST/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.json");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. xxtest_clusterxx\"}\r\n", 
                formatReply(result, null));
        
        // Get
        result = doCommand("appdata_get " + clusterName + " backup all");
        String expected = backupSchedule.toString();
        assertEquals("check appdata_get result.", expected, result.getMessages().get(0));

        // Usage
        result = doCommand("appdata_get");
        assertEquals("appdata_get <cluster_name> backup <backup_id>\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("appdata_get xx" + clusterName + "xx backup all");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. xx"
                        + clusterName + "xx\"}\r\n",
                formatReply(result, null));
        
        // Del
        result = doCommand("appdata_del " + clusterName + " backup " + backupScheduleId);
        assertEquals("check appdata_del resutl.", ok, result.getMessages().get(0));
		backupSchedule = mapper.readValue(
				zk.getData(PathUtil.clusterBackupSchedulePath(clusterName), null),
				ClusterBackupSchedule.class);
        assertTrue("check appdata after appdata_del", backupSchedule.getBackupSchedules().isEmpty());
        
        // Usage
        result = doCommand("appdata_del");
        assertEquals("appdata_del <cluster_name> backup <backup_id>\r\n"
                + "Ex) appdata_del c1 backup 1\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("appdata_del xx" + clusterName + "xx backup all");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. xx"
                        + clusterName + "xx\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void pmAdd() throws Exception {
        String pmName1 = "pm_add_01.test"; 
        
        // Add
        pmAdd(pmName1, "127.0.0.121");
        pmAdd("pm_add_02.test", "127.0.0.122");
        
        // Usage
        JobResult result = doCommand("pm_add");
        assertEquals("pm_add <pm_name> <pm_ip>\r\n"
                + "add a physical machine\r\n",
                formatReply(result, null));

        // Fail
        result = doCommand("pm_add " + pmName1 + " 127.0.0.121");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR duplicated pm\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void pmDel() throws Exception {
        pmAdd("pm_add_01.test", "127.0.0.121");
        pmAdd("pm_add_02.test", "127.0.0.122");
        pmDel("pm_add_01.test");
        PhysicalMachine pm = container.getPm("pm_add_02.test");
        assertNotNull("check pm after deleting another pm.", pm);
        
        // Del
        pmDel("pm_add_02.test");
        
        // Usage
        JobResult result = doCommand("pm_del");
        assertEquals("pm_del <pm_name>\r\n" + "delete a physical machine\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("pm_del 1111");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pm does not exist. pm:1111\"}\r\n",
                formatReply(result,  null));
    }
    
    @Test
    public void clusterAdd() throws Exception {
        String clusterName1 = "c1";
        
        // Add
        clusterAdd(clusterName1, "0:1");
        clusterAdd("c2", "0:1:2");
        clusterAdd("c3", "0:1:2:3");
        clusterAdd("c4", "0:1:2:3:4");
        clusterAdd("c5", "0:1:5");
        
        // Usage
        JobResult result = doCommand("cluster_add");
        assertEquals("cluster_add <cluster_name> <quorum policy>\r\n"
                + "Ex) cluster_add cluster1 0:1\r\n" + "add a new cluster\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("cluster_add " + clusterName1 + " 0:1");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR duplicated cluster\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void clusterAddWrongQuorum() throws Exception {
        JobResult result = doCommand("cluster_add c1 0:1:0");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR Invalid quorum policy\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void clusterDel() throws Exception {
        String clusterName = "c1";
        
        // Del
        clusterAdd(clusterName, "0:1");
        clusterDel(clusterName);
        
        // Usage
        JobResult result = doCommand("cluster_del");
        assertEquals("cluster_del <cluster_name>\r\n"
                + "Ex) cluster_del cluster1\r\n" + "delete a new cluster\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("cluster_del c1");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. c1\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void clusterDelNotExisting() throws Exception {
        JobResult result = doCommand("cluster_del c1");
        assertEquals("perform cluster_del not existing.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);

        result = doCommand("cluster_del c1");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. c1\"}\r\n",
                formatReply(result, null));
    }

    @Test
    public void clusterDelHasPg() throws Exception {
        for (int i = 0; i < MAX_PGS; i++) {
            deletePgs(i, (i == MAX_PGS - 1) ? true : false);
        }
        
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("perform cluster_del not empty.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR the cluster has a pg or more. "
                        + clusterName + "\"}\r\n",
                formatReply(result, null));
    }

    @Test
    public void clusterDelHasPgs() throws Exception {
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("perform cluster_del not empty.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);
    }

    @Test
    public void clusterDelHasGw() throws Exception {
        for (int i = 0; i < MAX_PGS; i++) {
            deletePgs(i, i == (MAX_PGS - 1));
        }
        deletePg();
        
        gwAdd("10", clusterName);
        
        JobResult result = doCommand("cluster_del " + clusterName);
        assertEquals("perform cluster_del not empty.", 1, result.getExceptions().size());
        assertTrue(result.getExceptions().get(0) instanceof IllegalArgumentException);
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR the cluster has a gw or more. "
                        + clusterName + "\"}\r\n", formatReply(result, null));
    }
    
    @Test
    public void clusterInfo() throws Exception {
        Cluster cluster = container.getCluster(clusterName);
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
        
        // Usage
        result = doCommand("cluster_info");
        assertEquals("cluster_info <cluster_name>\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("cluster_info xx" + clusterName);
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. xx"
                        + clusterName + "\"}\r\n", formatReply(result, null));
    }
    
    @Test
    public void clusterList() throws Exception {
        JobResult result = doCommand("cluster_ls");
        assertEquals("check result of cluster_ls",
                "{\"list\":[\"test_cluster\"]}", result.getMessages().get(0));
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
        
        // Usage
        JobResult result = doCommand("pgs_add");
        assertEquals("pgs_add <cluster_name> <pgsid> <pgid> <pm_name> <pm_ip> <base_port> <backend_port>\r\n", 
                formatReply(result, null));
        
        // Fail
        result = doCommand("pgs_add " + clusterName + " 10 " + pgName + " " + pmName
                + " 127.0.0.1 7000 7009");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR duplicated. cluster: "
                        + clusterName + ", pgs: " + 10 + "\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void pgsDel() throws Exception {
        pgsAdd("10", pgName, clusterName, 7010);
        pgsAdd("11", pgName, clusterName, 7020);
        pgsDel("10", false);
        pgsDel("11", true);
        
        // Usage
        JobResult result = doCommand("pgs_del");
        assertEquals("pgs_del <cluster_name> <pgsid>\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("pgs_del " + clusterName + " 99");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pgs does not exist. "
                + clusterName + "/pgs:99\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void gwAdd() throws Exception {
        gwAdd("10", clusterName);
        gwAdd("11", clusterName);
        
        // Usage
        JobResult result = doCommand("gw_add");
        assertEquals("gw_add <cluster_name> <gwid> <pm_name> <pm_ip> <port>\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("gw_add " + clusterName + " 10 " + pmName + " 127.0.0.1 6000");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR duplicated. cluster: "
                        + clusterName + ", gw: 10\"}\r\n",
                formatReply(result, null));
        
        result = doCommand("gw_add " + clusterName + " 12 local 127.0.0.1 6000");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pm does not exist. pm:local\"}\r\n",
                formatReply(result, null));
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
        
        // Usage
        JobResult result = doCommand("gw_del");
        assertEquals("gw_del <cluster_name> <gwid>\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("gw_del " + clusterName + " 10");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR gateway does not exist. "
                        + clusterName + "/gw:10\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void gwInfo() throws Exception {
        String command = "";
        for (String s : new String[]{"gw_add", clusterName, "10", pmName, pmData.ip, "6000"}) {
            command += s + " ";
        }
        JobResult result = doCommand(command.trim());
        assertEquals("check result of gw_add", ok, result.getMessages().get(0));
        
        result = doCommand("gw_info " + clusterName + " 10");
        
        GatewayData gwData = new GatewayData(pmName, pmData.ip, 6000);
        assertEquals("check result of gw_info", gwData.toString(), result.getMessages().get(0));
        
        // Usage
        result = doCommand("gw_info");
        assertEquals("gw_info <cluster_name> <gw_id>\r\n"
                + "get information of a Gateway\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("gw_info " + clusterName + " 99");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR gateway does not exist. "
                        + clusterName + "/gw:99\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void gwList() throws Exception {
        String command = "";
        for (String s : new String[]{"gw_add", clusterName, "10", pmName, pmData.ip, "6000"}) {
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
        for (String s : new String[]{"gw_add", clusterName, "10", pmName, pmData.ip, "6000"}) {
            command += s + " ";
        }
        result = doCommand(command.trim());
        assertEquals("check result of gw_add", ok, result.getMessages().get(0));

        // Mock GW
        final Gateway gateway = container.getGw(clusterName, "10");
        assertNotNull(gateway);
        GatewayData d = gateway.clonePersistentData();
        d.state = SERVER_STATE_NORMAL;
        gateway.setPersistentData(d);
        
        BlockingSocketImpl sock = mock(BlockingSocketImpl.class);
        ClusterComponentMock.setConnectionForCommand(gateway, sock);
        when(sock.execute(anyString())).thenReturn(S2C_OK);
        when(sock.execute(GW_PING)).thenReturn(GW_PONG);
        
        // Mock PGS
        final int id = 0;
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        ClusterComponentMock.getHeartbeatSession(getPgs(id)).setHandler(handler);

        PartitionGroupServerData pgsModified = getPgs(id).clonePersistentData();
        pgsModified.setRole(PGS_ROLE_MASTER);
        getPgs(id).setPersistentData(pgsModified);

        sock = mock(BlockingSocketImpl.class);
        ClusterComponentMock.setConnectionForCommand(getPgs(id), sock);
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
        PartitionGroup pg = container.getPg(clusterName, pgId);
        assertTrue(pg.getMasterGenMap().isEmpty());
        assertEquals(0, pg.getCopy());
        assertEquals(0, pg.getCopy());
        
        // Usage
        JobResult result = doCommand("pg_add");
        assertEquals(
                "pg_add <cluster_name> <pgid>\r\n" + 
                "add a single partition group\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("pg_add " + clusterName + " " + pgId);
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR duplicated pgid\"}\r\n",
                formatReply(result, null));
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
        
        // Usage
        JobResult result = doCommand("pg_del");
        assertEquals("pg_del <cluster_name> <pgid>\r\n" + 
                "delete a single partition group\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("pg_del " + clusterName + " 10");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pg does not exist. "
                + clusterName + "/pg:10\"}\r\n",
                formatReply(result, null));
    }

    @Test
    public void pgInfo() throws Exception {
        JobResult result = doCommand("pg_add " + clusterName + " 10");
        assertEquals("check result of pg_add", ok, result.getMessages().get(0));

        result = doCommand("pg_info " + clusterName + " 10");
        
        assertEquals("check result of pg_info", container.getPg(clusterName, "10")
                .info(), result.getMessages().get(0));
        
        // Usage
        result = doCommand("pg_info");
        assertEquals(
                "pg_info <cluster_name> <pg_id>\r\n" + 
                "get information of a Partition Group\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("pg_info " + clusterName + " 99");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pg does not exist. "
                + clusterName + "/pg:99\"}\r\n",
                formatReply(result, null));
        
        result = doCommand("pg_info " + clusterName + "xx 99");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. "
                        + clusterName + "xx\"}\r\n",
                formatReply(result, null));
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
        
        PartitionGroupServerData pgsData = new PartitionGroupServerData("0", pmName, pmData.ip, 8100, 8109);
        HBState hbcRefData = new HBState();
        hbcRefData.setLastState(pgsData.getRole());
        hbcRefData.setLastStateTimestamp(pgsData.stateTimestamp);
        hbcRefData.setSubmitMyOpinion(false);
        hbcRefData.setZkData(pgsData.getRole(), pgsData.stateTimestamp, 0);
        String pgsInfoAll = "{\"MGMT\":" + pgsData.toString() + ",\"HBC\":" + hbcRefData + "}";
        assertEquals("check result of pgs_info_all", pgsInfoAll, result.getMessages().get(0));

        // Usage
        result = doCommand("pgs_info_all");
        assertEquals("pgs_info_all <cluster_name> <pgs_id>\r\n"
                + "get all information of a Partition Group Server\r\n",
                formatReply(result, null));

        // Fail
        result = doCommand("pgs_info_all " + clusterName + " 99");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pgs does not exist. "
                + clusterName + "/pgs:99\"}\r\n",
                formatReply(result, null));
    }

    @Test
    public void pgsInfo() throws Exception {
        JobResult result = doCommand("pgs_info " + clusterName + " 0");
        PartitionGroupServerData pgsData = new PartitionGroupServerData("0", pmName, pmData.ip, 8100, 8109);
        assertEquals("check result of pgs_info", pgsData.toString(), result.getMessages().get(0));

        // Usage
        result = doCommand("pgs_info");
        assertEquals("pgs_info <cluster_name> <pgs_id>\r\n"
                + "get information of a Partition Group Server\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("pgs_info " + clusterName + " 99");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pgs does not exist. "
                + clusterName + "/pgs:99\"}\r\n",
                formatReply(result, null));
    }

    @Test
    public void pgsLconn() throws Exception {
        // TODO : elaboration
        int id = 0;
        setPgs(spy(getPgs(id)));
        
        // Mock Heartbeat Session Handler
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        ClusterComponentMock.getHeartbeatSession(getPgs(id)).setHandler(handler);

        BlockingSocketImpl msock = mock(BlockingSocketImpl.class);
        ClusterComponentMock.setConnectionForCommand(getPgs(id), msock);
        when(msock.execute(anyString())).thenReturn(S2C_OK);
        
        JobResult result = doCommand("pgs_lconn " + clusterName + " " + id);
        assertEquals("check result of pgs_lconn", ok, result.getMessages().get(0));
    }

    @Test
    public void pgsList() throws Exception {
        JobResult result = doCommand("pgs_ls " + clusterName);
        String pgsList = "{\"list\":[\"0\", \"1\", \"2\"]}";
        assertEquals("check result of pgs_ls", pgsList, result.getMessages().get(0));
        
        // Usage
        result = doCommand("pgs_ls");
        assertEquals("pgs_ls <cluster_name>\r\n"
                + "show a list of Partition Group Servers\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("pgs_ls aaaa");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. aaaa\"}\r\n",
                formatReply(result, null));
    }

    @Test
    public void pgsSync() throws Exception {
        // TODO : elaboration
        int id = 0;
        setPgs(spy(getPgs(id)));
        
        // Mock Heartbeat Session Handler
        final HBSessionHandler handler = mock(HBSessionHandler.class);
        doNothing().when(handler).handleResult(anyString(), anyString());
        ClusterComponentMock.getHeartbeatSession(getPgs(id)).setHandler(handler);

        BlockingSocketImpl msock = mock(BlockingSocketImpl.class);
        ClusterComponentMock.setConnectionForCommand(getPgs(id), msock);
        when(msock.execute(anyString())).thenReturn(S2C_OK);
        when(msock.execute("getseq log")).thenReturn(
                "+OK log min:0 commit:0 max:0 be_sent:0");
        when(msock.execute("ping")).thenReturn("+OK 1 1400000000");
        
        PartitionGroupServer pgs = getPgs(id);
        when(pgs.getRealState()).thenReturn(
                new PartitionGroupServer.RealState(false, 
                        SERVER_STATE_FAILURE, PGS_ROLE_NONE, 0L));
        
        JobResult result = doCommand("pgs_sync " + clusterName + " " + id + " " + FORCED);
        assertEquals("check result of pgs_sync", ok, result.getMessages().get(0));
    }

    @Test
    public void pmInfo() throws Exception {
        JobResult result = doCommand("pm_info " + pmName);
        PhysicalMachine pm = container.getPm(pmName);
        String pmInfo = "{\"pm_info\":" + pmData + ", \"cluster_list\":" + pm.getClusterListString(container) + "}";
        assertEquals("check result of pm_info", pmInfo, result.getMessages().get(0));
        
        // Usage
        result = doCommand("pm_info");
        assertEquals("pm_info <pm_name>\r\n"
                + "get information of a Physical Machine\r\n",
                formatReply(result, null));

        // Fail
        result = doCommand("pm_info aaaaaa");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR pm does not exist. pm:aaaaaa\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void pmList() throws Exception {
        JobResult result = doCommand("pm_ls");
        String pmInfo = "{\"list\":[\"test01.arc\"]}";
        assertEquals("check result of pm_ls", pmInfo, result.getMessages().get(0));
    }
    
    @Test
    public void slotSetPg() throws Exception {
        slotSetPg(clusterName, pgName);
        
        // Usage
        JobResult result = doCommand("slot_set_pg");
        assertEquals("slot_set_pg <cluster_name> <pg_range_inclusive> <pgid>\r\n" + 
                "Ex) slot_set_pg cluster1 0:8191 1\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("slot_set_pg aaaaa 0:8191 0");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. aaaaa\"}\r\n",
                formatReply(result, null));

        result = doCommand("slot_set_pg " + clusterName + " 0:8192 0");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR bad pg range:0:8192 (try to slot_set_pg)\"}\r\n",
                formatReply(result, null));

        result = doCommand("slot_set_pg " + clusterName + " 0:8191 99");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pg does not exist. "
                + clusterName + "/pg:99\"}\r\n", formatReply(result, null));
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
        
        // Usage
        result = doCommand("worklog_get");
        assertEquals("worklog_get <start log number> <end log number>\r\n"
                + "get workflow logs\r\n", formatReply(result, null));
    }

    @Test
    public void worklogHead() throws Exception {
        JobResult result = doCommand("worklog_head 1");
        assertNotNull("check result of worklog_head", result.getMessages().get(0));
        assertTrue("check result of worklog_head", result.getExceptions().isEmpty());
        
        // Usage
        result = doCommand("worklog_head");
        assertEquals("worklog_head <the number of logs>\r\n"
                + "get and delete workflow logs from beginning\r\n",
                formatReply(result, null));
    }

    @Test
    public void worklogInfo() throws Exception {
        // TODO : elaboration
        JobResult result = doCommand("worklog_info");
        assertNotNull("check result of worklog_info", result.getMessages().get(0));
        assertTrue("check result of worklog_info", result.getExceptions().isEmpty());
    }
    
    @Test
    public void opWf() throws Exception {
        JobResult result = doCommand("op_wf " + clusterName + " " + pgName + " RA false forced");
        assertEquals("{\"state\":\"success\",\"msg\":\"+OK\"}\r\n",
                formatReply(result, null));

        // Usage
        result = doCommand("op_wf");
        assertEquals("op_wf <cluster_name> <pg_id> <wf> <cascading> forced\r\n"
                + "wf: RA, QA, ME, YJ, BJ, MG\r\n",
                formatReply(result, null));
        
        // Fail
        result = doCommand("op_wf " + clusterName + " " + pgName + " AA false forced");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR not supported workflow.\"}\r\n",
                formatReply(result, null));
        
        result = doCommand("op_wf xx" + clusterName + " " + pgName + " AA false forced");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR cluster does not exist. xx" + clusterName + "\"}\r\n",
                formatReply(result, null));

        result = doCommand("op_wf " + clusterName + " " + "99" + " AA false forced");
        assertEquals("{\"state\":\"error\",\"msg\":\"-ERR pg does not exist. " + clusterName + "/pg:99\"}\r\n",
                formatReply(result, null));

        result = doCommand("op_wf " + clusterName + " " + pgName + " AA invalid forced");
        assertTrue(formatReply(result, null).startsWith("{\"state\":\"error\""));
        assertEquals(ConversionFailedException.class, result.getExceptions().get(0).getClass());
        
        result = doCommand("op_wf " + clusterName + " " + pgName + " AA false not-forced");
        assertEquals(
                "{\"state\":\"error\",\"msg\":\"-ERR not in forced mode.\"}\r\n",
                formatReply(result, null));
    }
    
    @Test
    public void clusterDump() throws Exception {
    }

    @Test
    public void clusterDumpAndLoad() throws Exception {
        final String cname = "test_cluster_1";
        final ObjectMapper mapper = new ObjectMapper();
        
        JobResult result = doCommand("cluster_dump " + clusterName);
        String dump = result.getMessages().get(0);
        System.out.println(dump);
        mapper.readTree(dump);
        
        dump = dump.replace(clusterName, cname).replace(pmName, "test02.arc");

        // Usage
        result = doCommand("cluster_dump");
        assertEquals("cluster_dump <cluster_name>\r\n",
                formatReply(result, null));
        
        result = doCommand("cluster_load " + dump);
        assertEquals(ok, result.getMessages().get(0));
        
        // Check
        assertNotNull(container.getCluster(cname));
        assertNotNull(container.getPg(cname, "0"));
        assertNotNull(container.getPgs(cname, "0"));
        assertNotNull(container.getPgs(cname, "1"));
        assertNotNull(container.getPgs(cname, "2"));
        assertNotNull(container.getPm("test02.arc"));        

        // Usage
        result = doCommand("cluster_load");
        assertEquals("cluster_load <dump>\r\n",
                formatReply(result, null));
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

    @Test
    public void clusterOff() throws Exception {
        MimicSMR[] mimics = new MimicSMR[MAX_PGS];
        PartitionGroup pg = getPg();

        // Create master
        mockPgs(0);
        joinPgs(0);
        PartitionGroupServer p0 = getPgs(0);
        mimics[0] = mimic(p0);
        mimics[0].init();
        mockHeartbeatResult(p0, PGS_ROLE_LCONN, mimics[0].execute(PGS_PING));

        MasterFinder masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        assertEquals(p0, masterCond.getMaster());
        assertEquals(SERVER_STATE_NORMAL, p0.getState());
        assertEquals(HB_MONITOR_YES, p0.getHeartbeat());
        assertEquals(1, p0.getMasterGen());
        assertEquals(0, pg.currentGen());
        QuorumValidator quorumValidator = new QuorumValidator(mimics[0], 0);
        await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);

        // Create slave
        for (int i = 1; i < 3; i++) {
            mockPgs(i);
            joinPgs(i);
            PartitionGroupServer slave = getPgs(i);
            mimics[i] = mimic(slave);
            mimics[i].init();
            mockHeartbeatResult(slave, PGS_ROLE_LCONN, mimics[i].execute(PGS_PING));

            RoleColorValidator slaveCond = new RoleColorValidator(slave, PGS_ROLE_SLAVE, GREEN);
            await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveCond);
            assertEquals(SERVER_STATE_NORMAL, slave.getState());
            assertEquals(HB_MONITOR_YES, slave.getHeartbeat());
            assertEquals(1, slave.getMasterGen());
            assertEquals(0, pg.currentGen());

            quorumValidator = new QuorumValidator(mimics[0], i);
            await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);

            // Master must not be changed
            assertEquals(p0, masterCond.getMaster());
            assertEquals(GREEN, p0.getColor());
            assertEquals(PGS_ROLE_MASTER, p0.getRole());
            assertEquals(SERVER_STATE_NORMAL, p0.getState());
            assertEquals(HB_MONITOR_YES, p0.getHeartbeat());
            assertEquals(1, p0.getMasterGen());
            assertEquals(0, pg.currentGen());
        }

        // inject fault
        for (int i = 1; i < 3; i++) {
            mimics[i].setInjector(new IInjector<String, String>() {
                @Override
                public String inject(String rqst) {
                    if (rqst.contains("role slave")) {
                        return "-ERR fault injection";
                    }
                    return null;
                }
            });
        }

        // pgs_leave
        Future<JobResult> future = commandExecutor.perform("pgs_leave " + p0.getClusterName() + " " + p0.getName(), null);
        assertEquals(S2C_OK, future.get().getMessages().get(0));

        future = commandExecutor.perform("pgs_lconn " + p0.getClusterName() + " " + p0.getName(), null);
        assertEquals(S2C_OK, future.get().getMessages().get(0));

        // cluster_off (fail)
        future = commandExecutor.perform("cluster_off " + clusterName, null);
        assertEquals(EXCEPTIONMSG_RUNNING_WORKFLOWS, future.get().getMessages().get(0));

        // remove fault
        for (int i = 1; i < 3; i++) {
            mimics[i].setInjector(null);
        }

        // cluster_off (success)
        await("cluster_off").atMost(30, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Future<JobResult> future = commandExecutor.perform("cluster_off " + clusterName, null);
                return S2C_OK.equals(future.get().getMessages().get(0));
            }
        });

        // cluster_on (success)
        future = commandExecutor.perform("cluster_on " + clusterName, null);
        assertEquals(S2C_OK, future.get().getMessages().get(0));

        masterCond = new MasterFinder(getPgsList());
        await("test for role master.").atMost(assertionTimeout, SECONDS).until(masterCond);
        PartitionGroupServer m = masterCond.getMaster();
        assertEquals(2, m.getMasterGen());
        assertEquals(1, pg.currentGen());

        // check state of slave
        SlaveFinder slaveCond = new SlaveFinder(getPgsList());
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveCond);
        PartitionGroupServer slave = slaveCond.getSlaves().iterator().next();
        assertEquals(SERVER_STATE_NORMAL, slave.getState());
        assertEquals(HB_MONITOR_YES, slave.getHeartbeat());
        assertEquals(2, slave.getMasterGen());
        assertEquals(1, pg.currentGen());

        quorumValidator = new QuorumValidator(mimics[Integer.valueOf(m.getName())], 1);
        await("quorum validation.").atMost(assertionTimeout, SECONDS).until(quorumValidator);

        // pgs_join
        mimics[0].init();
        future = commandExecutor.perform("pgs_join " + p0.getClusterName() + " " + p0.getName(), null);
        assertEquals(S2C_OK, future.get().getMessages().get(0));
        mockHeartbeatResult(p0, PGS_ROLE_LCONN, mimics[0].execute(PGS_PING));

        // check state of pgs0
        RoleColorValidator slaveValidator = new RoleColorValidator(p0, PGS_ROLE_SLAVE, GREEN);
        await("test for role slave.").atMost(assertionTimeout, SECONDS).until(slaveValidator);
        assertEquals(SERVER_STATE_NORMAL, p0.getState());
        assertEquals(HB_MONITOR_YES, p0.getHeartbeat());
        assertEquals(2, p0.getMasterGen());
        assertEquals(1, pg.currentGen());
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

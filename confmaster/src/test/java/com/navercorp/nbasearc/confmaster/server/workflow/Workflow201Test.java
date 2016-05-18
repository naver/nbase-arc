package com.navercorp.nbasearc.confmaster.server.workflow;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class Workflow201Test extends BasicSetting {

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

        BlockingSocketImpl msock = mock(BlockingSocketImpl.class);
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
                    return REDIS_PONG;
                }
                return S2C_OK;
            }
        });
        master.setServerConnection(msock);
        getRs(0).setServerConnection(msock);
        
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
            getPgs(slaveIdx).setServerConnection(msock);
            getRs(slaveIdx).setServerConnection(msock);

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
            assertEquals(getPg().getData().currentGen(), slave.getData()
                    .getMasterGen());
        }

        // MG
        MembershipGrantWorkflow mg = new MembershipGrantWorkflow(getPg(),
                false, context);
        mg.execute();
        System.out.println("MG\n" + getConfiguration());

        // Check quorum of master
        verify(msock).execute("setquorum 1");
        verify(msock).execute("setquorum 2");

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

        BlockingSocketImpl msock = mock(BlockingSocketImpl.class);
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
                    return REDIS_PONG;
                }
                return S2C_OK;
            }
        });
        master.setServerConnection(msock);
        getRs(0).setServerConnection(msock);

        doCommand("op_wf " + clusterName + " " + getPg().getName() + " QA false forced");
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " ME false forced");

        // Check Master
        assertEquals(PGS_ROLE_MASTER, master.getData().getRole());
        assertEquals(GREEN, master.getData().getColor());
        assertEquals(0, getPg().getData().currentGen());

        // Add Slave
        for (int slaveIdx = 1; slaveIdx < 3; slaveIdx++) {
            createPgs(slaveIdx);
            getPgs(slaveIdx).setServerConnection(msock);
            getRs(slaveIdx).setServerConnection(msock);

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
            assertEquals(getPg().getData().currentGen(), slave.getData()
                    .getMasterGen());
        }

        // MG
        doCommand("op_wf " + clusterName + " " + getPg().getName() + " MG false forced");
        System.out.println("MG\n" + getConfiguration());

        // Check quorum of master
        verify(msock).execute("setquorum 1");
        verify(msock).execute("setquorum 2");

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

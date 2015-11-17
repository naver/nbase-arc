package com.navercorp.nbasearc.confmaster.server.command;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.command.PartitionGroupService;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class PartitionGroupServiceTest extends BasicSetting {

    @Autowired
    PartitionGroupService service;

    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }
    
    @Before
    public void before() throws Exception {
        super.before();
    }
    
    @After
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void roleChangeRecover() throws Exception {
        createCluster();
        createPm();
        createPg();
        for (int id = 0; id < MAX_PGS; id ++) {
            createPgs(id);
            mockPgs(id);
        }

        for (int id = 0; id < MAX_PGS; id++) {
            mockPgsLconn(getPgs(id));
        }
        
        PartitionGroupServer master = waitRoleMaster(getPgsList());
        
        final String resp = "+OK";
        Map<String, LogSequence> logSeqMap = new HashMap<String, LogSequence>();
        for (PartitionGroupServer pgs : getPgsList()) {
            logSeqMap.put(pgs.getName(), new LogSequence());
            mockSocket(pgs, resp);
        }
        
        String ret = service.recoverMasterSlave(master, getPg(), getPgsList(),
                logSeqMap, getCluster(), 1L, workflowLogDao);
        assertEquals(ret, "-ERR there is no substitute. rollback. role_slave_error:[]");
    }
    
    @Test
    public void roleChangeRecoverWithFailedMaster() throws Exception {
        createCluster();
        createPm();
        createPg();
        for (int id = 0; id < MAX_PGS; id ++) {
            createPgs(id);
        }

        PartitionGroupServer master = getPgs(0);

        PartitionGroupServer[] slave = new PartitionGroupServer[2]; 
        for (int id = 1; id < MAX_PGS; id++) {
            slave[id-1] = getPgs(id);
        }
        
        Map<String, LogSequence> logSeqMap = new HashMap<String, LogSequence>();
        for (PartitionGroupServer pgs : getPgsList()) {
            logSeqMap.put(pgs.getName(), new LogSequence());
        }

        String ret = service.recoverMasterSlave(master, getPg(), getPgsList(),
                logSeqMap, getCluster(), 1L, workflowLogDao);
        assertEquals(0, ret.indexOf("-ERR there is no substitute and recover master fail."));
    }

    @Test
    public void roleChangeRecoverWithFailedSlave() throws Exception {
        createCluster();
        createPm();
        createPg();
        for (int id = 0; id < MAX_PGS; id ++) {
            createPgs(id);
            mockPgs(id);
        }

        PartitionGroupServer master = getPgs(0);

        PartitionGroupServer[] slaves = new PartitionGroupServer[2]; 
        for (int id = 1; id < MAX_PGS; id++) {
            slaves[id-1] = getPgs(id);
        }
        
        Map<String, LogSequence> logSeqMap = new HashMap<String, LogSequence>();
        for (PartitionGroupServer pgs : getPgsList()) {
            logSeqMap.put(pgs.getName(), new LogSequence());
        }

        // Mockup 
        final String resp = "+OK";
        mockSocket(master, resp);
        String ret = service.recoverMasterSlave(master, getPg(), getPgsList(),
                logSeqMap, getCluster(), 1L, workflowLogDao);
        assertEquals(0, ret.indexOf("-ERR there is no substitute. rollback. role_slave_error"));
        for (PartitionGroupServer slave : slaves) {
            assertTrue(ret.indexOf(slave.getData().getPmIp() + ":"
                    + slave.getData().getSmrMgmtPort()) > 0);
        }
    }

}

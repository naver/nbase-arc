package com.navercorp.nbasearc.confmaster.server.command;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.server.cluster.PGSComponentMock;
import com.navercorp.nbasearc.confmaster.server.cluster.PGSJoinLeaveSetting;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class PGSLeaveCommandTest extends BasicSetting {

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
    public void pgsJoinLeave() throws Exception {
        final int index = 0;
        createCluster();
        createPm();
        createPg();
        createPgs(index);

        // Mock up watchers
        PGSComponentMock mockup = new PGSComponentMock(getPgs(index), getRs(index));
        
        // PGS Join
        joinLeave.pgsJoin(getPgs(index), getRs(index), mockup);
        
        // PGS Leave
        joinLeave.pgsLeave(getPgs(index), getRs(index), mockup);

        deletePgs(index, true);
        deletePg();
        deleteCluster();
        deletePm();
    }

}

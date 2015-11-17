package com.navercorp.nbasearc.confmaster.repository;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class RepositoryTest extends BasicSetting {
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }

    @Override
    @Before
    public void before() throws Exception {
        super.before();
    }
    
    @Override
    @After
    public void after() throws Exception {
        super.after();
    }
    
    @Test
    public void pm() throws Exception {
        createPm();
        deletePm();
    }
    
    @Test
    public void cluster() throws Exception {
        createCluster();
        deleteCluster();
    }

    @Test
    public void pg() throws Exception {
        createPm();
        createCluster();
        createPg();
        deletePg();
        deleteCluster();
        deletePm();
    }
    
    @Test
    public void pgs() throws Exception {
        createPm();
        createCluster();
        createPg();
        createPgs(0);
        deletePgs(0, true);
        deletePg();
        deleteCluster();
        deletePm();
    }

    @Test
    public void gw() throws Exception {
        createPm();
        createCluster();
        createGw();
        deleteGw();
        deleteCluster();
        deletePm();
    }
    
}

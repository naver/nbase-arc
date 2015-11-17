package com.navercorp.nbasearc.confmaster.repository;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.EmbeddedZooKeeper;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-nozk.xml")
public class ZooKeeperHolderTest {
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    @Before
    public void before() throws Exception {
        EmbeddedZooKeeper.stop();
    }

    @Test(expected=MgmtZooKeeperException.class)
    public void initializationFail() throws MgmtZooKeeperException {
        /*
         * While EmbeddedZooKeeper is not running, ZooKeeperHolder.initialize()
         * must throw MgmtZooKeeperException.
         */
        zookeeper.initialize();
    }

}

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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-zkfi.xml")
public class PartitionGroupServerServiceTest extends BasicSetting {

    @Autowired
    private PartitionGroupServerDao dao;

    @Autowired
    private PhysicalMachineClusterImo pmClusterImo;

    @BeforeClass
    public static void beforeClass() throws Exception {
        LeaderState.setLeader();
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
    public void pgsDel() throws Exception {
        final int MAX_PGS = 3;

        // Initialize
        createCluster();
        createPm();
        createPg();

        for (int id = 0; id < MAX_PGS; id++) {
            createPgs(id);
        }

        // Delete pgs
        for (int id = 0; id < MAX_PGS; id++) {
            // Mock up
            ZooKeeperHolderFI zfi = (ZooKeeperHolderFI) zookeeper;
            zfi.setDeleteChildErr(new HashSet<Integer>(Arrays.asList(1, 3)));
            zfi.setMultiErr(new HashSet<Integer>(Arrays.asList(1)));

            zfi.setDeleteChildCnt(0);
            zfi.setMultiCnt(0);

            // Delete
            assertTrue("Delete pgs fail.",
                    delPgsAndCheckRetry(String.valueOf(id)));
        }

        // Check pgs
        for (int id = 0; id < MAX_PGS; id++) {
            assertTrue("Znode of pgs was not deleted.",
                    checkPgsDeleted(String.valueOf(id)));
        }
    }

    public boolean delPgsAndCheckRetry(String pgsId) throws Exception {
        try {
            // DB
            int retryCnt = dao.deletePgs(pgsId, clusterName, getPg(),
                    pmClusterImo.get(clusterName, pmName));

            if (retryCnt > 1) {
                return true;
            }
        } catch (MgmtZooKeeperException e) {
            return false;
        }

        return false;
    }

    public boolean checkPgsDeleted(String pgsId) throws MgmtZooKeeperException {
        try {
            dao.loadPgs(pgsId, clusterName, null, null);
        } catch (NoNodeException e) {
            return true;
        }
        return false;
    }

}

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

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

import static com.navercorp.nbasearc.confmaster.Constant.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.server.cluster.PGSComponentMock;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class PGSLeaveCommandTest extends BasicSetting {

    @Autowired
    ConfMaster confMaster;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        BasicSetting.beforeClass();
    }
    
    @Before
    public void before() throws Exception {
        super.before();
        confMaster.setState(ConfMaster.RUNNING);
    }
    
    @After
    public void after() throws Exception {
        super.after();
    }
    
    @Test
    public void pgsJoinLeave() throws Exception {
        PGSComponentMock mockup[] = new PGSComponentMock[2]; 
        createCluster();
        createPm();
        createPg();
        
        for (int i = 0; i < 2; i++) {
            createPgs(i);
    
            // Mock up watchers
            mockup[i] = new PGSComponentMock(getPgs(i), getRs(i));
            
            // PGS Join
            joinLeave.pgsJoin(getPgs(i), getRs(i), mockup[i]);
        }
        
        // PGS Leave
        joinLeave.pgsLeave(getPgs(1), getRs(1), mockup[1], "");
        joinLeave.pgsLeave(getPgs(0), getRs(0), mockup[0], FORCED);

        for (int i = 0; i < 2; i++) {
            deletePgs(i, true);
        }
        deletePg();
        deleteCluster();
        deletePm();
    }

}

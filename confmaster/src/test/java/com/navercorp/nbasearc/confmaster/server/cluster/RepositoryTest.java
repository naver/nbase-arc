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

package com.navercorp.nbasearc.confmaster.server.cluster;

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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class RepositoryTest extends BasicSetting {
    
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

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

import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_WRONG_NUMBER_ARGUMENTS;
import static com.navercorp.nbasearc.confmaster.server.JobResult.CommonKey.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.InvocationTargetException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.BasicSetting;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandNotFoundException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandWrongArgumentException;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.JobResult.CommonKey;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class CommandExecuterTest extends BasicSetting {
    
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
    public void commandNotFound() throws Exception {
        JobResult result = doCommand("command_not_exist");
        MgmtCommandNotFoundException exception = null;
        for (Throwable e : result.getExceptions()) {
            if (e instanceof MgmtCommandNotFoundException) {
                exception = (MgmtCommandNotFoundException)e;
            }
        }
        assertNotNull("It didn't throw CommandNotFoundException.", exception);
    }

    final String clusterAddUsage = "cluster_add <cluster_name> <quorum policy>\r\n" +
            "Ex) cluster_add cluster1 0:1\r\n" +
            "add a new cluster\r\n";
    
    @Test
    public void tooManyArguments() throws Exception {
        JobResult result = doCommand("cluster_add arg1 arg2 arg3");
        MgmtCommandWrongArgumentException e1 = null;
        for (Throwable e : result.getExceptions()) {
            if (e instanceof MgmtCommandWrongArgumentException) {
                e1 = (MgmtCommandWrongArgumentException)e;
            }
        }
        assertNotNull("It didn't throw CcCommandWrongArgumentException.", e1); 
        assertEquals(clusterAddUsage, e1.getUsage());
        assertEquals(EXCEPTIONMSG_WRONG_NUMBER_ARGUMENTS, 
                result.getExceptions().get(0).getMessage());
    }
    
    @Test
    public void lackOfArguments() throws Exception {
        JobResult result = doCommand("cluster_add arg1");
        MgmtCommandWrongArgumentException e2 = null;
        for (Throwable e : result.getExceptions()) {
            if (e instanceof MgmtCommandWrongArgumentException) {
                e2 = (MgmtCommandWrongArgumentException)e;
            }
        }
        assertNotNull("", e2);
        assertEquals(clusterAddUsage, e2.getUsage());
        assertEquals(EXCEPTIONMSG_WRONG_NUMBER_ARGUMENTS, 
                result.getExceptions().get(0).getMessage());
    }

}

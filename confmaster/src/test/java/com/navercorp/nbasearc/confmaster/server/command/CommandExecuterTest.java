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
import static org.junit.Assert.*;

import java.lang.reflect.Field;

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
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandNotFoundException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandWrongArgumentException;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ClientSessionHandler.ReplyFormatter;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class CommandExecuterTest extends BasicSetting {
    
    @Autowired
    ConfMaster confMaster;
    
    ReplyFormatter formatter = new ReplyFormatter();
    
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
            "add a new cluster";
    
    @Test
    public void tooManyArguments() throws Exception {
        Field state = confMaster.getClass().getDeclaredField("state");
        state.setAccessible(true);
        state.set(confMaster, ConfMaster.RUNNING);
        
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
        Field state = confMaster.getClass().getDeclaredField("state");
        state.setAccessible(true);
        state.set(confMaster, ConfMaster.RUNNING);
        
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
    
    @Test
    public void requiredState() throws Exception {
        final String leader = "127.0.0.1:1122";
        String[] requireLoading = { "help", "ping" }; 
        String[] requireReady = { "appdata_get", "cluster_info", "cluster_ls",
                "get_cluster_info", "gw_info", "gw_ls", "pg_info", "pg_ls",
                "pgs_info", "pgs_info_all", "pgs_ls", "pm_info", "pm_ls",
                "worklog_get", "worklog_head", "worklog_info", "cm_start" };
        String[] requireRunning = { "appdata_del", "appdata_set",
                "cluster_add", "cluster_del", "gw_add", "gw_affinity_sync",
                "gw_del", "mig2pc", "op_wf", "pg_add", "pg_del", "pg_dq",
                "pg_iq", "pgs_add", "pgs_del", "pgs_join", "pgs_lconn",
                "pgs_leave", "pgs_sync", "pm_add", "pm_del", "role_change",
                "slot_set_pg", "worklog_del", };
        
        // LOADING
        Field state = confMaster.getClass().getDeclaredField("state");
        state.setAccessible(true);
        state.set(confMaster, ConfMaster.LOADING);
        
        for (String c : requireLoading) {
            assertEquals(0, doCommand(c).getExceptions().size());
        }
        
        for (String c : requireReady) {
            assertEquals("{\"state\":\"error\",\"msg\":\"-ERR required state is 1, but 0\"}\r\n", 
                    formatter.get(doCommand(c), leader));
        }
        
        for (String c : requireRunning) {
            assertEquals("{\"state\":\"error\",\"msg\":\"-ERR required state is 2, but 0\"}\r\n", 
                    formatter.get(doCommand(c), leader));
        }
        
        // READY
        state.set(confMaster, ConfMaster.READY);
        
        for (String c : requireLoading) {
            assertEquals(0, doCommand(c).getExceptions().size());
        }
        
        for (String c : requireReady) {
            if (c.equals("cm_start")) {
                continue;
            }
            assertFalse(formatter.get(doCommand(c), leader).contains(
                    "-ERR required state is"));
        }
        
        for (String c : requireRunning) {
            assertEquals("{\"state\":\"error\",\"msg\":\"-ERR required state is 2, but 1\"}\r\n", 
                    formatter.get(doCommand(c), leader));
        }
        
        // RUNNING
        state.set(confMaster, ConfMaster.RUNNING);
        
        for (String c : requireLoading) {
            assertEquals(0, doCommand(c).getExceptions().size());
        }
        
        for (String c : requireReady) {
            assertFalse(formatter.get(doCommand(c), leader).contains(
                    "-ERR required state is"));
        }
        
        for (String c : requireRunning) {
            assertFalse(formatter.get(doCommand(c), leader).contains(
                    "-ERR required state is"));
        }
    }

}

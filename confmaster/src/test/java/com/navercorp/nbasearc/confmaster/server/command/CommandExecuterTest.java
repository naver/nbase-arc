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
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

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

    @Test
    public void requiedMode() throws Exception {
        // Initialize
        confMaster.setState(ConfMaster.RUNNING);
        createCluster();
        createPm();
        createPg();
        pgAdd("1", clusterName);
        createPgs(0);
        createPgs(1);
        createGw();

        JobResult result = doCommand("appdata_set " + clusterName
                + " backup 0 1 0 2 * * * * 02:00:00 3 70 base32hex \"\"");
        assertEquals(S2C_OK, result.getMessages().get(0));
        
        // Off cluster
        result = doCommand("cluster_off " + clusterName);
        assertEquals(1, result.getMessages().size());
        assertEquals(S2C_OK, result.getMessages().get(0));
        
        assertNotEquals(S2C_OK, doCommand("cluster_off " + clusterName).getMessages().get(0));

        // Failure cases
        String []fail = {
            "appdata_del " + clusterName + " backup all",
            "appdata_set " + clusterName + " backup 1 1 0 2 * * * * 02:00:00 3 70 base32hex \"\"",
            "cluster_del " + clusterName,
            "mig2pc " + clusterName + " " + pgName + " 1 0 8191",
            "slot_set_pg " + clusterName + " 0:8191 1",
            "gw_add " + clusterName + " 10 " + pmName + " 127.0.0.100 6100",
            "gw_affinity_sync " + clusterName,
            "gw_del " + clusterName + " " + gwName,
            "op_wf " + clusterName + " " + pgName + " RA true forced",
            "pg_add " + clusterName + " 10",
            "pg_del " + clusterName + " " + pgName,
            "pg_dq " + clusterName + " " + pgName,
            "pg_iq " + clusterName + " " + pgName,
            "role_change " + clusterName + " " + gwName,
            "pgs_add " + clusterName + " 10 " + pgName + " " + pmName + " 127.0.0.100 7100 7109",
            "pgs_del " + clusterName + " " + pgsNames[0],
            "pgs_join " + clusterName + " " + pgsNames[0],
            "pgs_lconn " + clusterName + " " + pgsNames[0],
            "pgs_leave " + clusterName + " " + pgsNames[0],
            "pgs_sync " + clusterName + " " + pgsNames[0] + " forced",
        };
        for (String cmd : fail) {
            result = doCommand(cmd);
            assertEquals("exception with \"" + cmd + "\", " + result.getExceptions(), 
                    0, result.getExceptions().size());
            assertEquals("assert fail with \"" + cmd + "\"",
                    REQUIRED_MODE_NOT_SATISFIED, result.getMessages().get(0));
        }
        
        // Success cases
        String []success = {
            "cm_info",
            "ping",
            "pm_add test02.arc 127.0.0.101",
            "pm_info test02.arc",
            "pm_del test02.arc",
            "appdata_get " + clusterName + " backup 0",
            "cluster_add test_cluster_2 0:1",
            "cluster_dump " + clusterName,
            "cluster_info " + clusterName,
            "get_cluster_info " + clusterName,
            "cluster_ls",
            "gw_info " + clusterName + " " + gwName,
            "gw_ls " + clusterName,
            "pg_info " + clusterName + " " + pgName,
            "pg_ls " + clusterName,
            "pgs_info " + clusterName + " " + pgsNames[0],
            "pgs_info_all " + clusterName + " " + pgsNames[0],
            "pgs_ls " + clusterName,
            "pm_ls",
            "worklog_del 1",
            "worklog_get 0 1",
            "worklog_head 1",
            "worklog_info",
            "cluster_purge " + clusterName
        };
        for (String cmd : success) {
            result = doCommand(cmd);
            assertEquals("exception with \"" + cmd + "\", " + result.getExceptions(), 
                    0, result.getExceptions().size());
            assertThat("assert fail with \"" + cmd + "\"",
                    result.getMessages().get(0),
                    anyOf(is(S2C_OK), is("+PONG"), startsWith("{"),
                            startsWith("8192"), containsString("start log no:")));
        }
    }
    
}

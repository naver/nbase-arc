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

package com.navercorp.nbasearc.confmaster.server;

import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_INTERNAL_ERROR;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_WRONG_NUMBER_ARGUMENTS;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_ZOOKEEPER;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandWrongArgumentException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.EventSelector;
import com.navercorp.nbasearc.confmaster.server.ClientSessionHandler;
import com.navercorp.nbasearc.confmaster.server.command.CommandExecutor;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionHandler;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext-test.xml")
public class ClientSessionHandlerTest {

    @Autowired
    protected CommandExecutor commandExecutor;
    
    @Autowired
    protected LeaderElectionHandler leaderElectoin;
    
    @Autowired
    private Config config;
    
    private EventSelector selector;
    
    @Test
    public void test() throws IOException {
        selector = new EventSelector(47);
        
        ClientSessionHandler handler = new ClientSessionHandler(commandExecutor,
                leaderElectoin, config, selector);
        
        assertEquals(
            handler.convertExceptionToReply(new NoNodeException()),
            EXCEPTIONMSG_ZOOKEEPER);
        
        assertEquals(
            handler.convertExceptionToReply(new InterruptedException()), 
            EXCEPTIONMSG_INTERNAL_ERROR);

        assertEquals(
            handler.convertExceptionToReply(new MgmtCommandWrongArgumentException("")), 
            EXCEPTIONMSG_WRONG_NUMBER_ARGUMENTS);
        
        assertEquals(
            handler.convertExceptionToReply(new IOException()), 
            "-ERR Can not convert raw-data to json-format");
        
        String msg = "IllegalArgumentException";
        assertEquals(
            handler.convertExceptionToReply(new IllegalArgumentException(msg)), 
            msg);
        
        String clusterName = "test_cluster";
        msg = "-ERR the cluster znode for notification already exists. cluster:" 
                + clusterName;
        Exception e = new MgmtZNodeAlreayExistsException("/RC/CLUSTER" + clusterName, msg);
        assertEquals(
            handler.convertExceptionToReply(e), 
            e.getMessage());

        msg = "-ERR the cluster znode for notification does not exist. cluster:" + clusterName;
        e = new MgmtZNodeDoesNotExistException("/RC/CLUSTER" + clusterName, msg);
        assertEquals(
            handler.convertExceptionToReply(e), 
            e.getMessage());
        
        msg = "-ERR exception occurs.";
        assertEquals(
            handler.convertExceptionToReply(new Exception(msg)), 
            "-ERR " + msg);
    }

}

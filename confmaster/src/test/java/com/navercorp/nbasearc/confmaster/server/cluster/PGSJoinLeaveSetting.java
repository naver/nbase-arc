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

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.Future;

import org.apache.zookeeper.WatchedEvent;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.EventSelector;
import com.navercorp.nbasearc.confmaster.io.MultipleGatewayInvocator;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer.RedisServerData;
import com.navercorp.nbasearc.confmaster.server.command.CommandExecutor;

public class PGSJoinLeaveSetting {

    EventSelector hbProcessor;
    
    Config config;
    
    CommandExecutor commandTemplate;
    
    final long atoSec = 5L;
    
    public void initialize(ApplicationContext context)throws Exception {
        hbProcessor = new EventSelector(47);
        config = context.getBean(Config.class);
        commandTemplate = context.getBean(CommandExecutor.class);
        MockitoAnnotations.initMocks(this);
    }

    public void pgsJoin(PartitionGroupServer pgs, RedisServer rs, ClusterComponentMock mock) throws Exception {
        MultipleGatewayInvocator broadcast = spy(new MultipleGatewayInvocator());
        
        // Prepare expected data
        PartitionGroupServerData pgsModified = pgs.clonePersistentData();
        pgsModified.hb = HB_MONITOR_YES;

        RedisServerData rsModified = rs.clonePersistentData();
        rsModified.hb = HB_MONITOR_YES;
        
        // Replace methods with mocks
        when(broadcast.request(anyString(), anyListOf(Gateway.class), anyString(), anyString(), (ThreadPool)anyObject())).thenReturn(null);

        // Join PGS
        Future<JobResult> future = commandTemplate.perform("pgs_join " + pgs.getClusterName() + " " + pgs.getName(), null);
        JobResult jobResult = future.get();
        assertEquals(S2C_OK, jobResult.getMessages().get(0));
        
        assertEquals(pgs.clonePersistentData(), pgsModified);
        assertEquals(rs.clonePersistentData(), rsModified);
    }
    
    public void pgsLeave(PartitionGroupServer pgs, RedisServer rs,
            ClusterComponentMock mock, String mode) throws Exception {        
        // Leave PGS
        Future<JobResult> future = commandTemplate.perform(
                "pgs_leave " + pgs.getClusterName() + " " + pgs.getName() + " "
                        + mode, null);
        JobResult jobResult = future.get();
        
        assertEquals(S2C_OK, jobResult.getMessages().get(0));
        assertEquals(HB_MONITOR_NO, pgs.getHeartbeat());
        assertEquals(HB_MONITOR_NO, rs.getHeartbeat());
    }
}


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

import static org.mockito.Mockito.spy;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;

public class ClusterComponentMock {
    private HBSession pgsHbcSessionSpy;
    private HBSession rsHbcSessionSpy;
    
    public ClusterComponentMock(PartitionGroupServer pgs, RedisServer rs)
            throws MgmtZooKeeperException {
        pgs.hbSession = pgsHbcSessionSpy = spy(pgs.hbSession);
        rs.hbSession = rsHbcSessionSpy = spy(rs.hbSession);
    }

    public HBSession getPgsHbcSession() {
        return pgsHbcSessionSpy;
    }

    public HBSession getRsHbcSession() {
        return rsHbcSessionSpy;
    }

    public static HBSession getHeartbeatSession(PartitionGroupServer pgs) {
        return pgs.hbSession;
    }

    public static void setConnectionForCommand(RedisServer rs, BlockingSocket bs) {
        rs.connectionForCommand = bs;
    }
    
    public static void setConnectionForCommand(Gateway gw, BlockingSocket bs) {
        gw.connectionForCommand = bs;
    }
    
    public static void setConnectionForCommand(PartitionGroupServer pgs, BlockingSocket bs) {
        pgs.connectionForCommand = bs;
    }
}
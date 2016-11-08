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

package com.navercorp.nbasearc.confmaster.server.workflow;

import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResultProcessor;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;

public class OpinionPublishWorkflow {
    
    private final HBResult result;
    private final HeartbeatTarget target;
    private final long hbcRefDataVersion;
    private final HBResultProcessor hbcProc;

    protected OpinionPublishWorkflow(HBResult result, ApplicationContext context) {
        this.result = result;
        this.target = result.getTarget();
        this.hbcRefDataVersion = target.getHeartbeatState().increaseAndGetHbcRefDataVersion();
        this.hbcProc = context.getBean(HBResultProcessor.class);
    }
    
    public String execute(ThreadPool executor) throws NodeExistsException,
            MgmtZooKeeperException {
        if (target.getHeartbeat().equals(Constant.HB_MONITOR_NO)) {
            // Ignore it.
            return null;
        }
        
        if (target.getHeartbeatState().getHbcRefDataVersion() > this.hbcRefDataVersion) {
            return null;
        }
        
        hbcProc.proc(result, true);
        return null;
    }

}

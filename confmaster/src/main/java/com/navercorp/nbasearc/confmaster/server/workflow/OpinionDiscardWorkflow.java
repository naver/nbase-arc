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

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBState;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;

public class OpinionDiscardWorkflow {
    
    private final HeartbeatTarget target;
    private final Config config;
    private final ZooKeeperHolder zk;
    
    protected OpinionDiscardWorkflow(HeartbeatTarget target, ApplicationContext context) {
        this.target = target;
        this.config = context.getBean(Config.class);
        this.zk = context.getBean(ZooKeeperHolder.class);
    }
    
    public String execute(ThreadPool executor) throws MgmtZooKeeperException {
        HBState hbState = getTarget().getHeartbeatState();
        if (!hbState.isSubmitMyOpinion()) {
            // Ignore it
            return null;
        }
        
        String path = getTarget().getPath() + "/" + config.getIp() + ":" + config.getPort();
        
        try {
            zk.deleteZNode(path, -1);
        } catch (MgmtZooKeeperException e) {
            Logger.error("Remove opinion fail. path: {}", path, e);
            throw e;
        }

        Logger.info("Delete opinion {} {}", path, target.getFullName());
        hbState.setSubmitMyOpinion(false);
        
        return null;
    }
    
    public HeartbeatTarget getTarget() {
        return target;
    }

}

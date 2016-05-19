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

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.COMMON_STATE_DECISION;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.PGS_STATE_DECISION;

import java.util.List;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.FailureDetector;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.FailureDetectorImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class UpdateHeartbeatCheckerWorkflow {
    
    private final WorkflowExecutor workflowExecutor;
    
    private final ZooKeeperHolder zookeeper;
    
    private final FailureDetectorImo fdImo;
    private final PartitionGroupServerImo pgsImo;
    private final GatewayImo gwImo;
    private final RedisServerImo rsImo; 
    
    private final WorkflowLogDao workflowLogDao; 

    protected UpdateHeartbeatCheckerWorkflow(ApplicationContext context) {
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        
        this.workflowExecutor = context.getBean(WorkflowExecutor.class);

        this.fdImo = context.getBean(FailureDetectorImo.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.gwImo = context.getBean(GatewayImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);

        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
    }
    
    public String execute(ThreadPool executor) throws MgmtZooKeeperException {
        String path = PathUtil.fdRootPath();
        FailureDetector fd = fdImo.get();
        
        try {
            int numberOfHeartbeatChecker = zookeeper.getNumberOfChildren(path);
            fd.setNumberOfHeartbeatCheckers(numberOfHeartbeatChecker);
            
            if (0 == numberOfHeartbeatChecker)
                workflowLog(Constant.SEVERITY_MAJOR, numberOfHeartbeatChecker, workflowLogDao);
            else
                workflowLog(Constant.SEVERITY_MODERATE, numberOfHeartbeatChecker, workflowLogDao);
        } catch (MgmtZooKeeperException e) {
            Logger.error("Update the number of heartbeat checkers fail.", e);
        }
        
        // PGS
        List<PartitionGroupServer> pgsList = pgsImo.getAll();
        for (PartitionGroupServer pgs : pgsList) {
            workflowExecutor.perform(PGS_STATE_DECISION, pgs);
        }
        
        // GW
        List<Gateway> gwList = gwImo.getAll();
        for (Gateway gw : gwList) {
            workflowExecutor.perform(COMMON_STATE_DECISION, gw);
        }
        
        // RS
        List<RedisServer> rsList = rsImo.getAll();
        for (RedisServer rs : rsList) {
            workflowExecutor.perform(COMMON_STATE_DECISION, rs);
        }
        
        return null;
    }
    
    private void workflowLog(String severity, int numberOfHeartbeatChecker,
            WorkflowLogDao workflowLogDao) {
        workflowLogDao.log(JobIDGenerator.getInstance().getID(), 
                severity, "UpdateHeartbeatCheckerWorkflow",
                Constant.LOG_TYPE_WORKFLOW, "",
                "change the number of heartbeat checker. no: " + numberOfHeartbeatChecker,
                String.format("{\"number_of_hbc\":\"%d\"}", numberOfHeartbeatChecker));
    }

}

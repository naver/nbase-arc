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

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;

public class TotalInspectionWorkflow {

    private final ConfMaster confmaster;
    
    private final WorkflowExecutor workflowExecutor;
    
    private final ZooKeeperHolder zk;
    
    private final ClusterComponentContainer container; 
    
    private final WorkflowLogger workflowLogger; 

    protected TotalInspectionWorkflow(ApplicationContext context) {
        this.confmaster = context.getBean(ConfMaster.class);
        
        this.zk = context.getBean(ZooKeeperHolder.class);
        
        this.workflowExecutor = context.getBean(WorkflowExecutor.class);

        this.container = context.getBean(ClusterComponentContainer.class);

        this.workflowLogger = context.getBean(WorkflowLogger.class);
    }
    
    public String execute(ThreadPool executor) throws MgmtZooKeeperException {
        String path = PathUtil.fdRootPath();
        
        try {
            int numberOfHeartbeatChecker = zk.getNumberOfChildren(path);
            confmaster.setNumberOfHeartbeatCheckers(numberOfHeartbeatChecker);
            
            if (0 == numberOfHeartbeatChecker)
                workflowLog(Constant.SEVERITY_MAJOR, numberOfHeartbeatChecker, workflowLogger);
            else
                workflowLog(Constant.SEVERITY_MODERATE, numberOfHeartbeatChecker, workflowLogger);
        } catch (MgmtZooKeeperException e) {
            Logger.error("Update the number of heartbeat checkers fail.", e);
        }
        
        // Cluster
        List<Cluster> clusterList = container.getAllCluster();
        for (Cluster cluster : clusterList) {   
            // PGS
            List<PartitionGroupServer> pgsList = container.getPgsList(cluster.getName());
            for (PartitionGroupServer pgs : pgsList) {
                workflowExecutor.perform(PGS_STATE_DECISION, pgs);
            }
            
            // GW
            List<Gateway> gwList = container.getGwList(cluster.getName());
            for (Gateway gw : gwList) {
                workflowExecutor.perform(COMMON_STATE_DECISION, gw);
            }
            
            // RS
            List<RedisServer> rsList = container.getRsList(cluster.getName());
            for (RedisServer rs : rsList) {
                workflowExecutor.perform(COMMON_STATE_DECISION, rs);
            }
        }
        
        return null;
    }
    
    private void workflowLog(String severity, int numberOfHeartbeatChecker,
            WorkflowLogger workflowLogger) {
        workflowLogger.log(JobIDGenerator.getInstance().getID(), 
                severity, "UpdateHeartbeatCheckerWorkflow",
                Constant.LOG_TYPE_WORKFLOW, "",
                "change the number of heartbeat checker. no: " + numberOfHeartbeatChecker,
                String.format("{\"number_of_hbc\":\"%d\"}", numberOfHeartbeatChecker));
    }

}

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

package com.navercorp.nbasearc.confmaster.server.leaderelection;

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.TOTAL_INSPECTION;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.command.ConfmasterService;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionSupport.LeaderElectionEventType;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;

@Component
public class LeaderElectionHandler implements LeaderElectionAware {
    
    @Autowired
    private Config config;
    
    @Autowired
    private ZooKeeperHolder zk;
    @Autowired
    private WorkflowLogger workflowLogger;

    @Autowired
    private WorkflowExecutor workflowExecutor;
    
    @Autowired
    private ConfmasterService confmasterService;
    
    @Autowired
    private LeaderElectionSupport electionSupport;
    
    @Autowired
    private ConfMaster confMaster;
    
    public LeaderElectionHandler() {
    }
    
    @Override
    public void onElectionEvent(LeaderElectionEventType eventType) {
        try {
            if (LeaderElectionEventType.ELECTED_COMPLETE == eventType) {
                becomeLeader();
            } else if (LeaderElectionEventType.READY_START == eventType) {
                becomeFollower();
            }
        } catch (Exception e) {
            Logger.error("failed initializing", e);
        }
    }
    
    public void initialize() {
        final String hostName = makeHostName(config.getIp(), config.getPort());
        Logger.info("start leader election. " + hostName);
        
        electionSupport.addListener(this);
        electionSupport.setZooKeeper(zk.getZooKeeper());
        electionSupport.setHostName(hostName);
        electionSupport.setRootNodeName(PathUtil.rootPathOfLE());
        electionSupport.start();
    }

    public void release() {
        electionSupport.stop();
    }
    
    public void becomeLeader() throws NoNodeException, MgmtZooKeeperException,
            NodeExistsException {
        if (LeaderState.isLeader()) {
            Logger.info("already leader");
            return;
        }
        
        boolean fromFollower = LeaderState.isFollower();
        
        Logger.info("become leader");
        LeaderState.setLeader();
        
        workflowLogger.initialize();
        
        JobIDGenerator.getInstance().initialize();

        workflowLogger.log(JobIDGenerator.getInstance().getID(), 
                Constant.SEVERITY_MAJOR, "LeaderElectionWorkflow",
                Constant.LOG_TYPE_WORKFLOW, "", "New leader was elected.", 
                String.format("{\"ip\":\"%s\",\"port\":%d}", config.getIp(), config.getPort()));
        
        confmasterService.loadAll();
        confmasterService.updateZNodeDirectoryStructure();
        
        /*
         * Leader ConfMaster.state transition:
         *   LOADING ----> READY --(start)--> RUNNING
         * Follower ConfMaster.state transition:
         *   LOADING ----> RUNNING --(become leader)--> RUNNING
         *   
         * When leader election is triggered by a crash of previous leader, 
         * no one can type cm_start command. 
         * Therefore followers get RUNNING-state without cm_start.  
         */
        if (!fromFollower) {
            confMaster.setState(ConfMaster.READY);
        } else {
            workflowExecutor.perform(TOTAL_INSPECTION);
        }
    }

    public void becomeFollower() throws NoNodeException, MgmtZooKeeperException {
        if (LeaderState.isFollower()) {
            Logger.info("already follower");
            return;
        } else if (LeaderState.isLeader()) {
            Logger.info("Leader --> Follower, Abort...");
            System.exit(0);
        }
        
        Logger.info("become follower");
        LeaderState.setFollower();

        confmasterService.loadAll();
        
        confMaster.setState(ConfMaster.RUNNING);
    }
    
    public String getCurrentLeaderHost() 
            throws KeeperException, InterruptedException {
        String host = electionSupport.getLeaderHostName();
        /*
         * If there is no leader, set ip and port are null; {"ip":null,"port":null},
         * otherwise return leader`s ip and port. {"ip":123.123.123.123,"port":12345}
         */
        return (host != null) ? host : makeHostName(null, null);
    }
    
    public String makeHostName(String ip, Integer port) {
        return String.format("{\"ip\":\"%s\",\"port\":\"%d\"}", ip, port);
    }

}

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

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState.ElectionState.*;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.io.IOException;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtUnexpectedStateTransitionException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockCluster;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockPGSList;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.WorkflowMapping;

@Service
public class WorkflowService {

    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ThreadPool executor;
    
    @Autowired
    private PartitionGroupServerImo pgsImo;
    
    @WorkflowMapping(name=FAILOVER_COMMON, privilege=LEADER)
    public JobResult failoverCommon(HeartbeatTarget target)
            throws NoNodeException, MgmtZooKeeperException,
            MgmtZNodeDoesNotExistException, MgmtZNodeAlreayExistsException {
        FailoverCommonWorkflow failoverCommandWorkflow = 
                new FailoverCommonWorkflow(target, context);
        
        failoverCommandWorkflow.execute(executor);
        
        return null;
    }
    
    @LockMapping(name=FAILOVER_COMMON)
    public void failoverCommonLock(HierarchicalLockHelper lockHelper,
            HeartbeatTarget target) {
        final String path = target.getPath();
        
        String clusterName = PathUtil.getClusterNameFromPath(path);
        NodeType nodeType = PathUtil.getNodeTypeFromPath(path);
        String name = target.getName();
        
        HierarchicalLockCluster lockCluster = lockHelper.root(READ).cluster(READ, clusterName);
        
        switch (nodeType) {
        case GW:
            lockCluster.gwList(READ).gw(WRITE, name);
            break;

        case RS:
            lockCluster.pgList(READ).pgsList(READ).pg(READ, null).pgs(WRITE, name).gwList(READ);
            break;

        default:
            Logger.error("Not supported node type. type: {}", nodeType);
            throw new AssertionError("Not supported node type. type: " + nodeType);
        }
    }

    @WorkflowMapping(name=FAILOVER_PGS, privilege=LEADER)
    public Future<JobResult> failoverPgs(PartitionGroupServer target)
            throws NoNodeException, MgmtZooKeeperException,
            MgmtUnexpectedStateTransitionException, IOException {
        FailoverPGSWorkflow failoverPGSWorkflow = 
                new FailoverPGSWorkflow(target, context);
        
        failoverPGSWorkflow.execute(executor);
        
        return null;
    }
    
    @LockMapping(name=FAILOVER_PGS)
    public void failoverPgsLock(HierarchicalLockHelper lockHelper,
            PartitionGroupServer target) {
        final String path = target.getPath();

        String clusterName = PathUtil.getClusterNameFromPath(path);
        NodeType nodeType = PathUtil.getNodeTypeFromPath(path);

        switch (nodeType) {
        case PGS:
            HierarchicalLockPGSList pgListLock = 
                lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ);
            PartitionGroupServer pgs = pgsImo.getByPath(path);
            pgListLock.pg(WRITE, String.valueOf(pgs.getData().getPgId()))
                    .pgs(WRITE, Constant.ALL_IN_PG).gwList(READ);
            break;
            
        default:
            Logger.error("Not supported node type. type: {}", nodeType);
            throw new AssertionError("Not supported node type. type: " + nodeType);
        }
    }
    
    
    @WorkflowMapping(name=OPINION_DISCARD, privilege=FOLLOWER)
    public JobResult discardOpinion(HeartbeatTarget target)
            throws MgmtZooKeeperException {
        OpinionDiscardWorkflow opinionDiscardWorkflow = 
                new OpinionDiscardWorkflow(target, context);
        
        opinionDiscardWorkflow.execute(executor);
        
        return null;
    }

    @LockMapping(name=OPINION_DISCARD)
    public void discardOpinionLock(HierarchicalLockHelper lockHelper,
            HeartbeatTarget target) {
        try {
            switch (target.getNodeType()) {
            case PGS:
                lockHelper.root(READ).cluster(READ, target.getClusterName())
                        .pgList(READ).pgsList(READ).pg(READ, null)
                        .pgs(WRITE, target.getName());
                break;
    
            case RS:
                lockHelper.root(READ).cluster(READ, target.getClusterName())
                        .pgList(READ).pgsList(READ).pg(READ, null)
                        .pgs(WRITE, target.getName());
                break;
    
            case GW:
                lockHelper.root(READ).cluster(READ, target.getClusterName())
                        .gwList(READ).gw(WRITE, target.getName());
                break;
    
            default:
                Logger.error("Not supported node type. type="
                        + target.getNodeType());
                throw new Exception();
            }
        } catch (Exception e) {
            Logger.debug(
                    "Discard opinion fail, because Resouce already delete. Ignore this exception. {}", 
                    target, e);
        }
    }

    @WorkflowMapping(name=OPINION_PUBLISH, privilege=FOLLOWER)
    public JobResult publishOpinion(HBResult result)
            throws NodeExistsException, MgmtZooKeeperException {
        OpinionPublishWorkflow opinionPublishWorkflow = 
                new OpinionPublishWorkflow(result, context);
        
        opinionPublishWorkflow.execute(executor);
        
        return null;
    }

    @LockMapping(name=OPINION_PUBLISH)
    public void publishOpinionLock(HierarchicalLockHelper lockHelper,
            HBResult result) {
        HeartbeatTarget target = result.getTarget();

        switch (target.getNodeType()) {
        case PGS:
            lockHelper.root(READ).cluster(READ, target.getClusterName())
                    .pgList(READ).pgsList(READ).pg(READ, null)
                    .pgs(WRITE, target.getName());
            break;

        case RS:
            lockHelper.root(READ).cluster(READ, target.getClusterName())
                    .pgList(READ).pgsList(READ).pg(READ, null)
                    .pgs(WRITE, target.getName());
            break;

        case GW:
            lockHelper.root(READ).cluster(READ, target.getClusterName())
                    .gwList(READ).gw(WRITE, target.getName());
            break;

        default:
            Logger.error("Not supported node type. type: {}", target.getNodeType());
            throw new AssertionError("Not supported node type. type: " + target.getNodeType());
        }
    }
    
    @WorkflowMapping(name=SET_QUORUM, privilege=LEADER)
    public JobResult setQuorum(String clusterName, String pgid)
            throws MgmtDuplicatedReservedCallException {
        SetquorumWorkflow setQuorumWorkflow = 
                new SetquorumWorkflow(clusterName, pgid, context);
        
        setQuorumWorkflow.execute(executor);
        
        return null;
    }

    @LockMapping(name=SET_QUORUM)
    public void setQuorumLock(HierarchicalLockHelper lockHelper,
            String clusterName, String pgid) throws Exception {
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, pgid).pgs(WRITE, Constant.ALL_IN_PG);
    }

    @WorkflowMapping(name=UPDATE_HEARTBEAT_CHECKER, privilege=LEADER)
    public Future<JobResult> updateHeartbeatChecker()
            throws MgmtZooKeeperException {
        UpdateHeartbeatCheckerWorkflow updateHeartbeatCheckerWorkflow = 
                new UpdateHeartbeatCheckerWorkflow(context);
        
        updateHeartbeatCheckerWorkflow.execute(executor);
        
        return null;
    }

    @LockMapping(name=UPDATE_HEARTBEAT_CHECKER)
    public void updateHeartbeatCheckerLock(HierarchicalLockHelper lockHelper) {
        lockHelper.root(WRITE);
    }

}

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

import static com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState.ElectionState.*;
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;
import static com.navercorp.nbasearc.confmaster.Constant.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.logger.EpochMsgDecorator;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.cluster.NodeType;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockCluster;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockPGSList;
import com.navercorp.nbasearc.confmaster.server.mapping.ClusterHint;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.WorkflowMapping;

@Service
public class WorkflowService {

    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ThreadPool executor;
    
    @WorkflowMapping(name=COMMON_STATE_DECISION, privilege=LEADER,
            requiredMode=CLUSTER_ON)
    public JobResult failoverCommon(@ClusterHint HeartbeatTarget target)
            throws NoNodeException, MgmtZooKeeperException,
            MgmtZNodeDoesNotExistException, MgmtZNodeAlreayExistsException {
        CommonStateDecisionWorkflow failoverCommandWorkflow = 
                new CommonStateDecisionWorkflow(target, context);
        
        failoverCommandWorkflow.execute(executor);
        
        return null;
    }
    
    @LockMapping(name=COMMON_STATE_DECISION)
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

    @WorkflowMapping(name=PGS_STATE_DECISION, privilege=LEADER,
            requiredMode=CLUSTER_ON)
    public Future<JobResult> failoverPgs(@ClusterHint PartitionGroupServer target)
            throws NoNodeException, MgmtZooKeeperException,
            IOException, BeansException, MgmtDuplicatedReservedCallException {
        PGSStateDecisionWorkflow failoverPGSWorkflow = 
                new PGSStateDecisionWorkflow(target, context);
        
        failoverPGSWorkflow.execute(executor);
        
        return null;
    }
    
    @LockMapping(name=PGS_STATE_DECISION)
    public void failoverPgsLock(HierarchicalLockHelper lockHelper,
            PartitionGroupServer target) {
        final String path = target.getPath();

        String clusterName = PathUtil.getClusterNameFromPath(path);
        NodeType nodeType = PathUtil.getNodeTypeFromPath(path);

        switch (nodeType) {
        case PGS:
            HierarchicalLockPGSList pgListLock = 
                lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(READ);
            pgListLock.pg(WRITE, String.valueOf(target.getPgId()))
                    .pgs(WRITE, Constant.ALL_IN_PG).gwList(READ);
            break;
            
        default:
            Logger.error("Not supported node type. type: {}", nodeType);
            throw new AssertionError("Not supported node type. type: " + nodeType);
        }
    }

    @WorkflowMapping(name = BLUE_JOIN, privilege = LEADER,
            requiredMode=CLUSTER_ON)
    public Future<JobResult> blueJoin(@ClusterHint PartitionGroup pg,
            long epoch, ApplicationContext context) throws Exception {
        BlueJoinWorkflow wf = new BlueJoinWorkflow(pg, true, context);
        checkEpochAndExecute(wf, epoch, pg.getLastWfEpoch());
        return null;
    }

    @LockMapping(name=BLUE_JOIN)
    public void blueJoinLock(HierarchicalLockHelper lockHelper,
            PartitionGroup pg) {
        final String path = pg.getPath();

        String clusterName = PathUtil.getClusterNameFromPath(path);

        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(WRITE, pg.getName())
                .pgs(WRITE, Constant.ALL_IN_PG).gwList(READ);
    }

    @WorkflowMapping(name = MASTER_ELECTION, privilege = LEADER,
            requiredMode=CLUSTER_ON)
    public Future<JobResult> masterElection(@ClusterHint PartitionGroup pg,
            List<PartitionGroupServer> masterHints, long epoch,
            ApplicationContext context) throws Exception {
        MasterElectionWorkflow wf = new MasterElectionWorkflow(pg, masterHints,
                true, context);
        checkEpochAndExecute(wf, epoch, pg.getLastWfEpoch());
        return null;
    }

    @LockMapping(name = MASTER_ELECTION)
    public void masterElectionLock(HierarchicalLockHelper lockHelper,
            PartitionGroup pg) {
        final String path = pg.getPath();

        String clusterName = PathUtil.getClusterNameFromPath(path);

        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(WRITE, pg.getName())
                .pgs(WRITE, Constant.ALL_IN_PG).gwList(READ);
    }

    @WorkflowMapping(name = MEMBERSHIP_GRANT, privilege = LEADER,
            requiredMode=CLUSTER_ON)
    public Future<JobResult> membershipGrant(@ClusterHint PartitionGroup pg,
            long epoch, ApplicationContext context) throws Exception {
        MembershipGrantWorkflow wf = new MembershipGrantWorkflow(pg, true,
                context);
        checkEpochAndExecute(wf, epoch, pg.getLastWfEpoch());
        return null;
    }

    @LockMapping(name=MEMBERSHIP_GRANT)
    public void membershipGrantLock(HierarchicalLockHelper lockHelper,
            PartitionGroup pg) {
        final String path = pg.getPath();
        String clusterName = PathUtil.getClusterNameFromPath(path);
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(WRITE, pg.getName())
                .pgs(WRITE, Constant.ALL_IN_PG).gwList(READ);
    }

    @WorkflowMapping(name = ROLE_ADJUSTMENT, privilege = LEADER,
            requiredMode=CLUSTER_ON)
    public Future<JobResult> roleAdjustment(@ClusterHint PartitionGroup pg,
            long epoch, ApplicationContext context) throws Exception {
        RoleAdjustmentWorkflow wf = new RoleAdjustmentWorkflow(pg, true, context);
        checkEpochAndExecute(wf, epoch, pg.getLastWfEpoch());
        return null;
    }

    @LockMapping(name=ROLE_ADJUSTMENT)
    public void roleAdjustmentLock(HierarchicalLockHelper lockHelper,
            PartitionGroup pg) {
        final String path = pg.getPath();

        String clusterName = PathUtil.getClusterNameFromPath(path);

        HierarchicalLockPGSList pgListLock = lockHelper.root(READ)
                .cluster(READ, clusterName).pgList(READ).pgsList(READ);
        pgListLock.pg(WRITE, pg.getName()).pgs(WRITE, Constant.ALL_IN_PG)
                .gwList(READ);
    }

    @WorkflowMapping(name = QUORUM_ADJUSTMENT, privilege = LEADER,
            requiredMode=CLUSTER_ON)
    public Future<JobResult> quorumAdjustment(@ClusterHint PartitionGroup pg,
            long epoch, ApplicationContext context) throws Exception {
        QuorumAdjustmentWorkflow wf = new QuorumAdjustmentWorkflow(pg, true,
                context);
        checkEpochAndExecute(wf, epoch, pg.getLastWfEpoch());
        return null;
    }

    @LockMapping(name=QUORUM_ADJUSTMENT)
    public void quorumAdjustmentLock(HierarchicalLockHelper lockHelper,
            PartitionGroup pg) {
        final String path = pg.getPath();
        String clusterName = PathUtil.getClusterNameFromPath(path);
        
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(WRITE, pg.getName())
                .pgs(WRITE, Constant.ALL_IN_PG).gwList(READ);
    }

    @WorkflowMapping(name = YELLOW_JOIN, privilege = LEADER,
            requiredMode=CLUSTER_ON)
    public Future<JobResult> yellowJoin(@ClusterHint PartitionGroup pg, long epoch,
            ApplicationContext context) throws Exception {
        YellowJoinWorkflow wf = new YellowJoinWorkflow(pg, true, context);
        checkEpochAndExecute(wf, epoch, pg.getLastWfEpoch());
        return null;
    }

    @LockMapping(name = YELLOW_JOIN)
    public void yellowJoinLock(HierarchicalLockHelper lockHelper,
            PartitionGroup pg) {
        final String path = pg.getPath();

        String clusterName = PathUtil.getClusterNameFromPath(path);

        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(WRITE, pg.getName())
                .pgs(WRITE, Constant.ALL_IN_PG).gwList(READ);
    }
    
    @WorkflowMapping(name=OPINION_DISCARD, privilege=FOLLOWER,
            requiredMode=CLUSTER_ON|CLUSTER_OFF)
    public JobResult discardOpinion(@ClusterHint HeartbeatTarget target)
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

    @WorkflowMapping(name=OPINION_PUBLISH, privilege=FOLLOWER,
            requiredMode=CLUSTER_ON)
    public JobResult publishOpinion(@ClusterHint HBResult result)
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
    
    @WorkflowMapping(name=TOTAL_INSPECTION, privilege=LEADER)
    public Future<JobResult> updateHeartbeatChecker()
            throws MgmtZooKeeperException {
        TotalInspectionWorkflow updateHeartbeatCheckerWorkflow = 
                new TotalInspectionWorkflow(context);
        
        updateHeartbeatCheckerWorkflow.execute(executor);
        
        return null;
    }

    @LockMapping(name=TOTAL_INSPECTION)
    public void updateHeartbeatCheckerLock(HierarchicalLockHelper lockHelper) {
        lockHelper.root(WRITE);
    }
    
    private void checkEpochAndExecute(CascadingWorkflow wf, long epoch, long lastEpoch)
            throws Exception {
        if (lastEpoch != epoch) {
            return;
        }

        Logger.setMsgDecorator(new EpochMsgDecorator(epoch));
        wf.pg.incWfCnt();
        try {
            wf.execute();
        } finally {
            wf.pg.decWfCnt();
            Logger.setMsgDecorator(null);
        }
    }

}

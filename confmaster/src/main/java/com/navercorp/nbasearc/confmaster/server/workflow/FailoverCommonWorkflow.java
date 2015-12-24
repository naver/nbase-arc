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

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.GatewayDao;
import com.navercorp.nbasearc.confmaster.repository.dao.NotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.OpinionDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.OpinionData;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.FailureDetector;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.imo.FailureDetectorImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;

public class FailoverCommonWorkflow {
    
    private ZooKeeperHolder zookeeper;
    private HeartbeatTarget target;
    private int majority;

    private final String path;
    private final long jobID = JobIDGenerator.getInstance().getID();

    private final FailureDetectorImo fdImo;
    private final GatewayImo gwImo;
    
    private final GatewayDao gwDao;
    private final OpinionDao opinionDao;
    private final WorkflowLogDao workflowLogDao;
    private final NotificationDao notificationDao;

    protected FailoverCommonWorkflow(HeartbeatTarget target, ApplicationContext context) {
        this.target = target;
        this.path = target.getPath();
        
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        
        this.fdImo = context.getBean(FailureDetectorImo.class);
        this.gwImo = context.getBean(GatewayImo.class); 
        
        this.gwDao = context.getBean(GatewayDao.class);
        this.opinionDao = context.getBean(OpinionDao.class);
        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
        this.notificationDao = context.getBean(NotificationDao.class);
    }
    
    public String execute(ThreadPool executor) throws NoNodeException,
            MgmtZooKeeperException, MgmtZNodeDoesNotExistException,
            MgmtZNodeAlreayExistsException {
        majority = fdImo.get().getMajority();
        if (NodeType.PGS == target.getNodeType()) {
            Logger.error("This workflow is not for a pgs.");
            return null;
        }
        Logger.info("Failover start. {}", target);
        
        GatherOpinionsResult gatherOpinionsResult = gatherOpinions();
        if (gatherOpinionsResult == null) {
            return null;
        }
        if (gatherOpinionsResult.success == false) {
            return null;
        }
        
        MakeDecisionResult makeDecisionResult = 
                makeDecision(gatherOpinionsResult.opinions);
        if (makeDecisionResult.success == false) {
            return null;
        }

        changeState(makeDecisionResult.newState);
        return null;
    }

    class GatherOpinionsResult {
        public final boolean success;
        public final List<OpinionData> opinions;
        
        public GatherOpinionsResult(final boolean success) {
            this.success = success;
            this.opinions = null;
        }
        
        public GatherOpinionsResult(
                final boolean success,
                final List<OpinionData> opinions) {
            this.success = success;
            this.opinions = opinions;
        }
    }
    
    /**
     * @return Returns GatherOpinionsResult if successful or null otherwise.
     */
    private GatherOpinionsResult gatherOpinions() throws NoNodeException,
            MgmtZooKeeperException {
        FailureDetector fd = fdImo.get();
        majority = fd.getMajority();
        
        List<OpinionData> opinions;
        opinions = opinionDao.getOpinions(path);
        if (0 == opinions.size()) {
            Logger.info("Majority check fail. "
                    + "Total: 0, Available: 0, Majority: " + majority);
            return null;
        }
        Logger.debug("Opinion: {}", opinions.toString());

        int numberOfOpinions = opinions.size();
        for (OpinionData data : opinions) {
            if (target.getVersion() > data.getVersion()) {
                numberOfOpinions--;
            }
        }

        final String log = "Total:" + opinions.size() + ", Available:"
                + numberOfOpinions + ", Majority:" + majority; 
        if (numberOfOpinions >= majority) {
            Logger.info("Majority check success. " + log);
            return new GatherOpinionsResult(true, opinions);
        } else {
            Logger.info("Majority check fail. " + log);
            return new GatherOpinionsResult(false, opinions);
        }
    }

    class MakeDecisionResult {
        public final boolean success;
        public final String newState;
        
        public MakeDecisionResult(boolean success) { 
            this.success = success;
            newState = null;
        }

        public MakeDecisionResult(boolean success,
                String newState) { 
            this.success = success;
            this.newState = newState;
        }
    }
    
    private MakeDecisionResult makeDecision(List<OpinionData> opinions) {
        int F = 0;
        int N = 0;

        for (OpinionData data : opinions) {
            if (target.getVersion() != data.getVersion()) {
                continue;
            }

            String opinion = data.getOpinion();
            if (opinion.equals(Constant.SERVER_STATE_FAILURE)) {
                F++;
            } else if (opinion.equals(Constant.SERVER_STATE_NORMAL)) {
                N++;
            }
        }

        String newState = Constant.SERVER_STATE_FAILURE;

        ArrayList<Integer> li = new ArrayList<Integer>();
        li.add(F);
        li.add(N);

        int maxIdx = 0;
        for (int i = 0; i < li.size(); i++) {
            if (li.get(maxIdx) < li.get(i)) {
                maxIdx = i;
            }
        }

        if (0 == maxIdx) {
            newState = Constant.SERVER_STATE_FAILURE;
        } else if (1 == maxIdx) {
            newState = Constant.SERVER_STATE_NORMAL;
        }

        Logger.info("PATH:" + target.getTargetOfHeartbeatPath() + ",  STAT:" + newState);

        if (newState.equals(Constant.SERVER_STATE_FAILURE)) {
            workflowLogDao.log(jobID,
                            Constant.SEVERITY_MAJOR,
                            "CommonFailureDetectionWorkflow",
                            Constant.LOG_TYPE_WORKFLOW,
                            target.getClusterName(),
                            "State changed. " + target + ", " + target.getState() + "->" + newState,
                            String.format(
                                    "{\"type\":\"%s\",\"id\":%s,\"old_state\":\"%s\",\"new_state\":\"%s\"}",
                                    target.getNodeType(), target.getName(),
                                    target.getState(), newState));
        } else if (newState.equals(Constant.SERVER_STATE_NORMAL)) {
            workflowLogDao.log(jobID,
                            Constant.SEVERITY_MODERATE,
                            "CommonFailureDetectionWorkflow",
                            Constant.LOG_TYPE_WORKFLOW,
                            target.getClusterName(),
                            "State changed. " + target + ", " + target.getState() + "->" + newState,
                            String.format(
                                    "{\"type\":\"%s\",\"id\":%s,\"old_state\":\"%s\",\"new_state\":\"%s\"}",
                                    target.getNodeType(), target.getName(),
                                    target.getState(), newState));
        }

        return new MakeDecisionResult(true, newState);
    }
    
    private void changeState(String newState) throws MgmtZooKeeperException,
            MgmtZNodeDoesNotExistException, MgmtZNodeAlreayExistsException {
        switch (target.getNodeType()) {
        case GW:
            chsnageStateGateway(target, newState);
            break;

        default:
            target.setState(newState, 0);
            zookeeper.reflectMemoryIntoZk(target.getZNode());
            break;
        }
    }
    
    private void chsnageStateGateway(HeartbeatTarget target, String newState)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException,
            MgmtZNodeAlreayExistsException {
        if (NodeType.GW != target.getNodeType()) {
            Logger.error("Incorrect type, it requires gateway, but {}", target);
            return;
        }
        
        Gateway gw = gwImo.get(target.getName(), target.getClusterName());
        boolean existInZookeeper = 
                notificationDao.isGatewayExist(gw.getClusterName(), gw.getName());

        List<Op> ops = new ArrayList<Op>();
        if (newState.equals(Constant.SERVER_STATE_FAILURE)) {
            if (existInZookeeper) {
                notificationDao.addDeleteGatewayOp(ops,
                        gw.getClusterName(), gw.getName());
            }
        } else if (newState.equals(Constant.SERVER_STATE_NORMAL) && !existInZookeeper) {
            notificationDao.addCreateGatewayOp(ops,
                    gw.getClusterName(), gw.getName(), gw.getData().getPmIp(),
                    gw.getData().getPort());
        }

        if (!ops.isEmpty()) {
            GatewayData gwData = (GatewayData) gw.getData().clone();
            gwData.setState(newState);

            MemoryObjectMapper mapper = new MemoryObjectMapper();
            byte[] data = mapper.writeValueAsBytes(gwData);

            ops.add(Op.setData(gw.getPath(), data, -1));

            List<OpResult> results = zookeeper.multi(ops);
            for (OpResult result : results) {
                if (result.getType() == ZooDefs.OpCode.error) {
                    OpResult.ErrorResult errRes = (OpResult.ErrorResult) result;
                    if (errRes.getErr() != 0) {
                        Logger.error("Change gateway state fail. {}, state: {}", gw, newState); 
                    }
                }
            }
        }
        
        gw.setState(newState, 0);
        gwDao.updateGw(gw);
    }

    public String getPath() {
        return path;
    }

}

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

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.GatewayLookup;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.Opinion;
import com.navercorp.nbasearc.confmaster.server.cluster.Opinion.OpinionData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.UsedOpinionSet;

public class PGSStateDecisionWorkflow {
    private final long jobID = JobIDGenerator.getInstance().getID();
    
    private Cluster cluster;
    private PartitionGroup pg;
    private PartitionGroupServer pgs;
    private HeartbeatTarget target;
    private int majority;

    private final ApplicationContext context;
    private final ConfMaster confmaster;
    private final WorkflowExecutor wfExecutor;

    private final String path;

    private final ZooKeeperHolder zk;
    private final GatewayLookup gwInfoNotifier;
    private final WorkflowLogger workflowLogger;
    private final ClusterComponentContainer container;
    private final Opinion opinion;

    protected PGSStateDecisionWorkflow(HeartbeatTarget target,
            ApplicationContext context) {
        this.target = target;
        this.path = target.getPath();

        this.context = context;
        this.confmaster = context.getBean(ConfMaster.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);

        this.zk = context.getBean(ZooKeeperHolder.class);
        this.gwInfoNotifier = context.getBean(GatewayLookup.class);
        this.workflowLogger = context.getBean(WorkflowLogger.class);
        this.container = context.getBean(ClusterComponentContainer.class);
        this.opinion = context.getBean(Opinion.class);
    }

    public String execute(ThreadPool executor) throws NoNodeException,
            MgmtZooKeeperException, IOException, BeansException,
            MgmtDuplicatedReservedCallException {
        pgs = (PartitionGroupServer) container.get(target.getPath());
        if (pgs == null) {
            Logger.error("{} does not exists.", target);
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST
                            + target.getFullName());
        }
        pg = container.getPg(target.getClusterName(), Integer.toString(pgs.getPgId()));
        cluster = container.getCluster(target.getClusterName());
        majority = confmaster.getMajority();

        Logger.info("start reconfiguration. {}", target);

        GatherOpinionsResult gatherOpinionsResult = gatherOpinions();
        if (gatherOpinionsResult == null) {
            return null;
        }
        if (gatherOpinionsResult.success == false) {
            return null;
        }

        MakeDecisionResult makeDecisionResult = makeDecision(
                gatherOpinionsResult.opinions,
                gatherOpinionsResult.maxStateTimestamp);
        if (makeDecisionResult == null) {
            return null;
        }

        if (timestampValidation(gatherOpinionsResult.maxStateTimestamp,
                target.getView(), makeDecisionResult.newView) == false) {
            return null;
        }
        
        String oldRole = pgs.getRole();
        PartitionGroupServer.PartitionGroupServerData pgsM = pgs.clonePersistentData();
        pgsM.setRole(makeDecisionResult.newView);
        pgsM.stateTimestamp = gatherOpinionsResult.maxStateTimestamp;
        zk.setData(pgs.getPath(), pgsM.toBytes(), -1);
        pgs.setPersistentData(pgsM);

        wfExecutor.perform(ROLE_ADJUSTMENT, pg, pg.nextWfEpoch(), context);

        if (makeDecisionResult.newView.equals(Constant.PGS_ROLE_NONE)) {
            workflowLogger.log(jobID,
                            Constant.SEVERITY_MAJOR,
                            "PGSStateDecisionWorkflow",
                            Constant.LOG_TYPE_WORKFLOW,
                            target.getClusterName(),
                            "state changed. " + target + ", " + oldRole + "->" + makeDecisionResult.newView,
                            String.format(
                                    "{\"type\":\"%s\",\"id\":%s,\"old_state\":\"%s\",\"new_state\":\"%s\"}",
                                    target.getNodeType(), target.getName(),
                                    oldRole, makeDecisionResult.newView));
        } else {
            workflowLogger.log(jobID,
                            Constant.SEVERITY_MODERATE,
                            "PGSStateDecisionWorkflow",
                            Constant.LOG_TYPE_WORKFLOW,
                            target.getClusterName(),
                            "state changed. " + target + ", " + oldRole + "->" + makeDecisionResult.newView,
                            String.format(
                                    "{\"type\":\"%s\",\"id\":%s,\"old_state\":\"%s\",\"new_state\":\"%s\"}",
                                    target.getNodeType(), target.getName(),
                                    oldRole, makeDecisionResult.newView));
        }

        gwInfoNotifier.updateGatewayAffinity(cluster);

        return null;
    }

    class GatherOpinionsResult {
        public final boolean success;
        public final List<OpinionData> opinions;
        public final long maxStateTimestamp;

        public GatherOpinionsResult(final boolean success,
                final List<OpinionData> opinions, final long maxStateTimestampy) {
            this.success = success;
            this.opinions = opinions;
            this.maxStateTimestamp = maxStateTimestampy;
        }
    }

    /**
     * @return Returns GatherOpinionsResult if successful or null otherwise.
     * @throws NoNodeException
     * @throws MgmtZooKeeperException
     */
    private GatherOpinionsResult gatherOpinions() throws NoNodeException,
            MgmtZooKeeperException {
        List<OpinionData> opinions;
        opinions = opinion.getOpinions(path);
        if (0 == opinions.size()) {
            Logger.info("majority check fail. "
                    + "total: 0, available: 0, majority: " + majority);
            return null;
        }
        Logger.debug("ipinion: {}", opinions.toString());

        UsedOpinionSet usedOpinions = target.getUsedOpinions();
        Logger.debug("used opinions: {}", usedOpinions.toString());
        usedOpinions.update(opinions);
        // remove used opinions to prevent duplicated decisions
        opinions.removeAll(usedOpinions.collection());

        long maxStateTimestamp = 0;
        for (OpinionData data : opinions) {
            maxStateTimestamp = Math.max(maxStateTimestamp,
                    data.getStatetimestamp());
        }

        Logger.debug("remaining opinions: {}", opinions.toString());
        int availableOpinionCount = opinions.size();
        for (OpinionData data : opinions) {
            if (maxStateTimestamp != data.getStatetimestamp()) {
                availableOpinionCount--;
            }
        }

        final String log = "total: " + opinions.size() + ", available: "
                + availableOpinionCount + ", majority: " + majority;
        if (availableOpinionCount >= majority) {
            Logger.info("majority check success. " + log);
            return new GatherOpinionsResult(true, opinions, maxStateTimestamp);
        } else {
            Logger.info("majority check fail. " + log);
            return new GatherOpinionsResult(false, opinions, maxStateTimestamp);
        }
    }

    class MakeDecisionResult {
        public final String newView;
        public final long maxStateTimestamp;

        public MakeDecisionResult(String newView, long maxStateTimestamp) {
            this.newView = newView;
            this.maxStateTimestamp = maxStateTimestamp;
        }
    }

    /**
     * @return Returns MakeDecisionResult if successful or null otherwise.
     */
    private MakeDecisionResult makeDecision(List<OpinionData> opinions,
            long maxStateTimestamp) {
        int F = 0;
        int L = 0;
        int S = 0;
        int M = 0;

        for (OpinionData data : opinions) {
            if (data.getStatetimestamp() != maxStateTimestamp) {
                continue;
            }

            String opinion = data.getOpinion();
            if (opinion.equals(PGS_ROLE_NONE)) {
                F++;
            } else if (opinion.equals(PGS_ROLE_LCONN)) {
                L++;
            } else if (opinion.equals(PGS_ROLE_SLAVE)) {
                S++;
            } else if (opinion.equals(PGS_ROLE_MASTER)) {
                M++;
            }
        }

        String role = SERVER_STATE_FAILURE;

        ArrayList<Integer> li = new ArrayList<Integer>();
        li.add(F);
        li.add(L);
        li.add(S);
        li.add(M);

        int maxIdx = 0;
        for (int i = 0; i < li.size(); i++) {
            if (li.get(maxIdx) < li.get(i)) {
                maxIdx = i;
            }
        }

        for (int i = 0; i < li.size(); i++) {
            if (i != maxIdx && li.get(maxIdx) == li.get(i)) {
                Logger.info(
                        "opinions diverged. N: {}, L: {}, S: {}, M: {}.",
                        new Object[] { li.get(0), li.get(1), li.get(2),
                                li.get(3) });
                return null;
            }
        }

        if (0 == maxIdx) {
            role = PGS_ROLE_NONE;
        } else if (1 == maxIdx) {
            role = PGS_ROLE_LCONN;
        } else if (2 == maxIdx) {
            role = PGS_ROLE_SLAVE;
        } else if (3 == maxIdx) {
            role = PGS_ROLE_MASTER;
        }

        UsedOpinionSet usedOpinions = target.getUsedOpinions();
        for (OpinionData data : opinions) {
            if (data.getStatetimestamp() == maxStateTimestamp
                    && data.getOpinion().equals(role)) {
                usedOpinions.add(data);
            }
        }

        Logger.info(
                "opinions converged to {}. N: {}, L: {}, S: {}, M: {}.",
                new Object[] { role, li.get(0), li.get(1), li.get(2), li.get(3) });
        return new MakeDecisionResult(role, maxStateTimestamp);
    }

    /**
     * @return Returns true if successful or false otherwise.
     * @throws IOException
     */
    private boolean timestampValidation(long maximumStateTimestamp,
            String oldView, String newView) throws IOException {
        boolean validStateTimestamp;

        if (!newView.equals(PGS_ROLE_NONE)) {
            long ats = pgs.getActiveStateTimestamp();
            if (maximumStateTimestamp >= ats) {
                validStateTimestamp = true;
                Logger.info(
                        "timestamp validation success. role: {}->{}, timestamp: zk{} active{}->hb{}",
                        new Object[] { oldView, newView,
                                pgs.getStateTimestamp(),
                                ats, maximumStateTimestamp });
            } else {
                validStateTimestamp = false;
                Logger.info(
                        "timestamp validation fail. role: {}->{}, timestamp: zk{} active{}->hb{}",
                        new Object[] { oldView, newView,
                                pgs.getStateTimestamp(),
                                ats, maximumStateTimestamp });
            }
        } else {
            if (maximumStateTimestamp < pgs.getStateTimestamp()) {
                validStateTimestamp = false;
                Logger.info(
                        "timestamp validation fail. role: {}->{}, timestamp: zk{}->hb{}",
                        new Object[] { oldView, newView,
                                pgs.getStateTimestamp(),
                                maximumStateTimestamp });
            } else {
                validStateTimestamp = true;
                Logger.info(
                        "timestamp validation success. role: {}->{}, timestamp: zk{}->hb{}",
                        new Object[] { oldView, newView,
                                pgs.getStateTimestamp(),
                                maximumStateTimestamp });
            }
        }

        if (validStateTimestamp) {
            return true;
        } else {
            return false;
        }
    }
}

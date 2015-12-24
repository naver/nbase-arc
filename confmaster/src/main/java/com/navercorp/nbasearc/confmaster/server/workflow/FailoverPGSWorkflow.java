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

import static com.navercorp.nbasearc.confmaster.Constant.ERROR;
import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST;
import static com.navercorp.nbasearc.confmaster.Constant.LOG_TYPE_WORKFLOW;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_PING;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_MASTER;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_NONE;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_LCONN;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_NORMAL;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MAJOR;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MODERATE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.SET_QUORUM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtUnexpectedStateTransitionException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.context.Context;
import com.navercorp.nbasearc.confmaster.context.ContextType;
import com.navercorp.nbasearc.confmaster.context.ExecutionContextPar;
import com.navercorp.nbasearc.confmaster.io.SMRRoleGetter;
import com.navercorp.nbasearc.confmaster.io.SMRRoleGetter.Result;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.NotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.OpinionDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.OpinionData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.UsedOpinionSet;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.FailureDetectorImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class FailoverPGSWorkflow {

    private ThreadPool executor;

    private Cluster cluster;
    private PartitionGroup pg;
    private PartitionGroupServer pgs;
    private List<PartitionGroupServer> joinedPgsList;
    private HeartbeatTarget target;
    private int majority;
    private TransitionType todo;

    private final String path;
    private final long jobID = JobIDGenerator.getInstance().getID();

    private final FailureDetectorImo fdImo;
    private final ClusterImo clusterImo;
    private final PartitionGroupImo pgImo;
    private final PartitionGroupServerImo pgsImo;

    private final PartitionGroupServerDao pgsDao;
    private final OpinionDao opinionDao;
    private final WorkflowLogDao workflowLogDao;
    private final NotificationDao notificationDao;

    private final WorkflowExecutor workflowExecutor;

    public enum TransitionType {
        TODO_UNKOWN(-1), TODO_EXCEPTION(1), TODO_SLAVE_FAILURE(2), TODO_MASTER_FAILURE(
                3), TODO_EXPECTED(4), TODO_FAILURE_TO_NORMAL(7);

        private int value;

        private TransitionType(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            switch (this) {
            case TODO_UNKOWN:
                return "TODO_UNKOWN";
            case TODO_EXCEPTION:
                return "TODO_EXCEPTION";
            case TODO_SLAVE_FAILURE:
                return "TODO_SLAVE_FAILURE";
            case TODO_MASTER_FAILURE:
                return "TODO_MASTER_FAILURE";
            case TODO_EXPECTED:
                return "TODO_EXPECTED";
            case TODO_FAILURE_TO_NORMAL:
                return "TODO_FAILURE_TO_NORMAL";
            }
            return "TODO_UNKOWN";
        }
    }

    protected FailoverPGSWorkflow(HeartbeatTarget target,
            ApplicationContext context) {
        this.target = target;
        this.path = target.getPath();

        this.fdImo = context.getBean(FailureDetectorImo.class);
        this.clusterImo = context.getBean(ClusterImo.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.pgImo = context.getBean(PartitionGroupImo.class);

        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.opinionDao = context.getBean(OpinionDao.class);
        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
        this.notificationDao = context.getBean(NotificationDao.class);

        this.workflowExecutor = context.getBean(WorkflowExecutor.class);
    }

    public String execute(ThreadPool executor) throws NoNodeException,
            MgmtZooKeeperException, MgmtUnexpectedStateTransitionException,
            IOException {
        this.executor = executor;

        pgs = pgsImo.getByPath(target.getPath());
        if (pgs == null) {
            Logger.error("{} does not exists.", target);
            throw new IllegalArgumentException(
                    EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST
                            + target.getFullName());
        }
        pg = pgImo.get(Integer.toString(pgs.getData().getPgId()),
                target.getClusterName());
        cluster = clusterImo.get(target.getClusterName());
        joinedPgsList = pg.getJoinedPgsList(pgsImo.getList(cluster.getName(),
                Integer.parseInt(pg.getName())));
        majority = fdImo.get().getMajority();

        Logger.info("Failover start. {}", target);

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

        todo = getTransitionState(target.getState(), makeDecisionResult.newState);

        if (timestampValidation(
                gatherOpinionsResult.maxStateTimestamp, target.getState(),
                makeDecisionResult.newState) == false) {
            return null;
        }

        try {
            if (consistencyCheck(gatherOpinionsResult.maxStateTimestamp,
                    target.getState(), makeDecisionResult.newState) == false) {
                return null;
            }

            // handle network recovery from network isolation
            handleNetworkRecovery();

            MasterElectionResult masterElectionResult = masterElection();
            if (masterElectionResult == null) {
                return null;
            }

            slaveJoin(masterElectionResult.master);
        } finally {
            notificationDao.updateGatewayAffinity(cluster);
        }
        
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
        opinions = opinionDao.getOpinions(path);
        if (0 == opinions.size()) {
            Logger.info("Majority check fail. "
                    + "Total: 0, Available: 0, Majority: " + majority);
            return null;
        }
        Logger.debug("Opinion: {}", opinions.toString());

        UsedOpinionSet usedOpinions = target.getUsedOpinions();
        Logger.debug("Used opinions: {}", usedOpinions.toString());
        usedOpinions.update(opinions);
        // remove used opinions to prevent duplicated decisions
        opinions.removeAll(usedOpinions.collection());

        long maxStateTimestamp = 0;
        for (OpinionData data : opinions) {
            maxStateTimestamp = Math.max(maxStateTimestamp,
                    data.getStatetimestamp());
        }

        Logger.debug("Remaining opinions: {}", opinions.toString());
        int availableOpinionCount = opinions.size();
        for (OpinionData data : opinions) {
            if (maxStateTimestamp != data.getStatetimestamp()) {
                availableOpinionCount--;
            }
        }
        
        final String log = "Total: " + opinions.size() + ", Available: "
                + availableOpinionCount + ", Majority: " + majority;
        if (availableOpinionCount >= majority) {
            Logger.info("Majority check success. " + log);
            return new GatherOpinionsResult(true, opinions, maxStateTimestamp);
        } else {
            Logger.info("Majority check fail. " + log);
            return new GatherOpinionsResult(false, opinions, maxStateTimestamp);
        }
    }

    class MakeDecisionResult {
        public final String newState;
        public final long maxStateTimestamp;

        public MakeDecisionResult(String newState, long maxStateTimestamp) {
            this.newState = newState;
            this.maxStateTimestamp = maxStateTimestamp;
        }
    }

    /**
     * @return Returns MakeDecisionResult if successful or null otherwise. 
     */
    private MakeDecisionResult makeDecision(List<OpinionData> opinions,
            long maxStateTimestamp) {
        int F = 0;
        int N = 0;
        int L = 0;

        for (OpinionData data : opinions) {
            if (data.getStatetimestamp() != maxStateTimestamp) {
                continue;
            }

            String opinion = data.getOpinion();
            if (opinion.equals(SERVER_STATE_FAILURE)) {
                F++;
            } else if (opinion.equals(SERVER_STATE_NORMAL)) {
                N++;
            } else if (opinion.equals(SERVER_STATE_LCONN)) {
                L++;
            }
        }

        String state = SERVER_STATE_FAILURE;

        ArrayList<Integer> li = new ArrayList<Integer>();
        li.add(F);
        li.add(N);
        li.add(L);

        int maxIdx = 0;
        for (int i = 0; i < li.size(); i++) {
            if (li.get(maxIdx) < li.get(i)) {
                maxIdx = i;
            }
        }

        for (int i = 0; i < li.size(); i++) {
            if (i != maxIdx && li.get(maxIdx) == li.get(i)) {
                Logger.info("Opinions diverged. F={}, N={}, L={}.",
                        new Object[] { li.get(0), li.get(1), li.get(2) });
                return null;
            }
        }

        if (0 == maxIdx) {
            state = SERVER_STATE_FAILURE;
        } else if (1 == maxIdx) {
            state = SERVER_STATE_NORMAL;
        } else if (2 == maxIdx) {
            state = SERVER_STATE_LCONN;
        }

        UsedOpinionSet usedOpinions = target.getUsedOpinions();
        for (OpinionData data : opinions) {
            if (data.getStatetimestamp() == maxStateTimestamp
                    && data.getOpinion().equals(state)) {
                usedOpinions.add(data);
            }
        }

        Logger.info("Opinions converged to {}. F: {}, N: {}, L: {}.",
                new Object[] { state, li.get(0), li.get(1), li.get(2) });
        return new MakeDecisionResult(state, maxStateTimestamp);
    }


    private TransitionType getTransitionState(String oldState, String newState) {
        TransitionType type  = TransitionType.TODO_EXCEPTION;
        if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
            if (oldState.equals(SERVER_STATE_FAILURE)) {
                if (newState.equals(SERVER_STATE_FAILURE)) {
                    type = TransitionType.TODO_EXCEPTION;
                } else if (newState.equals(SERVER_STATE_NORMAL)) {
                    type = TransitionType.TODO_EXCEPTION;
                } else if (newState.equals(SERVER_STATE_LCONN)) {
                    type = TransitionType.TODO_EXCEPTION;
                }
            } else if (oldState.equals(SERVER_STATE_NORMAL)) {
                if (newState.equals(SERVER_STATE_FAILURE)) {
                    type = TransitionType.TODO_MASTER_FAILURE;
                } else if (newState.equals(SERVER_STATE_NORMAL)) {
                    type = TransitionType.TODO_EXCEPTION;
                } else if (newState.equals(SERVER_STATE_LCONN)) {
                    type = TransitionType.TODO_MASTER_FAILURE;
                }
            } else if (oldState.equals(SERVER_STATE_LCONN)) {
                if (newState.equals(SERVER_STATE_FAILURE)) {
                    type = TransitionType.TODO_MASTER_FAILURE;
                } else if (newState.equals(SERVER_STATE_NORMAL)) {
                    type = TransitionType.TODO_EXPECTED;
                } else if (newState.equals(SERVER_STATE_LCONN)) {
                    type = TransitionType.TODO_MASTER_FAILURE;
                }
            }
        } else if (pgs.getData().getRole().equals(PGS_ROLE_SLAVE)) {
            if (oldState.equals(SERVER_STATE_FAILURE)) {
                if (newState.equals(SERVER_STATE_FAILURE)) {
                    type = TransitionType.TODO_EXCEPTION;
                } else if (newState.equals(SERVER_STATE_NORMAL)) {
                    type = TransitionType.TODO_EXCEPTION;
                } else if (newState.equals(SERVER_STATE_LCONN)) {
                    type = TransitionType.TODO_EXCEPTION;
                }
            } else if (oldState.equals(SERVER_STATE_NORMAL)) {
                if (newState.equals(SERVER_STATE_FAILURE)) {
                    type = TransitionType.TODO_SLAVE_FAILURE;
                } else if (newState.equals(SERVER_STATE_NORMAL)) {
                    type = TransitionType.TODO_EXCEPTION;
                } else if (newState.equals(SERVER_STATE_LCONN)) {
                    type = TransitionType.TODO_SLAVE_FAILURE;
                }
            } else if (oldState.equals(SERVER_STATE_LCONN)) {
                if (newState.equals(SERVER_STATE_FAILURE)) {
                    type = TransitionType.TODO_SLAVE_FAILURE;
                } else if (newState.equals(SERVER_STATE_NORMAL)) {
                    type = TransitionType.TODO_EXPECTED;
                } else if (newState.equals(SERVER_STATE_LCONN)) {
                    type = TransitionType.TODO_SLAVE_FAILURE;
                }
            }
        } else if (pgs.getData().getRole().equals(PGS_ROLE_NONE)) {
            if (oldState.equals(SERVER_STATE_FAILURE)) {
                if (newState.equals(SERVER_STATE_NORMAL)) {
                    type = TransitionType.TODO_FAILURE_TO_NORMAL;
                } else {
                    type = TransitionType.TODO_EXPECTED;
                }
            } else {
                type = TransitionType.TODO_EXPECTED;
            }
        }

        if (newState.equals(SERVER_STATE_FAILURE)) {
            try {
                pgs.closeConnection();
            } catch (IOException e) {
                Logger.error(
                        "Server state was failure so that it tried to close connection, but fail.",
                        e);
            }
        }

        Logger.debug("TransitionType: {}", type);
        return type;
    }

    /**
     * @return Returns true if successful or false otherwise. 
     */
    private boolean timestampValidation(long maximumStateTimestamp,
            String oldState, String newState) {
        boolean validStateTimestamp;

        if (oldState.equals(SERVER_STATE_FAILURE) &&
                !newState.equals(SERVER_STATE_FAILURE)) {
            validStateTimestamp = true;
        } else if (TransitionType.TODO_EXCEPTION == todo) {
            if (maximumStateTimestamp <= pgs.getData().getStateTimestamp()) {
                validStateTimestamp = false;
            } else {
                validStateTimestamp = true;
            }
        } else {
            /*
             * Case 1 : maximumStateTimestamp == pgs.data.getStateTimestamp() is
             * valid. If all PGS in a PG is HANG, State Role N S F N -> (Master,
             * HANG -> LCONN then, Slave HANG -> LCONN) L N -> TODO_EXPECTED N S
             * Case 2 : If maximumStateTimestamp <=
             * pgs.data.getStateTimestamp(), then this case will be handled as
             * an exception. State Timestamp N 100 F 101 L 101
             */
            if (maximumStateTimestamp < pgs.getData().getStateTimestamp()) {
                validStateTimestamp = false;
            } else {
                validStateTimestamp = true;
            }
        }

        if (validStateTimestamp) {
            Logger.info(
                    "Timestamp validation success. role: {}, state: {}->{}, timestamp: {}->{}",
                    new Object[] { pgs.getData().getRole(), oldState,
                            newState, pgs.getData().getStateTimestamp(),
                            maximumStateTimestamp });
            return true;
        } else {
            Logger.info(
                    "Timestamp validation fail. role: {}, state: {}->{}, timestamp: {}->{}",
                    new Object[] { pgs.getData().getRole(), oldState, newState,
                            pgs.getData().getStateTimestamp(),
                            maximumStateTimestamp });
            return false;
        }
    }

    /**
     * @return Returns true if successful or false otherwise. 
     * @throws MgmtUnexpectedStateTransitionException
     * @throws MgmtZooKeeperException
     */
    public boolean consistencyCheck(long maximumStateTimestamp,
            String oldState, String newState)
            throws MgmtUnexpectedStateTransitionException,
            MgmtZooKeeperException {
        String severity;
        if (!oldState.equals(SERVER_STATE_FAILURE)
                && newState.equals(SERVER_STATE_FAILURE)) {
            severity = SEVERITY_MAJOR;
        } else if (oldState.equals(SERVER_STATE_NORMAL)) {
            severity = SEVERITY_MAJOR;
        } else {
            severity = SEVERITY_MODERATE;
        }
        workflowLogDao
                .log(jobID,
                        severity,
                        "ConsistencyCheck",
                        LOG_TYPE_WORKFLOW,
                        pgs.getClusterName(),
                        "State changed. " + target + ", " + target.getState() + "->" + newState,
                        "{\"pgsid\":%s,\"old_state\":\"%s\",\"new_state\":\"%s\",\"todo\":\"%s\"}",
                        new Object[] { pgs.getName(), oldState, newState,
                                todo.toString() });

        switch (todo) {
        case TODO_EXCEPTION:
            throw new MgmtUnexpectedStateTransitionException(
                    String.format(
                            "path:%s, todo:%s, pgsid:%s, role:%s, oldstate:%s, newstate:%s, mts:%d",
                            pgs.getPath(), todo, pgs.getName(), pgs.getData().getRole(), 
                            oldState, newState, maximumStateTimestamp));

        case TODO_EXPECTED:
            PartitionGroupServerData pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withState(newState)
                    .withStateTimestamp(maximumStateTimestamp).build();
            
            pgsDao.updatePgs(pgs.getPath(), pgsModified);
            
            pgs.setData(pgsModified);
            break;

        case TODO_FAILURE_TO_NORMAL:
            // pgs.data.setState must be called before
            // MasterElectionHelper.getRole,
            // because MasterElectionHelper.getRole function references the
            // current state of PGS.
            pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withState(newState).build();
            
            final PartitionGroupServer.RealState smrState = pgs.getRealState();
            if (!smrState.isSuccess()) {
                pgs.setData(pgsModified);
                return false;
            }

            pgsModified = 
                PartitionGroupServerData.builder().from(pgsModified)
                    .withRole(smrState.getRole())
                    .withStateTimestamp(maximumStateTimestamp).build();

            pgsDao.updatePgs(pgs.getPath(), pgsModified);
            
            pgs.setData(pgsModified);
            break;

        case TODO_MASTER_FAILURE:
        case TODO_SLAVE_FAILURE:
            pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withRole(PGS_ROLE_NONE)
                    .withState(newState)
                    .withStateTimestamp(maximumStateTimestamp).build();
            
            pgsDao.updatePgs(pgs.getPath(), pgsModified);
            
            pgs.setData(pgsModified);
            break;
        }

        String result;
        final long startTimeMillis = System.currentTimeMillis();
        if (todo == TransitionType.TODO_FAILURE_TO_NORMAL) {
            result = checkPGConsistencyAllPGS(joinedPgsList);
        } else {
            result = checkPGConsistencyNormalPGS(joinedPgsList);
        }
        final long endTimeMillis = System.currentTimeMillis();

        if (null != result) {
            Logger.info("Consistency check fail. elapsed: {}ms",
                    endTimeMillis - startTimeMillis);
            if (todo == TransitionType.TODO_FAILURE_TO_NORMAL) {
                pgs.setTodoForFailover(TransitionType.TODO_FAILURE_TO_NORMAL);
            }
            return false;
        } else {
            Logger.info("Consistency check success. elapsed: {}ms",
                    endTimeMillis - startTimeMillis);
            return true;
        }
    }

    class HandleNetworkRecoveryResult {
        public final boolean end;

        public HandleNetworkRecoveryResult(boolean end) {
            this.end = end;
        }
    }

    /**
     * @return Returns true if workflow have to run on or false otherwise. 
     */
    private void handleNetworkRecovery() throws MgmtZooKeeperException {
        pgs.setTodoForFailover(todo);
        boolean setquorum = true;
        for (PartitionGroupServer joinedPGS : joinedPgsList) {
            try {
                switch (pgs.getTodoForFailover()) {
                case TODO_EXCEPTION:
                    break;

                case TODO_EXPECTED:
                    break;

                case TODO_FAILURE_TO_NORMAL:
                    handleFtoN(joinedPGS);
                    setquorum = false;
                    break;

                case TODO_MASTER_FAILURE:
                    // M S S
                    // F L L
                    // F M S or F S M
                    changeRolesOfAllSlavesToLCONN(joinedPGS);
                    break;

                case TODO_SLAVE_FAILURE:
                    break;
                }
            } finally {
                joinedPGS.setTodoForFailover(TransitionType.TODO_UNKOWN);
            }
        }

        if (!setquorum) {
            /*
             * PGS0(QUORUM) PGS1 M(1) S -> network isloation M(0) F -> network
             * recovery M(0) S -> have to set quorum with 1
             */
            workflowExecutor.perform(SET_QUORUM,
                    cluster.getName(), pg.getName());
        }
    }

    class MasterElectionResult {
        public final PartitionGroupServer master;
        public final LogSequence masterLogSeq;

        public MasterElectionResult(PartitionGroupServer master,
                LogSequence masterLogSeq) {
            this.master = master;
            this.masterLogSeq = masterLogSeq;
        }
    }

    /**
     * @return Returns MasterElectionResult if successful or null otherwise. 
     * @throws IOException
     * @throws MgmtZooKeeperException
     */
    public MasterElectionResult masterElection() throws IOException,
            MgmtZooKeeperException {
        /*
         * If there is no master and there is a slave or slaves, confmaster have to election the new master.
         * 
         * [normal scenario]
         *   M S S
         *   H S S
         *   H H S <- no master so confmaster make this slave lconn
         *   H H L
         *   H H M <- make the slave as the master
         *
         * [exceptional scenario]
         *   M S S
         *   I I I
         *   H S S <- don't make a slave as the master
         *   M S S
         *   
         * It should check consistency of PG, but it will take long time.
         * Gather opinions of all PGS and compare -> It violates primitives.
         */
        int masterCnt = 0;
        for (PartitionGroupServer joinedPGS : joinedPgsList) {
            if ((joinedPGS.getData().getState().equals(SERVER_STATE_NORMAL))
                    && joinedPGS.getData().getRole()
                            .equals(PGS_ROLE_MASTER)) {
                Logger.info("Master is {}", pgs);
                masterCnt++;
            }
        }

        if (masterCnt == 0) {
            Logger.info("No master. role lconn to all available pgs.");
            for (PartitionGroupServer joinedPGS : joinedPgsList) {
                if (joinedPGS.getData().getState().equals(SERVER_STATE_NORMAL)
                        && joinedPGS.getData().getRole().equals(PGS_ROLE_SLAVE)) {
                    joinedPGS.roleLconn();
                }
            }
        } else if (masterCnt > 1) {
            Logger.info("Duplicated master. role lconn to all available pgs.");
            for (PartitionGroupServer joinedPGS : joinedPgsList) {
                if (joinedPGS.getData().getState().equals(SERVER_STATE_NORMAL)
                        && joinedPGS.getData().getRole().equals(PGS_ROLE_MASTER)
                        && joinedPGS.getData().getMasterGen() != pg.getData().currentGen()) {
                    joinedPGS.roleLconn();
                }
            }
        }

        List<Integer> pgsList = pg.getData().getPgsIdList();

        // Log state of partition group servers.
        logStatesOfAllPartitionGropuServers(cluster.getName(), pgsList);

        // If master exists, this function jump to SlavejoinWorkflow.
        for (PartitionGroupServer pgs : joinedPgsList) {
            if ((pgs.getData().getState().equals(SERVER_STATE_LCONN) 
                    || pgs.getData().getState().equals(SERVER_STATE_NORMAL))
                        && pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
                LogSequence logSeq = getLogSequence(pgs);
                if (logSeq == null) {
                    return null;
                } else {
                    return new MasterElectionResult(pgs, logSeq);
                }
            }
        }

        for (PartitionGroupServer pgs : joinedPgsList) {
            PartitionGroupServerData pgsModified = 
                PartitionGroupServerData.builder().from(pgs.getData())
                    .withRole(PGS_ROLE_NONE).build();
            pgs.setData(pgsModified);
        }

        // get Logger sequences
        Map<String, LogSequence> logSeqMap = new LinkedHashMap<String, LogSequence>();
        long maxSeq = -1;
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getData().getState().equals(SERVER_STATE_LCONN)) {
                LogSequence logSeq = getLogSequence(pgs);
                if (logSeq == null) {
                    Logger.info(
                            "Get log sequence of master candidate fail. {}",
                            pgs);
                    continue;
                }

                Logger.info(
                        "Get log sequence of master candidate success. {}, sequence:\"{}\"",
                        pgs, logSeq);
                logSeqMap.put(pgs.getName(), logSeq);

                if (logSeq.getMax() > maxSeq) {
                    maxSeq = logSeq.getMax();
                }
            }
        }

        String nameOfMaster = pg.chooseMasterRandomly(logSeqMap, maxSeq);
        LogSequence masterLogSeq = logSeqMap.get(nameOfMaster);
        PartitionGroupServer master = pgsImo.get(nameOfMaster, cluster.getName());

        if (masterLogSeq == null) {
            Logger.error("Get log sequence of master candidate fail. pg{}",
                    pg.getName());
            return null;
        }
        if (master == null) {
            Logger.error("Get master from pgsImo fail. pg{}, pgs{}",
                    pg.getName(), nameOfMaster);
            return null;
        }

        if (pg.checkCopyQuorum(pg.getAlivePgsList(joinedPgsList).size()) == false) {
            return null;
        }
        
        try {
            master.roleMaster(pg, joinedPgsList, logSeqMap, cluster, 0, jobID,
                    workflowLogDao);
        } catch (MgmtRoleChangeException e) {
            return null;
        }

        return new MasterElectionResult(master, logSeqMap.get(master.getName()));
    }

    public String getPath() {
        return path;
    }

    /**
     * @param joinedPgsList
     * @return Returns null if successful or error message otherwise.
     */
    private String checkPGConsistencyNormalPGS(
            List<PartitionGroupServer> joinedPgsList) {
        Logger.info("Consistency for normal pgs. (exclude crashed pgs)");
        
        boolean consistency = true;
        
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getState().equals(SERVER_STATE_FAILURE)) {
                continue;
            }

            final PartitionGroupServer.RealState smrState = pgs.getRealState();
            if (!smrState.isSuccess()) {
                consistency = false;
            } else if (smrState.getStateTimestamp() != pgs.getData()
                    .getStateTimestamp()) {
                consistency = false;
            }

            if (consistency) {
                Logger.info(
                        "Consistency of {}, {}:{}, role: m({}) == a({}), state: {}",
                        new Object[] { pgs, pgs.getIP(), pgs.getPort(),
                                pgs.getData().getRole(), smrState.getRole(), 
                                pgs.getData().getState() });
            } else {
                Logger.warn(
                        "Inconsistency. {}, {}:{}, role: m({}) != a({}), state: {}",
                        new Object[] { pgs, pgs.getIP(), pgs.getPort(),
                                pgs.getData().getRole(), smrState.getRole(), 
                                pgs.getData().getState() });
            }
        }

        if (consistency) {
            return null;
        } else {
            return "-ERR inconsistent.";
        }
    }

    /**
     * @param joinedPgsList
     * @return Returns null if successful or error message otherwise.
     * @description Compare states of PGSes with real even dead PGSes.
     */
    private String checkPGConsistencyAllPGS(
            List<PartitionGroupServer> joinedPgsList) {
        Logger.info("Consistency for all pgs. (include crashed pgs)");
        
        // Send ping to all joined PGS.
        List<Future<Result>> futureTasks = new ArrayList<Future<Result>>();

        for (PartitionGroupServer pgs : joinedPgsList) {
            SMRRoleGetter job = new SMRRoleGetter(cluster.getName(), pgs, PGS_PING);

            Context<Result> context = new ExecutionContextPar<Result>(job,
                    ContextType.PS, Logger.getLogHistory());

            Future<Result> future = executor.perform(context);

            futureTasks.add(future);
        }

        boolean consistency = true;
        List<Result> results = new ArrayList<Result>();
        for (Future<Result> future : futureTasks) {
            try {
                results.add(future.get());
            } catch (InterruptedException e) {
                Logger.error("-ERR failed to get result from SMRRoleGetter", e);
            } catch (ExecutionException e) {
                Logger.error("-ERR failed to get result from SMRRoleGetter", e);
            }
        }

        for (Result result : results) {
            final PartitionGroupServer pgs = result.getPGS();

            if (!pgs.getState().equals(SERVER_STATE_FAILURE)
                    && result.getResult().equals(ERROR)) {
                Logger.warn(
                        "inconsistency. {}, {}:{}, role: m({}), a({}), state: {}, reply: {}",
                        new Object[] { pgs, pgs.getIP(), pgs.getPort(),
                                pgs.getData().getRole(), result.getRole(),
                                pgs.getData().getState(), result.getReply() });
                consistency = false;
            }

            if (result.getPGS().getState().equals(SERVER_STATE_FAILURE)
                    && result.getResult().equals(S2C_OK)) {
                Logger.warn(
                        "inconsistency. {}, {}:{}, role: m({}), a({}), state: {}, reply: {}",
                        new Object[] { pgs, pgs.getIP(), pgs.getPort(),
                                pgs.getData().getRole(), result.getRole(),
                                pgs.getData().getState(), result.getReply() });
                consistency = false;
            }

            if (consistency) {
                Logger.info(
                        "consistency. {}, {}:{}, role: m({}), a({}), state: m({}), reply: {}",
                        new Object[] { pgs, pgs.getIP(), pgs.getPort(),
                                pgs.getData().getRole(), result.getRole(),
                                pgs.getData().getState(), result.getReply() });
            }
        }

        if (consistency) {
            return null;
        } else {
            return "-ERR inconsistent.";
        }
    }

    private void logStatesOfAllPartitionGropuServers(String clusterName,
            List<Integer> pgsList) {
        Logger.info("List of pgs:");
        for (Integer pgsid : pgsList) {
            PartitionGroupServer pgs = pgsImo.get(Integer.toString(pgsid),
                    clusterName);
            Logger.info(
                    "{}, {}:{}, hb: {}, role: {}, state: {}, timestamp: {}",
                    new Object[] { pgs, pgs.getData().getPmIp(),
                            pgs.getData().getSmrBasePort(),
                            pgs.getData().getHb(), pgs.getData().getRole(),
                            pgs.getState(), pgs.getStateTimestamp() });
        }
    }

    private void handleFtoN(PartitionGroupServer pgs)
            throws MgmtZooKeeperException {
        // If a control flow reaches it, then this PG is consistent.
        try {
            // Get role of the real server.
            final PartitionGroupServer.RealState smrState = pgs.getRealState();
            if (!smrState.getRole().equals(PGS_ROLE_MASTER)
                    && !smrState.getRole().equals(PGS_ROLE_SLAVE)) {
                Logger.warn(
                        "State transition is N->F, but failed to get role of pgs. "
                                + "{}, role: m({}), a({}), state: m({}), mgen: pg{}, pgs{}",
                        new Object[] { pgs, pgs.getData().getRole(),
                                smrState.getRole(), pgs.getData().getState(),
                                pg.getData().currentGen(),
                                pgs.getData().getMasterGen() });
                return;
            }

            // Old number of Master_Gen
            if (pgs.getData().getMasterGen() < pg.getData().currentGen()) {
                Logger.error(
                        "Invalid master generation of pgs. "
                                + "{}, role: m({}), a({}), state: m({}), mgen: pg{}, pgs{}",
                        new Object[] { pgs, pgs.getData().getRole(),
                                smrState.getRole(), pgs.getData().getState(),
                                pg.getData().currentGen(),
                                pgs.getData().getMasterGen() });

                pgs.roleLconn();
                return;
            }

            if (smrState.getRole().equals(PGS_ROLE_SLAVE)) {
                // Find the master
                int masterCnt = 0;
                for (PartitionGroupServer joinedPGS : joinedPgsList) {
                    if (joinedPGS.getData().getState().equals(SERVER_STATE_NORMAL)
                            && joinedPGS.getData().getRole().equals(PGS_ROLE_MASTER)) {
                        Logger.info("Master is {}", pgs);
                        masterCnt++;
                    }
                }

                if (masterCnt == 0) {
                    Logger.info("No master. role lconn to all available pgs.");
                    pgs.roleLconn();
                    return;
                }
            }
        } catch (IOException e) {
            Logger.error("Exception occur while handle hang.", e);
        }
    }

    private void slaveJoin(PartitionGroupServer master) {
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (master.getName().equals(pgs.getName())) {
                continue;
            }

            if (!pgs.getData().getState().equals(SERVER_STATE_LCONN)
                    && !pgs.getData().getState().equals(SERVER_STATE_NORMAL)
                    && !pgs.getData().getState().equals(PGS_ROLE_NONE)) {
                continue;
            }

            LogSequence logSeq = getLogSequence(pgs);
            if (logSeq == null) {
                Logger.info("Get log sequence of slave candidate fail. {}",
                        pgs);
                continue;
            }
            Logger.info(
                    "Get log sequence of slave candidate fail. {}, sequence:\"{}\"",
                    pgs, logSeq);

            if (!pgs.getData().getState().equals(SERVER_STATE_LCONN)) {
                continue;
            }

            try {
                pgs.roleSlave(pg, logSeq, master, jobID, workflowLogDao);
            } catch (MgmtRoleChangeException e) {
                continue;
            }
        }

        workflowExecutor.perform(SET_QUORUM, cluster.getName(),
                pg.getName());
    }

    private LogSequence getLogSequence(PartitionGroupServer pgs) {
        LogSequence logSeq = new LogSequence();
        try {
            logSeq.initialize(pgs);
            return logSeq;
        } catch (IOException e) {
            Logger.warn(String
                    .format("an error occurs while getting Logger sequences. pgsName:%s",
                            pgs.getName()));
            return null;
        }
    }

    /**
     * When it is called, the Master-PGS must be in HANG and must have a quorum
     * that is larger than 0. In order to elect a new master among them, this
     * function sends 'role lconn' to all slave-PGSes in a PG that has the
     * Master-PGS. (After the Master-PGS recovers from hanging, Mgmt-CC will
     * send 'role lconn' to it, and then it could join to the new Master as a
     * slave.)
     * 
     * @param creahsedMaster
     */
    public void changeRolesOfAllSlavesToLCONN(
            PartitionGroupServer creahsedMaster) {
        Logger.info("Master crashed. {}", creahsedMaster);

        for (Integer pgsid : pg.getData().getPgsIdList()) {
            PartitionGroupServer pgs = pgsImo.get(Integer.toString(pgsid),
                    creahsedMaster.getClusterName());

            if (creahsedMaster.equals(pgs)) {
                continue;
            }

            if (!pgs.isAvailable()) {
                continue;
            }

            String cmd = "role lconn";
            try {
                String reply = pgs.executeQuery(cmd);
                Logger.info("Change role to lconn, because master crashed. "
                        + "{}, role: {}. state: {}, cmd: \"{}\", reply=\"{}\"",
                        new Object[] { pgs, pgs.getData().getRole(),
                                pgs.getData().getState(), cmd, reply });
            } catch (IOException e) {
                Logger.info("Change role to lconn, because master crashed. "
                        + "{}, role: {}. state: {}, cmd: \"{}\"", new Object[] {
                        pgs, pgs.getData().getRole(),
                        pgs.getData().getState(), cmd }, e);
            }
        }
    }
}

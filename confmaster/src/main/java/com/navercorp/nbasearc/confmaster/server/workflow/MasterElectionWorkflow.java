package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.SortedLogSeqSet;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class MasterElectionWorkflow extends CascadingWorkflow {
    private final long jobID = JobIDGenerator.getInstance().getID();

    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;

    final PartitionGroupDao pgDao;
    final WorkflowLogDao workflowLogDao;
    final PartitionGroupServerImo pgsImo;
    
    final MERoleMaster roleMaster;
    final MERoleLconn roleLconn;

    final PartitionGroupServer masterHint;
    
    public MasterElectionWorkflow(PartitionGroup pg,
            PartitionGroupServer masterHint, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg);
        
        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        
        this.roleMaster = context.getBean(MERoleMaster.class);
        this.roleLconn = context.getBean(MERoleLconn.class);

        this.masterHint = masterHint;
    }

    @Override
    protected void _execute() throws Exception {
        final List<PartitionGroupServer> joinedPgsList = pg
                .getJoinedPgsList(pgsImo.getList(pg.getClusterName(),
                        Integer.valueOf(pg.getName())));

        final PartitionGroupServer master = pg.getMaster(joinedPgsList);
        final int d = pg.getD(joinedPgsList);
        
        if (master != null || pg.getData().getQuorum() - d < 0) {
            return;
        }

        for (PartitionGroupServer pgs : joinedPgsList) {
            Color color = pgs.getData().getColor();
            if (color == GREEN || color == BLUE) {
                roleLconn.roleLconn(pgs, jobID);
            }
        }

        List<PartitionGroupServer> greens = new ArrayList<PartitionGroupServer>();
        List<PartitionGroupServer> blues = new ArrayList<PartitionGroupServer>();
        for (PartitionGroupServer pgs : joinedPgsList) {
            switch (pgs.getData().getColor()) {
            case BLUE:
                blues.add(pgs);
                break;
            case GREEN:
                greens.add(pgs);
                break;
            default:
                // Do nothing.
                break;
            }
        }

        if (greens.size() != 0) {
            Logger.error("{} numGreen is {}", pg, greens.size());
            for (PartitionGroupServer pgs : joinedPgsList) {
                Logger.error("{} {}", pgs, pgs.getData());
            }
            return;
        }

        SortedLogSeqSet logs = pg.getLogSeq(blues);
        for (LogSequence e : logs) {
            System.out.println(e.getPgs() + " " + e.getMax());
        }

        PartitionGroupServer newMaster;
        LogSequence newMasterLog;
        if (masterHint != null) {
            if (pg.isMasterCandidate(masterHint, logs, joinedPgsList)) {
                throw new MgmtRoleChangeException(masterHint + " has no recent logs");
            }
            newMaster = masterHint;
            newMasterLog = logs.get(masterHint);
        } else {
            newMaster = pg.chooseMasterRandomly(logs, joinedPgsList);
            newMasterLog = logs.get(newMaster);
        }

        int newQ = pg.getData().getQuorum() - d;
        roleMaster.roleMaster(newMaster, pg, newMasterLog, joinedPgsList, newQ, jobID);
    }

    @Override
    protected void onSuccess() throws Exception {
        final long nextEpoch = pg.nextWfEpoch();
        Logger.info("next {}", nextEpoch);
        wfExecutor.perform(YELLOW_JOIN, pg, nextEpoch, context);
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);
    }
}

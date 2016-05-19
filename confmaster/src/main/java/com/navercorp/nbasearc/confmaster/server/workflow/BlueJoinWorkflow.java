package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class BlueJoinWorkflow extends CascadingWorkflow {
    private final long jobID = JobIDGenerator.getInstance().getID();

    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final WorkflowLogDao workflowLogDao;
    final PartitionGroupServerImo pgsImo;

    final BJRoleSlave roleSlave;
    
    List<PartitionGroupServer> joinedPgsList;
    PartitionGroupServer master;
    
    public BlueJoinWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg);
        
        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        
        this.roleSlave = context.getBean(BJRoleSlave.class);
    }

    @Override
    protected void _execute() throws Exception {
        joinedPgsList = pg.getJoinedPgsList(pgsImo.getList(pg.getClusterName(),
                Integer.valueOf(pg.getName())));
        master = pg.getMaster(joinedPgsList);
        if (master == null) {
            return;
        }

        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getData().getColor() == BLUE
                    && pgs.getData().getRole().equals(PGS_ROLE_LCONN)) {
                LogSequence logSeq = pgs.getLogSeq();
                roleSlave.roleSlave(pgs, pg, logSeq, master, jobID);
            }
        }
    }

    @Override
    protected void onSuccess() throws Exception {
        final long nextEpoch = pg.nextWfEpoch();
        Logger.info("next {}", nextEpoch);
        wfExecutor.perform(MEMBERSHIP_GRANT, pg, nextEpoch, context);
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);
    }

}

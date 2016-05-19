package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class QuorumAdjustmentWorkflow extends CascadingWorkflow {
    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final PartitionGroupServerImo pgsImo;
    
    final QASetquorum setquorum;

    public QuorumAdjustmentWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg);

        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        
        this.setquorum = context.getBean(QASetquorum.class);
    }

    @Override
    protected void _execute() throws Exception {
        List<PartitionGroupServer> joinedPgsList = pg.getJoinedPgsList(pgsImo
                .getList(pg.getClusterName(), Integer.valueOf(pg.getName())));
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);
        final int d = pg.getD(joinedPgsList);

        if (master == null) {
            return;
        }

        final String q = master.executeQuery("getquorum");
        final int activeQ = Integer.valueOf(q);

        final int tobeQ = pg.getData().getQuorum() - d;
        if (tobeQ == activeQ) {
            return;
        }

        setquorum.setquorum(master, tobeQ);
    }

    @Override
    protected void onSuccess() throws Exception {
        final long nextEpoch = pg.nextWfEpoch();
        Logger.info("next {}", nextEpoch);
        wfExecutor.perform(MASTER_ELECTION, pg, null, nextEpoch, context);
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);
    }
}

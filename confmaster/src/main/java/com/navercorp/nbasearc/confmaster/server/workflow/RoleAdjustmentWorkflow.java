package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class RoleAdjustmentWorkflow extends CascadingWorkflow {
    private final long jobID = JobIDGenerator.getInstance().getID();

    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final PartitionGroupServerDao pgsDao;
    final WorkflowLogDao workflowLogDao;
    final PartitionGroupServerImo pgsImo;
    
    final RARoleLconn roleLconn;
    final RASetquorum setquorum;

    public RoleAdjustmentWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg);

        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        
        this.roleLconn = context.getBean(RARoleLconn.class);
        this.setquorum = context.getBean(RASetquorum.class);
    }

    @Override
    protected void _execute() throws Exception {
        final List<PartitionGroupServer> joinedPgsList = pg
                .getJoinedPgsList(pgsImo.getList(pg.getClusterName(),
                        Integer.valueOf(pg.getName())));
        
        final int pgQ = pg.getData().getQuorum();
        for (PartitionGroupServer pgs : joinedPgsList) {
            final String role = pgs.getData().getRole();
            final Color color = pgs.getData().getColor();
            final int d = pg.getD(joinedPgsList);

            if (role == PGS_ROLE_NONE && color != RED && pgQ - d > 0) {
                toRed(pgs, joinedPgsList);
            } else if (role != PGS_ROLE_NONE && color == RED) {
                toLconn(pgs);
            } else if (role == PGS_ROLE_LCONN && color == GREEN) {
                toBlue(pgs);
            } else if ((role == PGS_ROLE_MASTER || role == PGS_ROLE_SLAVE)
                    && color == BLUE) {
                toGreen(pgs);
            }
        }
    }

    private void toRed(PartitionGroupServer pgs,
            List<PartitionGroupServer> joinedPgsList) throws IOException,
            MgmtSetquorumException, MgmtZooKeeperException {
        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), pgs.getData().getRole(),
                pgs.getData().getColor(), RED });
        PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                .from(pgs.getData()).withColor(RED).build();
        pgsDao.updatePgs(pgs.getPath(), pgsM);
        pgs.setData(pgsM);

        PartitionGroupServer master = pg.getMaster(joinedPgsList);
        if (master != null) {
            setquorum.setquorum(master, pg.getData().getQuorum() - pg.getD(joinedPgsList));
        }
    }

    private void toLconn(PartitionGroupServer pgs) throws IOException,
            MgmtRoleChangeException {
        roleLconn.roleLconn(pgs, jobID);
    }

    private void toBlue(PartitionGroupServer pgs) throws MgmtZooKeeperException {
        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), pgs.getData().getRole(), 
                pgs.getData().getColor(), BLUE });
        PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                .from(pgs.getData()).withColor(BLUE).build();
        pgsDao.updatePgs(pgs.getPath(), pgsM);
        pgs.setData(pgsM);
    }

    private void toGreen(PartitionGroupServer pgs)
            throws MgmtZooKeeperException {
        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), pgs.getData().getRole(),
                pgs.getData().getColor(), GREEN });
        PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                .from(pgs.getData()).withColor(GREEN).build();
        pgsDao.updatePgs(pgs.getPath(), pgsM);
        pgs.setData(pgsM);
    }

    @Override
    protected void onSuccess() throws Exception {
        final long nextEpoch = pg.nextWfEpoch();
        Logger.info("next {}", nextEpoch);
        wfExecutor.perform(QUORUM_ADJUSTMENT, pg, nextEpoch, context);
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);        
    }
}

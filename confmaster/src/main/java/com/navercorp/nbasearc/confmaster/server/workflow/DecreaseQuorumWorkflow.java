package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class DecreaseQuorumWorkflow {
    final ApplicationContext context;
    final WorkflowExecutor wfExecutor;
    final PartitionGroupDao pgDao;
    final PartitionGroupServerImo pgsImo;

    final PartitionGroup pg;

    public DecreaseQuorumWorkflow(PartitionGroup pg, ApplicationContext context) {
        this.context = context;
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);

        this.pg = pg;
    }

    public String execute() throws MgmtZooKeeperException {
        final int d = pg.getD(pgsImo.getList(pg.getClusterName(),
                Integer.valueOf(pg.getName())));

        if (pg.getData().getQuorum() - d <= 0) {
            return "-ERR not enough avaliable pgs.";
        }

        PartitionGroupData pgM = PartitionGroupData.builder()
                .from(pg.getData()).withQuorum(pg.getData().getQuorum() - 1)
                .build();
        pgDao.updatePg(pg.getPath(), pgM);
        pg.setData(pgM);
        
        return S2C_OK;
    }
}

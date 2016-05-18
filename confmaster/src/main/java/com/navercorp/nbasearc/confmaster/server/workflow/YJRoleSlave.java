package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.Color.YELLOW;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

@Component("YJRoleSlave")
public class YJRoleSlave {

    @Autowired
    protected WorkflowLogDao workflowLogDao;

    public void roleSlave(PartitionGroupServer pgs, PartitionGroup pg,
            LogSequence logSeq, PartitionGroupServer master, long jobID)
            throws MgmtRoleChangeException {
        pgs.roleSlave(pg, logSeq, master, YELLOW, jobID, workflowLogDao);

        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), PGS_ROLE_SLAVE,
                pgs.getData().getColor(), YELLOW });

        pgs.setData(pgs.roleSlaveZk(jobID, pg.getData().currentGen(), YELLOW,
                workflowLogDao).pgsM);
    }

}

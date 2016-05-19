package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.Color.BLUE;
import static com.navercorp.nbasearc.confmaster.Constant.Color.GREEN;
import static com.navercorp.nbasearc.confmaster.Constant.Color.YELLOW;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

@Component("BJRoleSlave")
public class BJRoleSlave {

    // It is not allowed to decalre any member variable in this class.
    // Since it is a singleton instance and represents a part of workflow logic running in multiple threads.

    @Autowired
    protected WorkflowLogDao workflowLogDao;

    public void roleSlave(PartitionGroupServer pgs, PartitionGroup pg,
            LogSequence logSeq, PartitionGroupServer master, long jobID)
            throws MgmtRoleChangeException {
        pgs.roleSlave(pg, logSeq, master, BLUE, jobID, workflowLogDao);

        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), PGS_ROLE_SLAVE,
                pgs.getData().getColor(), GREEN });

        pgs.setData(pgs.roleSlaveZk(jobID, pg.getData().currentGen(), GREEN,
                workflowLogDao).pgsM);
    }

}

package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_LCONN;
import static com.navercorp.nbasearc.confmaster.Constant.Color.BLUE;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

@Component("MERoleLconn")
public class MERoleLconn {

    // It is not allowed to decalre any member variable in this class.
    // Since it is a singleton instance and represents a part of workflow logic running in multiple threads.

    @Autowired
    WorkflowLogDao workflowLogDao;

    public void roleLconn(PartitionGroupServer pgs, long jobID)
            throws IOException, MgmtRoleChangeException {
        pgs.roleLconn();
        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), PGS_ROLE_LCONN,
                pgs.getData().getColor(), BLUE });
        pgs.setData(pgs.roleLconnZk(jobID, BLUE, workflowLogDao).pgsM);
    }

}

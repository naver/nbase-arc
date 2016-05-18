package com.navercorp.nbasearc.confmaster.server.workflow;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.RoleMasterZkResult;

@Component("MERoleMaster")
public class MERoleMaster {

    @Autowired
    WorkflowLogDao workflowLogDao;

    public void roleMaster(PartitionGroupServer newMaster, PartitionGroup pg,
            LogSequence newMasterLog, List<PartitionGroupServer> joinedPgsList,
            int newQ, long jobID) throws MgmtRoleChangeException {
        newMaster.roleMaster(pg, newMasterLog, newQ, jobID, workflowLogDao);

        RoleMasterZkResult result = newMaster.roleMasterZk(pg, joinedPgsList,
                newMasterLog, newQ, jobID, workflowLogDao);
        newMaster.setData(result.pgsM);
        pg.setData(result.pgM);
    }

}

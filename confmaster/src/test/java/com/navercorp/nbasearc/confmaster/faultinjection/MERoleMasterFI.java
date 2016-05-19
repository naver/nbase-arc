package com.navercorp.nbasearc.confmaster.faultinjection;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.workflow.MERoleMaster;

public class MERoleMasterFI extends MERoleMaster {

    private int count = 0;
    private boolean successFail = false;

    @Autowired
    WorkflowLogDao workflowLogDao;

    @Override
    public synchronized void roleMaster(PartitionGroupServer newMaster,
            PartitionGroup pg, LogSequence newMasterLog,
            List<PartitionGroupServer> joinedPgsList, int newQ, long jobID)
            throws MgmtRoleChangeException {
        if (count > 0) {
            if (successFail) {
                newMaster.roleMaster(pg, newMasterLog, newQ, jobID,
                        workflowLogDao);
                count--;
                throw new MgmtRoleChangeException(
                        "[FI] ME role master success fail. " + newMaster);
            } else {
                count--;
                throw new MgmtRoleChangeException(
                        "[FI] ME role master fail fail. " + newMaster);
            }
        } else {
            super.roleMaster(newMaster, pg, newMasterLog, joinedPgsList, newQ,
                    jobID);
        }
    }

    public synchronized int getCount() {
        return count;
    }

    public synchronized void setCount(int count) {
        this.count = count;
    }

    public synchronized boolean isSuccessFail() {
        return successFail;
    }

    public synchronized void setSuccessFail(boolean successFail) {
        this.successFail = successFail;
    }

}

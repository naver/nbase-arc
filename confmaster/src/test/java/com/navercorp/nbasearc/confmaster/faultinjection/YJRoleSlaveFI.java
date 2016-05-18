package com.navercorp.nbasearc.confmaster.faultinjection;

import static com.navercorp.nbasearc.confmaster.Constant.Color.YELLOW;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.workflow.YJRoleSlave;

public class YJRoleSlaveFI extends YJRoleSlave {

    private int count = 0;
    private boolean successFail = false;

    @Override
    public synchronized void roleSlave(PartitionGroupServer pgs, PartitionGroup pg,
            LogSequence logSeq, PartitionGroupServer master, long jobID) throws MgmtRoleChangeException
             {
        if (count > 0) {
            if (successFail) {
                pgs.roleSlave(pg, logSeq, master, YELLOW, jobID, workflowLogDao);
                count--;
                throw new MgmtRoleChangeException(
                        "[FI] YJ role slave success fail. " + master);
            } else {
                count--;
                throw new MgmtRoleChangeException(
                        "[FI] YJ role slave lconn fail fail. " + master);
            }
        } else {
            super.roleSlave(pgs, pg, logSeq, master, jobID);
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

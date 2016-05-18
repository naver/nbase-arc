package com.navercorp.nbasearc.confmaster.faultinjection;

import java.io.IOException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.workflow.MERoleLconn;

public class MERoleLconnFI extends MERoleLconn {

    private int count = 0;
    private boolean successFail = false;
    
    @Override
    public synchronized void roleLconn(PartitionGroupServer pgs, long jobID)
            throws IOException, MgmtRoleChangeException {
        if (count > 0) {
            if (successFail) {
                pgs.roleLconn();
                count--;
                throw new MgmtRoleChangeException("[FI] ME role lconn success fail. " + pgs);
            } else {
                count--;
                throw new MgmtRoleChangeException("[FI] ME role lconn fail fail. " + pgs);
            }
        } else {
            super.roleLconn(pgs, jobID);
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

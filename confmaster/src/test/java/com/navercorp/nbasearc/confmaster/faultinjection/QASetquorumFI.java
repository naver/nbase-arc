package com.navercorp.nbasearc.confmaster.faultinjection;

import java.io.IOException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.workflow.QASetquorum;

public class QASetquorumFI extends QASetquorum {

    private int count = 0;
    private boolean successFail = false;

    @Override
    public synchronized void setquorum(PartitionGroupServer master, int q)
            throws IOException, MgmtSetquorumException {
        if (count > 0) {
            if (successFail) {
                master.setquorum(q);
                count--;
                throw new MgmtSetquorumException(
                        "[FI] QA setquorum success fail. " + master);
            } else {
                count--;
                throw new MgmtSetquorumException(
                        "[FI] QA setquorum lconn fail fail. " + master);
            }
        } else {
            super.setquorum(master, q);
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

package com.navercorp.nbasearc.confmaster.context;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;

public class ReservedCallHolder<T> {
    
    private ReservedCall<T> call = null;

    public boolean hasNextCall() {
        return call != null;
    }

    public ReservedCall<T> pollCall() {
        try {
            return call;
        } finally {
            call = null;
        }
    }
    
    public void setCall(ReservedCall<T> call) throws MgmtDuplicatedReservedCallException {
        if (this.call != null) {
            throw new MgmtDuplicatedReservedCallException(
                    "Workflow to run on next time is already set. Workfolw="
                            + this.call);
        }
        this.call = call;
    }
    
}

package com.navercorp.nbasearc.confmaster.server.workflow;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;

public abstract class CascadingWorkflow {
    final boolean cascading;
    final PartitionGroup pg;
    
    CascadingWorkflow(boolean cascading, PartitionGroup pg) {
        this.cascading = cascading;
        this.pg = pg;
    }
    
    public void execute() throws Exception {
        Logger.info("begin {}", pg);
        try {
            _execute();
            if (cascading) {
                onSuccess();
            }
        } catch (Exception e) {
            Logger.error("Failed to execute.", e);
            if (cascading) {
                final long nextEpoch = pg.nextWfEpoch(); 
                Logger.error("rerun {}", nextEpoch);
                onException(nextEpoch, e);
            }
            throw e;
        }
    }
    
    abstract void _execute() throws Exception;
    abstract void onSuccess() throws Exception;
    abstract void onException(long nextEpoch, Exception e);
}

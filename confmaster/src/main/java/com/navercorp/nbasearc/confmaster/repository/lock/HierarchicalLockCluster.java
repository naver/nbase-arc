package com.navercorp.nbasearc.confmaster.repository.lock;

import static com.navercorp.nbasearc.confmaster.Constant.*;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

public class HierarchicalLockCluster extends HierarchicalLock {
    
    private String clusterName;
    private Cluster cluster;

    public HierarchicalLockCluster() {
    }
    
    public HierarchicalLockCluster(HierarchicalLockHelper hlh,
            LockType lockType, String clusterName) {
        super(hlh, lockType);
        this.clusterName = clusterName;
        
        this.cluster = getHlh().getClusterImo().get(this.clusterName);
        if (this.cluster == null) {
            String message = EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                    + Cluster.fullName(clusterName);
            Logger.error(message);
            throw new IllegalArgumentException(message);
        }

        _lock();
    }

    public HierarchicalLockPGList pgList(LockType lockType) {
        return new HierarchicalLockPGList(getHlh(), lockType, cluster);
    }

    public HierarchicalLockGWList gwList(LockType lockType) {
        return new HierarchicalLockGWList(getHlh(), lockType, clusterName);
    }

    @Override
    protected void _lock() {
        switch (this.getLockType()) {
        case READ:
            getHlh().acquireLock(cluster.readLock());
            break;
        case WRITE:
            getHlh().acquireLock(cluster.writeLock());
            break;
        case SKIP:
            break;
        }
    }
    
}

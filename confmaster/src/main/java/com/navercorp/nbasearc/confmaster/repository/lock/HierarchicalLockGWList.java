package com.navercorp.nbasearc.confmaster.repository.lock;

import static com.navercorp.nbasearc.confmaster.Constant.EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

public class HierarchicalLockGWList extends HierarchicalLock {
    
    private String clusterName;
    
    public HierarchicalLockGWList() {
    }
    
    public HierarchicalLockGWList(HierarchicalLockHelper hlh,
            LockType lockType, String clusterName) {
        super(hlh, lockType);
        this.clusterName = clusterName;
        _lock();
    }
    
    public HierarchicalLockGW gw(LockType lockType, String gwID) {
        return new HierarchicalLockGW(getHlh(), lockType, clusterName, gwID);
    }
    
    @Override
    protected void _lock() {
        Cluster cluster = getHlh().getClusterImo().get(this.clusterName);
        if (cluster == null) {
            String message = EXCEPTIONMSG_CLUSTER_DOES_NOT_EXIST
                    + Cluster.fullName(clusterName);
            Logger.error(message);
            throw new IllegalArgumentException();
        }
        
        switch (getLockType()) {
        case READ: 
            getHlh().acquireLock(cluster.gwListReadLock());
            break;
        case WRITE: 
            getHlh().acquireLock(cluster.gwListWriteLock());
            break;
        case SKIP:
            break;
        }
    }

}

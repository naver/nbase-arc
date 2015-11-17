package com.navercorp.nbasearc.confmaster.repository.lock;

import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

public class HierarchicalLockPGSList extends HierarchicalLock {
    
    private Cluster cluster;
    
    public HierarchicalLockPGSList() {
    }
    
    public HierarchicalLockPGSList(HierarchicalLockHelper hlh,
            LockType lockType, Cluster cluster) {
        super(hlh, lockType);
        this.cluster = cluster;
        _lock();
    }
    
    public HierarchicalLockPG pg(LockType lockType, String pgID) {
        return new HierarchicalLockPG(getHlh(), lockType, cluster.getName(), pgID);
    }

    public HierarchicalLockGWList gwList(LockType lockType) {
        return new HierarchicalLockGWList(getHlh(), lockType, cluster.getName());
    }
    
    @Override
    protected void _lock() {
        switch (getLockType()) {
        case READ: 
            getHlh().acquireLock(cluster.pgsListReadLock());
            break;
        case WRITE: 
            getHlh().acquireLock(cluster.pgsListWriteLock());
            break;
        case SKIP:
            break;
        }
    }
    
}

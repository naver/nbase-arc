package com.navercorp.nbasearc.confmaster.repository.lock;

import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

public class HierarchicalLockPGList extends HierarchicalLock {
    
    private Cluster cluster;
    
    public HierarchicalLockPGList() {
    }

    public HierarchicalLockPGList(HierarchicalLockHelper hlh,
            LockType lockType, Cluster cluster) {
        super(hlh, lockType);
        this.cluster = cluster;
        _lock();
    }

    public HierarchicalLockPGSList pgsList(LockType lockType) {
        return new HierarchicalLockPGSList(getHlh(), lockType, cluster);
    }

    public HierarchicalLockGWList gwList(LockType lockType) {
        return new HierarchicalLockGWList(getHlh(), lockType, cluster.getName());
    }

    @Override
    protected void _lock() {
        switch (getLockType()) {
        case READ:
            getHlh().acquireLock(cluster.pgListReadLock());
            break;
        case WRITE:
            getHlh().acquireLock(cluster.pgListWriteLock());
            break;
        case SKIP:
            break;
        }
    }
    
}

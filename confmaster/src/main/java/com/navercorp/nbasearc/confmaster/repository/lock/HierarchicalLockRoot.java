package com.navercorp.nbasearc.confmaster.repository.lock;

public class HierarchicalLockRoot extends HierarchicalLock {
    
    public HierarchicalLockRoot() {
    }
    
    public HierarchicalLockRoot(HierarchicalLockHelper hlh, LockType lockType) {
        super(hlh, lockType);
        _lock();
    }
    
    public HierarchicalLockCluster cluster(LockType lockType, String clusterName) {
        return new HierarchicalLockCluster(getHlh(), lockType, clusterName);
    }

    @Override
    protected void _lock() {
        switch (getLockType()) {
        case READ:
            getHlh().acquireLock(getHlh().rootReadLock());
            break;
        case WRITE:
            getHlh().acquireLock(getHlh().rootWriteLock());
            break;
        case SKIP:
            break;
        }
    }
    
}

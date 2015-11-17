package com.navercorp.nbasearc.confmaster.repository.lock;

public class HierarchicalLockPMList extends HierarchicalLock {
    
    public HierarchicalLockPMList() {
    }
    
    public HierarchicalLockPMList(HierarchicalLockHelper hlh, LockType lockType) {
        super(hlh, lockType);
        _lock();
    }
    
    public HierarchicalLockPM pm(LockType lockType, String pmName) {
        return new HierarchicalLockPM(getHlh(), lockType, pmName);
    }
    
    @Override
    protected void _lock() {
        switch (getLockType()) {
        case READ: 
            getHlh().acquireLock(getHlh().pmListReadLock());
            break;
        case WRITE: 
            getHlh().acquireLock(getHlh().pmListWriteLock());
            break;
        case SKIP:
            break;
        }
    }
    
}

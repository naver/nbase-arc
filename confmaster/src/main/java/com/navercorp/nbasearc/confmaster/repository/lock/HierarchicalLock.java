package com.navercorp.nbasearc.confmaster.repository.lock;

public abstract class HierarchicalLock {

    private HierarchicalLockHelper hlh;
    private LockType lockType;

    public HierarchicalLock() {
    }

    public HierarchicalLock(HierarchicalLockHelper hlh, LockType lockType) {
        this.hlh = hlh;
        this.lockType = lockType;
    }

    protected abstract void _lock();

    public HierarchicalLockHelper getHlh() {
        return hlh;
    }

    public void setHlh(HierarchicalLockHelper hlh) {
        this.hlh = hlh;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

}

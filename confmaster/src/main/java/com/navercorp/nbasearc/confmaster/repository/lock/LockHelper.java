package com.navercorp.nbasearc.confmaster.repository.lock;

import java.util.*;
import java.util.concurrent.locks.Lock;

public class LockHelper {
    
    private final Deque<Lock> lockList = new ArrayDeque<Lock>();

    public void acquireLock(Lock lock) {
        lock.lock();
        lockList.push(lock);
    }

    public void releaseAllLock() {
        while (!lockList.isEmpty()) {
            lockList.pop().unlock();
        }
    }
    
}

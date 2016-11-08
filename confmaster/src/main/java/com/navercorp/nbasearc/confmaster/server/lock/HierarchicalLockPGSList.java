/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.server.lock;

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

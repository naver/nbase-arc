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

import com.navercorp.nbasearc.confmaster.ThreadLocalVariableHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;

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
        ThreadLocalVariableHolder.addPermission(PathUtil.pgRootPath(cluster.getName()), getLockType());
        switch (getLockType()) {
        case READ:
            getHlh().acquireLock(cluster.pgListReadLock());
            break;
        case WRITE:
            getHlh().acquireLock(cluster.pgListWriteLock());
            break;
        }
    }
    
}

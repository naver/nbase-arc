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
        Cluster cluster = getHlh().getContainer().getCluster(this.clusterName);
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

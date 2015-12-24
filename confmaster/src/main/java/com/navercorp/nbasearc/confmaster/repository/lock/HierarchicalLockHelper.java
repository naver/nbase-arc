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

package com.navercorp.nbasearc.confmaster.repository.lock;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class HierarchicalLockHelper {

    private static final ReentrantReadWriteLock rootRWLock = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock pmRWLock = new ReentrantReadWriteLock();
    
    private final Deque<Lock> acquiredLockList = new ArrayDeque<Lock>();
    
    private final ClusterImo clusterImo;
    private final PhysicalMachineImo pmImo;
    private final PartitionGroupImo pgImo;
    private final PartitionGroupServerImo pgsImo;
    private final RedisServerImo rsImo;
    private final GatewayImo gwImo;
    private final PhysicalMachineClusterImo clusterInPmImo;
    
    public HierarchicalLockHelper(ApplicationContext context) {
        this.clusterImo = context.getBean(ClusterImo.class);
        this.pmImo = context.getBean(PhysicalMachineImo.class);
        this.pgImo = context.getBean(PartitionGroupImo.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);
        this.gwImo = context.getBean(GatewayImo.class);
        this.clusterInPmImo = context.getBean(PhysicalMachineClusterImo.class);
    }
    
    public HierarchicalLockRoot root(LockType lockType) {
        return new HierarchicalLockRoot(this, lockType);
    }
    
    public HierarchicalLockPMList pmList(LockType lockType) {
        return new HierarchicalLockPMList(this, lockType);
    }

    public void acquireLock(Lock lock) {
        lock.lock();
        acquiredLockList.push(lock);
    }

    public void releaseAllLock() {
        while (!acquiredLockList.isEmpty()) {
            acquiredLockList.pop().unlock();
        }
    }

    public Lock rootReadLock() {
        return rootRWLock.readLock();
    }

    public Lock rootWriteLock() {
        return rootRWLock.writeLock();
    }

    public Lock pmListReadLock() {
        return pmRWLock.readLock();
    }

    public Lock pmListWriteLock() {
        return pmRWLock.writeLock();
    }
    
    public ClusterImo getClusterImo() {
        return clusterImo;
    }

    public PhysicalMachineImo getPmImo() {
        return pmImo;
    }

    public PartitionGroupImo getPgImo() {
        return pgImo;
    }

    public PartitionGroupServerImo getPgsImo() {
        return pgsImo;
    }

    public RedisServerImo getRsImo() {
        return rsImo;
    }

    public GatewayImo getGwImo() {
        return gwImo;
    }

    public PhysicalMachineClusterImo getClusterInPmImo() {
        return clusterInPmImo;
    }
    
}

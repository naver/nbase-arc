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
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;

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
        ThreadLocalVariableHolder.addPermission(PathUtil.pmRootPath(), getLockType());
        switch (getLockType()) {
        case READ: 
            getHlh().acquireLock(getHlh().pmListReadLock());
            break;
        case WRITE: 
            getHlh().acquireLock(getHlh().pmListWriteLock());
            break;
        }
    }
    
}

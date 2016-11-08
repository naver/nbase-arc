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

import static com.navercorp.nbasearc.confmaster.Constant.*;

import java.util.ArrayList;
import java.util.List;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;

public class HierarchicalLockPM extends HierarchicalLock {
    
    private String pmName;
    
    public HierarchicalLockPM() {
    }
    
    public HierarchicalLockPM(HierarchicalLockHelper hlh, LockType lockType, 
            String pmName) {
        super(hlh, lockType);
        this.pmName = pmName;
        _lock();
    }
    
    @Override
    protected void _lock() {
        List<PhysicalMachine> pmList;
        
        if (pmName.equals(Constant.ALL)) {
            pmList = getHlh().getContainer().getAllPm();
        } else {
            pmList = new ArrayList<PhysicalMachine>();
            PhysicalMachine pm = getHlh().getContainer().getPm(pmName);
            if (pm == null) {
                String message = EXCEPTIONMSG_PHYSICAL_MACHINE_DOES_NOT_EXIST
                        + PhysicalMachine.fullName(pmName);
                Logger.error(message);
                throw new IllegalArgumentException(message);
            }
            pmList.add(pm);
        }
        
        for (PhysicalMachine pm : pmList) {
            switch (getLockType()) {
            case READ: 
                getHlh().acquireLock(pm.readLock());
                break;
            case WRITE: 
                getHlh().acquireLock(pm.writeLock());
                break;
            case SKIP:
                break;
            }
        }
    }
    
}

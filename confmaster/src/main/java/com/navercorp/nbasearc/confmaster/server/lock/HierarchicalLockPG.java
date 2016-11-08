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
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;

public class HierarchicalLockPG extends HierarchicalLock {
    
    private String clusterName;
    private String pgID = null;
    private boolean locked = false;
    
    public HierarchicalLockPG() {
    }
    
    public HierarchicalLockPG(HierarchicalLockHelper hlh, LockType lockType,
            String clusterName, String pgID) {
        super(hlh, lockType);
        this.clusterName = clusterName;
        this.setPgID(pgID);
        _lock();
    }
    
    public HierarchicalLockPG pg(LockType lockType, String pgID) {
        return new HierarchicalLockPG(getHlh(), lockType, clusterName, pgID);
    }
    
    public HierarchicalLockPGS pgs(LockType lockType, String pgsID) {
        return new HierarchicalLockPGS(getHlh(), lockType, clusterName, this, pgID, pgsID);
    }
    
    public HierarchicalLockGWList gwList(LockType lockType) {
        return new HierarchicalLockGWList(getHlh(), lockType, clusterName);
    }
    
    @Override
    protected void _lock() {
        if (getLockType() == LockType.SKIP) {
            return;
        }
        
        if (locked == true || pgID == null) {
            return;
        }
        
        List<PartitionGroup> pgList;
        
        if (pgID.equals(Constant.ALL)) {
            pgList = getHlh().getContainer().getPgList(clusterName);
        } else {
            PartitionGroup pg = getHlh().getContainer().getPg(clusterName, pgID);
            if (pg == null) {
                String message = EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                        + PartitionGroup.fullName(clusterName, pgID);
                Logger.error(message);
                throw new IllegalArgumentException(message);
                
            }
            pgList = new ArrayList<PartitionGroup>();
            pgList.add(pg);
        }
        
        for (PartitionGroup pg : pgList) {
            switch (getLockType()) {
            case READ: 
                getHlh().acquireLock(pg.readLock());
                break;
            case WRITE: 
                getHlh().acquireLock(pg.writeLock());
                break;
            case SKIP:
                break;
            }
        }
        
        locked = true;
    }

    public String getPgID() {
        return pgID;
    }

    public void setPgID(String pgID) {
        this.pgID = pgID;
    }
    
    public boolean isLocked() {
        return locked;
    }

}

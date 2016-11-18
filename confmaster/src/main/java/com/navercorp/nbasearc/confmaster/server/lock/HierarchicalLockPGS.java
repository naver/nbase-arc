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
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

public class HierarchicalLockPGS extends HierarchicalLock {
    
    private HierarchicalLockPG pgLock;
    private String clusterName;
    private String pgID = null;
    private String pgsID;
    
    public HierarchicalLockPGS() {
    }

    public HierarchicalLockPGS(HierarchicalLockHelper hlh, LockType lockType,
            String clusterName, HierarchicalLockPG pgLock, String pgID,
            String pgsID) {
        super(hlh, lockType);
        this.clusterName = clusterName;
        this.pgLock = pgLock;
        this.pgID = pgID;
        this.pgsID = pgsID;

        if (!pgLock.isLocked() && pgID == null) {
            PartitionGroupServer pgs = getHlh().getContainer().getPgs(clusterName, pgsID);
            if (pgs == null) {
                String message = EXCEPTIONMSG_PARTITION_GROUP_SERVER_DOES_NOT_EXIST
                        + PartitionGroupServer.fullName(clusterName, pgsID);
                Logger.error(message);
                throw new IllegalArgumentException(message);
            }
            pgID = String.valueOf(pgs.getPgId());
            pgLock.setPgID(pgID);
            
            pgLock._lock();
        }
        
        _lock();
    }

    public HierarchicalLockPGS pgs(LockType lockType, String pgsID) {
        return new HierarchicalLockPGS(getHlh(), lockType, clusterName, pgLock, null, pgsID);
    }
    
    public HierarchicalLockPGS pgs(LockType lockType, String pgID, String pgsID) {
        return new HierarchicalLockPGS(getHlh(), lockType, clusterName, pgLock, pgID, pgsID);
    }
    
    public HierarchicalLockGWList gwList(LockType lockType) {
        return new HierarchicalLockGWList(getHlh(), lockType, clusterName);
    }
    
    @Override
    protected void _lock() {
        List<PartitionGroupServer> pgsList;
        
        if (pgsID.equals(Constant.ALL_IN_PG)) {
            pgsList = getHlh().getContainer().getPgsList(clusterName, pgID);
        } else if (pgsID.equals(Constant.ALL)) {
            pgsList = getHlh().getContainer().getPgsList(clusterName);
        } else {
            PartitionGroupServer pgs = getHlh().getContainer().getPgs(clusterName, pgsID);
            if (pgs == null) {
                throw new IllegalArgumentException(
                        EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                                + PartitionGroupServer.fullName(clusterName, pgsID));
            }
            pgsList = new ArrayList<PartitionGroupServer>();
            pgsList.add(pgs);
        }
        
        for (PartitionGroupServer pgs : pgsList) {
            switch (getLockType()) {
            case READ: 
                getHlh().acquireLock(pgs.readLock());
                break;
            case WRITE: 
                getHlh().acquireLock(pgs.writeLock());
                break;
            case SKIP:
                break;
            }
        }
    }
    
}

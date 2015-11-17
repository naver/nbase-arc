package com.navercorp.nbasearc.confmaster.repository.lock;

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
            PartitionGroupServer pgs = getHlh().getPgsImo().get(pgsID, clusterName);
            if (pgs == null) {
                String message = EXCEPTIONMSG_PARTITION_GROUP_DOES_NOT_EXIST
                        + PartitionGroupServer.fullName(clusterName, pgsID);
                Logger.error(message);
                throw new IllegalArgumentException(message);
            }
            pgID = String.valueOf(pgs.getData().getPgId());
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
            pgsList = getHlh().getPgsImo().getList(clusterName, Integer.valueOf(pgID));
        } else if (pgsID.equals(Constant.ALL)) {
            pgsList = getHlh().getPgsImo().getList(clusterName);
        } else {
            PartitionGroupServer pgs = getHlh().getPgsImo().get(pgsID, clusterName);
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

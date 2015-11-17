package com.navercorp.nbasearc.confmaster.repository.lock;

import static com.navercorp.nbasearc.confmaster.Constant.*;

import java.util.ArrayList;
import java.util.List;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;

public class HierarchicalLockGW extends HierarchicalLock {
    
    private String clusterName;
    private String gwId;
    
    public HierarchicalLockGW() {
    }
    
    public HierarchicalLockGW(HierarchicalLockHelper hlh, LockType lockType,
            String clusterName, String gwID) {
        super(hlh, lockType);
        this.clusterName = clusterName;
        this.gwId = gwID;
        _lock();
    }
    
    @Override
    protected void _lock() {
        List<Gateway> gateways;
        
        if (gwId.equals(ALL)) {
            gateways = getHlh().getGwImo().getList(clusterName);
        } else {
            Gateway gw = getHlh().getGwImo().get(gwId, clusterName);
            if (gw == null) {
                String message = EXCEPTIONMSG_GATEWAY_DOES_NOT_EXIST
                        + Gateway.fullName(clusterName, gwId);
                Logger.error(message);
                throw new IllegalArgumentException(message);
            }
            gateways = new ArrayList<Gateway>();
            gateways.add(gw);
        }
        
        for (Gateway gw : gateways) {
            switch (getLockType()) {
            case READ: 
                getHlh().acquireLock(gw.readLock());
                break;
            case WRITE: 
                getHlh().acquireLock(gw.writeLock());
                break;
            case SKIP:
                break;
            }
        }
    }

}
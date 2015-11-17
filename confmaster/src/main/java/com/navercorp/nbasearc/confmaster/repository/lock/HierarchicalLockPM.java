package com.navercorp.nbasearc.confmaster.repository.lock;

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
            pmList = getHlh().getPmImo().getAll();
        } else {
            pmList = new ArrayList<PhysicalMachine>();
            PhysicalMachine pm = getHlh().getPmImo().get(pmName);
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

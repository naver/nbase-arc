package com.navercorp.nbasearc.confmaster.server.watcher;

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPmRoot extends WatchEventHandler  {
    
    private final PhysicalMachineImo pmImo;
    
    public WatchEventHandlerPmRoot(ApplicationContext context) {
        super(context);
        this.pmImo = context.getBean(PhysicalMachineImo.class);
    }
    
    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException,
            NoNodeException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            registerBoth(event.getPath());
            
            // Delete
            List<String> deleted = getDeletedChild(event.getPath(), pmImo.getAll());
            for (String pmName : deleted) {
                pmImo.delete(PathUtil.pmPath(pmName));
            }
            
            // Created
            List<String> created = getCreatedChild(event.getPath(), pmImo.getAll());
            for (String pmName : created) {
                pmImo.load(pmName);
            }
        }
    }
    
    @Override
    public void onChangedEvent(WatchedEvent event) {
    }

    @Override
    public void lock(String path) {
        lockHelper.root(READ);
        lockHelper.pmList(WRITE);
    }
    
}

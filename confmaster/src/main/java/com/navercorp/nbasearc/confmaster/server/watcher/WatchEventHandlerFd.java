package com.navercorp.nbasearc.confmaster.server.watcher;

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.UPDATE_HEARTBEAT_CHECKER;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;

public class WatchEventHandlerFd extends WatchEventHandler {
    
    public WatchEventHandlerFd(ApplicationContext context) {
        super(context);
    }
    
    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException {
        registerBoth(event.getPath());
        
        workflowExecutor.perform(UPDATE_HEARTBEAT_CHECKER);
    }
    
    @Override
    public void onChangedEvent(WatchedEvent event) {
    }

    @Override
    public void lock(String path) {
        lockHelper.root(WRITE);
    }
    
}

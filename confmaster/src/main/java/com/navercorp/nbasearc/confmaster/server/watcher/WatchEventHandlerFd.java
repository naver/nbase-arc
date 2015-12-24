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

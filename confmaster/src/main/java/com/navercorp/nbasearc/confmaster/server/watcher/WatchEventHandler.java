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

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

public abstract class WatchEventHandler implements Watcher {
    
    protected final ApplicationContext context;
    protected final ZooKeeperHolder zookeeper;
    protected final WorkflowExecutor workflowExecutor;
    
    protected final PhysicalMachineClusterImo pmClusterImo;
    
    protected final HierarchicalLockHelper lockHelper;
    
    public WatchEventHandler(ApplicationContext context) {
        this.context = context;
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        this.workflowExecutor = context.getBean(WorkflowExecutor.class);

        this.pmClusterImo = context.getBean(PhysicalMachineClusterImo.class);
        
        this.lockHelper = new HierarchicalLockHelper(context);
    }
    
    @Override
    public void process(WatchedEvent event) {
        Level logLevel = INFO;
        
        try {
            if (event.getType() == Event.EventType.NodeDeleted) {
                return;
            }
            
            lock(event.getPath());
            work(event);
        } catch (Exception e) {
            Logger.error("Handle watch event fail. {} {}",
                    event.getPath(), event, e);
            
            logLevel = DEBUG;
        } finally {
            unlock();
        }
        
        Logger.flush(logLevel);
    }
    
    public void work(WatchedEvent event) {
        try {
            switch (event.getType()) {
            case NodeDataChanged:
                onChangedEvent(event);
                break;
            case NodeChildrenChanged:
                onChildEvent(event);
                break;
            default:
                break;
            }
        } catch (Exception e) {
            Logger.error("Handle watch event fail. {}", event, e);
        }
    }

    public void registerChangedEvent(final String path) 
            throws MgmtZooKeeperException {
        zookeeper.registerChangedEventWatcher(path, this);
    }
    
    public void registerChildEvent(final String path) 
            throws MgmtZooKeeperException {
        zookeeper.registerChildEventWatcher(path, this);
    }

    public void registerBoth(final String path) throws MgmtZooKeeperException {
        zookeeper.registerChangedEventWatcher(path, this);
        zookeeper.registerChildEventWatcher(path, this);
    }

    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException,
            NoNodeException {
    }

    public void onChangedEvent(WatchedEvent event) throws MgmtZooKeeperException {
    }

    public abstract void lock(String path);
    
    public void unlock() {
        lockHelper.releaseAllLock();
    }
    
    protected <D> List<String> getDeletedChild(
            String path, List<? extends ZNode<D>> inMemoryObjectList) 
                    throws MgmtZooKeeperException {
        List<String> deletedList = new ArrayList<String>();

        List<String> zkObjectList = zookeeper.getChildren(path);
        for (ZNode<D> imo : inMemoryObjectList) {
            deletedList.add(imo.getName());
        }
        for (String zko : zkObjectList) {
            deletedList.remove(zko);
        }
        
        return deletedList;
    }

    protected <D> List<String> getCreatedChild(String path,
            List<? extends ZNode<D>> inMemoryObjectList)
            throws MgmtZooKeeperException {
        List<String> createdList = zookeeper.getChildren(path);
        for (ZNode<D> imo : inMemoryObjectList) {
            createdList.remove(imo.getName());
        }

        return createdList;
    }

}

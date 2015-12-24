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

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPmCluster extends WatchEventHandler {

    private final PhysicalMachineClusterImo pmClusterImo;

    public WatchEventHandlerPmCluster(ApplicationContext context) {
        super(context);
        this.pmClusterImo = context.getBean(PhysicalMachineClusterImo.class);
    }

    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException {
        registerChildEvent(event.getPath());
    }

    @Override
    public void onChangedEvent(WatchedEvent event)
            throws MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            registerChangedEvent(event.getPath());

            PhysicalMachineCluster pmCluster = pmClusterImo.getByPath(event
                    .getPath());
            if (null == pmCluster) {
                // this znode already removed.
                return;
            }

            zookeeper.reflectZkIntoMemory(pmCluster);
        }
    }

    @Override
    public void lock(String path) {
        String pmName = PathUtil.getPmNameFromPath(path);
        lockHelper.root(READ);
        lockHelper.pmList(READ).pm(WRITE, pmName);
    }

}

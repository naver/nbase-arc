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

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPm extends WatchEventHandler {

    private final String pmName;

    public WatchEventHandlerPm(ApplicationContext context, String pmName) {
        super(context);
        this.pmName = pmName;
    }

    @Override
    public void onChildEvent(WatchedEvent event) throws NoNodeException,
            MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            registerBoth(event.getPath());

            // Delete
            List<String> deleted = getDeletedChild(event.getPath(),
                    pmClusterImo.getList(pmName));
            for (String clusterName : deleted) {
                pmClusterImo
                        .delete(PathUtil.pmClusterPath(clusterName, pmName));
            }

            // Created
            List<String> created = getCreatedChild(event.getPath(),
                    pmClusterImo.getList(pmName));
            for (String clusterName : created) {
                pmClusterImo.load(clusterName, pmName);
            }
        }
    }

    @Override
    public void lock(String path) {
        String pmName = PathUtil.getPmNameFromPath(path);

        lockHelper.root(READ);
        lockHelper.pmList(READ).pm(WRITE, pmName);
    }

}

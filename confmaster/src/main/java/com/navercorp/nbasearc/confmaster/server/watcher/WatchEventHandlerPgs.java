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
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.FAILOVER_PGS;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPgs extends WatchEventHandler {

    private final PartitionGroupServerImo pgsImo;

    public WatchEventHandlerPgs(ApplicationContext context) {
        super(context);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
    }

    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException {
        registerBoth(event.getPath());

        PartitionGroupServer pgs = pgsImo.getByPath(event.getPath());

        if (!event.getPath().equals(pgs.getPath())) {
            Logger.error("PATH INCONSISTENCY");
            return;
        }

        if (pgs.getData().getHb().equals(Constant.HB_MONITOR_YES)) {
            pgs.getHbc().urgent();
        }

        workflowExecutor.perform(FAILOVER_PGS, pgs);
    }

    @Override
    public void onChangedEvent(WatchedEvent event)
            throws MgmtZooKeeperException {
        registerBoth(event.getPath());

        PartitionGroupServer pgs = pgsImo.getByPath(event.getPath());
        if (null == pgs) {
            // this znode already removed.
            return;
        }

        if (LeaderState.isFollower()) {
            zookeeper.reflectZkIntoMemory(pgs);
        }

        try {
            pgs.updateHBRef();
            if (pgs.getData().getHb().equals(Constant.HB_MONITOR_YES)) {
                pgs.getHbc().urgent();
            }
        } catch (Exception e) {
            Logger.error("Change pgs fail. {}", pgs, e);
        }
    }

    @Override
    public void lock(String path) {
        String clusterName = PathUtil.getClusterNameFromPath(path);
        String pgsName = PathUtil.getPgsNameFromPath(path);

        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, null).pgs(WRITE, pgsName);
    }

}

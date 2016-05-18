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
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.COMMON_STATE_DECISION;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerRs extends WatchEventHandler {

    private final RedisServerImo rsImo;

    public WatchEventHandlerRs(ApplicationContext context) {
        super(context);
        this.rsImo = context.getBean(RedisServerImo.class);
    }

    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException {
        registerBoth(event.getPath());

        RedisServer rs = rsImo.getByPath(event.getPath());

        if (rs.getData().getHB().equals(Constant.HB_MONITOR_YES)) {
            rs.getHbc().urgent();
        }

        workflowExecutor.perform(COMMON_STATE_DECISION, rs);
    }

    @Override
    public void onChangedEvent(WatchedEvent event)
            throws MgmtZooKeeperException {
        registerBoth(event.getPath());

        RedisServer rs = rsImo.getByPath(event.getPath());
        if (null == rs) {
            // this znode already removed.
            return;
        }

        if (LeaderState.isFollower()) {
            zookeeper.reflectZkIntoMemory(rs);
        }

        try {
            rs.updateHBRef();
            if (rs.getData().getHB().equals(Constant.HB_MONITOR_YES)) {
                rs.getHbc().urgent();
            }
        } catch (Exception e) {
            Logger.error("failed while change rs. {} {}", event.getPath(),
                    event, e);
        }
    }

    @Override
    public void lock(String path) {
        String clusterName = PathUtil.getClusterNameFromPath(path);
        String rsName = PathUtil.getRsNameFromPath(path);

        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(READ, null).pgs(WRITE, rsName);
    }

}

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
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.FAILOVER_COMMON;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerGw extends WatchEventHandler {

    private final GatewayImo gwImo;
    
    public WatchEventHandlerGw(ApplicationContext context) {
        super(context);
        this.gwImo = context.getBean(GatewayImo.class);
    }
    
    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException {
        registerBoth(event.getPath());

        Gateway gw = gwImo.getByPath(event.getPath());

        if (gw.getData().getHB().equals(Constant.HB_MONITOR_YES)) {
            gw.getHbc().urgent();
        }
        
        workflowExecutor.perform(FAILOVER_COMMON, gw);
    }
    
    @Override
    public void onChangedEvent(WatchedEvent event) throws MgmtZooKeeperException {
        registerBoth(event.getPath());
        
        Gateway gw = gwImo.getByPath(event.getPath());
        if (null == gw) {
            // this znode already removed.
            return;
        }
        
        if (LeaderState.isFollower()) {
            zookeeper.reflectZkIntoMemory(gw);
        }

        try {
            gw.getRefData().setZkData(
                    gw.getData().getState(),
                    gw.getData().getStateTimestamp(),
                    gw.getStat().getVersion());
            gw.getHbc().updateState(gw.getData().getHB());
            if (gw.getData().getHB().equals(Constant.HB_MONITOR_YES)) {
                gw.getHbc().urgent();
            }
        } catch (Exception e) {
            Logger.error("Change gateway fail. {}", gw, e);
        }
    }

    @Override
    public void lock(String path) {
        String clusterName = PathUtil.getClusterNameFromPath(path);
        String gwName = PathUtil.getGwNameFromPath(path);
        
        lockHelper.root(READ).cluster(READ, clusterName).gwList(READ).gw(WRITE, gwName);
    }
    
}

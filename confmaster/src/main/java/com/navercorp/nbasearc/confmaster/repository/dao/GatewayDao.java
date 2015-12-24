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

package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;

public interface GatewayDao {

    String createGw(String gwid, GatewayData data, String clusterName,
            String pmName, PhysicalMachineCluster pmCluster)
            throws MgmtZooKeeperException;

    void deleteGw(final String name, final String clusterName,
            PhysicalMachineCluster pmCluster)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    byte[] loadGw(String name, String clusterName, Stat stat, Watcher watch)
            throws MgmtZooKeeperException, NoNodeException;

    void updateGw(Gateway gateway) throws MgmtZooKeeperException;

}

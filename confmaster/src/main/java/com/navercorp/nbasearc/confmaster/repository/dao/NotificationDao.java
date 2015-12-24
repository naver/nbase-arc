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

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

public interface NotificationDao {

    void initialize() throws KeeperException, InterruptedException, MgmtZooKeeperException;

    void addCreateClusterOp(List<Op> ops, String clusterName)
            throws MgmtZNodeAlreayExistsException, MgmtZooKeeperException;

    void addDeleteClusterOp(List<Op> ops, String clusterName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    void addCreateGatewayOp(List<Op> ops, String clusterName,
            String gatewayName, String ip, int port)
            throws MgmtZNodeAlreayExistsException,
            MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    void addDeleteGatewayOp(List<Op> ops, String clusterName, String gatewayName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    boolean isGatewayExist(String clusterName, String gatewayName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    Op createGatewayAffinityUpdateOperation(Cluster cluster);

    String updateGatewayAffinity(Cluster cluster) 
            throws MgmtZooKeeperException;

}

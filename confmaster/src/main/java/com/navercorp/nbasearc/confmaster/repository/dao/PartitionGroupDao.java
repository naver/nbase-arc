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
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;

public interface PartitionGroupDao {

    String createPg(String name, String clusterName,
            final PartitionGroupData data) throws MgmtZooKeeperException,
            NodeExistsException;

    void deletePg(String name, String cluster) throws MgmtZooKeeperException;

    byte[] loadPg(String name, String clusterName, Stat stat, Watcher watch)
            throws MgmtZooKeeperException, NoNodeException;

    void updatePg(String path, PartitionGroupData pg)
            throws MgmtZooKeeperException;

    public Op createUpdatePgOperation(String path, PartitionGroupData pg);

}

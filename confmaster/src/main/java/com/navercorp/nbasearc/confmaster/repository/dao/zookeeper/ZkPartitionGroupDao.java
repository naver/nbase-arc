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

package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;

@Repository
public class ZkPartitionGroupDao implements PartitionGroupDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();

    @Override
    public String createPg(String name, String clusterName,
            final PartitionGroupData data) throws MgmtZooKeeperException,
            NodeExistsException {
        String path = PathUtil.pgPath(name, clusterName);
        byte rawData[];

        rawData = mapper.writeValueAsBytes(data);

        return zookeeper.createPersistentZNode(path, rawData);
    }

    @Override
    public void deletePg(String name, String cluster)
            throws MgmtZooKeeperException {
        String path = PathUtil.pgPath(name, cluster);
        zookeeper.deleteZNode(path, -1);
    }

    @Override
    public byte[] loadPg(String name, String clusterName, Stat stat,
            Watcher watch) throws MgmtZooKeeperException, NoNodeException {
        return zookeeper.getData(PathUtil.pgPath(name, clusterName), stat, watch);
    }

    @Override
    public void updatePg(String path, PartitionGroupData pg)
            throws MgmtZooKeeperException {
        zookeeper.reflectMemoryIntoZk(path, pg);
    }
    
    @Override
    public Op createUpdatePgOperation(String path, PartitionGroupData pg) {
        return zookeeper.createReflectMemoryIntoZkOperation(path, pg, -1);
    }
    
}

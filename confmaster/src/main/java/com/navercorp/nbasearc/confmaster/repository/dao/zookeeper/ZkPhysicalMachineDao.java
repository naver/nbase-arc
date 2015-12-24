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

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PhysicalMachineDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;

@Repository
public class ZkPhysicalMachineDao implements PhysicalMachineDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();

    @Override
    public boolean pmExist(final String name) throws MgmtZooKeeperException {
        String path = PathUtil.pmPath(name);
        return zookeeper.isExists(path);
    }
    
    @Override
    public String createPm(final String name, final PhysicalMachineData data)
            throws NodeExistsException, MgmtZooKeeperException {
        String path = PathUtil.pmPath(name);
        String createdPath;
        byte rawData[];

        rawData = mapper.writeValueAsBytes(data);            
        createdPath = zookeeper.createPersistentZNode(path, rawData);
        return createdPath;
    }

    @Override
    public void deletePm(final String name) throws MgmtZooKeeperException  {
        String path = PathUtil.pmPath(name);
        zookeeper.deleteZNode(path, -1);
    }

    @Override
    public byte[] loadPm(String name) throws NoNodeException, MgmtZooKeeperException {
        return zookeeper.getData(PathUtil.pmPath(name), null, null);
    }
    
}

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
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.GatewayDao;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.repository.znode.PmClusterData;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;

@Repository
public class ZkGatewayDao implements GatewayDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    @Autowired
    private ZkNotificationDao notificationDao;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();

    @Override
    public String createGw(String gwid, GatewayData data, String clusterName,
            String pmName, PhysicalMachineCluster pmCluster)
            throws MgmtZooKeeperException {
        // Prepare
        List<Op> ops = new ArrayList<Op>();
        byte gwDataOfBytes[] = mapper.writeValueAsBytes(data);
        
        ops.add(Op.create(PathUtil.gwPath(gwid, clusterName), gwDataOfBytes,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

        if (null != pmCluster) {
            PmClusterData cimDataClon = (PmClusterData) pmCluster.getData().clone();
            cimDataClon.addGwId(Integer.valueOf(gwid));
            ops.add(Op.setData(pmCluster.getPath(),
                    mapper.writeValueAsBytes(cimDataClon), -1));
        } else {
            PmClusterData cimData = new PmClusterData();
            cimData.addGwId(Integer.valueOf(gwid));
            ops.add(Op.create(PathUtil.pmClusterPath(clusterName, pmName),
                    mapper.writeValueAsBytes(cimData), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT));
        }
        
        List<OpResult> results = zookeeper.multi(ops);
        zookeeper.handleResultsOfMulti(results);
        
        return ((OpResult.CreateResult) results.get(0)).getPath();
    }

    @Override
    public void deleteGw(final String name, final String clusterName,
            PhysicalMachineCluster pmCluster) throws MgmtZooKeeperException,
            MgmtZNodeDoesNotExistException {
        String path = PathUtil.gwPath(name, clusterName);

        List<Op> ops = new ArrayList<Op>();
        List<String> children = zookeeper.getChildren(path);

        for (String childName : children) {
            StringBuilder builder = new StringBuilder(path);
            builder.append("/").append(childName);
            String childPath = builder.toString();

            ops.add(Op.delete(childPath, -1));
        }

        ops.add(Op.delete(path, -1));

        PmClusterData pmClusterDataClone = (PmClusterData) pmCluster.getData().clone();
        pmClusterDataClone.deleteGwId(Integer.valueOf(name));
        byte cimDataOfBytes[] = mapper.writeValueAsBytes(pmClusterDataClone);
        ops.add(Op.setData(pmCluster.getPath(), cimDataOfBytes, -1));

        if (pmClusterDataClone.getPgsIdList().isEmpty()
                && pmClusterDataClone.getGwIdList().isEmpty()) {
            ops.add(Op.delete(pmCluster.getPath(), -1));
        }

        if (notificationDao.isGatewayExist(clusterName, name)) {
            notificationDao.addDeleteGatewayOp(ops, clusterName, name);
        }
        
        List<OpResult> results = zookeeper.multi(ops);
        zookeeper.handleResultsOfMulti(results);
    }

    @Override
    public byte[] loadGw(String name, String clusterName, Stat stat,
            Watcher watch) throws NoNodeException, MgmtZooKeeperException {
        return zookeeper.getData(PathUtil.gwPath(name, clusterName), stat, watch);
    }

    @Override
    public void updateGw(Gateway gateway) throws MgmtZooKeeperException {
        zookeeper.reflectMemoryIntoZk(gateway);
    }
    
}

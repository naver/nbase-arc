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

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.PmClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;

@Repository
public class ZkPartitionGroupServerDao implements PartitionGroupServerDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    
    @Autowired
    private Config config;
    
    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    @Override
    public String createPgs(String pgsId, String clusterName,
            PartitionGroupServerData data, PartitionGroup pg,
            PhysicalMachineCluster pmCluster) throws MgmtZooKeeperException {
        List<Op> ops = new ArrayList<Op>();
        
        byte pgsDataOfBytes[] = mapper.writeValueAsBytes(data);
        ops.add(Op.create(PathUtil.pgsPath(pgsId, clusterName), pgsDataOfBytes,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

        RedisServerData redisServerData = new RedisServerData();
        redisServerData.initialize(data.getPmName(), data.getPmIp(),
                data.getRedisPort(), data.getState(), data.getHb());
        byte rsDataOfBytes[] = mapper.writeValueAsBytes(redisServerData);
        ops.add(Op.create(PathUtil.rsPath(pgsId, clusterName), rsDataOfBytes,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

        if (null != pmCluster) {
            PmClusterData cimDataClone = (PmClusterData) pmCluster.getData().clone();
            cimDataClone.addPgsId(Integer.valueOf(pgsId));
            ops.add(Op.setData(pmCluster.getPath(),
                    mapper.writeValueAsBytes(cimDataClone), -1));
        } else {
            PmClusterData cimData = new PmClusterData();
            cimData.addPgsId(Integer.valueOf(pgsId));
            ops.add(Op.create(PathUtil.pmClusterPath(clusterName, data.getPmName()),
                    mapper.writeValueAsBytes(cimData), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT));
        }
        
        PartitionGroupData pgModified = 
            PartitionGroupData.builder().from(pg.getData())
                .addPgsId(Integer.parseInt(pgsId)).build();
        
        ops.add(Op.setData(pg.getPath(), mapper.writeValueAsBytes(pgModified), -1));
        
        List<OpResult> results = zookeeper.multi(ops);
        zookeeper.handleResultsOfMulti(results);
        
        OpResult.CreateResult resultForCreatingPGS = 
                (OpResult.CreateResult) results.get(0);
        return resultForCreatingPGS.getPath();
    }

    /** 
     * 
     * SUBJECT 
     *   Q. Why deletePgs() method uses for-loop statement?
     *   A. Due to the synchronization problem for creating or deleting opinions 
     *      with pgs_leave and pgs_del
     *   
     * SCENARIO (remove an opinion) 
     * 
     *         PGS
     *        /   \            OP1 : Leader's
     *       /     \           OP2 : Follower's
     *      OP1    OP2
     *    
     *        LEADER                          FOLLOWER
     *   1.   pgs_leave                       
     *   2.   pgs_del                         get a watch event of pgs_leave
     *   2.1. get children of PGS z-node.
     *   2.2.                                 delete an opinion, OP2
     * F 2.3. (fail) delete the children.
     *
     * SCENARIO (put an opinion)
     * 
     *         PGS
     *        /                OP1 : Leader's
     *       /    
     *      OP1    
     *       
     *        LEADER                          FOLLOWER
     *   1.   pgs_leave                       
     *   2.   pgs_del                         
     *   2.1. get children of PGS z-node.
     *   2.2.                                 put an opinion, OP2
     *   2.3. delete the children.
     *         PGS
     *            \            
     *             \           OP2 : Follower's
     *             OP2
     * F 2.4. (fail) delete the PGS z-node.
     *   2.5.                                 get a watch event of pgs_leave
     *
     * But, eventually good things will happen.
     * 
     * @Return Returns the number of retries if successful.
     */
    @Override
    public int deletePgs(String name, String clusterName, PartitionGroup pg,
            PhysicalMachineCluster pmCluster) throws MgmtZooKeeperException {
        final String path = PathUtil.pgsPath(name, clusterName);
        final String pathForRs = PathUtil.rsPath(name, clusterName);
        final int MAX = config.getServerCommandPgsdelMaxretry();
        int retryCnt;
        
        for (retryCnt = 1; retryCnt <= MAX; retryCnt++) {
            // Delete children(opinions) of PGS
            try {
                zookeeper.deleteChildren(path);
            } catch (MgmtZooKeeperException e) {
                if (e.getCause() instanceof KeeperException.NoNodeException && retryCnt != MAX) {
                    // Retry
                    Logger.info("Delete children of {} fail. retry {}", 
                            PartitionGroupServer.fullName(clusterName, name), retryCnt, e);
                    continue;
                } else {
                    throw e;
                }
            }
            
            // Delete children(opinions) of RS
            try {
                zookeeper.deleteChildren(pathForRs);
            } catch (MgmtZooKeeperException e) {
                if (e.getCause() instanceof KeeperException.NoNodeException && retryCnt != MAX) {
                    // Retry
                    Logger.info("Delete children of {} fail. retry {}", 
                            RedisServer.fullName(clusterName, name), retryCnt, e);
                    continue;
                } else {
                    throw e;
                }
            }
            
            // Delete PGS and RS & Update PG
            List<Op> ops = new ArrayList<Op>();
            ops.add(Op.delete(path, -1));
            ops.add(Op.delete(pathForRs, -1));
            
            PmClusterData cimData = (PmClusterData) pmCluster.getData().clone();
            cimData.deletePgsId(Integer.valueOf(name));
            byte cimDataOfBytes[] = mapper.writeValueAsBytes(cimData);
            ops.add(Op.setData(pmCluster.getPath(), cimDataOfBytes, -1));

            PartitionGroupData pgModified = 
                PartitionGroupData.builder().from(pg.getData())
                    .deletePgsId(Integer.parseInt(name)).build();
            
            byte pgDataOfBytes[] = mapper.writeValueAsBytes(pgModified);
            ops.add(Op.setData(pg.getPath(), pgDataOfBytes, -1));

            if (cimData.getPgsIdList().isEmpty() && cimData.getGwIdList().isEmpty()) {
                ops.add(Op.delete(pmCluster.getPath(), -1));
            }
            
            try {
                List<OpResult> results = zookeeper.multi(ops);
                zookeeper.handleResultsOfMulti(results);
                return retryCnt;
            } catch (MgmtZooKeeperException e) {
                if (e.getCause() instanceof KeeperException.NotEmptyException && retryCnt != MAX) {
                    // Retry
                    Logger.info("Delete {} fail. retry {}", 
                            PartitionGroupServer.fullName(clusterName, name), retryCnt, e);
                } else {
                    throw e;
                }
            }
        }

        return retryCnt;
    }
    
    @Override
    public byte[] loadPgs(String name, String clusterName, Stat stat,
            Watcher watch) throws NoNodeException, MgmtZooKeeperException {
        return zookeeper.getData(PathUtil.pgsPath(name, clusterName), stat, watch);
    }

    @Override
    public void updatePgs(String path, PartitionGroupServerData pgs)
            throws MgmtZooKeeperException {
        zookeeper.reflectMemoryIntoZk(path, pgs);
    }
    
    @Override
    public Op createUpdatePgsOperation(String path, PartitionGroupServerData pgs) {
        return zookeeper.createReflectMemoryIntoZkOperation(path, pgs, -1);
    }
    
    @Override
    public void updateRs(String path, RedisServerData rs)
            throws MgmtZooKeeperException {
        zookeeper.reflectMemoryIntoZk(path, rs);
    }
    
    @Override
    public Op createUpdateRsOperation(String path, RedisServerData rs) {
        return zookeeper.createReflectMemoryIntoZkOperation(path, rs, -1);
    }
    
}

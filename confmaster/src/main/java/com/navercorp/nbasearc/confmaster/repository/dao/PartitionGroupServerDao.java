package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;

public interface PartitionGroupServerDao {

    String createPgs(String pgsId, String clusterName,
            PartitionGroupServerData data, PartitionGroup pg,
            PhysicalMachineCluster pmCluster) throws MgmtZooKeeperException;

    int deletePgs(String name, String clusterName, PartitionGroup pg,
            PhysicalMachineCluster pmCluster) throws MgmtZooKeeperException;

    byte[] loadPgs(String name, String clusterName, Stat stat,
            Watcher watch) throws MgmtZooKeeperException, NoNodeException;

    void updatePgs(String path, PartitionGroupServerData pgs)
            throws MgmtZooKeeperException;

    public Op createUpdatePgsOperation(String path, PartitionGroupServerData pgs);

    void updateRs(String path, RedisServerData rs)
            throws MgmtZooKeeperException;
    
    public Op createUpdateRsOperation(String path, RedisServerData rs);
    
}

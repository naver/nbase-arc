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

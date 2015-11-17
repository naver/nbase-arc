package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

public interface ClusterDao {

    String createCluster(final String name, final ClusterData data)
            throws MgmtZNodeAlreayExistsException, MgmtZooKeeperException;

    void deleteCluster(final String name)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    byte[] loadCluster(String name, Stat stat, Watcher watch)
            throws MgmtZooKeeperException, NoNodeException;

    void updateCluster(Cluster cluster) throws MgmtZooKeeperException;

}

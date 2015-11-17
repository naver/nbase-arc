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

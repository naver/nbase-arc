package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import com.navercorp.nbasearc.confmaster.ConfMasterException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterBackupSchedule;

public interface ClusterBackupScheduleDao {

    String createAppData(String clusterName, ClusterBackupSchedule data)
            throws MgmtZooKeeperException, NodeExistsException;

    void setAppData(String clusterName, ClusterBackupSchedule data)
            throws MgmtZooKeeperException;

    ClusterBackupSchedule loadClusterBackupSchedule(String clusterName)
            throws NoNodeException, MgmtZooKeeperException,
            ConfMasterException;

    byte[] load(String clusterName) throws MgmtZooKeeperException, NoNodeException;

}

package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;

public interface PhysicalMachineDao {

    boolean pmExist(final String name) throws MgmtZooKeeperException;

    String createPm(final String name, final PhysicalMachineData data)
            throws MgmtZooKeeperException, NodeExistsException;

    void deletePm(final String name) throws MgmtZooKeeperException;

    byte[] loadPm(String name) throws MgmtZooKeeperException, NoNodeException;

}

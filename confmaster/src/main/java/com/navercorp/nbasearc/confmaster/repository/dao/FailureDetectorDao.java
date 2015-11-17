package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.FailureDetectorData;
import com.navercorp.nbasearc.confmaster.server.cluster.FailureDetector;

public interface FailureDetectorDao {

    String createFd(final FailureDetectorData data)
            throws MgmtZooKeeperException, NodeExistsException;

    FailureDetector loadFd() throws MgmtZooKeeperException, NoNodeException;

}

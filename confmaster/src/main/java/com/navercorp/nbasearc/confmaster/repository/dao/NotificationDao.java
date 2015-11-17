package com.navercorp.nbasearc.confmaster.repository.dao;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

public interface NotificationDao {

    void initialize() throws KeeperException, InterruptedException, MgmtZooKeeperException;

    void addCreateClusterOp(List<Op> ops, String clusterName)
            throws MgmtZNodeAlreayExistsException, MgmtZooKeeperException;

    void addDeleteClusterOp(List<Op> ops, String clusterName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    void addCreateGatewayOp(List<Op> ops, String clusterName,
            String gatewayName, String ip, int port)
            throws MgmtZNodeAlreayExistsException,
            MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    void addDeleteGatewayOp(List<Op> ops, String clusterName, String gatewayName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    boolean isGatewayExist(String clusterName, String gatewayName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException;

    Op createGatewayAffinityUpdateOperation(Cluster cluster);

    String updateGatewayAffinity(Cluster cluster) 
            throws MgmtZooKeeperException;

}

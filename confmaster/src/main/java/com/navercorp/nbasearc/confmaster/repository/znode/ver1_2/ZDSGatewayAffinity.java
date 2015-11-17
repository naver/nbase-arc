package com.navercorp.nbasearc.confmaster.repository.znode.ver1_2;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkNotificationDao;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;

/*
 * @date 20140812
 * @version 1.2 ~ 1.3
 * @desc Gateway Affinity 적용을 위해 주키퍼의 znode 구조에 변경이 생겼다. 기존 버전에는 이 노드가 없으니까 만들어주자. 
 */
@Repository
public class ZDSGatewayAffinity {
    
    @Autowired
    private ApplicationContext context;
    @Autowired
    private ZooKeeperHolder zookeeper;
    @Autowired
    private ClusterImo clusterImo;
    @Autowired
    private ZkNotificationDao notificationDao;
    
    public void updateZNodeDirectoryStructure(String charset)
            throws NodeExistsException, MgmtZooKeeperException {
        List<Cluster> clusterList = clusterImo.getAll();
        for (Cluster cluster : clusterList) {
            createZNode(cluster, charset);
        }
    }
    
    private void createZNode(Cluster cluster, String charset)
            throws MgmtZooKeeperException, NodeExistsException {
        String affinityPath = getRepoNoti().pathOfGWAffinity(cluster.getName());
        if (zookeeper.isExists(affinityPath)) {
            return;
        }

        String gatewayAffinity = cluster.getGatewayAffinity(context);

        try {
            zookeeper.createPersistentZNode(affinityPath,
                    gatewayAffinity.getBytes(charset));
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(charset + " is unkown.");
        }
    }

    public ZkNotificationDao getRepoNoti() {
        return notificationDao;
    }
    
    public void setRepoNoti(ZkNotificationDao notificationDao) {
        this.notificationDao = notificationDao;
    }

}

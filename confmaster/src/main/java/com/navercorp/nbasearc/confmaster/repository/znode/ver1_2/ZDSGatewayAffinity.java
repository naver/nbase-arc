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

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

package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.NotificationDao;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;

@Repository
public class ZkNotificationDao implements NotificationDao {

    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    
    @Autowired
    private Config config;
    
    private static final String NOTIFICATION = "NOTIFICATION";
    private static final String CLUSTER = "CLUSTER";
    private static final String GW = "GW";
    private static final String AFFINITY = "AFFINITY";

    private static final String ROOT_PATH_OF_NOTIFICATION = 
            PathUtil.rcRootPath() + "/" + NOTIFICATION;
    private static final String ROOT_PATH_OF_CLUSTER = 
            ROOT_PATH_OF_NOTIFICATION + "/" + CLUSTER;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    public ZkNotificationDao() {
    }

    @Override
    public void initialize() throws MgmtZooKeeperException {
        try {
            zookeeper.createPersistentZNode(rootPathOfNotification());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zookeeper.createPersistentZNode(rootPathOfCluster());
        } catch (NodeExistsException e) {
            // Ignore.
        }
    }

    @Override
    public void addCreateClusterOp(List<Op> ops, String clusterName)
            throws MgmtZooKeeperException, MgmtZNodeAlreayExistsException {
        String path = pathOfCluster(clusterName);
        boolean exist = zookeeper.isExists(path);
        if (exist) {
            throw new MgmtZNodeAlreayExistsException(
                path, 
                String.format(
                    "-ERR the cluster znode for notification already exists. cluster:%s.", 
                    clusterName));
        }

        ops.add(Op.create(path, Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        ops.add(Op.create(rootPathOfGateway(clusterName), Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        try {
            ops.add(Op.create(pathOfGWAffinity(clusterName),
                    Constant.AFFINITY_ZNODE_INITIAL_DATA.getBytes(config
                            .getCharset()), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT));
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unknown.");
        }
    }

    @Override
    public void addDeleteClusterOp(List<Op> ops, String clusterName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        String path = pathOfCluster(clusterName);
        boolean exist = zookeeper.isExists(path);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                path, 
                String.format(
                    "-ERR the cluster znode for notification does not exist. cluster:%s.", 
                    clusterName));
        }

        ops.add(Op.delete(pathOfGWAffinity(clusterName), -1));
        ops.add(Op.delete(rootPathOfGateway(clusterName), -1));
        ops.add(Op.delete(path, -1));
    }

    @Override
    public void addCreateGatewayOp(List<Op> ops, String clusterName,
            String gatewayName, String ip, int port)
            throws MgmtZNodeAlreayExistsException, MgmtZooKeeperException,
            MgmtZNodeDoesNotExistException {
        String clusterPath = pathOfCluster(clusterName);
        boolean exist = zookeeper.isExists(clusterPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                clusterPath, 
                String.format(
                    "-ERR the cluster znode for notification already exists. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        String gatewayPath = pathOfGateway(clusterName, gatewayName);
        exist = zookeeper.isExists(gatewayPath);
        if (exist) {
            throw new MgmtZNodeAlreayExistsException(
                gatewayPath, 
                String.format(
                    "-ERR the gateway znode for notification already exists. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        DataOfGateway data = new DataOfGateway(ip, port);
        byte rawData[] = mapper.writeValueAsBytes(data);
        ops.add(Op.create(gatewayPath, rawData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    }

    @Override
    public void addDeleteGatewayOp(List<Op> ops, String clusterName, String gatewayName) 
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        String clusterPath = pathOfCluster(clusterName);
        boolean exist =  zookeeper.isExists(clusterPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                clusterPath, 
                String.format(
                    "-ERR the cluster znode for notification does not exist. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        String gatewayPath = pathOfGateway(clusterName, gatewayName);
        exist = zookeeper.isExists(gatewayPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                gatewayPath, 
                String.format(
                    "-ERR the gateway znode for notification does not exist. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        ops.add(Op.delete(gatewayPath, -1));
    }

    @Override
    public boolean isGatewayExist(String clusterName, String gatewayName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        String clusterPath = pathOfCluster(clusterName);
        boolean exist =  zookeeper.isExists(clusterPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                clusterPath, 
                String.format(
                    "-ERR the cluster znode for notification does not exist. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        String gatewayPath = pathOfGateway(clusterName, gatewayName);
        return zookeeper.isExists(gatewayPath);
    }

    @Override
    public Op createGatewayAffinityUpdateOperation(Cluster cluster) {
        final String gatewayAffinity = cluster.getGatewayAffinity(context);
        final String path = pathOfGWAffinity(cluster.getName());
        
        try {
            return Op.setData(path, gatewayAffinity.getBytes(config.getCharset()), -1);
        } catch (UnsupportedEncodingException e) {
            Logger.error("Update gateway affinity fail. {}", cluster, e);
            throw new AssertionError(config.getCharset() + " is unknown.");
        }            
    }
    
    @Override
    public String updateGatewayAffinity(Cluster cluster)
            throws MgmtZooKeeperException {
            final String gatewayAffinity = cluster.getGatewayAffinity(context);
            final String path = pathOfGWAffinity(cluster.getName());
            
            try {
                zookeeper.setData(path, gatewayAffinity.getBytes(config.getCharset()), -1);
            } catch (MgmtZooKeeperException e) {
                Logger.error("Update gateway affinity fail. {}", cluster, e);
                throw e;
            } catch (UnsupportedEncodingException e) {
                Logger.error("Update gateway affinity fail. {}", cluster, e);
                throw new AssertionError(config.getCharset() + " is unknown.");
            }
            
            return gatewayAffinity;
    }

    public String rootPathOfNotification() {
        return ROOT_PATH_OF_NOTIFICATION;
    }
    
    public String rootPathOfCluster() {
        return ROOT_PATH_OF_CLUSTER;
    }
    
    public String rootPathOfGateway(String clusterName) {
        return pathOfCluster(clusterName) + "/" + GW;
    }
    
    public String pathOfCluster(String clusterName) {
        return rootPathOfCluster() + "/" + clusterName;
    }
    
    public String pathOfGateway(String clusterName, String gatewayName) {
        return rootPathOfGateway(clusterName) + "/" + gatewayName;
    }
    
    public String pathOfGWAffinity(String clusterName) {
        return pathOfCluster(clusterName) + "/" + AFFINITY;
    }
    
    class DataOfGateway {
        private String ip;
        private int port;
        public DataOfGateway(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
        public String getIp() {
            return ip;
        }
        public void setIp(String ip) {
            this.ip = ip;
        }
        public int getPort() {
            return port;
        }
        public void setPort(int port) {
            this.port = port;
        }
    }
    
}

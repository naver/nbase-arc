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

package com.navercorp.nbasearc.confmaster.server.cluster;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeAlreayExistsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZNodeDoesNotExistException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;

@Component
public class GatewayLookup {

    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zk;
    
    @Autowired
    private Config config;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    public GatewayLookup() {
    }

    public void initialize() throws MgmtZooKeeperException {
        try {
            zk.createPersistentZNode(PathUtil.rootPathOfGwLookup());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zk.createPersistentZNode(PathUtil.rootPathOfGwLookupCluster());
        } catch (NodeExistsException e) {
            // Ignore.
        }
    }

    public void addCreateClusterOp(List<Op> ops, String clusterName)
            throws MgmtZooKeeperException, MgmtZNodeAlreayExistsException {
        String path = PathUtil.pathOfGwLookupCluster(clusterName);
        boolean exist = zk.isExists(path);
        if (exist) {
            throw new MgmtZNodeAlreayExistsException(
                path, 
                String.format(
                    "-ERR the cluster znode for notification already exists. cluster:%s.", 
                    clusterName));
        }

        ops.add(Op.create(path, Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        ops.add(Op.create(PathUtil.rootPathOfGwLookup(clusterName), Constant.ZERO_BYTE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        try {
            ops.add(Op.create(PathUtil.pathOfGwAffinity(clusterName),
                    Constant.AFFINITY_ZNODE_INITIAL_DATA.getBytes(config
                            .getCharset()), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT));
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unknown.");
        }
    }

    public void addDeleteClusterOp(List<Op> ops, String clusterName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        String path = PathUtil.pathOfGwLookupCluster(clusterName);
        boolean exist = zk.isExists(path);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                path, 
                String.format(
                    "-ERR the cluster znode for notification does not exist. cluster:%s.", 
                    clusterName));
        }

        ops.add(Op.delete(PathUtil.pathOfGwAffinity(clusterName), -1));
        ops.add(Op.delete(PathUtil.rootPathOfGwLookup(clusterName), -1));
        ops.add(Op.delete(path, -1));
    }

    public void addCreateGatewayOp(List<Op> ops, String clusterName,
            String gatewayName, String ip, int port)
            throws MgmtZNodeAlreayExistsException, MgmtZooKeeperException,
            MgmtZNodeDoesNotExistException {
        String clusterPath = PathUtil.pathOfGwLookupCluster(clusterName);
        boolean exist = zk.isExists(clusterPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                clusterPath, 
                String.format(
                    "-ERR the cluster znode for notification already exists. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        String gatewayPath = PathUtil.pathOfGwLookup(clusterName, gatewayName);
        exist = zk.isExists(gatewayPath);
        if (exist) {
            throw new MgmtZNodeAlreayExistsException(
                gatewayPath, 
                String.format(
                    "-ERR the gateway znode for notification already exists. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        GatewayLookupData data = new GatewayLookupData(ip, port);
        byte rawData[] = mapper.writeValueAsBytes(data);
        ops.add(Op.create(gatewayPath, rawData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    }

    public void addDeleteGatewayOp(List<Op> ops, String clusterName, String gatewayName) 
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        String clusterPath = PathUtil.pathOfGwLookupCluster(clusterName);
        boolean exist =  zk.isExists(clusterPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                clusterPath, 
                String.format(
                    "-ERR the cluster znode for notification does not exist. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        String gatewayPath = PathUtil.pathOfGwLookup(clusterName, gatewayName);
        exist = zk.isExists(gatewayPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                gatewayPath, 
                String.format(
                    "-ERR the gateway znode for notification does not exist. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        ops.add(Op.delete(gatewayPath, -1));
    }

    public boolean isGatewayExist(String clusterName, String gatewayName)
            throws MgmtZNodeDoesNotExistException, MgmtZooKeeperException {
        String clusterPath = PathUtil.pathOfGwLookupCluster(clusterName);
        boolean exist =  zk.isExists(clusterPath);
        if (!exist) {
            throw new MgmtZNodeDoesNotExistException(
                clusterPath, 
                String.format(
                    "-ERR the cluster znode for notification does not exist. cluster:%s, gateway:%s", 
                    clusterName, gatewayName));
        }
        
        String gatewayPath = PathUtil.pathOfGwLookup(clusterName, gatewayName);
        return zk.isExists(gatewayPath);
    }

    public Op createGatewayAffinityUpdateOperation(Cluster cluster) {
        final String gatewayAffinity = cluster.getGatewayAffinity(context);
        final String path = PathUtil.pathOfGwAffinity(cluster.getName());
        
        try {
            return Op.setData(path, gatewayAffinity.getBytes(config.getCharset()), -1);
        } catch (UnsupportedEncodingException e) {
            Logger.error("Update gateway affinity fail. {}", cluster, e);
            throw new AssertionError(config.getCharset() + " is unknown.");
        }            
    }
    
    public String updateGatewayAffinity(Cluster cluster)
            throws MgmtZooKeeperException {
            final String gatewayAffinity = cluster.getGatewayAffinity(context);
            final String path = PathUtil.pathOfGwAffinity(cluster.getName());
            
            try {
                zk.setData(path, gatewayAffinity.getBytes(config.getCharset()), -1);
            } catch (MgmtZooKeeperException e) {
                Logger.error("Update gateway affinity fail. {}", cluster, e);
                throw e;
            } catch (UnsupportedEncodingException e) {
                Logger.error("Update gateway affinity fail. {}", cluster, e);
                throw new AssertionError(config.getCharset() + " is unknown.");
            }
            
            return gatewayAffinity;
    }
    
    class GatewayLookupData {
        private String ip;
        private int port;
        public GatewayLookupData(String ip, int port) {
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

    @JsonAutoDetect(
            fieldVisibility=Visibility.ANY, 
            getterVisibility=Visibility.NONE, 
            setterVisibility=Visibility.NONE)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @JsonPropertyOrder({"affinity", "gw_id"})
    public static class GatewayAffinityData {
        
        @JsonProperty("gw_id")
        private Integer gwId;
        @JsonProperty("affinity")
        private String affinity;
        
        public GatewayAffinityData(String gw_id, String affinity) {
            setGwId(Integer.valueOf(gw_id));
            setAffinity(affinity);
        }

        public Integer getGwId() {
            return gwId;
        }

        public void setGwId(Integer gw_id) {
            this.gwId = gw_id;
        }

        public String getAffinity() {
            return affinity;
        }

        public void setAffinity(String affinity) {
            this.affinity = affinity;
        }
    }
    
}

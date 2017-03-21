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

package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.server.mapping.ArityType.LESS;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.TOTAL_INSPECTION;
import static org.apache.log4j.Level.INFO;
import static com.navercorp.nbasearc.confmaster.Constant.*;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.GatewayLookup;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.Param;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;

@Service
public class ConfmasterService {
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zk;
    
    @Autowired
    private CommandExecutor commandTemplate;
    @Autowired
    private WorkflowExecutor wfExecutor;

    @Autowired
    private ClusterComponentContainer container;

    @Autowired
    private GatewayLookup gwLookup;
    @Autowired
    private WorkflowLogger workflowLogger;
    
    @Autowired
    private Config config;
    
    @Autowired
    private ConfMaster confMaster;

    private boolean clusterRootWatcherRegistered = false;

    @CommandMapping(
            name="help", 
            arityType=LESS, 
            usage="help <command>", 
            requiredState=ConfMaster.LOADING)
    public String help(@Param(type=NULLABLE) String commandName) {
        if (commandName == null) {
            return commandTemplate.getHelp();
        } else {
            return commandTemplate.getUsage(commandName);
        }
    }
    
    @LockMapping(name="help")
    public void helpLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
    @CommandMapping(
            name="ping", 
            usage="ping", 
            requiredState=ConfMaster.LOADING)
    public String ping() {
        return "+PONG";
    }

    @LockMapping(name="ping")
    public void pingLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
    @CommandMapping(
            name="cm_start", 
            requiredState=ConfMaster.READY)
    public String start() {
        if (confMaster.getState() == ConfMaster.RUNNING) {
            return EXCEPTIONMSG_CM_ALREADY_RUNNING;
        }
        
        confMaster.setState(ConfMaster.RUNNING);

        wfExecutor.perform(TOTAL_INSPECTION);
        
        return S2C_OK;
    }
    
    @LockMapping(name="cm_start")
    public void startLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
    @CommandMapping(
            name="cm_info", 
            requiredState=ConfMaster.LOADING)
    public String info() {
        return "{\"state\":\"" + confMaster.getState() + "\"}";
    }
    
    @LockMapping(name="cm_info")
    public void infoLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
    public void initialize() throws MgmtZooKeeperException,
            NodeExistsException, NoNodeException {
        // Create default znode structure for ConfMaster.
        try {
            zk.createPersistentZNode(PathUtil.rcRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zk.createPersistentZNode(PathUtil.pmRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zk.createPersistentZNode(PathUtil.clusterRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zk.createPersistentZNode(PathUtil.ccRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zk.createPersistentZNode(PathUtil.rootPathOfLE());
        } catch (NodeExistsException e) {
            // Ignore.
        }

        // Create failure detector znode in order to decide majority used on failover.
        try {
            zk.createPersistentZNode(PathUtil.fdRootPath(), Constant.ZERO_BYTE);
        } catch (NodeExistsException e) {
            // Ignore.
        }

        String heartbeaterPath = 
            PathUtil.fdRootPath() + "/" + config.getIp() + ":" + config.getPort();
        zk.createEphemeralZNode(heartbeaterPath);

        gwLookup.initialize();
        workflowLogger.initialize();        
    }
    
    public void release() throws MgmtZooKeeperException {
        final String path = PathUtil.fdRootPath() + "/" + config.getIp() + ":" + config.getPort();
        zk.deleteZNode(path, -1);
        container.relase();
    }

    public void loadAll() throws MgmtZooKeeperException, NoNodeException {
        // Load physical machines.
        List<String> pmList = zk.getChildren(PathUtil.pmRootPath());
        for (String pmName : pmList) {
            final String path = PathUtil.pmPath(pmName);
            byte []d = zk.getData(path, null);
            PhysicalMachine pm = (PhysicalMachine) container.get(path); 
            if (pm != null) {
                pm.setPersistentData(d);
            } else {
                pm = new PhysicalMachine(d, pmName);
                container.put(pm.getPath(), pm);
            }
            
            if (LeaderState.isLeader()) {
                List<String> pmClusterList = zk.getChildren(path, null);
                for (String pmClusterName : pmClusterList) {
                    PhysicalMachineCluster pmCluster = new PhysicalMachineCluster(
                    		zk.getData(PathUtil.pmClusterPath(pmClusterName, pmName), null), 
                    		pmClusterName, pmName);
                    container.put(pmCluster.getPath(), pmCluster);
                }
            }
        }
        
        // Register failure detector watch
        String path = PathUtil.fdRootPath();
        zk.registerChildEventWatcher(path);
        
        // Register watcher to the root of cluster.
        if (clusterRootWatcherRegistered == false) {
            zk.registerChangedEventWatcher(PathUtil.clusterRootPath());
            zk.registerChildEventWatcher(PathUtil.clusterRootPath());
            clusterRootWatcherRegistered = true;
        }

        // Load clusters
        List<String> children = zk.getChildren(PathUtil.clusterRootPath());
        for (String clusterName : children) {
        	Cluster.loadClusterFromZooKeeper(context, clusterName);
            Logger.info("Load cluster success. {}", clusterName);
            Logger.flush(INFO);
        }
    }

    /*
     * @date 20140812
     * @version 1.2 ~ 1.3
     * @desc Gateway Affinity 적용을 위해 주키퍼의 znode 구조에 변경이 생겼다. 기존 버전에는 이 노드가 없으니까 만들어주자. 
     */
    public void updateZNodeDirectoryStructure()
            throws NodeExistsException, MgmtZooKeeperException {
        List<Cluster> clusterList = container.getAllCluster();
        for (Cluster cluster : clusterList) {
            String affinityPath = PathUtil.pathOfGwAffinity(cluster.getName());
            if (zk.isExists(affinityPath)) {
                return;
            }

            String gatewayAffinity = cluster.getGatewayAffinity(context);

            try {
                zk.createPersistentZNode(affinityPath,
                        gatewayAffinity.getBytes(config.getCharset()));
            } catch (UnsupportedEncodingException e) {
                throw new AssertionError(config.getCharset() + " is unkown.");
            }
        }
    }
}

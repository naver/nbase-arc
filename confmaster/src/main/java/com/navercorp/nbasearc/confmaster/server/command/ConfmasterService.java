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

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.FailureDetectorDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkNotificationDao;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.znode.FailureDetectorData;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.FailureDetectorImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.Param;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerClusterRoot;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerPmRoot;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

@Service
public class ConfmasterService {
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    
    @Autowired
    private CommandExecutor commandTemplate;
    @Autowired
    private WorkflowExecutor wfExecutor;

    @Autowired
    private ClusterImo clusterImo;
    @Autowired
    private PhysicalMachineImo pmImo;
    @Autowired
    private PartitionGroupImo pgImo;
    @Autowired
    private PartitionGroupServerImo pgsImo;
    @Autowired
    private RedisServerImo rsImo;
    @Autowired
    private GatewayImo gwImo;
    @Autowired
    private PhysicalMachineClusterImo clusterInPmImo;
    @Autowired
    private FailureDetectorImo fdImo;

    @Autowired
    private FailureDetectorDao failureDetectorDao;
    @Autowired
    private ZkNotificationDao notificationDao;
    @Autowired
    private ZkWorkflowLogDao workflowLogDao;
    
    @Autowired
    private Config config;
    
    @Autowired
    private ConfMaster confMaster;

    private WatchEventHandlerClusterRoot watchClusterRoot = null;
    private WatchEventHandlerPmRoot watchPmRoot = null;

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
            zookeeper.createPersistentZNode(PathUtil.rcRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zookeeper.createPersistentZNode(PathUtil.pmRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zookeeper.createPersistentZNode(PathUtil.clusterRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zookeeper.createPersistentZNode(PathUtil.ccRootPath());
        } catch (NodeExistsException e) {
            // Ignore.
        }
        try {
            zookeeper.createPersistentZNode(PathUtil.rootPathOfLE());
        } catch (NodeExistsException e) {
            // Ignore.
        }

        // Create failure detector znode in order to decide majority used on failover.
        FailureDetectorData fdd = new FailureDetectorData();
        try {
            failureDetectorDao.createFd(fdd);
        } catch (NodeExistsException e) {
            // Ignore.
        }

        String heartbeaterPath = 
            PathUtil.fdRootPath() + "/" + config.getIp() + ":" + config.getPort();
        zookeeper.createEphemeralZNode(heartbeaterPath);

        // Initialize dao.
        notificationDao.initialize();
        workflowLogDao.initialize();        
    }
    
    public void release() {
        clusterImo.relase();
        pmImo.relase();
        pgImo.relase();
        pgsImo.relase();
        rsImo.relase();
        gwImo.relase();
        clusterInPmImo.relase();
    }

    public void loadAll() throws MgmtZooKeeperException, NoNodeException {
        // Load physical machines.
        List<String> pmList = zookeeper.getChildren(PathUtil.pmRootPath());
        for (String pmName : pmList) {
            pmImo.load(pmName);
            
            // Load cluster information in this physical machine.
            List<String> clusterInThisPM = 
                    zookeeper.getChildren(PathUtil.pmClusterRootPath(pmName));
            for (String clusterName : clusterInThisPM) {
                clusterInPmImo.load(clusterName, pmName);
            }
        }

        // Load failure detector information
        fdImo.load();
        
        // Register watcher to the root of cluster.
        if (null == watchClusterRoot) {
            watchClusterRoot = new WatchEventHandlerClusterRoot(context);
            watchClusterRoot.registerBoth(PathUtil.clusterRootPath());
        }

        // Register watcher to the root of physical machine.
        if (null == watchPmRoot) {
            watchPmRoot = new WatchEventHandlerPmRoot(context);
            watchPmRoot.registerBoth(PathUtil.pmRootPath());
        }
        
        // Load clusters
        List<String> children = zookeeper.getChildren(PathUtil.clusterRootPath());
        for (String clusterName : children) {
            clusterImo.load(clusterName);
            Logger.info("Load cluster success. {}", clusterName);
            Logger.flush(INFO);
        }
    }
    
}

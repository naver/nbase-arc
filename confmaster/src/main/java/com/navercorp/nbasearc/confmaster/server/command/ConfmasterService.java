package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.server.mapping.ArityType.LESS;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static org.apache.log4j.Level.INFO;

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

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

@Service
public class ConfmasterService {
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    
    @Autowired
    private CommandExecutor commandTemplate;

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

    private WatchEventHandlerClusterRoot watchClusterRoot = null;
    private WatchEventHandlerPmRoot watchPmRoot = null;

    @CommandMapping(
            name="help",
            arityType=LESS,
            usage="help <command>")
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
            usage="ping")
    public String ping() {
        return "+PONG";
    }

    @LockMapping(name="ping")
    public void pingLock(HierarchicalLockHelper lockHelper) {
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

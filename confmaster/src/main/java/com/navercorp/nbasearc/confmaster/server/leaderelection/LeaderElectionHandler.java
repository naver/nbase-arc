package com.navercorp.nbasearc.confmaster.server.leaderelection;

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.UPDATE_HEARTBEAT_CHECKER;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.ver1_2.ZDSGatewayAffinity;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.command.ConfmasterService;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionSupport.LeaderElectionEventType;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

@Component
public class LeaderElectionHandler implements LeaderElectionAware {
    
    @Autowired
    private Config config;
    
    @Autowired
    private ZooKeeperHolder zk;
    @Autowired
    private ZkWorkflowLogDao workflowLogDao;
    @Autowired
    private ZDSGatewayAffinity znodeStructAffinity;

    @Autowired
    private WorkflowExecutor workflowExecutor;
    
    @Autowired
    private ConfmasterService confmasterService;
    
    @Autowired
    private LeaderElectionSupport electionSupport;
    
    public LeaderElectionHandler() {
    }
    
    @Override
    public void onElectionEvent(LeaderElectionEventType eventType) {
        try {
            if (LeaderElectionEventType.ELECTED_COMPLETE == eventType) {
                becomeLeader();
            } else if (LeaderElectionEventType.READY_START == eventType) {
                becomeFollower();
            }
        } catch (Exception e) {
            Logger.error("failed initializing", e);
        }
    }
    
    public void initialize() {
        final String hostName = makeHostName(config.getIp(), config.getPort());
        Logger.info("start leader election. " + hostName);
        
        electionSupport.addListener(this);
        electionSupport.setZooKeeper(zk.getZooKeeper());
        electionSupport.setHostName(hostName);
        electionSupport.setRootNodeName(PathUtil.rootPathOfLE());
        electionSupport.start();
    }

    public void release() {
        electionSupport.stop();
    }
    
    public void becomeLeader() throws NoNodeException, MgmtZooKeeperException,
            NodeExistsException {
        if (LeaderState.isLeader()) {
            Logger.info("already leader");
            return;
        }
        
        Logger.info("become leader");
        LeaderState.setLeader();
        
        workflowLogDao.initialize();
        
        JobIDGenerator.getInstance().initialize();

        workflowLogDao.log(JobIDGenerator.getInstance().getID(), 
                Constant.SEVERITY_MAJOR, "LeaderElectionWorkflow",
                Constant.LOG_TYPE_WORKFLOW, "", "New leader was elected.", 
                String.format("{\"ip\":\"%s\",\"port\":%d}", config.getIp(), config.getPort()));
        
        confmasterService.loadAll();
        
        initialCheck();
        
        znodeStructAffinity.updateZNodeDirectoryStructure(config.getCharset());
    }

    public void becomeFollower() throws NoNodeException, MgmtZooKeeperException {
        if (LeaderState.isFollower()) {
            Logger.info("already follower");
            return;
        } else if (LeaderState.isLeader()) {
            Logger.info("Leader --> Follower, Abort...");
            System.exit(0);
        }
        
        Logger.info("become follower");
        LeaderState.setFollower();

        confmasterService.loadAll();
    }
    
    private void initialCheck() {
        workflowExecutor.perform(UPDATE_HEARTBEAT_CHECKER);
    }
    
    public String getCurrentLeaderHost() 
            throws KeeperException, InterruptedException {
        String host = electionSupport.getLeaderHostName();
        /*
         * If there is no leader, set ip and port are null; {"ip":null,"port":null},
         * otherwise return leader`s ip and port. {"ip":123.123.123.123,"port":12345}
         */
        return (host != null) ? host : makeHostName(null, null);
    }
    
    public String makeHostName(String ip, Integer port) {
        return String.format("{\"ip\":\"%s\",\"port\":\"%d\"}", ip, port);
    }

}

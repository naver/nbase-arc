package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.FAILOVER_COMMON;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.FAILOVER_PGS;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.SET_QUORUM;

import java.util.List;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.FailureDetector;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.FailureDetectorImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class UpdateHeartbeatCheckerWorkflow {
    
    private final WorkflowExecutor workflowExecutor;
    
    private final ZooKeeperHolder zookeeper;
    
    private final FailureDetectorImo fdImo;
    private final PartitionGroupImo pgImo;
    private final PartitionGroupServerImo pgsImo;
    private final GatewayImo gwImo;
    private final RedisServerImo rsImo; 
    
    private final WorkflowLogDao workflowLogDao; 

    protected UpdateHeartbeatCheckerWorkflow(ApplicationContext context) {
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        
        this.workflowExecutor = context.getBean(WorkflowExecutor.class);

        this.fdImo = context.getBean(FailureDetectorImo.class);
        this.pgImo = context.getBean(PartitionGroupImo.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.gwImo = context.getBean(GatewayImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);

        this.workflowLogDao = context.getBean(WorkflowLogDao.class);
    }
    
    public String execute(ThreadPool executor) throws MgmtZooKeeperException {
        String path = PathUtil.fdRootPath();
        FailureDetector fd = fdImo.get();
        
        try {
            int numberOfHeartbeatChecker = zookeeper.getNumberOfChildren(path);
            fd.setNumberOfHeartbeatCheckers(numberOfHeartbeatChecker);
            
            if (0 == numberOfHeartbeatChecker)
                workflowLog(Constant.SEVERITY_MAJOR, numberOfHeartbeatChecker, workflowLogDao);
            else
                workflowLog(Constant.SEVERITY_MODERATE, numberOfHeartbeatChecker, workflowLogDao);
        } catch (MgmtZooKeeperException e) {
            Logger.error("Update the number of heartbeat checkers fail.", e);
        }
        
        // PG
        List<PartitionGroup> pgList = pgImo.getAll();
        for (PartitionGroup pg : pgList) {
            workflowExecutor.perform(SET_QUORUM, pg.getClusterName(), pg.getName());
        }
        
        // PGS
        List<PartitionGroupServer> pgsList = pgsImo.getAll();
        for (PartitionGroupServer pgs : pgsList) {
            workflowExecutor.perform(FAILOVER_PGS, pgs);
        }
        
        // GW
        List<Gateway> gwList = gwImo.getAll();
        for (Gateway gw : gwList) {
            workflowExecutor.perform(FAILOVER_COMMON, gw);
        }
        
        // RS
        List<RedisServer> rsList = rsImo.getAll();
        for (RedisServer rs : rsList) {
            workflowExecutor.perform(FAILOVER_COMMON, rs);
        }
        
        return null;
    }
    
    private void workflowLog(String severity, int numberOfHeartbeatChecker,
            WorkflowLogDao workflowLogDao) {
        workflowLogDao.log(JobIDGenerator.getInstance().getID(), 
                severity, "UpdateHeartbeatCheckerWorkflow",
                Constant.LOG_TYPE_WORKFLOW, "",
                "change the number of heartbeat checker. no: " + numberOfHeartbeatChecker,
                String.format("{\"number_of_hbc\":\"%d\"}", numberOfHeartbeatChecker));
    }

}

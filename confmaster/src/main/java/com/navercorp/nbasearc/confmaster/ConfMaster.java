package com.navercorp.nbasearc.confmaster;

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HeartbeatChecker;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.ClusterContollerServer;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.command.CommandExecutor;
import com.navercorp.nbasearc.confmaster.server.command.ConfmasterService;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionHandler;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.statistics.Statistics;

@Component
public class ConfMaster {
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    
    @Autowired
    private Config config;
    
    @Autowired
    private ThreadPool jobExecutor;
    @Autowired
    private ConfmasterService confmasterService;

    @Autowired
    private LeaderElectionHandler leaderElection;
    
    @Autowired
    private CommandExecutor commandExecutor;
    @Autowired
    private WorkflowExecutor workflowExecutor;

    @Autowired
    private HeartbeatChecker heartbeatChecker;
    
    @Autowired
    private GracefulTerminator terminator;
    
    private ClusterContollerServer server;
    
    public void initialize() throws InterruptedException,
            MgmtZooKeeperException, KeeperException, Exception {        
        Statistics.initialize(config, jobExecutor);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(terminator);
        
        // Heartbeat layer
        heartbeatChecker.initialize();

        // Database layer
        zookeeper.initialize();
        
        // Mgmt initialize
        confmasterService.initialize();
        
        // Controller layer
        jobExecutor.initialize();
        commandExecutor.initialize();
        workflowExecutor.initialize();
        
        // Service layer
        leaderElection.initialize();
        server = new ClusterContollerServer(context);
        server.initialize();
    }
    
    public void release() throws InterruptedException, ExecutionException,
            IOException {
        terminator.await();
        
        // Service layer
        server.relase();
        leaderElection.release();
        
        // Controller layer
        jobExecutor.release();
        
        // Database layer
        zookeeper.release();
    }

    public void run() {
        while (!terminator.isTerminated()) {
            heartbeatChecker.process();
            Logger.flush(INFO);
        }
    }
    
    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = null;
        ConfMaster cc = null;
        
        try {
            context = new ClassPathXmlApplicationContext(
                    "classpath:applicationContext.xml");
            cc = context.getBean(ConfMaster.class);

            cc.initialize();

            cc.run();

            cc.release();
            context.close();
        } catch (Exception e) {
            Logger.error("Exception propagated to the main.", e);
            Logger.flush(DEBUG);
        }
    }
    
}

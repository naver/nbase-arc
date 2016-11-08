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
import com.navercorp.nbasearc.confmaster.server.ClusterContollerServer;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.command.CommandExecutor;
import com.navercorp.nbasearc.confmaster.server.command.ConfmasterService;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionHandler;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionSupport;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;
import com.navercorp.nbasearc.confmaster.statistics.Statistics;

@Component
public class ConfMaster {
    
    public final static int LOADING = 0;
    public final static int READY = 1;
    public final static int RUNNING = 2;
    private Integer state = LOADING;

    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zk;
    
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
    
    private GracefulTerminator terminator;
    
    private ClusterContollerServer server;
    
    private int numberOfHeartbeatCheckers = 0;

    public void initialize() throws InterruptedException,
            MgmtZooKeeperException, KeeperException, Exception {
        terminator = new GracefulTerminator(
                context.getBean(LeaderElectionSupport.class));
        
        Statistics.initialize(config, jobExecutor);
        
        // Heartbeat layer
        heartbeatChecker.initialize();

        // Database layer
        zk.initialize();
        
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
        zk.release();
    }

    public void run() {
        while (!terminator.isTerminated()) {
            heartbeatChecker.process();
            Logger.flush(INFO);
        }
    }

    public int getMajority() {
        return getNumberOfHeartbeatCheckers() / 2 + 1;
    }

    public int getNumberOfHeartbeatCheckers() {
        return numberOfHeartbeatCheckers;
    }

    public void setNumberOfHeartbeatCheckers(int numberOfHeartbeatChecker) {
        this.numberOfHeartbeatCheckers = numberOfHeartbeatChecker;
    }
    
    public static void main(String[] args) {
        String applicationContextPath = "classpath:applicationContext.xml";
        if (args.length != 0) {
            applicationContextPath = "classpath:" + args[0];
        }
        
        ClassPathXmlApplicationContext context = null;
        ConfMaster cc = null;
        
        try {
            context = new ClassPathXmlApplicationContext(applicationContextPath);
            cc = context.getBean(ConfMaster.class);
            
            cc.initialize();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(cc.terminator);

            cc.run();

            cc.release();
            context.close();
        } catch (Exception e) {
            Logger.error("Exception propagated to the main.", e);
            Logger.flush(DEBUG);
        }
    }

    public void terminate() {
        terminator.terminate();;
        terminator.await();
    }

    public Integer getState() {
        synchronized (this.state) {
            return state;
        }
    }

    public void setState(int state) {
        synchronized (this.state) {
            if (this.state < state) {
                this.state = state;
            }
        }
    }
    
}

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

import java.util.concurrent.CountDownLatch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionSupport;

@Component
public class GracefulTerminator extends Thread {

    @Autowired
    private LeaderElectionSupport electionSupport;

    @Autowired
    private ZooKeeperHolder zookeeper;
    
    private volatile boolean terminated = false;
    
    private final CountDownLatch latch = new CountDownLatch(1);
        
    public boolean isTerminated() {
        return terminated;
    }

    public void terminate() {
        // Yield the leader or an offer for other ClusterConstoller.
        electionSupport.stop();
                
        zookeeper.release();
        
        latch.countDown();
    }

    @Override
    public void run() {
        terminate();
    }
    
    public void await() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Logger.error("Wait for termination fail.", e);
        }
        
        terminated = true;
    }

}

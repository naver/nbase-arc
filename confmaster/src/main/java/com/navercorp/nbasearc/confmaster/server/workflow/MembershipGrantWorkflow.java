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

package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;

public class MembershipGrantWorkflow extends CascadingWorkflow {
    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final ZooKeeperHolder zk;
    
    final MGSetquorum setquorum;

    List<PartitionGroupServer> joinedPgsList;

    public MembershipGrantWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg, context.getBean(ClusterComponentContainer.class));
        
        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.zk = context.getBean(ZooKeeperHolder.class);
        
        this.setquorum = context.getBean(MGSetquorum.class);
    }

    @Override
    protected void _execute() throws MgmtSmrCommandException,
            MgmtZooKeeperException, MgmtSetquorumException {
        joinedPgsList = pg
                .getJoinedPgsList(container.getPgsList(pg.getClusterName(), pg.getName()));
        
        final Map<String, RedisServer> rsMap = new HashMap<String, RedisServer>();
        for (RedisServer rs : container.getRsList(pg.getClusterName())) {
            rsMap.put(rs.getName(), rs);
        }
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);

        for (PartitionGroupServer pgs : joinedPgsList) {
            final String role = pgs.getRole();
            final Color color = pgs.getColor();

            if (role != PGS_ROLE_SLAVE || color != YELLOW) {
                continue;
            }

            RedisServer rs = rsMap.get(pgs.getName());
            try {
                String response = rs.executeQuery("ping");
                if (!response.equals(REDIS_PONG)) {
                    continue;
                }
            } catch (IOException e) {
                continue;
            }

            final int q = pg.getQuorum();
            final int d = pg.getD(joinedPgsList);

            final int curQ = q - d;
            final int newQ = curQ + 1;
            final String quorumMembers = pg.getQuorumMembersString(master,
                    joinedPgsList) + " " + pgs.getName();
            setquorum.setquorum(master, newQ, quorumMembers);

            try {
                String response = rs.executeQuery("ping");
                if (!response.equals(REDIS_PONG)) {
                    continue;
                }
            } catch (IOException e) {
                setquorum.setquorum(master, curQ,
                        pg.getQuorumMembersString(master, joinedPgsList));
                continue;
            }

            Logger.info("{} {}->{} {}->{} {}->{}", new Object[] { pgs,
                    pgs.getRole(), pgs.getRole(),
                    pgs.getColor(), GREEN, curQ, newQ });
            PartitionGroupServer.PartitionGroupServerData pgsM = pgs.clonePersistentData();
            pgsM.color = GREEN;
            zk.setData(pgs.getPath(), pgsM.toBytes(), -1);
            pgs.setPersistentData(pgsM);
        }
    }

    @Override
    protected void onSuccess() throws Exception {
        int numGreen = 0;
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getColor() == GREEN) {
                numGreen++;
            }
        }
        
        if (numGreen != joinedPgsList.size()) {
            Logger.info(
                    "retry to reconfig {}, copy: {}, joined: {}, green: {}",
                    new Object[] { pg, joinedPgsList.size(),
                            pg.getCopy(), numGreen });
            
            final long nextEpoch = pg.nextWfEpoch();
            Logger.info("next {}", nextEpoch);
            wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                    config.getServerJobWorkflowPgReconfigDelay(),
                    TimeUnit.MILLISECONDS, pg, nextEpoch, context);
        } else {
            cleanMGen();
        }
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);
    }

    /*
     * When all PGSes in a PG are green, it is able to
     * clean mgen-history of PG to prevent history from growing
     * too long to cause slow operations.
     */ 
    private void cleanMGen() throws MgmtZooKeeperException {
        try {
        	PartitionGroup.PartitionGroupData pgM = pg.clonePersistentData();
            pgM.cleanMGen(config.getClusterPgMgenHistorySize());
            zk.setData(pg.getPath(), pgM.toBytes(), -1);
            pg.setPersistentData(pgM);
        } catch (Exception e) {
            throw new MgmtZooKeeperException(
                    "Clean mgen history of " + pg + " fail, "
                            + e.getMessage());
        }
    }
}

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
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class MembershipGrantWorkflow extends CascadingWorkflow {
    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final ZooKeeperHolder zookeeper;
    final PartitionGroupServerDao pgsDao;
    final PartitionGroupServerImo pgsImo;
    final RedisServerImo rsImo;
    
    final MGSetquorum setquorum;

    List<PartitionGroupServer> joinedPgsList;

    public MembershipGrantWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg);
        
        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);
        
        this.setquorum = context.getBean(MGSetquorum.class);
    }

    @Override
    protected void _execute() throws MgmtSmrCommandException,
            MgmtZooKeeperException, MgmtSetquorumException {
        joinedPgsList = pg
                .getJoinedPgsList(pgsImo.getList(pg.getClusterName(),
                        Integer.valueOf(pg.getName())));
        
        final Map<String, RedisServer> rsMap = new HashMap<String, RedisServer>();
        for (RedisServer rs : rsImo.getList(pg.getClusterName())) {
            rsMap.put(rs.getName(), rs);
        }
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);

        for (PartitionGroupServer pgs : joinedPgsList) {
            final String role = pgs.getData().getRole();
            final Color color = pgs.getData().getColor();

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

            final int q = pg.getData().getQuorum();
            final int d = pg.getD(joinedPgsList);

            final int curQ = q - d;
            final int newQ = curQ + 1;
            setquorum.setquorum(master, newQ);

            try {
                String response = rs.executeQuery("ping");
                if (!response.equals(REDIS_PONG)) {
                    continue;
                }
            } catch (IOException e) {
                setquorum.setquorum(master, curQ);
                continue;
            }

            Logger.info("{} {}->{} {}->{} {}->{}", new Object[] { pgs,
                    pgs.getData().getRole(), pgs.getData().getRole(),
                    pgs.getData().getColor(), GREEN, curQ, newQ });
            PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                    .from(pgs.getData()).withColor(GREEN).build();
            pgsDao.updatePgs(pgs.getPath(), pgsM);
            pgs.setData(pgsM);
        }
    }

    @Override
    protected void onSuccess() throws Exception {
        int numGreen = 0;
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getData().getColor() == GREEN) {
                numGreen++;
            }
        }
        
        if (numGreen != joinedPgsList.size()) {
            Logger.info(
                    "retry to reconfig {}, copy: {}, joined: {}, green: {}",
                    new Object[] { pg, joinedPgsList.size(),
                            pg.getData().getCopy(), numGreen });
            
            final long nextEpoch = pg.nextWfEpoch();
            Logger.info("next {}", nextEpoch);
            wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                    config.getServerJobWorkflowPgReconfigDelay(),
                    TimeUnit.MILLISECONDS, pg, nextEpoch, context);
        } else {
            /*
             * TODO : When all PGSes in a PG are green, it is able to
             * clean mgen-history of PG to prevent history from growing
             * too long to cause slow operations.
             */ 
        }
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);
    }
}

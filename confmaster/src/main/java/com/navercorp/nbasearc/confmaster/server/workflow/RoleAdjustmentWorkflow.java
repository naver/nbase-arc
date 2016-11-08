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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.JobIDGenerator;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

public class RoleAdjustmentWorkflow extends CascadingWorkflow {
    private final long jobID = JobIDGenerator.getInstance().getID();

    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final ZooKeeperHolder zk;
    final WorkflowLogger workflowLogger;
    
    final RARoleLconn roleLconn;

    public RoleAdjustmentWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg, context.getBean(ClusterComponentContainer.class));

        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.zk = context.getBean(ZooKeeperHolder.class);
        this.workflowLogger = context.getBean(WorkflowLogger.class);
        
        this.roleLconn = context.getBean(RARoleLconn.class);
    }

    @Override
    protected void _execute() throws MgmtSmrCommandException,
            MgmtZooKeeperException, MgmtSetquorumException {
        final List<PartitionGroupServer> joinedPgsList = 
                pg.getJoinedPgsList(
                        container.getPgsList(pg.getClusterName(), pg.getName()));
        
        final int pgQ = pg.getQuorum();
        for (PartitionGroupServer pgs : joinedPgsList) {
            final String role = pgs.getRole();
            final Color color = pgs.getColor();
            final int d = pg.getD(joinedPgsList);

            if (role == PGS_ROLE_NONE && color != RED && pgQ - d > 0) {
                toRed(pgs, joinedPgsList);
            } else if (role != PGS_ROLE_NONE && color == RED) {
                toLconn(pgs);
            } else if (role == PGS_ROLE_LCONN && color == GREEN) {
                toBlue(pgs);
            } else if ((role == PGS_ROLE_MASTER || role == PGS_ROLE_SLAVE)
                    && color == BLUE) {
                toGreen(pgs);
            }
        }
    }

    private void toRed(PartitionGroupServer pgs,
            List<PartitionGroupServer> joinedPgsList)
            throws MgmtZooKeeperException, MgmtSetquorumException, MgmtSmrCommandException {
        PartitionGroupServer master = pg.getMaster(joinedPgsList);
        if (master != null) {
            roleLconn.roleLconn(master, BLUE, jobID);
        }

        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getRole(), pgs.getRole(),
                pgs.getColor(), RED });
        PartitionGroupServer.PartitionGroupServerData pgsM = pgs.clonePersistentData();
        pgsM.color = RED;
        zk.setData(pgs.getPath(), pgsM.toBytes(), -1);
        pgs.setPersistentData(pgsM);
    }

    private void toLconn(PartitionGroupServer pgs) throws MgmtSmrCommandException {
        roleLconn.roleLconn(pgs, YELLOW, jobID);
    }

    private void toBlue(PartitionGroupServer pgs) throws MgmtZooKeeperException {
        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getRole(), pgs.getRole(), 
                pgs.getColor(), BLUE });
        PartitionGroupServer.PartitionGroupServerData pgsM = pgs.clonePersistentData();
        pgsM.color = BLUE;
        zk.setData(pgs.getPath(), pgsM.toBytes(), -1);
        pgs.setPersistentData(pgsM);
    }

    private void toGreen(PartitionGroupServer pgs)
            throws MgmtZooKeeperException {
        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getRole(), pgs.getRole(),
                pgs.getColor(), GREEN });
        PartitionGroupServer.PartitionGroupServerData pgsM = pgs.clonePersistentData();
        pgsM.color = GREEN;
        zk.setData(pgs.getPath(), pgsM.toBytes(), -1);
        pgs.setPersistentData(pgsM);
    }

    @Override
    protected void onSuccess() throws Exception {
        final long nextEpoch = pg.nextWfEpoch();
        Logger.info("next {}", nextEpoch);
        wfExecutor.perform(QUORUM_ADJUSTMENT, pg, nextEpoch, context);
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);        
    }
}

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
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

/*
 * Dead workflow. This workflow does nothing since RA/toRed makes master lconn.
 */
public class QuorumAdjustmentWorkflow extends CascadingWorkflow {
    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    
    final QASetquorum setquorum;

    public QuorumAdjustmentWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg, context.getBean(ClusterComponentContainer.class));

        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        
        this.setquorum = context.getBean(QASetquorum.class);
    }

    @Override
    protected void _execute() throws MgmtSmrCommandException, MgmtSetquorumException  {
        List<PartitionGroupServer> joinedPgsList = pg.getJoinedPgsList(
                container.getPgsList(pg.getClusterName(), pg.getName()));
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);
        final int d = pg.getD(joinedPgsList);

        if (master == null) {
            return;
        }
        
        if (master.smrVersion().equals(SMR_VERSION_201)) {
            _201(pg, joinedPgsList, master, d);
        } else {
            _101(joinedPgsList, master, d);
        }
    }
    
    private void _201(PartitionGroup pg,
            List<PartitionGroupServer> joinedPgsList,
            PartitionGroupServer master, int d) throws MgmtSmrCommandException,
            MgmtSetquorumException {
        final List<String> activeQuorumMembers = master.getQuorumV();
        final int activeQ = Integer.valueOf(activeQuorumMembers.get(0));
        activeQuorumMembers.remove(0);

        final List<String> tobeQuorumMembers = pg.getQuorumMembers(master,
                joinedPgsList);
        final int tobeQ = pg.getQuorum() - d;

        boolean e = true;
        if (activeQ != tobeQ) {
            e = false;
        } else {
            for (String m : tobeQuorumMembers) {
                if (!activeQuorumMembers.contains(m)) {
                    e = false;
                    break;
                }
            }
        }

        if (!e) {
            setquorum.setquorum(master, tobeQ,
                    pg.getQuorumMembersString(master, joinedPgsList));
        }
    }

    private void _101(List<PartitionGroupServer> joinedPgsList,
            PartitionGroupServer master, int d) throws MgmtSmrCommandException,
            MgmtSetquorumException {
        final int activeQ = master.getQuorum();

        final int tobeQ = pg.getQuorum() - d;
        if (tobeQ == activeQ) {
            return;
        }

        setquorum.setquorum(master, tobeQ,
                pg.getQuorumMembersString(master, joinedPgsList));
    }

    @Override
    protected void onSuccess() throws Exception {
        final long nextEpoch = pg.nextWfEpoch();
        Logger.info("next {}", nextEpoch);
        wfExecutor.perform(MASTER_ELECTION, pg, null, nextEpoch, context);
    }

    @Override
    protected void onException(long nextEpoch, Exception e) {
        wfExecutor.performDelayed(ROLE_ADJUSTMENT,
                config.getServerJobWorkflowPgReconfigDelay(),
                TimeUnit.MILLISECONDS, pg, nextEpoch, context);
    }
}

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

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class QuorumAdjustmentWorkflow extends CascadingWorkflow {
    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final PartitionGroupServerImo pgsImo;
    
    final QASetquorum setquorum;

    public QuorumAdjustmentWorkflow(PartitionGroup pg, boolean cascading,
            ApplicationContext context) {
        super(cascading, pg);

        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        
        this.setquorum = context.getBean(QASetquorum.class);
    }

    @Override
    protected void _execute() throws MgmtSmrCommandException, MgmtSetquorumException  {
        List<PartitionGroupServer> joinedPgsList = pg.getJoinedPgsList(pgsImo
                .getList(pg.getClusterName(), Integer.valueOf(pg.getName())));
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);
        final int d = pg.getD(joinedPgsList);

        if (master == null) {
            return;
        }
        
        String q; 
        try {
            q = master.executeQuery("getquorum");
        } catch (IOException e) {
            throw new MgmtSmrCommandException(e.getMessage());
        }
        
        final int activeQ = Integer.valueOf(q);

        final int tobeQ = pg.getData().getQuorum() - d;
        if (tobeQ == activeQ) {
            return;
        }

        setquorum.setquorum(master, tobeQ);
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

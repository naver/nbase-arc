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

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;

public class DecreaseQuorumWorkflow {
    final ApplicationContext context;
    final WorkflowExecutor wfExecutor;
    final ClusterComponentContainer container;

    final PartitionGroup pg;
    final ZooKeeperHolder zk;

    public DecreaseQuorumWorkflow(PartitionGroup pg, ApplicationContext context) {
        this.context = context;
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.container = context.getBean(ClusterComponentContainer.class);
        this.zk = context.getBean(ZooKeeperHolder.class);

        this.pg = pg;
    }

    public String execute() throws MgmtZooKeeperException {
        final int d = pg.getD(container.getPgsList(pg.getClusterName(), pg.getName()));

        if (pg.getQuorum() - d <= 0) {
            return EXCEPTIONMSG_INVALID_QUORUM;
        }

        PartitionGroup.PartitionGroupData pgM = pg.clonePersistentData();
        pgM.quorum = pg.getQuorum() - 1;
        
        zk.setData(pg.getPath(), pgM.toBytes(), -1);
        pg.setPersistentData(pgM);
        
        return S2C_OK;
    }
}

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

import java.io.IOException;
import java.util.List;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

public class IncreaseQuorumWorkflow {
    final ApplicationContext context;
    final ClusterComponentContainer container;

    final PartitionGroup pg;
    final ZooKeeperHolder zk;

    public IncreaseQuorumWorkflow(PartitionGroup pg, ApplicationContext context) {
        this.context = context;
        this.container = context.getBean(ClusterComponentContainer.class);
        this.zk = context.getBean(ZooKeeperHolder.class);

        this.pg = pg;
    }

    public String execute() throws MgmtZooKeeperException, IOException,
            MgmtSetquorumException, MgmtSmrCommandException {
        final List<PartitionGroupServer> joinedPgsList = pg
                .getJoinedPgsList(container.getPgsList(pg.getClusterName(), pg.getName()));
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);

        int numGreen = 0;
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getColor() == GREEN) {
                numGreen++;
            }
        }

        if (master == null) {
            return "-ERR no master.";
        }
        if (numGreen <= pg.getQuorum() + 1) {
            return "-ERR not enough available pgs.";
        }

        master.setQuorum(pg.getQuorum() + 1,
                pg.getQuorumMembersString(master, joinedPgsList));

        PartitionGroup.PartitionGroupData pgM = pg.clonePersistentData();
        pgM.quorum = pg.getQuorum() + 1;
        
        zk.setData(pg.getPath(), pgM.toBytes(), -1);
        pg.setPersistentData(pgM);
        
        return S2C_OK;
    }
}

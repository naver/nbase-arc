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
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;

public class IncreaseQuorumWorkflow {
    final ApplicationContext context;
    final PartitionGroupDao pgDao;
    final PartitionGroupServerImo pgsImo;

    final PartitionGroup pg;

    public IncreaseQuorumWorkflow(PartitionGroup pg, ApplicationContext context) {
        this.context = context;
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);

        this.pg = pg;
    }

    public String execute() throws MgmtZooKeeperException, IOException,
            MgmtSetquorumException {
        final List<PartitionGroupServer> joinedPgsList = pg
                .getJoinedPgsList(pgsImo.getList(pg.getClusterName(),
                        Integer.valueOf(pg.getName())));
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);

        int numGreen = 0;
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getData().getColor() == GREEN) {
                numGreen++;
            }
        }

        if (master == null) {
            return "-ERR no master.";
        }
        if (numGreen <= pg.getData().getQuorum() + 1) {
            return "-ERR not enough available pgs.";
        }

        master.setquorum((pg.getData().getQuorum() + 1));

        PartitionGroupData pgM = PartitionGroupData.builder()
                .from(pg.getData()).withQuorum(pg.getData().getQuorum() + 1)
                .build();
        pgDao.updatePg(pg.getPath(), pgM);

        pg.setData(pgM);
        
        return S2C_OK;
    }
}

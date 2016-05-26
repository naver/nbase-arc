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

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class IncreaseCopyWorkflow {
    final ApplicationContext context;
    final ZooKeeperHolder zookeeper;
    final PartitionGroupServerDao pgsDao;
    final PartitionGroupDao pgDao;
    final RedisServerImo rsImo;

    final PartitionGroupServer pgs;
    final PartitionGroup pg;
    final RedisServer rs;

    public IncreaseCopyWorkflow(PartitionGroupServer pgs, PartitionGroup pg,
            ApplicationContext context) {
        this.context = context;
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.rsImo = context.getBean(RedisServerImo.class);

        this.pgs = pgs;
        this.pg = pg;
        this.rs = rsImo.get(pgs.getName(), pgs.getClusterName());
    }

    public void execute() throws MgmtZooKeeperException {
        PartitionGroupServerData pgsM = PartitionGroupServerData.builder()
                .from(pgs.getData()).withColor(RED).withRole(PGS_ROLE_NONE)
                .withHb(HB_MONITOR_YES).build();

        RedisServerData rsM = RedisServerData.builder().from(rs.getData())
                .withHb(HB_MONITOR_YES).build();

        PartitionGroupData pgM = PartitionGroupData.builder()
                .from(pg.getData()).withCopy(pg.getData().getCopy() + 1)
                .withQuorum(pg.getData().getQuorum() + 1).build();

        List<Op> opList = new ArrayList<Op>();
        opList.add(pgsDao.createUpdatePgsOperation(pgs.getPath(), pgsM));
        opList.add(pgsDao.createUpdateRsOperation(rs.getPath(), rsM));
        opList.add(pgDao.createUpdatePgOperation(pg.getPath(), pgM));

        List<OpResult> results = null;
        try {
            results = zookeeper.multi(opList);
        } finally {
            zookeeper.handleResultsOfMulti(results);
        }

        OpResult.SetDataResult rsd = (OpResult.SetDataResult) results.get(0);
        pgs.setData(pgsM);
        pgs.setStat(rsd.getStat());

        rsd = (OpResult.SetDataResult) results.get(1);
        rs.setData(rsM);
        rs.setStat(rsd.getStat());

        pg.setData(pgM);
    }
}

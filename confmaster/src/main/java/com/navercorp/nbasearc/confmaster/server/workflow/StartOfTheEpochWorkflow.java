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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;

public class StartOfTheEpochWorkflow {
    final ApplicationContext context;
    final ZooKeeperHolder zk;
    final ClusterComponentContainer container;

    final PartitionGroup pg;
    final PartitionGroupServer pgs;
    RedisServer rs;

    PartitionGroupServer.PartitionGroupServerData pgsM;
    RedisServer.RedisServerData rsM;
    PartitionGroup.PartitionGroupData pgM;
    List<OpResult> results;

    public StartOfTheEpochWorkflow(PartitionGroup pg, PartitionGroupServer pgs,
            ApplicationContext context) {
        this.context = context;
        this.zk = context.getBean(ZooKeeperHolder.class);
        this.container = context.getBean(ClusterComponentContainer.class);

        this.pg = pg;
        this.pgs = pgs;
    }

    public void execute() throws InterruptedException, KeeperException {
        final int mgen = pg.currentGen();
        final int copy = pg.getCopy();

        if (mgen != -1 || copy != 0) {
            return;
        }

        pgsM = pgs.clonePersistentData();
        pgsM.hb = HB_MONITOR_YES;
        pgsM.color = BLUE;

        rs = container.getRs(pgs.getClusterName(), pgs.getName());
        rsM = rs.clonePersistentData();
    	rsM.hb = HB_MONITOR_YES;

        pgM = pg.clonePersistentData();
        pgM.copy = 1;
        pgM.quorum = 0;

        List<Op> opList = new ArrayList<Op>();
        opList.add(Op.setData(pgs.getPath(), pgsM.toBytes(), -1));
        opList.add(Op.setData(rs.getPath(), rsM.toBytes(), -1));
        opList.add(Op.setData(pg.getPath(), pgM.toBytes(), -1));

        results = zk.getZooKeeper().multi(opList);
        pgs.setZNodeVersion(((OpResult.SetDataResult) results.get(0)).getStat().getVersion());
        pgs.setPersistentData(pgsM);
        rs.setPersistentData(rsM);
        pg.setPersistentData(pgM);
    }
}

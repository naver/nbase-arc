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

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtNoAvaliablePgsException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtInvalidQuorumPolicyException;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;

public class DecreaseCopyWorkflow {
    final ApplicationContext context;
    final ZooKeeperHolder zk;
    final ClusterComponentContainer container;

    final PartitionGroupServer pgs;
    final PartitionGroup pg;
    final String mode;

    public DecreaseCopyWorkflow(PartitionGroupServer pgs, PartitionGroup pg,
            String mode, ApplicationContext context) {
        this.context = context;
        this.zk = context.getBean(ZooKeeperHolder.class);
        this.container = context.getBean(ClusterComponentContainer.class);

        this.pgs = pgs;
        this.pg = pg;
        this.mode = mode;
    }

    public void execute() throws MgmtZooKeeperException,
            MgmtNoAvaliablePgsException, MgmtInvalidQuorumPolicyException {
        final RedisServer rs = container.getRs(pgs.getClusterName(), pgs.getName());
        final int d = pg.getD(container.getPgsList(pgs.getClusterName(), String.valueOf(pgs.getPgId())));
        final Color c = pgs.getColor();
        
        if (pg.getCopy() - 1 < 0) {
            throw new MgmtNoAvaliablePgsException();
        }
        
        if (pg.getQuorum() - 1 < 0) {
            if (pg.getCopy() == 1) {
                // Ignore Quorum validation check in this case, because there will be no pgs and data.
            } else {
                // [Event]   [Copy] [Quorum]
                //            2      1
                //  pg_dq     2      0
                //  pgs_leave 1      0 <-- At this point, it is possible to loss latest data.
                throw new MgmtInvalidQuorumPolicyException(
                        "quorum must be larger than or equal to 0. quorum: " + (pg.getQuorum() - 1));
            }
        }
        
        if (!(mode != null && mode.equals(FORCED))) {
            if ((pg.getQuorum() - d <= 0)
                    && (c != YELLOW && c != RED)) {
                throw new MgmtNoAvaliablePgsException();
            }
        }

        PartitionGroupServer.PartitionGroupServerData pgsM = 
        		(PartitionGroupServer.PartitionGroupServerData) pgs.clonePersistentData();
        pgsM.hb = HB_MONITOR_NO;
        
        RedisServer.RedisServerData rsM = rs.clonePersistentData();
        rsM.hb = HB_MONITOR_NO;
        
        PartitionGroup.PartitionGroupData pgM = pg.clonePersistentData();
        pgM.copy = pg.getCopy() - 1; 
        pgM.quorum = pg.getQuorum() - 1; 

        List<Op> opList = new ArrayList<Op>();
        opList.add(Op.setData(pgs.getPath(), pgsM.toBytes(), -1));
        opList.add(Op.setData(rs.getPath(), rsM.toBytes(), -1));
        opList.add(Op.setData(pg.getPath(), pgM.toBytes(), -1));

        List<OpResult> results = null;
        try {
            results = zk.multi(opList);
        } finally {
            zk.handleResultsOfMulti(results);
        }

        OpResult.SetDataResult rsd = (OpResult.SetDataResult) results.get(0);
        pgs.setPersistentData(pgsM);
        pgs.setZNodeVersion(rsd.getStat().getVersion());

        rsd = (OpResult.SetDataResult) results.get(1);
        rs.setPersistentData(rsM);
        rs.setZNodeVersion(rsd.getStat().getVersion());

        pg.setPersistentData(pgM);
        
        container.getCluster(pgs.getClusterName()).performUpdateGwAff();
    }
}

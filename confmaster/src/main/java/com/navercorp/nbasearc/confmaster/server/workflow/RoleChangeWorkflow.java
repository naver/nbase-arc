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

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

public class RoleChangeWorkflow {
    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final ZooKeeperHolder zk;
    final ClusterComponentContainer container;

    final PartitionGroup pg;
    // first come first served
    final List<PartitionGroupServer> masterHints;
    
    // Result
    String resultString = null;

    public RoleChangeWorkflow(PartitionGroup pg, List<PartitionGroupServer> masterHints, ApplicationContext context) {
        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.zk = context.getBean(ZooKeeperHolder.class);
        this.container = context.getBean(ClusterComponentContainer.class);

        this.pg = pg;
        this.masterHints = masterHints;
    }

    public void execute() {
        try {
            _execute();
        } catch (Exception e) {
            resultString = "-ERR role change fail. masterHint: " + masterHints
                    + ", exception: " + e.getMessage();
            Logger.error(resultString, masterHints);
        }
    }
    
    private void _execute() throws Exception {
        final List<PartitionGroupServer> joinedPgsList = pg
                .getJoinedPgsList(container.getPgsList(pg.getClusterName(), pg.getName()));
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);
        
        final String cmd = "role lconn";
        Logger.info("RC pgs: {}, cmd: \"{}\"", master, cmd);
        master.executeQuery(cmd);

        PartitionGroupServer.PartitionGroupServerData masterM = master.clonePersistentData();
        masterM.setRole(PGS_ROLE_LCONN);
        zk.setData(master.getPath(), masterM.toBytes(), -1);
        master.setPersistentData(masterM);
        
        try {
            RoleAdjustmentWorkflow ra = new RoleAdjustmentWorkflow(pg, false,
                    context);
            ra.execute();

            MasterElectionWorkflow me = new MasterElectionWorkflow(pg,
                    masterHints, false, context);
            me.execute();

            YellowJoinWorkflow yj = new YellowJoinWorkflow(pg, false, context);
            yj.execute();

            BlueJoinWorkflow bj = new BlueJoinWorkflow(pg, false, context);
            bj.execute();
        } catch (Exception e) {
            wfExecutor.perform(ROLE_ADJUSTMENT, pg, pg.nextWfEpoch(), context);
            throw e;
        }

        List<String> resSlaveJoinFail = new ArrayList<String>();
        PartitionGroupServer newMaster = null;
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs.getRole().equals(PGS_ROLE_MASTER)) {
                newMaster = pgs;
                continue;
            }
            
            if (!pgs.getRole().equals(PGS_ROLE_SLAVE)) {
                resSlaveJoinFail.add(pgs.getName());
            }
        }

        resultString = "{\"master\":"
                + newMaster.getName()
                + ",\"role_slave_error\":["
                + StringUtils.arrayToDelimitedString(
                        resSlaveJoinFail.toArray(), ", ") + "]}";
    }
    
    public String getResultString() {
        return resultString;
    }

}

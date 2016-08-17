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
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;

public class RoleChangeWorkflow {
    final ApplicationContext context;
    final Config config;
    final WorkflowExecutor wfExecutor;
    final PartitionGroupServerDao pgsDao;
    final PartitionGroupServerImo pgsImo;
    final RedisServerImo rsImo;

    final PartitionGroup pg;
    // first come first served
    final List<PartitionGroupServer> masterHints;
    
    // Result
    String resultString = null;

    public RoleChangeWorkflow(PartitionGroup pg, List<PartitionGroupServer> masterHints, ApplicationContext context) {
        this.context = context;
        this.config = context.getBean(Config.class);
        this.wfExecutor = context.getBean(WorkflowExecutor.class);
        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);

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
                .getJoinedPgsList(pgsImo.getList(pg.getClusterName(),
                        Integer.valueOf(pg.getName())));
        final PartitionGroupServer master = pg.getMaster(joinedPgsList);
        
        final String cmd = "role lconn";
        Logger.info("RC pgs: {}, cmd: \"{}\"", master, cmd);
        master.executeQuery(cmd);

        PartitionGroupServerData masterM = PartitionGroupServerData.builder()
                .from(master.getData()).withRole(PGS_ROLE_LCONN).build();
        pgsDao.updatePgs(master.getPath(), masterM);
        master.setData(masterM);
        
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
            if (pgs.getData().getRole().equals(PGS_ROLE_MASTER)) {
                newMaster = pgs;
                continue;
            }
            
            if (!pgs.getData().getRole().equals(PGS_ROLE_SLAVE)) {
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

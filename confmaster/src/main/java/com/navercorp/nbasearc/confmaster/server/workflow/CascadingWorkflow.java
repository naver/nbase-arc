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

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;

public abstract class CascadingWorkflow {
    final boolean cascading;
    final PartitionGroup pg;
    final ClusterComponentContainer container;
    
    CascadingWorkflow(boolean cascading, PartitionGroup pg,
            ClusterComponentContainer container) {
        this.cascading = cascading;
        this.pg = pg;
        this.container = container;
    }
    
    public void execute() throws Exception {
        if (container.getPg(pg.getClusterName(), pg.getName()) == null) {
            Logger.info("pg does not exist. exit workflow. {}", pg);
            return;
        }

        Logger.info("begin {}", pg);
        
        try {
            _execute();
            if (cascading) {
                onSuccess();
            }
        } catch (Exception e) {
            if (cascading) {
                final long nextEpoch = pg.nextWfEpoch(); 
                Logger.error("rerun {}", nextEpoch);
                onException(nextEpoch, e);
            }
            throw e;
        }
    }
    
    abstract void _execute() throws MgmtSmrCommandException,
            MgmtZooKeeperException, MgmtSetquorumException;
    abstract void onSuccess() throws Exception;
    abstract void onException(long nextEpoch, Exception e);
}

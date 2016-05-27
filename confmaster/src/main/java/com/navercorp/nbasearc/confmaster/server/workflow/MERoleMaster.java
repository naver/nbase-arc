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

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer.RoleMasterZkResult;

@Component("MERoleMaster")
public class MERoleMaster {

    // It is not allowed to decalre any member variable in this class.
    // Since it is a singleton instance and represents a part of workflow logic running in multiple threads.

    @Autowired
    WorkflowLogDao workflowLogDao;

    public void roleMaster(PartitionGroupServer newMaster, PartitionGroup pg,
            LogSequence newMasterLog, List<PartitionGroupServer> joinedPgsList,
            int newQ, long jobID) throws MgmtSmrCommandException {
        final String smrVersion = newMaster.smrVersion();
        
        newMaster.roleMaster(pg, newMasterLog, newQ, jobID, workflowLogDao);

        RoleMasterZkResult result = newMaster.roleMasterZk(pg, joinedPgsList,
                newMasterLog, newQ, smrVersion, jobID, workflowLogDao);
        newMaster.setData(result.pgsM);
        pg.setData(result.pgM);
    }

}

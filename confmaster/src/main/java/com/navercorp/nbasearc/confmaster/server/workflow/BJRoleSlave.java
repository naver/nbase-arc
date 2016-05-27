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

import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.Color.BLUE;
import static com.navercorp.nbasearc.confmaster.Constant.Color.GREEN;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

@Component("BJRoleSlave")
public class BJRoleSlave {

    // It is not allowed to decalre any member variable in this class.
    // Since it is a singleton instance and represents a part of workflow logic running in multiple threads.

    @Autowired
    protected WorkflowLogDao workflowLogDao;

    public void roleSlave(PartitionGroupServer pgs, PartitionGroup pg,
            LogSequence logSeq, PartitionGroupServer master, long jobID)
            throws MgmtSmrCommandException {
        final String masterVersion = master.smrVersion();
        pgs.roleSlave(pg, logSeq, master, BLUE, jobID, workflowLogDao);

        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), PGS_ROLE_SLAVE,
                pgs.getData().getColor(), GREEN });

        pgs.setData(pgs.roleSlaveZk(jobID, pg.getData().currentGen(), GREEN,
                masterVersion, workflowLogDao).pgsM);
    }

}

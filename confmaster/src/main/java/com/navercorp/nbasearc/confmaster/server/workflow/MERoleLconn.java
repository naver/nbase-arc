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

import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_LCONN;
import static com.navercorp.nbasearc.confmaster.Constant.Color.BLUE;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

@Component("MERoleLconn")
public class MERoleLconn {

    // It is not allowed to decalre any member variable in this class.
    // Since it is a singleton instance and represents a part of workflow logic running in multiple threads.

    @Autowired
    WorkflowLogDao workflowLogDao;

    public void roleLconn(PartitionGroupServer pgs, long jobID)
            throws MgmtSmrCommandException {
        pgs.roleLconn();
        Logger.info("{} {}->{} {}->{}", new Object[] { pgs,
                pgs.getData().getRole(), PGS_ROLE_LCONN,
                pgs.getData().getColor(), BLUE });
        pgs.setData(pgs.roleLconnZk(jobID, BLUE, workflowLogDao).pgsM);
    }

}

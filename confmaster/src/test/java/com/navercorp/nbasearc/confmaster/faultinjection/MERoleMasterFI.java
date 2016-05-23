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

package com.navercorp.nbasearc.confmaster.faultinjection;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.workflow.MERoleMaster;

public class MERoleMasterFI extends MERoleMaster {

    private int count = 0;
    private boolean successFail = false;

    @Autowired
    WorkflowLogDao workflowLogDao;

    @Override
    public synchronized void roleMaster(PartitionGroupServer newMaster,
            PartitionGroup pg, LogSequence newMasterLog,
            List<PartitionGroupServer> joinedPgsList, int newQ, long jobID)
            throws MgmtSmrCommandException {
        if (count > 0) {
            if (successFail) {
                newMaster.roleMaster(pg, newMasterLog, newQ, jobID,
                        workflowLogDao);
                count--;
                throw new MgmtSmrCommandException(
                        "[FI] ME role master success fail. " + newMaster);
            } else {
                count--;
                throw new MgmtSmrCommandException(
                        "[FI] ME role master fail fail. " + newMaster);
            }
        } else {
            super.roleMaster(newMaster, pg, newMasterLog, joinedPgsList, newQ,
                    jobID);
        }
    }

    public synchronized int getCount() {
        return count;
    }

    public synchronized void setCount(int count) {
        this.count = count;
    }

    public synchronized boolean isSuccessFail() {
        return successFail;
    }

    public synchronized void setSuccessFail(boolean successFail) {
        this.successFail = successFail;
    }

}

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

import static com.navercorp.nbasearc.confmaster.Constant.Color.YELLOW;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.server.cluster.LogSequence;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.workflow.YJRoleSlave;

public class YJRoleSlaveFI extends YJRoleSlave {

    private int count = 0;
    private boolean successFail = false;

    @Override
    public synchronized void roleSlave(PartitionGroupServer pgs, PartitionGroup pg,
            LogSequence logSeq, PartitionGroupServer master, long jobID) throws MgmtSmrCommandException
             {
        if (count > 0) {
            if (successFail) {
                pgs.roleSlave(pg, logSeq, master, YELLOW, jobID, workflowLogDao);
                count--;
                throw new MgmtSmrCommandException(
                        "[FI] YJ role slave success fail. " + master);
            } else {
                count--;
                throw new MgmtSmrCommandException(
                        "[FI] YJ role slave lconn fail fail. " + master);
            }
        } else {
            super.roleSlave(pgs, pg, logSeq, master, jobID);
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

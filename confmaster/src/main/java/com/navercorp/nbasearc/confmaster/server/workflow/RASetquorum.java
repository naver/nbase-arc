package com.navercorp.nbasearc.confmaster.server.workflow;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

@Component("RASetquorum")
public class RASetquorum {

    // It is not allowed to decalre any member variable in this class.
    // Since it is a singleton instance and represents a part of workflow logic running in multiple threads.

    public void setquorum(PartitionGroupServer master, int q)
            throws IOException, MgmtSetquorumException {
        master.setquorum(q);
    }

}

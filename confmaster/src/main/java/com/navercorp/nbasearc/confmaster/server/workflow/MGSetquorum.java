package com.navercorp.nbasearc.confmaster.server.workflow;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

@Component("MGSetquorum")
public class MGSetquorum {

    public void setquorum(PartitionGroupServer master, int q)
            throws IOException, MgmtSetquorumException {
        master.setquorum(q);
    }

}

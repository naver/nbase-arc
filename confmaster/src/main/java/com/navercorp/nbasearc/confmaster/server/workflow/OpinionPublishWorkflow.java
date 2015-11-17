package com.navercorp.nbasearc.confmaster.server.workflow;

import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.heartbeat.HBResultProcessor;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;

public class OpinionPublishWorkflow {
    
    private final HBResult result;
    private final HeartbeatTarget target;
    private final long hbcRefDataVersion;
    private final HBResultProcessor hbcProc;

    protected OpinionPublishWorkflow(HBResult result, ApplicationContext context) {
        this.result = result;
        this.target = result.getTarget();
        this.hbcRefDataVersion = target.getRefData().increaseAndGetHbcRefDataVersion();
        this.hbcProc = context.getBean(HBResultProcessor.class);
    }
    
    public String execute(ThreadPool executor) throws NodeExistsException,
            MgmtZooKeeperException {
        if (target.getHB().equals(Constant.HB_MONITOR_NO)) {
            // Ignore it.
            return null;
        }
        
        if (target.getRefData().getHbcRefDataVersion() > this.hbcRefDataVersion) {
            return null;
        }
        
        hbcProc.proc(result, true);
        return null;
    }

}

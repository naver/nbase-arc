package com.navercorp.nbasearc.confmaster.server.workflow;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBRefData;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;

public class OpinionDiscardWorkflow {
    
    private final HeartbeatTarget target;
    private final Config config;
    private final ZooKeeperHolder zookeeper;
    
    protected OpinionDiscardWorkflow(HeartbeatTarget target, ApplicationContext context) {
        this.target = target;
        this.config = context.getBean(Config.class);
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
    }
    
    public String execute(ThreadPool executor) throws MgmtZooKeeperException {
        if (getTarget().getHB().equals(Constant.HB_MONITOR_YES)) {
            Logger.debug(getTarget().getNodeType().toString()
                    + getTarget().getName() + " is a target of heartbeat.");
            return null;
        }

        HBRefData refData = getTarget().getRefData();
        if (!refData.isSubmitMyOpinion()) {
            // Ignore it
            return null;
        }
        
        String path = getTarget().getPath() + "/" + config.getIp() + ":" + config.getPort();
        
        try {
            zookeeper.deleteZNode(path, -1);
        } catch (MgmtZooKeeperException e) {
            Logger.error("Remove opinion fail. path: {}", path, e);
            throw e;
        }

        refData.setSubmitMyOpinion(false);
        
        return null;
    }
    
    public HeartbeatTarget getTarget() {
        return target;
    }

}

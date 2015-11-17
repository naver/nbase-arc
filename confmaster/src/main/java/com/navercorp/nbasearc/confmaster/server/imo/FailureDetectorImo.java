package com.navercorp.nbasearc.confmaster.server.imo;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.dao.FailureDetectorDao;
import com.navercorp.nbasearc.confmaster.server.cluster.FailureDetector;

@Component
public class FailureDetectorImo {

    @Autowired
    private FailureDetectorDao failureDetectorDao;
    
    private FailureDetector fd;
    
    public FailureDetector load() throws NoNodeException,
            MgmtZooKeeperException {
        this.fd = failureDetectorDao.loadFd();
        return fd;
    }
    
    public FailureDetector get() {
        return fd;
    }

}

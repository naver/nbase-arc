package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.FailureDetectorDao;
import com.navercorp.nbasearc.confmaster.repository.znode.FailureDetectorData;
import com.navercorp.nbasearc.confmaster.server.cluster.FailureDetector;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerFd;

@Repository
public class ZkFailureDetectorDao implements FailureDetectorDao {
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    
    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    @Override
    public String createFd(final FailureDetectorData data)
            throws NodeExistsException, MgmtZooKeeperException {
        byte rawData[];
        final String path = PathUtil.fdRootPath();

        rawData = mapper.writeValueAsBytes(data);

        return zookeeper.createPersistentZNode(path, rawData);
    }
    
    @Override
    public FailureDetector loadFd() throws MgmtZooKeeperException, NoNodeException  {
        String path = PathUtil.fdRootPath();
        
        Stat stat = new Stat();
        WatchEventHandlerFd watch = new WatchEventHandlerFd(context);
        watch.registerChildEvent(path);
        byte[] data = zookeeper.getData(path, stat, watch);
        FailureDetector fd = new FailureDetector(context, path, PathUtil.FD, data);
        fd.setStat(stat);
        fd.setWatch(watch);
        
        return fd;
    }
}

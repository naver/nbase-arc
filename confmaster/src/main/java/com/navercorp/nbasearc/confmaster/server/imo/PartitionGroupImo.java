package com.navercorp.nbasearc.confmaster.server.imo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.heartbeat.HeartbeatChecker;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandler;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerPg;

@Component
public class PartitionGroupImo {

    protected Map<String, PartitionGroup> container = 
            new ConcurrentHashMap<String, PartitionGroup>();
    
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected ApplicationContext context;
    
    @Autowired
    protected HeartbeatChecker heartbeatChecker;
    
    @Autowired
    private PartitionGroupDao pgDao;
    
    public void relase() {
        container.clear();
    }
    
    public PartitionGroup load(String name, String parent)
            throws NoNodeException, MgmtZooKeeperException {
        final String path = PathUtil.pgPath(name, parent);
        PartitionGroup pg = build(path, name, parent);
        container.put(path, pg);
        return pg;
    }
    
    public PartitionGroup build(String path, String name, String clusterName)
            throws NoNodeException, MgmtZooKeeperException {
        Stat stat = new Stat();
        WatchEventHandler watch = new WatchEventHandlerPg(context);
        byte[] data = pgDao.loadPg(name, clusterName, stat, null);
        
        PartitionGroup pg = new PartitionGroup(context, path, name, clusterName, data);
        pg.setStat(stat);
        pg.setWatch(watch);
        pg.getWatch().registerBoth(path);
        
        return pg;
    }

    public List<PartitionGroup> getList(final String clusterName) {
        List<PartitionGroup> list = new ArrayList<PartitionGroup>();
        for (PartitionGroup elem : container.values()) {
            if (elem.getClusterName().equals(clusterName)) {
                list.add(elem);
            }
        }
        Collections.sort(list);
        return list;
    }

    public PartitionGroup get(String pg, String cluster) {
        return container.get(PathUtil.pgPath(pg, cluster));
    }
    
    public PartitionGroup getByPath(String path) {
        return container.get(path);
    }
    
    public List<PartitionGroup> getAll() {
        List<PartitionGroup> list = new ArrayList<PartitionGroup>();
        for (PartitionGroup elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }

    public void delete(final String path) {
        container.remove(path);
    }

}

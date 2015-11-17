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
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.heartbeat.HeartbeatChecker;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.GatewayDao;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerGw;

@Component
public class GatewayImo {

    /*
     * Gateway list is only one, but there are many clusters.
     * Two cluster add gateway concurrently, two threads modify this map.
     * If the map variable references HashMap, it will throw ConcurrentModificationException.
     * So that, the map variable must be ConcurrentHashMap.
     * And, it also applied to other imo`s containers. 
     */
    protected Map<String, Gateway> container = 
            new ConcurrentHashMap<String, Gateway>();
    
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected ApplicationContext context;
    
    @Autowired
    protected HeartbeatChecker heartbeatChecker;
    
    @Autowired
    private GatewayDao gwDao;
    
    public void relase() {
        container.clear();
    }

    public Gateway load(String name, String clusterName)
            throws NoNodeException, MgmtZooKeeperException {
        final String path = PathUtil.gwPath(name, clusterName);
        Gateway gw = getByPath(path);
        if (gw == null) {
            gw = build(path, name, clusterName);
            container.put(path, gw);
        } else {
            Stat stat = new Stat();
            byte[] data = gwDao.loadGw(name, clusterName, stat, gw.getWatch());
            gw.setData(data);
        }
        return gw;
    }
    
    public Gateway build(String path, String name, String clusterName)
            throws NoNodeException, MgmtZooKeeperException {
        Stat stat = new Stat();
        WatchEventHandlerGw watch = new WatchEventHandlerGw(context);
        byte[] data = gwDao.loadGw(name, clusterName, stat, watch);
        
        Gateway gw = new Gateway(context, path, name, clusterName, data);
        gw.setWatch(watch);
        gw.getWatch().registerBoth(path);

        gw.setHbc(
            new HBSession(
                context, heartbeatChecker.getEventSelector(), gw, 
                gw.getData().getPmIp(), gw.getData().getPort(), 
                gw.getData().getHB()));
        
        return gw;
    }
    
    public List<Gateway> getList(final String clusterName) {
        List<Gateway> list = new ArrayList<Gateway>();
        for (Gateway elem : container.values()) {
            if (elem.getClusterName().equals(clusterName)) {
                list.add(elem);
            }
        }
        Collections.sort(list);
        return list;
    }
    
    public Gateway get(String gw, String cluster) {
        return container.get(PathUtil.gwPath(gw, cluster));
    }

    public Gateway getByPath(String path) {
        return container.get(path);
    }
    
    public void delete(final String name, final String clusterName) {
        final String path = PathUtil.gwPath(name, clusterName);
        final Gateway gw = container.get(path);
        
        gw.release();
        container.remove(path);
    }
    
    public List<Gateway> getAll() {
        List<Gateway> list = new ArrayList<Gateway>();
        for (Gateway elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }
    
}

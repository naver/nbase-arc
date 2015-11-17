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
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerPmCluster;

@Component
public class PhysicalMachineClusterImo {

    protected Map<String, PhysicalMachineCluster> container = 
            new ConcurrentHashMap<String, PhysicalMachineCluster>();
    
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected ApplicationContext context;
    
    public void relase() {
        container.clear();
    }
    
    public PhysicalMachineCluster load(String clusterName, String pmName)
            throws NoNodeException, MgmtZooKeeperException {
        final String path = PathUtil.pmClusterPath(clusterName, pmName);
        PhysicalMachineCluster pmCluster = getByPath(path);
        if (pmCluster == null) {
            pmCluster = build(path, clusterName, pmName);
            container.put(path, pmCluster);
        } else {
            Stat stat = new Stat();
            byte[] data = zookeeper.getData(path, stat, pmCluster.getWatch());
            pmCluster.setData(data);
        }
        return pmCluster;
    }
    
    public PhysicalMachineCluster build(String path, String clusterName,
            String pmName) throws NoNodeException, MgmtZooKeeperException {
        Stat stat = new Stat();
        WatchEventHandlerPmCluster watch = new WatchEventHandlerPmCluster(context);
        
        byte[] data = zookeeper.getData(path, stat, watch);
        PhysicalMachineCluster pmCluster = new PhysicalMachineCluster(
                context, path, clusterName, pmName, data);
        pmCluster.setStat(stat);
        pmCluster.setWatch(watch);
        pmCluster.getWatch().registerBoth(path);
        
        return pmCluster;
    }
    
    public PhysicalMachineCluster get(String cluster, String pm) {
        return container.get(PathUtil.pmClusterPath(cluster, pm));
    }

    public PhysicalMachineCluster getByPath(String path) {
        return container.get(path);
    }
    
    public List<PhysicalMachineCluster> getList(final String pmName) {
        List<PhysicalMachineCluster> list = new ArrayList<PhysicalMachineCluster>();
        for (PhysicalMachineCluster elem : container.values()) {
            if (elem.getPmName().equals(pmName)) {
                list.add(elem);
            }
        }
        Collections.sort(list);
        return list;
    }

    public void delete(final String path) {
        container.remove(path);
    }

    public List<PhysicalMachineCluster> getAll() {
        List<PhysicalMachineCluster> list = new ArrayList<PhysicalMachineCluster>();
        for (PhysicalMachineCluster elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }
    
}

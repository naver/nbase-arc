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
import com.navercorp.nbasearc.confmaster.repository.dao.ClusterDao;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerCluster;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerGwRoot;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerPgRoot;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerPgsRoot;

@Component
public class ClusterImo {
    
    protected Map<String, Cluster> container = 
            new ConcurrentHashMap<String, Cluster>();
    
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected ApplicationContext context;

    @Autowired
    private ClusterDao clusterDao;
    
    @Autowired
    private PartitionGroupImo pgImo;
    @Autowired
    private PartitionGroupServerImo pgsImo;
    @Autowired
    private RedisServerImo rsImo;
    @Autowired
    private GatewayImo gwImo;
    
    public void relase() {
        container.clear();
    }
    
    public Cluster load(String name) throws NoNodeException, MgmtZooKeeperException {
        final String path = PathUtil.clusterPath(name);
        Cluster cluster = container.get(path);
        if (container.get(path) == null) {
            cluster = build(path, name);
            container.put(path, cluster);   
        } else {
            Stat stat = new Stat();
            byte[] data = clusterDao.loadCluster(name, stat, cluster.getWatch());
            cluster.setData(data);
        }
        
        // Load PartitionGroup in this Cluster
        WatchEventHandlerPgRoot watchPgRoot = 
                new WatchEventHandlerPgRoot(context, name);
        List<String> pgList = zookeeper.getChildren(PathUtil.pgRootPath(name), watchPgRoot);
        for (String pgName : pgList) {
            pgImo.load(pgName, name);
        }
        
        // Load PartitionGroupServers in this Cluster
        WatchEventHandlerPgsRoot watchPgsRoot = 
                new WatchEventHandlerPgsRoot(context, cluster);
        List<String> pgsList = zookeeper.getChildren(PathUtil.pgsRootPath(name), watchPgsRoot);
        for (String pgsName : pgsList ) {
            pgsImo.load(pgsName, cluster);
            rsImo.load(pgsName, cluster);
        }
        
        // Load Gateways in this Cluster
        WatchEventHandlerGwRoot watchGwRoot = 
                new WatchEventHandlerGwRoot(context, cluster);
        List<String> gwInThisCluster = 
                zookeeper.getChildren(PathUtil.gwRootPath(name), watchGwRoot);
        for (String gwName : gwInThisCluster) {
            gwImo.load(gwName, cluster);
        }
        
        return cluster;
    }
    
    public Cluster build(String path, String name)
            throws MgmtZooKeeperException, NoNodeException {
        Stat stat = new Stat();
        WatchEventHandlerCluster watch = 
                new WatchEventHandlerCluster(context);
        watch.registerBoth(path);
        
        byte[] data = clusterDao.loadCluster(name, stat, watch);
        Cluster cluster = new Cluster(context, path, name, data);
        
        return cluster;
    }
    
    public Cluster get(String cluster) {
        return container.get(PathUtil.clusterPath(cluster));
    }

    public Cluster getByPath(String path) {
        return container.get(path);
    }

    public void delete(final String path) {
        container.remove(path);
    }

    public List<Cluster> getAll() {
        List<Cluster> list = new ArrayList<Cluster>();
        for (Cluster elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }
    
}

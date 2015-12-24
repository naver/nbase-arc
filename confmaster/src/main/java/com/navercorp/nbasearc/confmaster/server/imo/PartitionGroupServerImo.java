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
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.heartbeat.HeartbeatChecker;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandler;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerPgs;

// TODO : redisImo + pgsImo
@Component
public class PartitionGroupServerImo {

    private Map<String, PartitionGroupServer> container = 
            new ConcurrentHashMap<String, PartitionGroupServer>();
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private HeartbeatChecker heartbeatChecker;
    
    @Autowired
    private PartitionGroupServerDao pgsDao;
    
    public void relase() {
        container.clear();
    }
    
    public PartitionGroupServer load(String name, String clusterName)
            throws NoNodeException, MgmtZooKeeperException {
        final String path = PathUtil.pgsPath(name, clusterName);
        PartitionGroupServer pgs = getByPath(path);
        if (pgs == null) {
            pgs = build(path, name, clusterName);
            container.put(path, pgs);
        } else {
            Stat stat = new Stat();
            byte[] data =  pgsDao.loadPgs(name, clusterName, stat, pgs.getWatch());
            pgs.setData(data);
        }
        return pgs;
    }
    
    public PartitionGroupServer build(String path, String name,
            String clusterName) throws NoNodeException, MgmtZooKeeperException {
        Stat stat = new Stat();
        WatchEventHandler watch = new WatchEventHandlerPgs(context);
        byte[] data =  pgsDao.loadPgs(name, clusterName, stat, watch);
        
        PartitionGroupServer pgs = 
                new PartitionGroupServer(context, path, name, clusterName, data);
        pgs.setWatch(watch);
        pgs.getWatch().registerBoth(path);
        
        pgs.setHbc(
            new HBSession(
                context, heartbeatChecker.getEventSelector(), pgs, 
                pgs.getData().getPmIp(), pgs.getData().getSmrMgmtPort(), 
                pgs.getData().getHb()));

        return pgs;
    }

    public List<PartitionGroupServer> getList(final String clusterName) {
        List<PartitionGroupServer> list = new ArrayList<PartitionGroupServer>();
        for (PartitionGroupServer elem : container.values()) {
            if (elem.getClusterName().equals(clusterName)) {
                list.add(elem);
            }
        }
        Collections.sort(list);
        return list;
    }

    public List<PartitionGroupServer> getList() {
        List<PartitionGroupServer> list = new ArrayList<PartitionGroupServer>();
        for (PartitionGroupServer elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }
    
    public List<PartitionGroupServer> getList(final String clusterName, final Integer pgId) {
        List<PartitionGroupServer> list = new ArrayList<PartitionGroupServer>();
        for (PartitionGroupServer elem : container.values()) {
            if (elem.getClusterName().equals(clusterName) && elem.getData().getPgId() == pgId) {
                list.add(elem);
            }
        }
        Collections.sort(list);
        return list;
    }

    public PartitionGroupServer get(String pgs, String cluster) {
        return container.get(PathUtil.pgsPath(pgs, cluster));
    }
    
    public PartitionGroupServer getByPath(String path) {
        return container.get(path);
    }
    
    public void delete(final String name, final String clusterName) {
        final PartitionGroupServer pgs = get(name, clusterName);
        
        pgs.release();
        container.remove(PathUtil.pgsPath(name, clusterName));
    }

    public List<PartitionGroupServer> getAll() {
        List<PartitionGroupServer> list = new ArrayList<PartitionGroupServer>();
        for (PartitionGroupServer elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }

}

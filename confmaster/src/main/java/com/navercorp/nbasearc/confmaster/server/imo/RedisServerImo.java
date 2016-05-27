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
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandler;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerRs;

@Component
public class RedisServerImo {

    protected Map<String, RedisServer> container = 
            new ConcurrentHashMap<String, RedisServer>();
    
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected ApplicationContext context;
    
    @Autowired
    protected HeartbeatChecker heartbeatChecker;
    
    public void relase() {
        container.clear();
    }
    
    public RedisServer load(String name, String clusterName)
            throws NoNodeException, MgmtZooKeeperException {
        final String path = getPath(name, clusterName);
        RedisServer rs = getByPath(path);
        if (rs == null) {
            rs = build(path, name, clusterName);
            container.put(path, rs);
        } else {
            Stat stat = new Stat();
            byte[] data =  zookeeper.getData(path, stat, rs.getWatch());
            rs.setData(data);
        }
        return rs;
    }
    
    public RedisServer build(String path, String name, String clusterName)
            throws NoNodeException, MgmtZooKeeperException {
        Stat stat = new Stat();
        WatchEventHandler watch = new WatchEventHandlerRs(context);
        byte[] data =  zookeeper.getData(path, stat, watch);
        
        RedisServer rs = new RedisServer(context, path, name, clusterName, data);
        rs.setWatch(watch);
        rs.getWatch().registerBoth(path);
        rs.setHbc(
            new HBSession(
                context, heartbeatChecker.getEventSelector(), rs, 
                rs.getData().getPmIp(), rs.getData().getRedisPort(), 
                rs.getData().getHB()));

        return rs;
    }
    
    public List<RedisServer> getList(final String clusterName) {
        List<RedisServer> list = new ArrayList<RedisServer>();
        for (RedisServer elem : container.values()) {
            if (elem.getClusterName().equals(clusterName)) {
                list.add(elem);
            }
        }
        Collections.sort(list);
        return list;
    }

    public RedisServer get(String rs, String cluster) {
        return container.get(PathUtil.rsPath(rs, cluster));
    }

    public RedisServer getByPath(String path) {
        return container.get(path);
    }

    public void delete(final String name, final String clusterName) {
        final RedisServer pgs = get(name, clusterName);
        
        pgs.release();
        
        container.remove(PathUtil.rsPath(name, clusterName));
    }
    
    public String getPath(String rs, String cluster) {
        return PathUtil.rsPath(rs, cluster);
    }

    public List<RedisServer> getAll() {
        List<RedisServer> list = new ArrayList<RedisServer>();
        for (RedisServer elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }
    
}

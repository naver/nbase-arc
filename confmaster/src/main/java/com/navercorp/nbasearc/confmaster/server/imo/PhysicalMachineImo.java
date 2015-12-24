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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PhysicalMachineDao;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachine;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerPm;

@Component
public class PhysicalMachineImo {

    protected Map<String, PhysicalMachine> container = 
            new ConcurrentHashMap<String, PhysicalMachine>();
    
    @Autowired
    protected ZooKeeperHolder zookeeper;
    
    @Autowired
    protected ApplicationContext context;
    
    @Autowired
    private PhysicalMachineDao pmDao;

    @Autowired 
    private PhysicalMachineClusterImo pmClusterImo;
    
    public void relase() {
        container.clear();
    }

    public PhysicalMachine load(String name) throws NoNodeException,
            MgmtZooKeeperException {
        final String path = PathUtil.pmPath(name);
        PhysicalMachine pm = getByPath(path);
        if (pm == null) {
            pm = build(path, name);
            container.put(path, pm);
        } else {
            byte[] data = pmDao.loadPm(name);
            pm.setData(data);
        }
        
        WatchEventHandlerPm watchPm = 
                new WatchEventHandlerPm(context, name);
        List<String> pmClusterList = zookeeper.getChildren(path, watchPm);
        for (String pmClusterName : pmClusterList) {
            pmClusterImo.load(pmClusterName, name);
        }
        pm.setWatch(watchPm);
        pm.getWatch().registerBoth(path);
        
        return pm;
    }
    
    public PhysicalMachine build(String path, String name)
            throws NoNodeException, MgmtZooKeeperException {
        byte[] data = pmDao.loadPm(name);
        return new PhysicalMachine(context, path, name, data);
    }
    
    public PhysicalMachine get(String pm) {
        return container.get(PathUtil.pmPath(pm));
    }
    
    public PhysicalMachine getByPath(String path) {
        return container.get(path);
    }

    public void delete(final String path) {
        container.remove(path);
    }
    
    public String getPath(String pm, String notUsed) {
        return PathUtil.pmPath(pm);
    }

    public List<PhysicalMachine> getAll() {
        List<PhysicalMachine> list = new ArrayList<PhysicalMachine>();
        for (PhysicalMachine elem : container.values()) {
            list.add(elem);
        }
        Collections.sort(list);
        return list;
    }
}

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

package com.navercorp.nbasearc.confmaster.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;

@Repository("zooKeeperHolder")
public class ZooKeeperHolder {
    
    private ZooKeeper zk = null;
    
    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    public ZooKeeper getZooKeeper() {
        return zk;
    }
    
    @Value("${confmaster.zookeeper.address}")
    private String ipPort;
    
    @Autowired
    private Config config;
    
    @Autowired
    private ApplicationContext context;

    public void initialize() throws MgmtZooKeeperException {
        Logger.info("Connect to zookeeper. address: {}", getIpPort());
        
        final int sessionTimeout = 10000;
        
        try {
            WatchEventHandler sync = new WatchEventHandler(context);
            zk = new ZooKeeper(getIpPort(), sessionTimeout, sync);
            if (!sync.awaitConnection(sessionTimeout)) {
                zk.close();
                String msg = "Connect to ZooKeeper failed, connection time out. address:"
                        + getIpPort() + ", timeout: " + sessionTimeout;
                Logger.error(msg);
                throw new MgmtZooKeeperException(msg);
            }
        } catch (InterruptedException e) {
            Logger.error(
                    "Connect to ZooKeeper fail. address: {}, timeout: {}", 
                    getIpPort(), sessionTimeout, e);
            throw new MgmtZooKeeperException(e);
        } catch (IOException e) {
            Logger.error(
                    "Connect to ZooKeeper fail. address: {}, timeout: {}", 
                    getIpPort(), sessionTimeout, e);
            throw new MgmtZooKeeperException(e);
        }
    }
    
    public void release() {
        if (null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                Logger.error("Close ZooKeeper handle error.", e);
            }
            zk = null;
        }
    }

    public <T> Op createReflectMemoryIntoZkOperation(String path, T data, final int version) {
        return Op.setData(path, mapper.writeValueAsBytes(data), version);
    }

    public <T> Stat reflectMemoryIntoZk(String path, T dataObject)
            throws MgmtZooKeeperException {
        try {
            final byte[] data = mapper.writeValueAsBytes(dataObject);
            return zk.setData(path, data, -1);
        } catch (KeeperException e) {
            Logger.error("Set data from memory into ZooKeeper fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Set data from memory into ZooKeeper fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getData(String path, Stat stat, Watcher watcher,
            TypeReference<T> typeRef) throws MgmtZooKeeperException {
        try {
            final byte[] data = zk.getData(path, watcher, stat);
            return (T)mapper.readValue(data, typeRef);
        } catch (KeeperException e) {
            Logger.error("Get data from ZooKeeper fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Get data from ZooKeeper fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public void handleResultsOfMulti(List<OpResult> results) {
        if (null == results) {
            Logger.error("Multi operations of ZooKeeper result is null.");
            return;
        }
        
        for (OpResult result : results) {
            switch (result.getType()) {
            case ZooDefs.OpCode.create:
                OpResult.CreateResult rc = (OpResult.CreateResult) result;
                Logger.debug("CreateResult. {}, path: {}", rc, rc.getPath());
                break;

            case ZooDefs.OpCode.delete:
                OpResult.DeleteResult rd = (OpResult.DeleteResult) result;
                Logger.debug("DeleteResult. {} ", rd);
                break;

            case ZooDefs.OpCode.setData:
                OpResult.SetDataResult rsd = (OpResult.SetDataResult) result;
                Logger.debug("DeleteResult. " + rsd.toString());
                break;

            case ZooDefs.OpCode.error:
                OpResult.ErrorResult rer = (OpResult.ErrorResult) result;
                Logger.debug("DeleteResult. {}, error: {}", rer, rer.getErr());
                break;
            }
            Logger.debug(result.toString());
        }
    }
    
    String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws MgmtZooKeeperException, NodeExistsException {
        String createdPath = null;
        try {
            createdPath = zk.create(path, data, acl, createMode);
        } catch (KeeperException.NodeExistsException e) {
            // Do not logging this exception. error handling depends on a caller.
            throw e;
        } catch (KeeperException e) {
            Logger.error("Create znode fail. path: {}, data: {}", path, data, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Create znode fail. path: {}, data: {}", path, data, e);
            throw new MgmtZooKeeperException(e);
        }
        
        return createdPath;
    }

    public String createPersistentZNode(String path, byte data[])
            throws MgmtZooKeeperException, NodeExistsException
    {
        return this.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String createPersistentZNode(String path)
            throws MgmtZooKeeperException, NodeExistsException
    {
        final byte[] ZERO_BYTE = new byte[1];
        return this.create(path, ZERO_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String createEphemeralZNode(String path, byte data[])
            throws MgmtZooKeeperException, NodeExistsException {
        return this.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
    }

    public String createEphemeralZNode(String path)
            throws MgmtZooKeeperException, NodeExistsException {
        final byte[] ZERO_BYTE = new byte[1];
        return this.create(path, ZERO_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void registerChangedEventWatcher(final String path,
            final Watcher watcher) throws MgmtZooKeeperException {
        try {
            zk.exists(path, watcher);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Register changed event watcher fail. path: {}", path, e);
            // ignore
        } catch (KeeperException e) {
            Logger.error("Register changed event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Register changed event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public void registerChangedEventWatcher(final String path) throws MgmtZooKeeperException {
        try {
            zk.exists(path, true);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Register changed event watcher fail. path: {}", path, e);
            // ignore
        } catch (KeeperException e) {
            Logger.error("Register changed event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Register changed event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public void registerChildEventWatcher(final String path) throws MgmtZooKeeperException {
        try {
            zk.getChildren(path, true);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Register child event watcher fail. path: {}", path, e);
            // ignore
        } catch (KeeperException e) {
            Logger.error("Register child event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Register child event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }
    
    public void registerChildEventWatcher(final String path,
            final Watcher watcher) throws MgmtZooKeeperException {
        try {
            zk.getChildren(path, watcher);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Register child event watcher fail. path: {}", path, e);
            // ignore
        } catch (KeeperException e) {
            Logger.error("Register child event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Register child event watcher fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public Stat setData(final String path, final byte[] data, final int version)
            throws MgmtZooKeeperException {
        try {
            return zk.setData(path, data, version);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Set znode fail. path: {}, data: {}", path, data, e);
            throw new MgmtZooKeeperException(e);
        } catch (KeeperException e) {
            Logger.error("Set znode fail. path: {}, data: {}", path, data, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Set znode fail. path: {}, data: {}", path, data, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public byte[] getData(final String path, Stat stat, Watcher watcher)
            throws MgmtZooKeeperException, NoNodeException {
        try {
            return zk.getData(path, watcher, stat);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Get data fail. path: {}", path, e);
            throw e;
        } catch (KeeperException e) {
            Logger.error("Get data fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Get data fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public byte[] getData(final String path, Stat stat, boolean watch)
            throws MgmtZooKeeperException, NoNodeException {
        try {
            return zk.getData(path, watch, stat);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Get data fail. path: {}", path, e);
            throw e;
        } catch (KeeperException e) {
            Logger.error("Get data fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Get data fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public byte[] getData(final String path, Stat stat)
            throws MgmtZooKeeperException, NoNodeException {
        return getData(path, stat, null);
    }

    public void deleteZNode(final String path, final int version)
            throws MgmtZooKeeperException {
        try {
            zk.delete(path, version);
        } catch (KeeperException.NoNodeException e) {
            Logger.warn("Delete znode fail. path: {}", path, e);
            // ignore
        } catch (KeeperException.NotEmptyException e) {
            Logger.error("Delete znode fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (KeeperException e) {
            Logger.error("Delete znode fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Delete znode fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }
    
    public void deleteChildren(String path) throws MgmtZooKeeperException {
        List<String> children = getChildren(path);
        
        for (String childName : children) {
            StringBuilder childPath = new StringBuilder(path);
            childPath.append("/").append(childName);
            deleteZNode(childPath.toString(), -1);
        }
    }

    public void deleteAllZNodeRecursive() throws MgmtZooKeeperException {
        List<String> tree = listSubTreeBFS("/");
        for (int i = tree.size() - 1; i >= 0; --i)
        {
            deleteZNode(tree.get(i), -1);
        }
    }
    
    public void deleteZNodeRecursive(final String path)
            throws MgmtZooKeeperException 
    {
        List<String> tree = listSubTreeBFS(path);
        for (int i = tree.size() - 1; i >= 0; --i)
        {
            deleteZNode(tree.get(i), -1);
        }
    }

    private List<String> listSubTreeBFS(final String pathRoot)
            throws MgmtZooKeeperException {
        if (pathRoot.equals("/zookeeper")) {
            return new ArrayList<String>();
        }
        
        Deque<String> queue = new LinkedList<String>();
        List<String> tree = new ArrayList<String>();
        queue.add(pathRoot);
        tree.add(pathRoot);
        while (true) {
            String node = queue.pollFirst();
            if (node == null) {
                break;
            }

            List<String> children = getChildren(node);
            for (final String child : children) {
                final String childPath = (node.equals("/") ? node : node + "/")
                        + child;
                if (childPath.equals("/zookeeper")) {
                    continue;
                }
                queue.add(childPath);
                tree.add(childPath);
            }
        }

        if (!tree.isEmpty() && tree.get(0).equals("/")) {
            tree.remove(0);
        }
        
        return tree;
    }

    public List<String> getChildren(final String path)
            throws MgmtZooKeeperException {
        return getChildren(path, null);
    }

    public List<String> getChildren(final String path, final boolean watch)
            throws MgmtZooKeeperException {
        try {
            return zk.getChildren(path, watch);
        } catch (KeeperException e) {
            Logger.error("Get children of znode fail. path: {} ", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Get children of znode fail. path: {} ", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }
    
    public List<String> getChildren(final String path, final Watcher watcher)
            throws MgmtZooKeeperException {
        try {
            return zk.getChildren(path, watcher);
        } catch (KeeperException e) {
            Logger.error("Get children of znode fail. path: {} ", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Get children of znode fail. path: {} ", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public int getNumberOfChildren(final String path)
            throws MgmtZooKeeperException {
        List<String> children = getChildren(path, null);
        return children.size();
    }
    
    public boolean isExists(final String path) throws MgmtZooKeeperException {
        try {
            return (zk.exists(path, true) != null);
        } catch (KeeperException e) {
            Logger.error("Check znode fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Check znode fail. path: {}", path, e);
            throw new MgmtZooKeeperException(e);
        }
    }
    
    public List<OpResult> multi(Iterable<Op> ops) throws MgmtZooKeeperException {
        try {
            return zk.multi(ops);
        } catch (KeeperException e) {
            Logger.error("Multi operation fail. {}, {}", e.getPath(), ops, e);
            throw new MgmtZooKeeperException(e);
        } catch (InterruptedException e) {
            Logger.error("Multi operation fail. {}", ops, e);
            throw new MgmtZooKeeperException(e);
        }
    }

    public String getIpPort() {
        return ipPort;
    }

    public void setIpPort(String ipPort) {
        this.ipPort = ipPort;
    }

    public States getStates() {
        return zk.getState();
    }
    
}

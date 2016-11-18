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

import static com.navercorp.nbasearc.confmaster.server.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.server.lock.LockType.WRITE;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.COMMON_STATE_DECISION;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.PGS_STATE_DECISION;
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.TOTAL_INSPECTION;
import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.Cluster;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponent;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

public class WatchEventHandler implements Watcher {
    private final CountDownLatch connWait = new CountDownLatch(1);
    
    private final ApplicationContext context;
    private final ZooKeeperHolder zk;
    private final WorkflowExecutor workflowExecutor;
    
    private final HierarchicalLockHelper lockHelper;
    
    private final ClusterComponentContainer container;
    
    public boolean awaitConnection(final int milliSec) throws InterruptedException {
        return connWait.await(milliSec, TimeUnit.MILLISECONDS);
    }
    
    public WatchEventHandler(ApplicationContext context) {
        this.context = context;
        this.zk = context.getBean(ZooKeeperHolder.class);
        this.workflowExecutor = context.getBean(WorkflowExecutor.class);
        this.lockHelper = new HierarchicalLockHelper(context);
        this.container = context.getBean(ClusterComponentContainer.class);
    }
    
    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            Logger.info(event.toString());
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                Logger.info("Connection to zookeeper is established. {}", event);
                Logger.flush(INFO);
                connWait.countDown();
            } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
                Logger.info("ZooKeeper session expired, shutdown... {}", event);
                Logger.flush(DEBUG);
                System.exit(-1);
            } else {
                Logger.info("Connection to ZooKeeper state changed. {}", event);
                Logger.flush(INFO);
            }
        } else if (event.getType() == Event.EventType.NodeChildrenChanged
                || event.getType() == Event.EventType.NodeDataChanged) {
            processChildrenOrDataChangedEvent(event);
        }
    }
    
    public void processChildrenOrDataChangedEvent(WatchedEvent event) {
        Level logLevel = INFO;
        
        try {
            PathUtil.WatchTarget wt = PathUtil.getWatchType(event.getPath());
            if (wt == null) {
                return;
            }
            
            lock(wt, event);
            
            switch (event.getType()) {
            case NodeDataChanged:
                processChangedEvent(wt, event);
                break;
            case NodeChildrenChanged:
                processChildEvent(wt, event);
                break;
            default:
                break;
            }
        } catch (Exception e) {
            Logger.error("Handle watch event fail. {} {}",
                    event.getPath(), event, e);
            
            logLevel = DEBUG;
        } finally {
            unlock();
        }
        
        Logger.flush(logLevel);
    }
    
    public void processChildEvent(PathUtil.WatchTarget wt, WatchedEvent event) throws MgmtZooKeeperException,
            NoNodeException {
        switch (wt.nodeType) {
        case PGS:
            onChildEventPgs(wt, event);
            break;
        case RS:
            onChildEventRs(wt, event);
            break;
        case GW:
            onChildEventGw(wt, event);
            break;
        case PG:
            onChildEventPg(wt, event);
            break;
        case PGS_ROOT:
            onChildEventPgsRoot(wt, event);
            break;
        case RS_ROOT:
            break;
        case GW_ROOT:
            onChildEventGwRoot(wt, event);
            break;
        case PG_ROOT:
            onChildEventPgRoot(wt, event);
            break;
        case FD:
            onChildEventFd(wt, event);
            break;
        case CLUSTER:
            onChildEventCluster(wt, event);
            break;
        case CLUSTER_ROOT:
            onChildEventClusterRoot(wt, event);
            break;
        default:
            // Ignore
            break;
        }
    }

    public void processChangedEvent(PathUtil.WatchTarget wt, WatchedEvent event) throws MgmtZooKeeperException, 
            NoNodeException {
        switch (wt.nodeType) {
        case PGS:
            onChangedEventPgs(wt, event);
            break;
        case RS:
            onChangedEventRs(wt, event);
            break;
        case GW:
            onChangedEventGw(wt, event);
            break;
        case PG:
            onChangedEventPg(wt, event);
            break;
        case PGS_ROOT:
            onChangedEventPgsRoot(wt, event);
            break;
        case RS_ROOT:
            break;
        case GW_ROOT:
            onChangedEventGwRoot(wt, event);
            break;
        case PG_ROOT:
            onChangedEventPgRoot(wt, event);
            break;
        case FD:
            onChangedEventFd(wt, event);
            break;
        case CLUSTER:
            onChangedEventCluster(wt, event);
            break;
        case CLUSTER_ROOT:
            onChangedEventClusterRoot(wt, event);
            break;
        default:
            // Ignore
            break;
        }
    }

    public void lock(PathUtil.WatchTarget wt, WatchedEvent event) {
        switch (wt.nodeType) {
        case PGS:
            lockHelper.root(READ).cluster(READ, wt.clusterName).pgList(READ).pgsList(READ).pg(READ, null).pgs(WRITE, wt.name);
            break;
        case RS:
            lockHelper.root(READ).cluster(READ, wt.clusterName).pgList(READ)
            .pgsList(READ).pg(READ, null).pgs(WRITE, wt.name);
            break;
        case GW:
            lockHelper.root(READ).cluster(READ, wt.clusterName).gwList(READ).gw(WRITE, wt.name);
            break;
        case PG:
            lockHelper.root(READ).cluster(READ, wt.clusterName).pgList(READ).pgsList(READ).pg(WRITE, wt.name);
            break;
        case PGS_ROOT:
            lockHelper.root(READ).cluster(READ, wt.clusterName).pgList(READ).pgsList(WRITE);
            break;
        case RS_ROOT:
            lockHelper.root(READ).cluster(READ, wt.clusterName).pgList(READ).pgsList(READ).pg(READ, null).pgs(WRITE, wt.name);
            break;
        case GW_ROOT:
            lockHelper.root(READ).cluster(READ, wt.clusterName).gwList(WRITE);
            break;
        case PG_ROOT:
            lockHelper.root(WRITE).cluster(READ, wt.clusterName).pgList(WRITE);
            break;
        case FD:
            lockHelper.root(WRITE);
            break;
        case CLUSTER:
            lockHelper.root(READ).cluster(WRITE, wt.clusterName);
            break;
        default:
            // Ignore
            break;
        }
    }
    
    public void unlock() {
        lockHelper.releaseAllLock();
    }
    
    protected List<String> getDeletedChildren(String path, List<? extends ClusterComponent> inMemoryObjectList)
            throws MgmtZooKeeperException {
        List<String> deletedList = new ArrayList<String>();

        List<String> zkObjectList = zk.getChildren(path);
        for (ClusterComponent o : inMemoryObjectList) {
            deletedList.add(o.getName());
        }
        for (String zko : zkObjectList) {
            deletedList.remove(zko);
        }

        return deletedList;
    }

    protected List<String> getCreatedChildren(String path, List<? extends ClusterComponent> inMemoryObjectList)
            throws MgmtZooKeeperException {
        List<String> createdList = zk.getChildren(path);
        for (ClusterComponent o : inMemoryObjectList) {
            createdList.remove(o.getName());
        }

        return createdList;
    }
    
    public void onChildEventCluster(PathUtil.WatchTarget wt, WatchedEvent event) {
    }

    public void onChangedEventCluster(PathUtil.WatchTarget wt,
            WatchedEvent event) throws MgmtZooKeeperException, NoNodeException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            zk.registerChangedEventWatcher(event.getPath());

            Cluster cluster = (Cluster) container.get(event.getPath());
            if (null == cluster) {
                // this znode already removed.
                return;
            }

            cluster.setPersistentData(zk.getData(cluster.getPath(), null));

            cluster.propagateModeToHeartbeatSession();
        }
    }

    public void onChildEventClusterRoot(PathUtil.WatchTarget wt,
            WatchedEvent event) throws MgmtZooKeeperException, NoNodeException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            zk.registerChildEventWatcher(event.getPath());

            // Delete
            List<String> deleted = getDeletedChildren(event.getPath(), container.getAllCluster());
            for (String clusterName : deleted) {
                container.delete(PathUtil.clusterPath(clusterName));
            }

            // Created
            List<String> created = getCreatedChildren(event.getPath(), container.getAllCluster());
            for (String clusterName : created) {
                // TDOO : eliminate null
                Cluster.loadClusterFromZooKeeper(context, clusterName);
            }
        }
    }

    public void onChangedEventClusterRoot(PathUtil.WatchTarget wt,
            WatchedEvent event) {
    }

    public void onChildEventFd(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        zk.registerChildEventWatcher(event.getPath());

        workflowExecutor.perform(TOTAL_INSPECTION);
    }

    public void onChangedEventFd(PathUtil.WatchTarget wt, WatchedEvent event) {
    }

    public void onChildEventGw(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        zk.registerChildEventWatcher(event.getPath());

        Gateway gw = (Gateway) container.get(event.getPath());

        if (gw.getHeartbeat().equals(Constant.HB_MONITOR_YES)) {
            gw.turnOnUrgentHeartbeat();
        }

        workflowExecutor.perform(COMMON_STATE_DECISION, gw);
    }

    public void onChangedEventGw(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException, NoNodeException {
        zk.registerChangedEventWatcher(event.getPath());

        Gateway gw = (Gateway) container.get(event.getPath());
        if (null == gw) {
            // this znode already removed.
            return;
        }

        Cluster cluster = (Cluster) container.getCluster(PathUtil.getClusterNameFromPath(event.getPath()));
        if (null == cluster) {
            // this znode already removed.
            return;
        }

        if (LeaderState.isFollower()) {
            Stat stat = new Stat();
            gw.setPersistentData(zk.getData(gw.getPath(), stat));
            gw.setZNodeVersion(stat.getVersion());
        }

        try {
            gw.propagateStateToHeartbeatSession(cluster.getMode());
            if (gw.getHeartbeat().equals(Constant.HB_MONITOR_YES)) {
                gw.turnOnUrgentHeartbeat();
            }
        } catch (Exception e) {
            Logger.error("Change gateway fail. {}", gw, e);
        }
    }

    public void onChildEventGwRoot(PathUtil.WatchTarget wt, WatchedEvent event)
            throws NoNodeException, MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            zk.registerChildEventWatcher(event.getPath());

            // Delete
            List<String> deleted = getDeletedChildren(event.getPath(), container.getGwList(wt.clusterName));
            for (String gwName : deleted) {
                container.delete(PathUtil.gwPath(gwName, wt.clusterName));
            }

            // Created
            List<String> created = getCreatedChildren(event.getPath(), container.getGwList(wt.clusterName));
            for (String gwName : created) {
                Stat stat = new Stat();
                String gwPath = PathUtil.gwPath(gwName, wt.clusterName);
                byte[] data = zk.getData(gwPath, stat, true);
                Gateway gw = new Gateway(context, wt.clusterName, gwName, data,
                        stat.getVersion());
                zk.registerChildEventWatcher(gwPath);
                container.put(gw.getPath(), gw);
            }
        }
    }

    public void onChangedEventGwRoot(PathUtil.WatchTarget wt, WatchedEvent event) {
    }

    public void onChildEventPg(PathUtil.WatchTarget wt, WatchedEvent event) {
    }

    public void onChangedEventPg(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            zk.registerChangedEventWatcher(event.getPath());

            PartitionGroup pg = (PartitionGroup) container.get(event.getPath());
            if (null == pg) {
                // this znode already removed.
                return;
            }

            try {
                pg.setPersistentData(zk.getData(pg.getPath(), null));
            } catch (NoNodeException e) {
                throw new MgmtZooKeeperException(e);
            }
        }
    }

    public void onChildEventPgRoot(PathUtil.WatchTarget wt, WatchedEvent event)
            throws NoNodeException, MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            zk.registerChildEventWatcher(event.getPath());

            // Delete
            List<String> deleted = getDeletedChildren(event.getPath(), container.getPgList(wt.clusterName));
            for (String pgName : deleted) {
                container.delete(PathUtil.pgPath(pgName, wt.clusterName));
            }

            // Created
            List<String> created = getCreatedChildren(event.getPath(), container.getPgList(wt.clusterName));
            for (String pgName : created) {
                final String pgPath = PathUtil.pgPath(pgName, wt.clusterName);
                byte[] data = zk.getData(pgPath, null, true);
                PartitionGroup pg = new PartitionGroup(context, pgPath, pgName,
                        wt.clusterName, data);
                zk.registerChildEventWatcher(pgPath);
                container.put(pg.getPath(), pg);
            }
        }
    }

    public void onChangedEventPgRoot(PathUtil.WatchTarget wt, WatchedEvent event) {
    }

    public void onChildEventPgs(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        zk.registerChildEventWatcher(event.getPath());

        PartitionGroupServer pgs = (PartitionGroupServer) container.get(event.getPath());

        if (!event.getPath().equals(pgs.getPath())) {
            Logger.error("PATH INCONSISTENCY");
            return;
        }

        if (pgs.getHeartbeat().equals(Constant.HB_MONITOR_YES)) {
            pgs.turnOnUrgentHeartbeat();
        }

        workflowExecutor.perform(PGS_STATE_DECISION, pgs);
    }

    public void onChangedEventPgs(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        zk.registerChangedEventWatcher(event.getPath());

        PartitionGroupServer pgs = (PartitionGroupServer) container.get(event.getPath());
        if (null == pgs) {
            // this znode already removed.
            return;
        }

        if (LeaderState.isFollower()) {
            Stat stat = new Stat();
            try {
                byte[] d = zk.getData(pgs.getPath(), stat);
                pgs.setPersistentData(d);
                pgs.setZNodeVersion(stat.getVersion());
            } catch (NoNodeException e) {
                throw new MgmtZooKeeperException(e);
            }
        }

        try {
            pgs.propagateStateToHeartbeatSession();
            if (pgs.getHeartbeat().equals(Constant.HB_MONITOR_YES)) {
                pgs.turnOnUrgentHeartbeat();
            }
        } catch (Exception e) {
            Logger.error("Change pgs fail. {}", pgs, e);
        }
    }

    public void onChildEventPgsRoot(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            zk.registerChildEventWatcher(event.getPath());

            // Delete
            List<String> deleted = getDeletedChildren(event.getPath(), container.getPgsList(wt.clusterName));
            for (String pgsName : deleted) {
                container.delete(PathUtil.pgsPath(pgsName, wt.clusterName));
                container.delete(PathUtil.rsPath(pgsName, wt.clusterName));
            }

            // Created
            List<String> created = getCreatedChildren(event.getPath(), container.getPgsList(wt.clusterName));
            for (String pgsName : created) {
                try {
                    Stat stat = new Stat();
                    String path = PathUtil.pgsPath(pgsName, wt.clusterName);
                    byte[] d = context.getBean(ZooKeeperHolder.class).getData(
                            path, stat, true);
                    PartitionGroupServer pgs = new PartitionGroupServer(
                            context, d, wt.clusterName, pgsName, stat.getVersion());
                    zk.registerChildEventWatcher(path);
                    container.put(pgs.getPath(), pgs);

                    String rsPath = PathUtil.rsPath(pgsName, wt.clusterName);
                    d = zk.getData(rsPath, stat, true);
                    RedisServer rs = new RedisServer(context, d,
                            wt.clusterName, pgsName, pgs.getPgId(),
                            stat.getVersion());
                    zk.registerChildEventWatcher(rsPath);
                    container.put(rs.getPath(), rs);
                } catch (Exception e) {
                    Logger.error("Load pgs fail. cluster:{}/pgs:{}",
                            wt.clusterName, pgsName, e);
                }

            }
        }
    }

    public void onChangedEventPgsRoot(PathUtil.WatchTarget wt,
            WatchedEvent event) {
    }
    public void onChildEventRs(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        zk.registerChildEventWatcher(event.getPath());

        RedisServer rs = (RedisServer) container.get(event.getPath());

        if (rs.getHeartbeat().equals(Constant.HB_MONITOR_YES)) {
            rs.turnOnUrgentHeartbeat();
        }

        workflowExecutor.perform(COMMON_STATE_DECISION, rs);
    }

    public void onChangedEventRs(PathUtil.WatchTarget wt, WatchedEvent event)
            throws MgmtZooKeeperException {
        zk.registerChangedEventWatcher(event.getPath());

        RedisServer rs = (RedisServer) container.get(event.getPath());
        if (null == rs) {
            // this znode already removed.
            return;
        }

        if (LeaderState.isFollower()) {
            Stat stat = new Stat();
            try {
                byte[] d = zk.getData(rs.getPath(), stat);
                rs.setPersistentData(d);
                rs.setZNodeVersion(stat.getVersion());
            } catch (NoNodeException e) {
                throw new MgmtZooKeeperException(e);
            }
        }

        try {
            rs.propagateStateToHeartbeatSession();
            if (rs.getHeartbeat().equals(Constant.HB_MONITOR_YES)) {
                rs.turnOnUrgentHeartbeat();
            }
        } catch (Exception e) {
            Logger.error("failed while change rs. {} {}", event.getPath(),
                    event, e);
        }
    }

}
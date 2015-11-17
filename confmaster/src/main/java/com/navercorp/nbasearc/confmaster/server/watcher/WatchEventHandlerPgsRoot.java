package com.navercorp.nbasearc.confmaster.server.watcher;

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.RedisServerImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPgsRoot extends WatchEventHandler  {
    
    private final String clusterName;
    private final PartitionGroupServerImo pgsImo;
    private final RedisServerImo rsImo;
    
    public WatchEventHandlerPgsRoot(ApplicationContext context, String clusterName) {
        super(context);
        this.clusterName = clusterName;
        this.pgsImo = context.getBean(PartitionGroupServerImo.class);
        this.rsImo = context.getBean(RedisServerImo.class);
    }
    
    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            registerChildEvent(event.getPath());
    
            // Delete
            List<String> deleted = getDeletedChild(
                    event.getPath(), pgsImo.getList(getClusterName()));
            for (String pgsName : deleted) {
                pgsImo.delete(pgsName, getClusterName());
                rsImo.delete(pgsName, getClusterName());
            }
            
            // Created
            List<String> created = getCreatedChild(
                    event.getPath(), pgsImo.getList(getClusterName()));
            for (String pgsName : created) {
                try {
                    pgsImo.load(pgsName, getClusterName());
                } catch (Exception e) {
                    Logger.error("Load pgs fail. cluster:{}/pgs:{}", getClusterName(), pgsName, e);
                }
                
                try {
                    rsImo.load(pgsName, getClusterName());
                } catch (Exception e) {
                    Logger.error("Load rs fail. cluster:{}/rs:{}", getClusterName(), pgsName, e);
                }
                
            }
        }
    }
    
    @Override
    public void onChangedEvent(WatchedEvent event) {
    }
    
    @Override
    public void lock(String path) {
        String clusterName = PathUtil.getClusterNameFromPath(path);
        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ).pgsList(WRITE);
    }

    public String getClusterName() {
        return clusterName;
    }

}

package com.navercorp.nbasearc.confmaster.server.watcher;

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPgRoot extends WatchEventHandler {

    private final String clusterName;
    private final PartitionGroupImo pgImo;

    public WatchEventHandlerPgRoot(ApplicationContext context,
            String clusterName) {
        super(context);
        this.clusterName = clusterName;
        this.pgImo = context.getBean(PartitionGroupImo.class);
    }

    @Override
    public void onChildEvent(WatchedEvent event) throws NoNodeException,
            MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            registerChildEvent(event.getPath());

            // Delete
            List<String> deleted = getDeletedChild(event.getPath(),
                    pgImo.getList(clusterName));
            for (String pgName : deleted) {
                pgImo.delete(PathUtil.pgPath(pgName, clusterName));
            }

            // Created
            List<String> created = getCreatedChild(event.getPath(),
                    pgImo.getList(clusterName));
            for (String pgName : created) {
                pgImo.load(pgName, clusterName);
            }
        }
    }

    @Override
    public void onChangedEvent(WatchedEvent event) {
    }

    @Override
    public void lock(String path) {
        lockHelper.root(WRITE).cluster(READ, clusterName).pgList(WRITE);
    }

    public String getClusterName() {
        return clusterName;
    }

}

package com.navercorp.nbasearc.confmaster.server.watcher;

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPg extends WatchEventHandler {

    private final PartitionGroupImo pgImo;

    public WatchEventHandlerPg(ApplicationContext context) {
        super(context);
        this.pgImo = context.getBean(PartitionGroupImo.class);
    }

    @Override
    public void onChildEvent(WatchedEvent event) {
    }

    @Override
    public void onChangedEvent(WatchedEvent event)
            throws MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            registerBoth(event.getPath());

            PartitionGroup pg = pgImo.getByPath(event.getPath());
            if (null == pg) {
                // this znode already removed.
                return;
            }

            zookeeper.reflectZkIntoMemory(pg);
        }
    }

    @Override
    public void lock(String path) {
        String clusterName = PathUtil.getClusterNameFromPath(path);
        String pgName = PathUtil.getPgNameFromPath(path);

        lockHelper.root(READ).cluster(READ, clusterName).pgList(READ)
                .pgsList(READ).pg(WRITE, pgName);
    }

}

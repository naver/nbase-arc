package com.navercorp.nbasearc.confmaster.server.watcher;

import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.READ;
import static com.navercorp.nbasearc.confmaster.repository.lock.LockType.WRITE;

import org.apache.zookeeper.WatchedEvent;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.server.cluster.PhysicalMachineCluster;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;

public class WatchEventHandlerPmCluster extends WatchEventHandler {

    private final PhysicalMachineClusterImo pmClusterImo;

    public WatchEventHandlerPmCluster(ApplicationContext context) {
        super(context);
        this.pmClusterImo = context.getBean(PhysicalMachineClusterImo.class);
    }

    @Override
    public void onChildEvent(WatchedEvent event) throws MgmtZooKeeperException {
        registerChildEvent(event.getPath());
    }

    @Override
    public void onChangedEvent(WatchedEvent event)
            throws MgmtZooKeeperException {
        if (LeaderState.isLeader()) {
            return;
        } else {
            registerChangedEvent(event.getPath());

            PhysicalMachineCluster pmCluster = pmClusterImo.getByPath(event
                    .getPath());
            if (null == pmCluster) {
                // this znode already removed.
                return;
            }

            zookeeper.reflectZkIntoMemory(pmCluster);
        }
    }

    @Override
    public void lock(String path) {
        String pmName = PathUtil.getPmNameFromPath(path);
        lockHelper.root(READ);
        lockHelper.pmList(READ).pm(WRITE, pmName);
    }

}

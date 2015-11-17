package com.navercorp.nbasearc.confmaster.server.cluster;

import static org.mockito.Mockito.spy;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.cluster.RedisServer;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandler;

public class PGSComponentMock {
    private WatchEventHandler pgsWatcher;
    private WatchEventHandler rsWatcher;
    private HBSession pgsHbcSession;
    private HBSession rsHbcSession;
    
    public PGSComponentMock(PartitionGroupServer pgs, RedisServer rs)
            throws MgmtZooKeeperException {
        // Mock up watchers
        setPgsWatcher(spy(pgs.getWatch()));
        pgs.setWatch(getPgsWatcher());
        getPgsWatcher().registerBoth(pgs.getPath());
        
        setRsWatcher(spy(rs.getWatch()));
        rs.setWatch(getRsWatcher() );
        getRsWatcher().registerBoth(rs.getPath());
        
        // Mock up HBCSessionWrapper
        setPgsHbcSession(spy(pgs.getHbc()));
        pgs.setHbc(getPgsHbcSession());

        setRsHbcSession(spy(rs.getHbc()));
        rs.setHbc(getRsHbcSession());
    }

    public WatchEventHandler getPgsWatcher() {
        return pgsWatcher;
    }

    public void setPgsWatcher(WatchEventHandler pgsWatcher) {
        this.pgsWatcher = pgsWatcher;
    }

    public WatchEventHandler getRsWatcher() {
        return rsWatcher;
    }

    public void setRsWatcher(WatchEventHandler rsWatcher) {
        this.rsWatcher = rsWatcher;
    }

    public HBSession getPgsHbcSession() {
        return pgsHbcSession;
    }

    public void setPgsHbcSession(HBSession pgsHbcSession) {
        this.pgsHbcSession = pgsHbcSession;
    }

    public HBSession getRsHbcSession() {
        return rsHbcSession;
    }

    public void setRsHbcSession(HBSession rsHbcSession) {
        this.rsHbcSession = rsHbcSession;
    }
}
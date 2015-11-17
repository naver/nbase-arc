package com.navercorp.nbasearc.confmaster.server.command;

import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;

public class ZooKeeperHolderFI extends ZooKeeperHolder {

    private Set<Integer> deleteChildErr = null;
    private Set<Integer> multiErr = null;
    
    private int deleteChildCnt = 0;
    private int multiCnt = 0;
    
    @Override
    public void deleteChildren(String path) throws MgmtZooKeeperException {
        setDeleteChildCnt(getDeleteChildCnt() + 1);
        if (deleteChildErr != null) {
            if (deleteChildErr.contains(getDeleteChildCnt())) {
                throw new MgmtZooKeeperException(new KeeperException.NoNodeException());
            }
        }
        super.deleteChildren(path);
    }

    @Override
    public List<OpResult> multi(Iterable<Op> ops) throws MgmtZooKeeperException {
        setMultiCnt(getMultiCnt() + 1);
        if (multiErr != null) {
            if (multiErr.contains(getMultiCnt())) {
                throw new MgmtZooKeeperException(new KeeperException.NotEmptyException());
            }
        }
        return super.multi(ops);
    }

    public Set<Integer> getDeleteChildErr() {
        return deleteChildErr;
    }

    public void setDeleteChildErr(Set<Integer> deleteChildErr) {
        this.deleteChildErr = deleteChildErr;
    }

    public Set<Integer> getMultiErr() {
        return multiErr;
    }

    public void setMultiErr(Set<Integer> multiErr) {
        this.multiErr = multiErr;
    }

    public int getDeleteChildCnt() {
        return deleteChildCnt;
    }

    public void setDeleteChildCnt(int deleteChildCnt) {
        this.deleteChildCnt = deleteChildCnt;
    }

    public int getMultiCnt() {
        return multiCnt;
    }

    public void setMultiCnt(int multiCnt) {
        this.multiCnt = multiCnt;
    }

}

package com.navercorp.nbasearc.confmaster.repository.dao;

import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.OpinionData;

public interface OpinionDao {

    List<OpinionData> getOpinions(final String path)
            throws MgmtZooKeeperException, NoNodeException;

}

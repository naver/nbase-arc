package com.navercorp.nbasearc.confmaster.repository.dao;

import org.apache.zookeeper.KeeperException.NoNodeException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.znode.ZkWorkflowLog;

public interface WorkflowLogDao {

    void initialize() throws MgmtZooKeeperException, NoNodeException;

    void log(long jobID, String severity, String name, String type,
            String clusterName, String msg, String infoAgs);

    void log(long jobID, String severity, String name, String type,
            String clusterName, String msg);

    void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object msgArg1, Object msgArg2);

    void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object msgArg1, String infoFmt,
            Object[] infoArgs);

    void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object[] msgArgs);

    void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object[] msgArgs,
            String infoFmt, Object[] infoArgs);

    ZkWorkflowLog getLog(long logNo) throws MgmtZooKeeperException;

    ZkWorkflowLog getLogFromBeginning(long offset)
            throws MgmtZooKeeperException, NoNodeException;

    boolean deleteLog(long boundaryNo);

    int getNumLogs();

    long getLast();

    long getNumOfStartLog();

    String isValidLogNo(long startNo, long endNo);

}

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

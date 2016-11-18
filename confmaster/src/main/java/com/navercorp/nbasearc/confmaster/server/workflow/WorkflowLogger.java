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

package com.navercorp.nbasearc.confmaster.server.workflow;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.PathUtil;

@Repository
public class WorkflowLogger {
    
    @Autowired
    private ZooKeeperHolder zk;
    
    @Autowired
    private Config config;

    private Integer numLogs;
    private Long numOfStartLog = 1L;

    private final String LOG = "LOG";
    private final String rootPathForLog = PathUtil.ccRootPath() + "/" + LOG;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    public synchronized void initialize() throws MgmtZooKeeperException,
            NoNodeException {
        try {
            String zeroLogNo = "0";
            zk.createPersistentZNode(
                    rootPathOfLog(), zeroLogNo.getBytes(config.getCharset()));
        } catch (NodeExistsException e) {
            // Ignore.
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unknown.");
        }
        
        Stat stat = new Stat();
        byte [] data = zk.getData(rootPathOfLog(), stat);
        String maxLogNo;
        try {
            maxLogNo = new String(data, config.getCharset());
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unkown");
        }
        numLogs = stat.getNumChildren();

        List<String> children = zk.getChildren(rootPathOfLog());
        List<Integer> logNoList = new ArrayList<Integer>();
        
        for(String strNo : children) {
            logNoList.add(Integer.valueOf(strNo));
        }
        
        Collections.sort(logNoList);

        Iterator<Integer> iter = logNoList.iterator();
        if (iter.hasNext()) {
            Integer logNo = iter.next();
            setNumOfStartLog(logNo);
        } else {
            setNumOfStartLog(Long.parseLong(maxLogNo) + 1L);
        }
        
        numLogs = children.size();
    }
    
    public synchronized void log(long jobID, String severity, String name,
            String type, String clusterName, String msg, String jsonArg) {
        while (numLogs >= config.getServerJobWorkflowLogMax()) {
            deleteLog(getNumOfStartLog());
        }
        
        byte[] data;
        String path = pathOfLog(getLast());
        List<Op> ops = new ArrayList<Op>();
        ZkWorkflowLog workflowLog = new ZkWorkflowLog(getLast(), new Date(),
                jobID, type, severity, name, msg, clusterName, jsonArg);
        
        try {
            data = mapper.writeValueAsBytes(workflowLog);
            Long maxLogNo = getNumOfStartLog() + getNumLogs();
            
            ops.add(Op.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            ops.add(Op.setData(rootPathOfLog(), 
                    String.valueOf(maxLogNo).getBytes(config.getCharset()), -1));
            
            List<OpResult> results = zk.multi(ops);
            zk.handleResultsOfMulti(results);
        } catch (Exception e) {
            Logger.error(workflowLog.toString(), e);
        }
        Logger.info(workflowLog.toStringWithoutInfo());
        
        increaseNumLogs();
    }

    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msg) {
        log(jobID, severity, name, type, clusterName, msg, "null");
    }

    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object msgArg1, Object msgArg2) {
        String message = MessageFormatter.format(msgFmt, msgArg1, msgArg2);
        log(jobID, severity, name, type, clusterName, message, "null");
    }

    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object msgArg1, String infoFmt,
            Object[] infoArgs) {
        String message = MessageFormatter.format(msgFmt, msgArg1);
        String additionalInfo = MessageFormatter.arrayFormat(infoFmt, infoArgs);
        log(jobID, severity, name, type, clusterName, message, additionalInfo);
    }

    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object[] msgArgs) {
        String message = MessageFormatter.arrayFormat(msgFmt, msgArgs);
        log(jobID, severity, name, type, clusterName, message, "null");
    }

    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object[] msgArgs,
            String infoFmt, Object[] infoArgs) {
        String message = MessageFormatter.arrayFormat(msgFmt, msgArgs);
        String additionalInfo = MessageFormatter.arrayFormat(infoFmt, infoArgs);
        log(jobID, severity, name, type, clusterName, message, additionalInfo);
    }

    public synchronized ZkWorkflowLog getLog(long logNo)
            throws MgmtZooKeeperException {
        String path = rootPathOfLog() + "/" + String.valueOf(logNo);
        
        Stat stat = new Stat();
        try {
            byte [] data = zk.getData(path, stat);
            ZkWorkflowLog log = mapper.readValue(data, new TypeReference<ZkWorkflowLog>() {});
            return log;
        } catch (NoNodeException e) {
            Logger.error("workflog log " + logNo + " is already deleted.");
            return null;
        }
    }

    public synchronized ZkWorkflowLog getLogFromBeginning(long offset)
            throws NoNodeException, MgmtZooKeeperException {
        long logNo = getNumOfStartLog() + offset;
        if (logNo >= this.getLast()) {
            return null;
        }
        
        String path = rootPathOfLog() + "/" + String.valueOf(logNo);
        
        Stat stat = new Stat();
        byte [] data = zk.getData(path, stat);
        ZkWorkflowLog log = mapper.readValue(data, new TypeReference<ZkWorkflowLog>() {});
        return log;
    }

    public synchronized boolean deleteLog(long boundaryNo) {
        if (boundaryNo < getNumOfStartLog() || getLast() <= boundaryNo) {
            return false;
        }
        
        String path = rootPathOfLog() + "/" + String.valueOf(getNumOfStartLog());
        increaseNumOfStartLog();
        
        try {
            zk.deleteZNode(path, -1);
        } catch (MgmtZooKeeperException e) {
            Logger.error(
                    "Delete workflow log in zookeeper fail. path: {}", path, e);
        }
        
        decreaseNumLogs();
        return true;
    }

    public synchronized int getNumLogs() {
        return numLogs;
    }
    
    private synchronized void increaseNumLogs() {
        numLogs ++;
    }
    
    private synchronized void decreaseNumLogs() {
        numLogs --;
    }

    public synchronized long getLast() {
        return numOfStartLog + numLogs;
    }

    public synchronized long getNumOfStartLog() {
        return numOfStartLog;
    }
    
    private synchronized void setNumOfStartLog(long numOfStartLog) {
        this.numOfStartLog = numOfStartLog;
    }
    
    private synchronized void increaseNumOfStartLog() {
        numOfStartLog ++;
    }

    public synchronized String isValidLogNo(long startNo, long endNo) {
        if (getNumLogs() == 0) {
            return "-ERR confmaster does not have any log.";
        }
        
        final long logStx = getNumOfStartLog();
        final long logEdx = logStx + getNumLogs() - 1;
        if (startNo > endNo) {
            return "-ERR start log number should be lower than end.";
        } else if (startNo < logStx) {
            return "-ERR start log no:" + logStx + ", end log no:" + logEdx;
        } else if (endNo > logEdx) {
            return "-ERR start log no:" + logStx + ", end log no:" + logEdx;
        }
        
        return null;
    }
    
    public String rootPathOfLog() {
        return rootPathForLog; 
    }
    
    public String pathOfLog(long logID) {
        return rootPathOfLog() + "/" + logID;
    }

    @JsonAutoDetect(
            fieldVisibility=Visibility.ANY, 
            getterVisibility=Visibility.NONE, 
            setterVisibility=Visibility.NONE)
    @JsonIgnoreProperties(
            ignoreUnknown=true)
    @JsonPropertyOrder(
            { "logID", "logTime", "jobID", "type", "name", "severity",
            "msg", "severity", "msg", "clusterName", "arguments" })
    public static class ZkWorkflowLog {
        
        @JsonProperty("logID")
        private long logID;
        @JsonProperty("logTime")
        private Date logTime;
        @JsonProperty("jobID")
        private long jobID;
        @JsonProperty("type")
        private String type;
        @JsonProperty("name")
        private String name;
        @JsonProperty("severity")
        private String severity;
        @JsonProperty("msg")
        private String msg;
        @JsonProperty("clusterName")
        private String clusterName;
        @JsonProperty("arguments")
        private String arguments;

        public ZkWorkflowLog() {
        }

        public ZkWorkflowLog(final long logID, 
                                final Date logTime,
                                final long jobID,
                                final String type, 
                                final String severity,
                                final String name, 
                                final String mgs, 
                                final String clusterName,
                                final String arguments) {
            this.setLogID(logID);
            this.setLogTime(logTime);
            this.setJobID(jobID);
            this.setType(type);
            this.setSeverity(severity);
            this.setName(name);
            this.setMsg(mgs);
            this.setClusterName(clusterName);
            this.setArguments(arguments);
        }

        public String toJsonString() {
            return String
                    .format("{\"logID\":%d,\"logTime\":\"%s\",\"jobID\":%d,\"type\":\"%s\",\"severity\":\"%s\",\"name\":\"%s\",\"msg\":\"%s\",\"clusterName\":\"%s\",\"arguments\":%s}",
                            getLogID(), getLogTime(), getJobID(), getType(),
                            getSeverity(), getName(), getMsg(), getClusterName(),
                            getArguments());
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(getSeverity() + " ");

            sb.append("MSG=\"" + getMsg() + "\", ");

            if (getClusterName() != null && getClusterName().length() > 0) {
                sb.append("CLUSTER=" + getClusterName() + ", ");
            }

            if (getArguments() != null && !getArguments().equals("null")
                    && getArguments().length() > 0) {
                sb.append("ARG=" + getArguments() + ", ");
            }

            sb.append("LID=" + getLogID() + ", ");

            return sb.toString();
        }
        
        public String toStringWithoutInfo() {
            StringBuilder sb = new StringBuilder();

            sb.append(getSeverity() + " ");

            sb.append("MSG=\"" + getMsg() + "\", ");

            if (getClusterName() != null && getClusterName().length() > 0) {
                sb.append("CLUSTER=" + getClusterName() + ", ");
            }

            sb.append("LID=" + getLogID() + ", ");

            return sb.toString();
        }

        public long getLogID() {
            return logID;
        }

        public void setLogID(long logID) {
            this.logID = logID;
        }

        public Date getLogTime() {
            return logTime;
        }

        public void setLogTime(Date logTime) {
            this.logTime = logTime;
        }

        public long getJobID() {
            return jobID;
        }

        public void setJobID(long jobID) {
            this.jobID = jobID;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSeverity() {
            return severity;
        }

        public void setSeverity(String severity) {
            this.severity = severity;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public String getArguments() {
            return arguments;
        }

        public void setArguments(String arguments) {
            this.arguments = arguments;
        }

    }

}

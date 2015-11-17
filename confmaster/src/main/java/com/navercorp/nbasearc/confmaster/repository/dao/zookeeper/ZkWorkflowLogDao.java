package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

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
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.ZkWorkflowLog;

@Repository
public class ZkWorkflowLogDao implements WorkflowLogDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;
    
    @Autowired
    private Config config;

    private Integer numLogs;
    private Long numOfStartLog = 1L;

    private final String LOG = "LOG";
    private final String rootPathForLog = PathUtil.ccRootPath() + "/" + LOG;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    @Override
    public synchronized void initialize() throws MgmtZooKeeperException,
            NoNodeException {
        try {
            String zeroLogNo = "0";
            zookeeper.createPersistentZNode(
                    rootPathOfLog(), zeroLogNo.getBytes(config.getCharset()));
        } catch (NodeExistsException e) {
            // Ignore.
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unknown.");
        }
        
        Stat stat = new Stat();
        byte [] data = zookeeper.getData(rootPathOfLog(), stat);
        String maxLogNo;
        try {
            maxLogNo = new String(data, config.getCharset());
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(config.getCharset() + " is unkown");
        }
        numLogs = stat.getNumChildren();

        List<String> children = zookeeper.getChildren(rootPathOfLog());
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
    
    @Override
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
            
            ZooKeeper zk = zookeeper.getZooKeeper();
            List<OpResult> results = zk.multi(ops);
            zookeeper.handleResultsOfMulti(results);
        } catch (Exception e) {
            Logger.error(workflowLog.toString(), e);
        }
        Logger.info(workflowLog.toStringWithoutInfo());
        
        increaseNumLogs();
    }

    @Override
    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msg) {
        log(jobID, severity, name, type, clusterName, msg, "null");
    }

    @Override
    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object msgArg1, Object msgArg2) {
        String message = MessageFormatter.format(msgFmt, msgArg1, msgArg2);
        log(jobID, severity, name, type, clusterName, message, "null");
    }

    @Override
    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object msgArg1, String infoFmt,
            Object[] infoArgs) {
        String message = MessageFormatter.format(msgFmt, msgArg1);
        String additionalInfo = MessageFormatter.arrayFormat(infoFmt, infoArgs);
        log(jobID, severity, name, type, clusterName, message, additionalInfo);
    }

    @Override
    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object[] msgArgs) {
        String message = MessageFormatter.arrayFormat(msgFmt, msgArgs);
        log(jobID, severity, name, type, clusterName, message, "null");
    }

    @Override
    public void log(long jobID, String severity, String name, String type,
            String clusterName, String msgFmt, Object[] msgArgs,
            String infoFmt, Object[] infoArgs) {
        String message = MessageFormatter.arrayFormat(msgFmt, msgArgs);
        String additionalInfo = MessageFormatter.arrayFormat(infoFmt, infoArgs);
        log(jobID, severity, name, type, clusterName, message, additionalInfo);
    }

    @Override
    public synchronized ZkWorkflowLog getLog(long logNo)
            throws MgmtZooKeeperException {
        String path = rootPathOfLog() + "/" + String.valueOf(logNo);
        
        Stat stat = new Stat();
        try {
            byte [] data = zookeeper.getData(path, stat);
            ZkWorkflowLog log = mapper.readValue(data, new TypeReference<ZkWorkflowLog>() {});
            return log;
        } catch (NoNodeException e) {
            Logger.error("workflog log " + logNo + " is already deleted.");
            return null;
        }
    }

    @Override
    public synchronized ZkWorkflowLog getLogFromBeginning(long offset)
            throws NoNodeException, MgmtZooKeeperException {
        long logNo = getNumOfStartLog() + offset;
        if (logNo >= this.getLast()) {
            return null;
        }
        
        String path = rootPathOfLog() + "/" + String.valueOf(logNo);
        
        Stat stat = new Stat();
        byte [] data = zookeeper.getData(path, stat);
        ZkWorkflowLog log = mapper.readValue(data, new TypeReference<ZkWorkflowLog>() {});
        return log;
    }

    @Override
    public synchronized boolean deleteLog(long boundaryNo) {
        if (boundaryNo < getNumOfStartLog() || getLast() <= boundaryNo) {
            return false;
        }
        
        String path = rootPathOfLog() + "/" + String.valueOf(getNumOfStartLog());
        increaseNumOfStartLog();
        
        try {
            zookeeper.deleteZNode(path, -1);
        } catch (MgmtZooKeeperException e) {
            Logger.error(
                    "Delete workflow log in zookeeper fail. path: {}", path, e);
        }
        
        decreaseNumLogs();
        return true;
    }

    @Override
    public synchronized int getNumLogs() {
        return numLogs;
    }
    
    private synchronized void increaseNumLogs() {
        numLogs ++;
    }
    
    private synchronized void decreaseNumLogs() {
        numLogs --;
    }

    @Override
    public synchronized long getLast() {
        return numOfStartLog + numLogs;
    }

    @Override
    public synchronized long getNumOfStartLog() {
        return numOfStartLog;
    }
    
    private synchronized void setNumOfStartLog(long numOfStartLog) {
        this.numOfStartLog = numOfStartLog;
    }
    
    private synchronized void increaseNumOfStartLog() {
        numOfStartLog ++;
    }

    @Override
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

}

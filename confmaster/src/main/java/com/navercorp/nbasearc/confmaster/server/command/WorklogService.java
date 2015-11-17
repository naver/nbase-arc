package com.navercorp.nbasearc.confmaster.server.command;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.dao.zookeeper.ZkWorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.repository.znode.ZkWorkflowLog;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;

@Service
public class WorklogService {

    @Autowired
    private ZkWorkflowLogDao workflowLogDao;

    @CommandMapping(name="worklog_info", usage="worklog_info")
    public String worklogInfo() throws JsonParseException, JsonMappingException,
            KeeperException, InterruptedException, IOException {
        if (workflowLogDao.getNumLogs() == 0) {
            return "-ERR confmaster does not have any log.";
        }
        
        final long logStx = workflowLogDao.getNumOfStartLog();
        final long logEdx = logStx + workflowLogDao.getNumLogs() - 1;
        
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"start\":");
        sb.append(logStx);
        sb.append(",\"end\":");
        sb.append(logEdx);
        sb.append("}");
        
        return sb.toString();
    }
    
    @LockMapping(name="worklog_info")
    public void worklogInfoLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
    @CommandMapping(name="worklog_get", 
            usage="worklog_get <start log number> <end log number>\r\n" +
                    "get workflow logs")
    public String worklogGet(Long requestedLogStartNo, Long requestedLogEndNo)
            throws MgmtZooKeeperException {
        String err = workflowLogDao.isValidLogNo(requestedLogStartNo, requestedLogEndNo);
        if (null != err) {
            return err;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("{\"list\":[");
        
        long deletedNo = -1;
        long no = requestedLogStartNo;
        for (; no <= requestedLogEndNo ; no ++) {
            ZkWorkflowLog log = workflowLogDao.getLog(no);
            if (null == log) {
                deletedNo = no;
                break;
            }
            
            sb.append(log.toJsonString());
            sb.append(", ");
        }

        if (sb.length() > "{\"list\":[".length()) {
            sb.delete(sb.length() - 2, sb.length());
        }
        
        if (-1 != deletedNo) {
            sb.append("],\"msg\":\"workflog log " + deletedNo
                    + " is already deleted.\"}");
        } else {
            sb.append("]}");
        }

        return sb.toString();
    }

    @LockMapping(name="worklog_get")
    public void worklogGetLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
    @CommandMapping(name="worklog_head",
            usage="worklog_head <the number of logs>\r\n" +
                    "get and delete workflow logs from beginning")
    public String worklogHead(Long logCount) throws NoNodeException,
            MgmtZooKeeperException {
        if (workflowLogDao.getNumLogs() == 0) {
            return "-ERR confmaster does not have any log.";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{\"list\":[");
        
        long offset = 0;
        for (; offset < logCount; offset++) {
            ZkWorkflowLog log = workflowLogDao.getLogFromBeginning(offset);
            if (null == log) {
                break;
            }
            sb.append(log.toJsonString());
            sb.append(", ");
        }
        
        if (offset > 0) {
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append("]}");

        return sb.toString();
    }

    @LockMapping(name="worklog_head")
    public void worklogHeadLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
    @CommandMapping(name="worklog_del",
            usage="worklog_del <max log nubmer>\r\n" +
                    "delete workflow logs")
    public String worklogDel(Long maxLogNo) {
        if (workflowLogDao.getNumLogs() == 0) {
            return "-ERR confmaster does not have any log.";
        }
        
        while (true) {
            boolean ret = workflowLogDao.deleteLog(maxLogNo);
            if (!ret) {
                break;
            }
        }
        
        return Constant.S2C_OK;
    }

    @LockMapping(name="worklog_del")
    public void worklogDelLock(HierarchicalLockHelper lockHelper) {
        // Do nothing...
    }
    
}

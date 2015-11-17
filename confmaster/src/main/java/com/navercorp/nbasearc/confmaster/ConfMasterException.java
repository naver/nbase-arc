package com.navercorp.nbasearc.confmaster;

import static com.navercorp.nbasearc.confmaster.Constant.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.OpResult;

import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.server.mapping.WorkflowCaller;

@SuppressWarnings("serial")
public class ConfMasterException extends Exception {
    
    private List<OpResult> results;

    public ConfMasterException(String errMsg) {
        super(errMsg); 
    }

    public ConfMasterException(String errMsg, Throwable e) {
        super(errMsg, e);
    }

    void setMultiResults(List<OpResult> results) {
        this.results = results;
    }

    public List<OpResult> getResults() {
        return results != null ? new ArrayList<OpResult>(results) : null;
    }

    public static class MgmtZooKeeperException extends Exception {
        public MgmtZooKeeperException(String msg) {
            super(msg);
        }
        
        public MgmtZooKeeperException(Throwable e) {
            super(e);
        }
    }
    
    public static class MgmtZNodeAlreayExistsException extends Exception {
        private final String path;
        private final String msg;
        
        public MgmtZNodeAlreayExistsException(String path, String msg) {
            this.path = path;
            this.msg = msg;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + ". path: " + path + ", msg: " + getMsg();
        }

        public String getMsg() {
            return msg;
        }
    }
    
    public static class MgmtZNodeDoesNotExistException extends Exception {
        private final String path;
        private final String msg;
        
        public MgmtZNodeDoesNotExistException(String path, String msg) {
            this.path = path;
            this.msg = msg;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + ". path: " + path + ", msg: " + getMsg();
        }

        public String getMsg() {
            return msg;
        }
    }
    
    public static class MgmtUnkownRoleException extends Exception {
        private final String path;
        private final String ip;
        private final int port;
        private final String role;
        
        public MgmtUnkownRoleException(String path, String ip, int port, String role) {
            this.path = path;
            this.ip = ip;
            this.port = port;
            this.role = role;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + ". path: " + path + ", ip: " + ip
                    + ", port: " + port + ", role: " + role;
        }
    }
    
    public static class MgmtUnexpectedStateTransitionException extends Exception {
        private final String msg;
        
        public MgmtUnexpectedStateTransitionException(String msg) {
            this.msg = msg;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + ". " + msg;
        }
    }
    
    public static class MgmtRoleChangeException extends Exception {
        private final String msg;
        
        public MgmtRoleChangeException(String msg) {
            this.msg = msg;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + ". " + msg;
        }
    }
    
    public static class MgmtCommandNotFoundException extends Exception {
    }

    public static class MgmtCommandWrongArgumentException extends Exception {
        private final String usage;
        
        public MgmtCommandWrongArgumentException(String usage) {
            this.usage = usage;
        }

        @Override
        public String getMessage() {
            return EXCEPTIONMSG_WRONG_NUMBER_ARGUMENTS;
        }
        
        public String getUsage() {
            return usage;
        }
    }
    
    public static class MgmtWorkflowNotFoundException extends Exception {
        private WorkflowCaller caller;
        
        public MgmtWorkflowNotFoundException(WorkflowCaller caller) {
            this.caller = caller;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + " " + caller;
        }
    }
    
    public static class MgmtWorkflowWrongArgumentException extends Exception {
        private WorkflowCaller caller;
        
        public MgmtWorkflowWrongArgumentException(WorkflowCaller caller) {
            this.caller = caller;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + " " + caller;
        }
    }
    
    public static class MgmtPrivilegeViolationException extends Exception {
        private String message;
        
        public MgmtPrivilegeViolationException(String message) {
            this.message = message;
        }
        
        @Override
        public String getMessage() {
            return getClass().getName() + " " + message;
        }
    }
    
    public static class MgmtInvalidQuorumPolicyException extends Exception {
    }
    
    public static class MgmtDuplicatedReservedCallException extends Exception {
        public MgmtDuplicatedReservedCallException(String message) {
            super(message);
        }
    }
    
    public static class MgmtHbException extends Exception {
        public MgmtHbException(String message) {
            super(message);
        }
    }
}

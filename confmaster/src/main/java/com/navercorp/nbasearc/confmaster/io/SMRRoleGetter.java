package com.navercorp.nbasearc.confmaster.io;

import java.util.concurrent.Callable;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;

public class SMRRoleGetter implements Callable<SMRRoleGetter.Result> {
    
    private String clusterName;
    private PartitionGroupServer pgs;
    private String command;
    private Result result = new Result();

    public SMRRoleGetter(String clusterName, PartitionGroupServer pgs,
            String command) {
        this.clusterName = clusterName;
        this.setPGS(pgs);
        this.command = command;
        this.result.setReply("");
        this.result.setPGS(pgs);
    }

    @Override
    public SMRRoleGetter.Result call() {
        if (getPGS() == null) {
            Logger.error(
                "Send smr command fail. smr variable is null. cluster: {}, command: \"{}\"",
                clusterName, command);
            result.setResult(Constant.ERROR);
            return result;
        }
        
        PartitionGroupServer.RealState realState = getPGS().getRealState();
        
        if (!realState.isSuccess()) {
            result.setResult(Constant.ERROR);
            
            Logger.info("Send smr command fail. {} {}:{}, cmd: \"{}\", reply: \"{}\"", 
                    new Object[]{getPGS(), getPGS().getIP(),
                            getPGS().getPort(), command, result.getReply()});
            
            return result;
        } else {
            this.result.setRole(realState.getRole());
            this.result.setStateTimestamp(realState.getStateTimestamp());
            
            Logger.info("Send smr command success. {} {}:{}, cmd: \"{}\", reply: \"{}\"", 
                    new Object[]{getPGS(), getPGS().getIP(),
                    getPGS().getPort(), command, result.getReply()});
        }

        result.setResult(Constant.S2C_OK);
        return result;
    }
        
    public PartitionGroupServer getPGS() {
        return pgs;
    }

    public void setPGS(PartitionGroupServer pgs) {
        this.pgs = pgs;
    }
    
    public class Result {
        private String result;
        private String reply;
        private String role;
        private Long stateTimestamp;
        private PartitionGroupServer pgs;

        public String getReply() {
            return reply;
        }

        public void setReply(String reply) {
            this.reply = reply;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }

        public Long getStatetimestamp() {
            return stateTimestamp;
        }

        public void setStateTimestamp(Long stateTimestamp) {
            this.stateTimestamp = stateTimestamp;
        }

        public PartitionGroupServer getPGS() {
            return pgs;
        }

        public void setPGS(PartitionGroupServer pgs) {
            this.pgs = pgs;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }
    }

}

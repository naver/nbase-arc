package com.navercorp.nbasearc.confmaster.server.cluster;

import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_NO;
import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_YES;
import static com.navercorp.nbasearc.confmaster.Constant.LOG_TYPE_WORKFLOW;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_PING;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_RESPONSE_OK;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_LCONN_IN_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_MASTER;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_MASTER_IN_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_NONE;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_NONE_IN_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE;
import static com.navercorp.nbasearc.confmaster.Constant.PGS_ROLE_SLAVE_IN_PONG;
import static com.navercorp.nbasearc.confmaster.Constant.S2C_OK;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_LCONN;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_NORMAL;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_UNKNOWN;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MAJOR;
import static com.navercorp.nbasearc.confmaster.Constant.SEVERITY_MODERATE;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.type.TypeReference;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtRoleChangeException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBRefData;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup.SlaveJoinInfo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.workflow.FailoverPGSWorkflow;

public class PartitionGroupServer extends ZNode<PartitionGroupServerData>
        implements HeartbeatTarget {
    
    private BlockingSocket serverConnection;
    private String clusterName;
    
    private HBSession hbc;
    private HBRefData hbcRefData;
    private UsedOpinionSet usedOpinions;
    private FailoverPGSWorkflow.TransitionType todoForFailover = 
            FailoverPGSWorkflow.TransitionType.TODO_UNKOWN;
    
    private final PartitionGroupDao pgDao;
    private final PartitionGroupServerDao pgsDao;
    private final Config config;

    public PartitionGroupServer(ApplicationContext context, String path,
            String name, String cluster, byte[] data) {
        super(context);
        this.pgDao = context.getBean(PartitionGroupDao.class);
        this.pgsDao = context.getBean(PartitionGroupServerDao.class);
        this.config = context.getBean(Config.class);
        
        setTypeRef(new TypeReference<PartitionGroupServerData>(){});
        
        setPath(path);
        setName(name);
        setClusterName(cluster);
        setNodeType(NodeType.PGS);
        setData(data);
        
        setServerConnection(
            new BlockingSocket(
                getData().getPmIp(), getData().getSmrMgmtPort(), 
                config.getClusterPgsTimeout(), PGS_PING, 
                config.getDelim(), config.getCharset()));
        
        hbcRefData = new HBRefData();
        hbcRefData.setZkData(getData().getState(), getData().getStateTimestamp(), stat.getVersion())
                  .setLastState(SERVER_STATE_UNKNOWN)
                  .setLastStateTimestamp(0L)
                  .setSubmitMyOpinion(false);
        
        usedOpinions = new UsedOpinionSet();
    }
    
    /*
     * Return true, if a master PGS and a slave PGS are in the same machine and
     * both are in the same PG. For instance, If an instance of this class and
     * Master PGS are in the same machine than this function return true,
     * otherwise return false.
     */
    public boolean isMasterInSameMachine(
            PhysicalMachineClusterImo pmClusterImo,
            PartitionGroupServerImo pgsImo) {
        final String machineName = getData().getPmName();
        final PhysicalMachineCluster machineInfo = 
                pmClusterImo.get(getClusterName(), machineName);
        final List<Integer> localPgsIdList = machineInfo.getData().getPgsIdList();
        
        for (final Integer id : localPgsIdList) {
            PartitionGroupServer anotherPGS = 
                    pgsImo.get(String.valueOf(id), getClusterName());
            if (getData().getPgId() == anotherPGS.getData().getPgId()
                    && anotherPGS.getData().getRole().equals(PGS_ROLE_MASTER)
                    && machineName.equals(anotherPGS.getData().getPmName())) {
                return true;
            }
        }
        return false;
    }
    
    public void updateHBRef() {
        hbcRefData.setZkData(
                getData().getState(), getData().getStateTimestamp(), stat.getVersion());
        getHbc().updateState(getData().getHb());
    }

    public void release() {
        try {
            getHbc().updateState(HB_MONITOR_NO);
            getServerConnection().close();
        } catch (Exception e) {
            Logger.error("stop heartbeat fail. PGS:" + getName(), e);
        }

        try {
            getHbc().callbackDelete();
            getHbc().urgent();
        } catch (Exception e) {
            Logger.error("failed while delete pgs. " + toString(), e);
        }
    }

    public String executeQuery(String query) throws IOException {
        return getServerConnection().execute(query);
    }

    public String executeQuery(String query, int retryCount) throws IOException {
        return getServerConnection().execute(query, retryCount);
    }
    
    public void closeConnection() throws IOException {
        getServerConnection().close();
    }
    
    public long getActiveStateTimestamp() throws IOException {
        String response = executeQuery(PGS_PING);

        String[] resAry = response.split(" ");
        if (resAry.length < 3)
        {
            Logger.error("Invalid response. PGS_ID=" + getName() +
                    ", IP=" + getData().getPmIp() +
                    ", PORT=" + getData().getSmrBasePort() +
                    ", RESPONSE=" + response);
            return -1;
        }
        
        return Long.parseLong(resAry[2]);
    }
    
    public String getActiveRole() throws IOException {
        String response = executeQuery(PGS_PING);

        String[] resAry = response.split(" ");
        if (resAry.length < 3)
        {
            Logger.error("Invalid response. PGS_ID=" + getName() +
                    ", IP=" + getData().getPmIp() +
                    ", PORT=" + getData().getSmrBasePort() +
                    ", RESPONSE=" + response);
            return "";
        }
        
        return resAry[1];
    }

    @Override
    public int getVersion() {
        return this.stat.getVersion();
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String getState() {
        return this.getData().getState();
    }
    
    @Override
    public void setState(String state, long state_timestamp) {
        PartitionGroupServerData modified = 
             PartitionGroupServerData.builder()
                .from(getData()).withState(state).withStateTimestamp(state_timestamp).build();
        this.setData(modified);
    }
    
    @Override
    public long getStateTimestamp() {
        return getData().getStateTimestamp();
    }
    
    @Override
    public String getTargetOfHeartbeatPath() {
        return this.getPath();
    }
    
    @Override
    public String getHB() {
        return this.getData().getHb();
    }
    
    @Override
    public boolean isHBCResponseCorrect(String recvedLine) {
        return recvedLine.contains(PGS_RESPONSE_OK);
    }
    
    @Override
    public String getPingMsg() {
        return PGS_PING + "\r\n";
    }
    
    @Override
    public HBRefData getRefData() {
        return hbcRefData;
    }

    @Override
    public String getIP() {
        return getData().getPmIp();
    }

    @Override
    public int getPort() {
        return getData().getSmrMgmtPort();
    }

    @Override
    public UsedOpinionSet getUsedOpinions() {
        return usedOpinions;
    }

    public FailoverPGSWorkflow.TransitionType getTodoForFailover() {
        return this.todoForFailover;
    }

    public void setTodoForFailover(FailoverPGSWorkflow.TransitionType todoForFailover) {
        this.todoForFailover = todoForFailover;
    }

    public void syncLastState() {
        hbcRefData.setLastState(getData().getState())
            .setLastStateTimestamp(getData().getStateTimestamp());
    }

    @Override
    @SuppressWarnings("unchecked")
    public ZNode<PartitionGroupServerData> getZNode() {
        return this;
    }

    public HBSession getHbc() {
        return hbc;
    }

    public void setHbc(HBSession hbc) {
        this.hbc = hbc;
    }

    public BlockingSocket getServerConnection() {
        return serverConnection;
    }

    public void setServerConnection(BlockingSocket serverConnection) {
        this.serverConnection = serverConnection;
    }

    public static class RealState {
        private final boolean success;
        private final String state;
        private final String role;
        private final Long stateTimestamp;
        
        public RealState(boolean success, String state, String role, Long stateTimestamp) {
            this.success = success;
            this.state = state;
            this.role = role;
            this.stateTimestamp = stateTimestamp;
        }
        
        public String getState() { 
            return state;
        }
        
        public String getRole() {
            return role;
        }
        
        public Long getStateTimestamp() {
            return stateTimestamp;
        }

        public boolean isSuccess() {
            return success;
        }

        @Override
        public String toString() {
            return "role: " + getRole() + ", state: " + getState()
                    + ", timestamp: " + getStateTimestamp();
        }
    }

    public boolean isAvailable() {
        return (getData().getState().equals(SERVER_STATE_NORMAL) ||
                getData().getState().equals(SERVER_STATE_LCONN)) &&
               getData().getHb().equals(HB_MONITOR_YES);
    }
    
    public RealState getRealState() {        
        try {
            String cmd = "ping";
            String reply = executeQuery(cmd);
            Logger.debug("CMD=\"" + cmd + 
                    "\", REPLY=\"" + reply + 
                    "\", CLUSTER:" + getClusterName() + 
                    ", PGS:" + getName() + 
                    ", STATE:" + getData().getState() + 
                    ", ROLE:" + getData().getRole());
    
            String[] tokens = reply.split(" ");
            String role;
            String state;
            if (tokens[1].equals(PGS_ROLE_NONE_IN_PONG)) {
                state = SERVER_STATE_FAILURE;
                role = PGS_ROLE_NONE;
            } else if (tokens[1].equals(PGS_ROLE_LCONN_IN_PONG)) {
                state = SERVER_STATE_LCONN;
                role = PGS_ROLE_NONE;
            } else if (tokens[1].equals(PGS_ROLE_MASTER_IN_PONG)) {
                state = SERVER_STATE_NORMAL;
                role = PGS_ROLE_MASTER;
            } else if (tokens[1].equals(PGS_ROLE_SLAVE_IN_PONG)) {
                state = SERVER_STATE_NORMAL;
                role = PGS_ROLE_SLAVE;
            } else {
                state = SERVER_STATE_FAILURE;
                role = PGS_ROLE_NONE;
            }
            
            final Long stateTimestamp = Long.parseLong(tokens[2]);
            return new RealState(true, state, role, stateTimestamp);
        } catch (IOException e) {
            return new RealState(false, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                    getStateTimestamp());
        }
    }
    
    public LogSequence getLogSeq() {
        if (!getData().getState().equals(SERVER_STATE_LCONN) &&
                !getData().getState().equals(SERVER_STATE_NORMAL)) {
            Logger.error("getseq Logger fail. target pgs if not available" +
                    "CLUSTER=" + getClusterName() + 
                    ", PG=" + getData().getPgId() + 
                    ", PGS=" + getName() +
                    ", IP=" + getData().getPmIp() +
                    ", PORT=" + getData().getSmrBasePort());
            return null;
        }
        
        LogSequence logSeq = new LogSequence();
        try {
            logSeq.initialize(this);
        } catch (IOException e) {
            Logger.error("getseq Logger fail. " +
                    "CLUSTER=" + getClusterName() + 
                    ", PG=" + getData().getPgId() + 
                    ", PGS=" + getName() +
                    ", IP=" + getData().getPmIp() +
                    ", PORT=" + getData().getSmrBasePort(), e);
            return null;
        }
        
        return logSeq;
    }
    
    public void roleMaster(PartitionGroup pg,
            List<PartitionGroupServer> pgsList,
            Map<String, LogSequence> logSeqMap, Cluster cluster,
            int quorum, long jobID, WorkflowLogDao workflowLogDao)
            throws MgmtRoleChangeException {
        final LogSequence targetLogSeq = logSeqMap.get(getName());
        final String cmd = "role master " + getName() + " " + quorum
                + " " + targetLogSeq.getMax();
        
        if (!pg.checkJoinConstraintMaster(this, targetLogSeq, jobID, workflowLogDao)) {
            String infoFmt = "{\"PGS\":{},\"IP\":\"{}\",\"PORT\":\"{}\"}";
            Object[] infoArgs = new Object[]{getName(), getData().getPmIp(), 
                    getData().getSmrBasePort()};
            
            workflowLogDao.log(
                    jobID, SEVERITY_MAJOR, "RoleMaster", 
                    LOG_TYPE_WORKFLOW, getClusterName(), 
                    "Role master failed, it has invalid mgen. {}, msg: pg({}), pgs({}), cseq: pg({}), pgs(cseq{} maxseq{})",
                    new Object[] { this, pg.getData().currentGen(),
                            getData().getMasterGen(),
                            pg.getData().currentSeq(), targetLogSeq.getLogCommit(), targetLogSeq.getMax() }, 
                    infoFmt, infoArgs);
            throw new MgmtRoleChangeException("Role master fail. "
                    + MessageFormatter.arrayFormat(infoFmt, infoArgs));
        }
        
        try {
            workflowLogDao.log(jobID,
                    SEVERITY_MODERATE, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "try role master. cmd: " + cmd);
            
            // Send 'role master' command
            String reply = executeQuery(cmd);

            // Make information for logging
            String infoFmt = 
                    "{\"PGS\":{},\"IP\":\"{}\",\"PORT\":\"{}\",\"CMD\":\"{}\",\"REPLY\":\"{}\"}";
            Object[] infoArgs = new Object[] { getName(), getData().getPmIp(),
                    getData().getSmrBasePort(), cmd, reply };
            
            // Check result
            if (!reply.equals(S2C_OK)) {
                workflowLogDao.log(jobID,
                        SEVERITY_MAJOR, "RoleMaster",
                        LOG_TYPE_WORKFLOW, getClusterName(),
                        "Role master fail. {}, cmd: '{}', reply: '{}'", 
                        new Object[]{this, cmd, reply}, 
                        infoFmt, infoArgs);
                throw new MgmtRoleChangeException("Role master fail. "
                        + MessageFormatter.arrayFormat(infoFmt, infoArgs));
            } else {
                workflowLogDao.log(jobID,
                        SEVERITY_MAJOR, "RoleMaster",
                        LOG_TYPE_WORKFLOW, getClusterName(),
                        "Role master success. {}, cmd: '{}', reply: '{}'", 
                        new Object[]{this, cmd, reply},  
                        infoFmt, infoArgs);
            }
        } catch (IOException e) {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "Role master error. {}, cmd: '{}', exception: '{}'", 
                    new Object[]{this, cmd, e.getMessage()});
            throw new MgmtRoleChangeException("Master election Error. " + e.getMessage());
        }

        // Update state data
        long stateTimestamp = getData().getStateTimestamp() + 1;
        try {
            closeConnection();
            stateTimestamp = getActiveStateTimestamp();
        } catch (IOException e) {
            throw new MgmtRoleChangeException("Master election Error, " + e.getMessage());
        } finally {
            try {
                PartitionGroupData pgModified = 
                    PartitionGroupData.builder().from(pg.getData())
                        .addMasterGen(targetLogSeq.getMax())
                        .withCopy(pgsList.size()).withQuorum(quorum).build();
    
                pgDao.updatePg(pg.getPath(), pgModified);
                
                pg.setData(pgModified);
            } catch (Exception e) {
                workflowLogDao.log(jobID,
                        SEVERITY_MAJOR, "RoleMaster",
                        LOG_TYPE_WORKFLOW, getClusterName(),
                        "Role master success, but update zookeeper error. {}, exception: '{}'", 
                        this, e.getMessage());
                throw new MgmtRoleChangeException("Master election Error, " + e.getMessage());
            }
            
            try {
                PartitionGroupServerData pgsModified = 
                    PartitionGroupServerData.builder().from(getData())
                        .withMasterGen(pg.getData().currentGen())
                        .withRole(PGS_ROLE_MASTER)
                        .withOldRole(PGS_ROLE_MASTER)
                        .withState(SERVER_STATE_NORMAL)
                        .withStateTimestamp(stateTimestamp).build();
            
                pgsDao.updatePgs(getPath(), pgsModified);
                
                setData(pgsModified);
            } catch (Exception e) {
                workflowLogDao.log(jobID,
                        SEVERITY_MAJOR, "RoleMaster",
                        LOG_TYPE_WORKFLOW, getClusterName(),
                        "Role master success, but update zookeeper error. {}, exception: '{}'", 
                        this, e.getMessage());
                throw new MgmtRoleChangeException("Master election Error, " + e.getMessage());
            }
        }
    }

    public void roleSlave(PartitionGroup pg, LogSequence targetLogSeq,
            PartitionGroupServer master, long jobID,
            WorkflowLogDao workflowLogDao) throws MgmtRoleChangeException {
        // Get commit sequence
        SlaveJoinInfo joinInfo = pg.checkJoinConstraintSlave(this, targetLogSeq, jobID, workflowLogDao);
        if (!joinInfo.isSuccess()) {
            String infoFmt = "{\"PGS\":{},\"IP\":\"{}\",\"PORT\":\"{}\"}";
            Object[] infoArgs = new Object[]{getName(), getData().getPmIp(), 
                    getData().getSmrBasePort()};
            
            workflowLogDao.log(
                    jobID, SEVERITY_MAJOR, "RoleSlave", 
                    LOG_TYPE_WORKFLOW, getClusterName(), 
                    "Slave join failed, it has invalid mgen. {}, msg: pg({}), pgs({}), cseq: pg({}), pgs({})",
                    new Object[] { this, pg.getData().currentGen(),
                            getData().getMasterGen(),
                            pg.getData().currentSeq(), targetLogSeq.getLogCommit() }, 
                    infoFmt, infoArgs);
            throw new MgmtRoleChangeException("Slave join Error. PGS '" + getName()
                    + "' has invalid commit sequence.");
        }
        
        // Make 'role slave' command
        String cmd = "role slave " + getName() + " "
                + master.getData().getPmIp() + " "
                + master.getData().getSmrBasePort() + " "
                + joinInfo.getJoinSeq();
        try {
            workflowLogDao.log(jobID,
                    SEVERITY_MODERATE, "RolsSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "try slave join. cmd: " + cmd);
            
            // Send 'role slave' command
            String reply = executeQuery(cmd);

            // Make information for logging
            String infoFmt = 
                    "{\"PGS\":{},\"IP\":\"{}\",\"PORT\":\"{}\",\"CMD\":\"{}\",\"REPLY\":\"{}\"}";
            Object[] infoArgs = new Object[] { getName(), getData().getPmIp(),
                    getData().getSmrBasePort(), cmd, reply };
            
            // Check result
            if (!reply.equals(S2C_OK)) {
                Logger.info("", 
                        new Object[]{this, cmd, reply});
                workflowLogDao.log(jobID,
                        SEVERITY_MAJOR, "RoleSlave",
                        LOG_TYPE_WORKFLOW, getClusterName(),
                        "Slave join fail. {}, cmd:\"{}\", reply:\"{}\"",
                        new Object[]{this, cmd, reply},
                        infoFmt, infoArgs);
                throw new MgmtRoleChangeException("Slave join Fail. " + 
                        MessageFormatter.arrayFormat(infoFmt, infoArgs));
            } else {
                workflowLogDao.log(jobID,
                        SEVERITY_MAJOR, "RoleSlave",
                        LOG_TYPE_WORKFLOW, getClusterName(),
                        "Slave join success. {}, cmd:\"{}\", reply:\"{}\"",
                        new Object[]{this, cmd, reply},
                        infoFmt, infoArgs);
            }
        } catch (IOException e) {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "Slave join error. {}, cmd: \"{}\", exception: \"{}\"",
                    new Object[]{this, cmd, e.getMessage()});
            throw new MgmtRoleChangeException("Slave join Error. " + e.getMessage());
        }
        
        long stateTimestamp = getData().getStateTimestamp() + 1;
        try {
            Logger.info("Role changed, {}, role: {}->{}", 
                    new Object[]{this, getData().getRole(), PGS_ROLE_SLAVE});
            closeConnection();
            stateTimestamp = getActiveStateTimestamp();
        } catch (IOException e) {
            throw new MgmtRoleChangeException("Slave join Error, " + e.getMessage());
        } finally {
            PartitionGroupServerData pgsModified = 
                PartitionGroupServerData.builder().from(getData())
                    .withMasterGen(master.getData().getMasterGen())
                    .withRole(PGS_ROLE_SLAVE)
                    .withOldRole(PGS_ROLE_SLAVE)
                    .withState(SERVER_STATE_NORMAL)
                    .withStateTimestamp(stateTimestamp).build();
            try {
                pgsDao.updatePgs(getPath(), pgsModified);
                
                setData(pgsModified);
            } catch (Exception e) {
                workflowLogDao.log(jobID,
                        SEVERITY_MAJOR, "RoleSlave",
                        LOG_TYPE_WORKFLOW, getClusterName(),
                        "Slave join success, but update zookeeper error. {}, exception: \"{}\"", 
                        this, e.getMessage());
                throw new MgmtRoleChangeException("Slave join Error, " + e.getMessage());
            }
        }
    }
    
    public void roleLconn() throws IOException, MgmtZooKeeperException {
        String cmd = "role lconn";
        try {
            String reply = executeQuery(cmd);
            
            Logger.info("Role lconn success. pgs; {}, reply: {}", this, reply);
            if (reply.equals(S2C_OK)) {
                PartitionGroupServerData pgsModified = 
                        PartitionGroupServerData.builder().from(getData())
                            .withRole(PGS_ROLE_NONE)
                            .withState(SERVER_STATE_LCONN).build();
                
                pgsDao.updatePgs(getPath(), pgsModified);
                
                setData(pgsModified);
            }
        } catch (IOException e) {
            Logger.warn("Role lconn fail. pgs; {}", this);
            throw e;
        }
    }
    
    @Override
    public String toString() {
        return fullName(getClusterName(), getData().getPgId(), getName());
    }
    
    public static String fullName(String clusterName, Integer pgId, String pgsId) {
        return  clusterName + "/pg:" + pgId + "/pgs:" + pgsId;
    }
    
    public static String fullName(String clusterName, String pgsId) {
        return  clusterName + "/pgs:" + pgsId;
    }

    @Override
    public String getFullName() {
        return toString();
    }
    
}

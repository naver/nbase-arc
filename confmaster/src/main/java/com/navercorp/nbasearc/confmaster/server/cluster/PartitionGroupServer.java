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

package com.navercorp.nbasearc.confmaster.server.cluster;

import static com.navercorp.nbasearc.confmaster.Constant.*;
import static com.navercorp.nbasearc.confmaster.Constant.Color.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBRefData;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupDao;
import com.navercorp.nbasearc.confmaster.repository.dao.PartitionGroupServerDao;
import com.navercorp.nbasearc.confmaster.repository.dao.WorkflowLogDao;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;

public class PartitionGroupServer extends ZNode<PartitionGroupServerData>
        implements HeartbeatTarget {
    
    private BlockingSocket serverConnection;
    private String clusterName;
    
    private HBSession hbc;
    private HBRefData hbcRefData;
    private UsedOpinionSet usedOpinions;
    
    private final ZooKeeperHolder zookeeper;
    private final PartitionGroupDao pgDao;
    private final PartitionGroupServerDao pgsDao;
    private final Config config;

    public PartitionGroupServer(ApplicationContext context, String path,
            String name, String cluster, byte[] data) {
        super(context);
        this.zookeeper = context.getBean(ZooKeeperHolder.class);
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
            new BlockingSocketImpl(
                getData().getPmIp(), getData().getSmrMgmtPort(), 
                config.getClusterPgsTimeout(), PGS_PING, 
                config.getDelim(), config.getCharset()));
        
        hbcRefData = new HBRefData();
        hbcRefData.setZkData(getData().getRole(), getData().getStateTimestamp(), stat.getVersion())
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
                getData().getRole(), getData().getStateTimestamp(), stat.getVersion());
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
        String role = extractRole(response);
        if (role == null) {
            Logger.error("Invalid response. PGS_ID=" + getName() +
                    ", IP=" + getData().getPmIp() +
                    ", PORT=" + getData().getSmrBasePort() +
                    ", RESPONSE=" + response);
            return "";
        }
        
        return role;
    }
    
    static public String extractRole(String response) {
        String[] resAry = response.split(" ");
        if (resAry.length < 3) {
            return null;
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
    public String getView() {
        return this.getData().getRole();
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
    
            return convertToState(reply);
        } catch (IOException e) {
            return new RealState(false, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                    getStateTimestamp());
        }
    }
    
    static public RealState convertToState(String reply) {
        String[] tokens = reply.split(" ");
        String role;
        String state;
        
        if (tokens.length < 3) {
            state = SERVER_STATE_FAILURE;
            role = PGS_ROLE_NONE;
            return new RealState(false, state, role, -1L);
        }
        
        if (tokens[1].equals(PGS_ROLE_NONE_IN_PONG)) {
            state = SERVER_STATE_FAILURE;
            role = PGS_ROLE_NONE;
        } else if (tokens[1].equals(PGS_ROLE_LCONN_IN_PONG)) {
            state = SERVER_STATE_LCONN;
            role = PGS_ROLE_LCONN;
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
    }
    
    static public String roleToNumber(String role) {
        if (role.equals(PGS_ROLE_NONE)) {
            return PGS_ROLE_NONE_IN_PONG;
        } else if (role.equals(PGS_ROLE_LCONN)) {
            return PGS_ROLE_LCONN_IN_PONG;
        } else if (role.equals(PGS_ROLE_MASTER)) {
            return PGS_ROLE_MASTER_IN_PONG;
        } else if (role.equals(PGS_ROLE_SLAVE)) {
            return PGS_ROLE_SLAVE_IN_PONG;
        } else {
            throw new RuntimeException("Invalid parameter");
        }
    }
    
    public LogSequence getLogSeq() {
        if (!getData().getRole().equals(PGS_ROLE_LCONN) &&
                !getData().getRole().equals(PGS_ROLE_SLAVE) &&
                !getData().getRole().equals(PGS_ROLE_MASTER)) {
            Logger.error("getseq Logger fail. target pgs if not available" +
                    "CLUSTER=" + getClusterName() + 
                    ", PG=" + getData().getPgId() + 
                    ", PGS=" + getName() +
                    ", IP=" + getData().getPmIp() +
                    ", PORT=" + getData().getSmrBasePort());
            return null;
        }
        
        LogSequence logSeq = new LogSequence(this);
        try {
            logSeq.initialize();
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
    
    private void roleMasterCmdResult(String cmd, String reply, long jobID,
            WorkflowLogDao workflowLogDao) throws MgmtSmrCommandException {
        // Make information for logging
        String infoFmt = 
                "{\"PGS\":{},\"IP\":\"{}\",\"PORT\":\"{}\",\"CMD\":\"{}\",\"REPLY\":\"{}\"}";
        Object[] infoArgs = new Object[] { getName(), getData().getPmIp(),
                getData().getSmrBasePort(), cmd, reply };
        
        if (!reply.equals(S2C_OK)) {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election fail. {}, cmd: \"{}\", reply: \"{}\"", 
                    new Object[]{this, cmd, reply}, 
                    infoFmt, infoArgs);
            throw new MgmtSmrCommandException("Role master fail. "
                    + MessageFormatter.arrayFormat(infoFmt, infoArgs));
        } else {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election success. {}, cmd: \"{}\", reply: \"{}\"", 
                    new Object[]{this, cmd, reply},  
                    infoFmt, infoArgs);
        }
    }
    
    public void roleMaster(PartitionGroup pg,
            LogSequence logSeq,
            int quorum, long jobID, WorkflowLogDao workflowLogDao)
            throws MgmtSmrCommandException {
        final String cmd = "role master " + getName() + " " + quorum
                + " " + logSeq.getMax();
        
        try {
            // Workflow log
            workflowLogDao.log(jobID, SEVERITY_MODERATE, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "try master election. cmd: \"" + cmd + "\"");
            
            // Send 'role master'
            String reply = executeQuery(cmd);
            
            // Check result
            roleMasterCmdResult(cmd, reply, jobID, workflowLogDao);
        } catch (IOException e) {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election fail. {}, cmd: \"{}\", e: \"{}\"", 
                    new Object[]{this, cmd, e.getMessage()});
            throw new MgmtSmrCommandException("Master election Error. " + e.getMessage());
        }
    }
    
    public class RoleMasterZkResult {
        public final PartitionGroupData pgM;
        public final PartitionGroupServerData pgsM;
        
        public RoleMasterZkResult(PartitionGroupData pgM, PartitionGroupServerData pgsM) {
            this.pgM = pgM;
            this.pgsM = pgsM;
        }
    }
    
    public RoleMasterZkResult roleMasterZk(PartitionGroup pg,
            List<PartitionGroupServer> pgsList, LogSequence logSeq, int quorum,
            long jobID, WorkflowLogDao workflowLogDao)
            throws MgmtSmrCommandException {
        try {
            PartitionGroupData pgM = 
                PartitionGroupData.builder().from(pg.getData())
                    .addMasterGen(logSeq.getMax()).build();
            
            PartitionGroupServerData pgsM = 
                PartitionGroupServerData.builder().from(getData())
                    .withMasterGen(pg.getData().currentGen() + 1)
                    .withRole(PGS_ROLE_MASTER)
                    .withOldRole(PGS_ROLE_MASTER)
                    .withColor(GREEN).build();
            
            List<Op> ops = new ArrayList<Op>();
            ops.add(pgsDao.createUpdatePgsOperation(getPath(), pgsM));
            ops.add(pgDao.createUpdatePgOperation(pg.getPath(), pgM));

            List<OpResult> results = zookeeper.multi(ops);
            zookeeper.handleResultsOfMulti(results);
            return new RoleMasterZkResult(pgM, pgsM);
        } catch (Exception e) {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election success but update zookeeper error. {}, e: \"{}\"", 
                    this, e.getMessage());
            throw new MgmtSmrCommandException("Role master zookeeper Error, " + e.getMessage());
        }
    }
    
    private void roleSlaveCmdResult(String cmd, String reply, Color color, long jobID,
            WorkflowLogDao workflowLogDao) throws MgmtSmrCommandException {
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
                    "slave join fail. {}, cmd: \"{}\", color: {}, reply: \"{}\"",
                    new Object[]{this, cmd, color, reply},
                    infoFmt, infoArgs);
            throw new MgmtSmrCommandException("Slave join Fail. " + 
                    MessageFormatter.arrayFormat(infoFmt, infoArgs));
        } else {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "slave join success. {}, cmd: \"{}\", color: {}, reply: \"{}\"",
                    new Object[]{this, cmd, color, reply},
                    infoFmt, infoArgs);
        }
    }

    public void roleSlave(PartitionGroup pg, LogSequence targetLogSeq,
            PartitionGroupServer master, Color color, long jobID,
            WorkflowLogDao workflowLogDao) throws MgmtSmrCommandException {
        // Make 'role slave' command
        String cmd = "role slave " + getName() + " "
                + master.getData().getPmIp() + " "
                + master.getData().getSmrBasePort() + " "
                + targetLogSeq.getLogCommit();
        try {
            workflowLogDao.log(jobID,
                    SEVERITY_MODERATE, "RolsSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "try slave join. cmd: \"" + cmd + "\", color: " + color);
            
            // Send 'role slave'
            String reply = executeQuery(cmd);
            
            // Check reply
            roleSlaveCmdResult(cmd, reply, color, jobID, workflowLogDao);
        } catch (IOException e) {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "Slave join error. {}, cmd: \"{}\", color: {}, exception: \"{}\"",
                    new Object[]{this, cmd, color, e.getMessage()});
            throw new MgmtSmrCommandException("Slave join Error. " + e.getMessage());
        }
    }

    public class RoleSlaveZkResult {
        public final PartitionGroupServerData pgsM;
        
        public RoleSlaveZkResult(PartitionGroupServerData pgsM) {
            this.pgsM = pgsM;
        }
    }
    
    public RoleSlaveZkResult roleSlaveZk(long jobID, int mGen, Color color,
            WorkflowLogDao workflowLogDao) throws MgmtSmrCommandException {
        PartitionGroupServerData pgsM = 
            PartitionGroupServerData.builder().from(getData())
                .withMasterGen(mGen)
                .withRole(PGS_ROLE_SLAVE)
                .withOldRole(PGS_ROLE_SLAVE)
                .withColor(color).build();
        try {
            pgsDao.updatePgs(getPath(), pgsM);
            return new RoleSlaveZkResult(pgsM);
        } catch (Exception e) {
            workflowLogDao.log(jobID,
                    SEVERITY_MAJOR, "RoleSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "slave join success, but update zookeeper error. {}, e: \"{}\"", 
                    this, e.getMessage());
            throw new MgmtSmrCommandException("Slave join Error, " + e.getMessage());
        }
    }
    
    public void roleLconn() throws MgmtSmrCommandException {
        try {
            String reply = executeQuery("role lconn");
            if (!reply.equals(S2C_OK) && !reply.equals(S2C_ALREADY_LCONN)) {
                throw new MgmtSmrCommandException(
                        "role lconn fail. " + this + ", reply: \"" + reply + "\"");
            }
            Logger.info("role lconn success. {}, reply: \"{}\"", this, reply);
        } catch (IOException e) {
            Logger.warn("role lconn fail. {} {}", this, e.getMessage());
            throw new MgmtSmrCommandException("Role lconn Error. " + e.getMessage());
        }
    }

    public RoleLconnZkResult roleLconnZk(long jobID, Color color, WorkflowLogDao workflowLogDao) 
                    throws MgmtSmrCommandException {        
        try {
            PartitionGroupServerData pgsM = 
                PartitionGroupServerData.builder().from(getData())
                    .withRole(PGS_ROLE_LCONN).withColor(color).build();
            pgsDao.updatePgs(getPath(), pgsM);
            return new RoleLconnZkResult(pgsM);
        } catch (Exception e) {
            workflowLogDao.log(jobID,
                    SEVERITY_MODERATE, "RoleLconn",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "role lconn sucess but update zookeeper error. {}, e: \"{}\"", 
                    this, e.getMessage());
            throw new MgmtSmrCommandException("Role lconn zookeeper Error, " + e.getMessage());
        }
    }

    public class RoleLconnZkResult {
        public final PartitionGroupServerData pgsM;
        
        public RoleLconnZkResult(PartitionGroupServerData pgsM) {
            this.pgsM = pgsM;
        }
    }

    public void setquorum(int q) throws MgmtSetquorumException {
        final String cmd = "setquorum " + q;
        try {
            String reply = executeQuery(cmd);
            if (!reply.equals(S2C_OK)) {
                final String msg = "setquorum fail. " + this + ", cmd: " + cmd
                        + ", reply: " + reply;
                Logger.error(msg);
                throw new MgmtSetquorumException(msg);
            }
            Logger.info("setquorum success. {}, q: {}", this, q);
        } catch (IOException e) {
            final String msg = "setquorum fail. " + this + ", cmd: " + cmd + ", exception: " + e.getMessage();
            Logger.warn(msg);
            throw new MgmtSetquorumException(msg);
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

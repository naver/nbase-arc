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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSmrCommandException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtSetquorumException;
import com.navercorp.nbasearc.confmaster.Constant.Color;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBState;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowLogger;
 
public class PartitionGroupServer implements HeartbeatTarget,
        Comparable<PartitionGroupServer>, ClusterComponent {
    
    protected BlockingSocket connectionForCommand;
    
    protected HBSession hbSession;
    private UsedOpinionSet usedOpinions;
    
    private ZooKeeperHolder zk;
    private Config config;
    private MemoryObjectMapper mapper = new MemoryObjectMapper();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Cluster cluster;
    
    private String path;
    private String name;
    private PartitionGroupServerData persistentData;
    private int znodeVersion;

    public PartitionGroupServer(ApplicationContext context, byte[] d,
            String clusterName, String pgsId, int znodeVersion) {
        persistentData = mapper.readValue(d, PartitionGroupServerData.class);
        init(context, clusterName, pgsId, znodeVersion);
    }

    public PartitionGroupServer(ApplicationContext context,
            PartitionGroupServerData d, String clusterName, String pgsId,
            int znodeVersion) {
        persistentData = d;
        init(context, clusterName, pgsId, znodeVersion);
    }
    
    public PartitionGroupServer(ApplicationContext context, String clusterName,
            String pgsId, String pgId, String pmName, String pmIp,
            int basePort, int backendPort, int znodeVersion) {
        persistentData = new PartitionGroupServerData(
                pgId, pmName, pmIp, basePort, backendPort);	
    	init(context, clusterName, pgsId, znodeVersion);
    }
    
    private void init(ApplicationContext context, String clusterName, String pgsId, int znodeVersion) {
        this.zk = context.getBean(ZooKeeperHolder.class);
        this.config = context.getBean(Config.class);
        
        this.path = PathUtil.pgsPath(pgsId, clusterName);
        this.name = pgsId;
        this.cluster = context.getBean(ClusterComponentContainer.class).getCluster(clusterName);
        
        this.setZNodeVersion(znodeVersion);
        
        BlockingSocketImpl con = new BlockingSocketImpl(persistentData.pmIp,
                persistentData.smrMgmtPort, config.getClusterPgsTimeout(),
                PGS_PING, config.getDelim(), config.getCharset());
        con.setFirstHandshaker(new BlockingSocketImpl.FirstHandshaker() {
            @Override
            public void handshake(BufferedReader in, PrintWriter out, String delim)
                    throws IOException {
                String cmd = null;
                
                try {
                    cmd = "smrversion";
                    out.print(cmd + delim);
                    out.flush();
                    
                    final String reply = in.readLine();
                    if (reply.equals("+OK 201") == false) {
                        return;
                    }
                } catch (IOException e) {
                    Logger.info("Singleton request to SMR fail. cmd: " + cmd);
                    throw e;
                }
                
                try {
                    cmd = "singleton confmaster";
                    out.print(cmd + delim);
                    out.flush();
                    
                    final String reply = in.readLine();
                    if (reply.equals("+OK") == false) {
                        Logger.info("Singleton request to SMR fail. cmd: " + cmd
                                + ", reply: \"" + reply + "\"");
                        throw new IOException("Singleton request to SMR fail. " + this);
                    }
                } catch (IOException e) {
                    Logger.info("Singleton request to SMR fail. cmd: " + cmd);
                    throw e;
                }
            }
        });
        connectionForCommand = con;
        

        HBState hbRefData = new HBState();
        hbRefData.setZkData(persistentData.role, persistentData.stateTimestamp, 0)
                  .setLastState(persistentData.role)
                  .setLastStateTimestamp(persistentData.stateTimestamp)
                  .setSubmitMyOpinion(false);
        
        usedOpinions = new UsedOpinionSet();

        hbSession = new HBSession(context, this, persistentData.pmIp,
                persistentData.smrMgmtPort, cluster.getMode(),
                persistentData.hb, PGS_PING + "\r\n", hbRefData);
    }
    
    /*
     * Return true, if a master PGS and a slave PGS are in the same machine and
     * both are in the same PG. For instance, If an instance of this class and
     * Master PGS are in the same machine than this function return true,
     * otherwise return false.
     */
    public boolean isMasterInSameMachine(ClusterComponentContainer container) {
        final String machineName = persistentData.pmName;
        final PhysicalMachineCluster machineInfo = container.getPmc(machineName, getClusterName());
        final List<Integer> localPgsIdList = machineInfo.getPgsIdList();
        
        for (final Integer id : localPgsIdList) {
            PartitionGroupServer anotherPGS = container.getPgs(getClusterName(), String.valueOf(id));
            if (persistentData.pgId == anotherPGS.persistentData.pgId
                    && anotherPGS.persistentData.role.equals(PGS_ROLE_MASTER)
                    && machineName.equals(anotherPGS.persistentData.pmName)) {
                return true;
            }
        }
        return false;
    }
    
    public void propagateStateToHeartbeatSession() {
        hbSession.getHeartbeatState().setZkData(
                persistentData.role, persistentData.stateTimestamp, znodeVersion);
        hbSession.toggleHearbeat(cluster.getMode(), persistentData.hb);
    }

    @Override
    public void release() {
        try {
            hbSession.toggleHearbeat(cluster.getMode(), HB_MONITOR_NO);
            connectionForCommand.close();
        } catch (Exception e) {
            Logger.error("stop heartbeat fail. PGS:" + getName(), e);
        }

        try {
            hbSession.callbackDelete();
            turnOnUrgentHeartbeat();
        } catch (Exception e) {
            Logger.error("failed while delete pgs. " + toString(), e);
        }
    }

    public String executeQuery(String query) throws IOException {
        return connectionForCommand.execute(query);
    }

    public String executeQuery(String query, int retryCount) throws IOException {
        return connectionForCommand.execute(query, retryCount);
    }
    
    public long getActiveStateTimestamp() throws IOException {
        String response = executeQuery(PGS_PING);

        String[] resAry = response.split(" ");
        if (resAry.length < 3)
        {
            Logger.error("Invalid response. PGS_ID=" + getName() +
                    ", IP=" + persistentData.pmIp +
                    ", PORT=" + persistentData.smrBasePort +
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
                    ", IP=" + persistentData.pmIp +
                    ", PORT=" + persistentData.smrBasePort +
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
    public int getZNodeVersion() {
        return znodeVersion;
    }

    @Override
    public String getClusterName() {
        return cluster.getName();
    }

    @Override
    public String getView() {
        return this.persistentData.role;
    }
    
    @Override
    public void setState(String state, long state_timestamp) {
        persistentData.state = state;
        persistentData.stateTimestamp = state_timestamp;
    }
    
    public long getStateTimestamp() {
        return persistentData.stateTimestamp;
    }
    
    @Override
    public String getHeartbeat() {
        return persistentData.hb;
    }
    
    @Override
    public boolean isHBCResponseCorrect(String recvedLine) {
        return recvedLine.contains(PGS_RESPONSE_OK);
    }
    
    @Override
    public HBState getHeartbeatState() {
        return hbSession.getHeartbeatState();
    }

    @Override
    public String getIP() {
        return persistentData.pmIp;
    }

    @Override
    public int getPort() {
        return persistentData.smrMgmtPort;
    }

    @Override
    public UsedOpinionSet getUsedOpinions() {
        return usedOpinions;
    }

    public void syncLastState() {
        hbSession.getHeartbeatState().setLastState(persistentData.state)
            .setLastStateTimestamp(persistentData.stateTimestamp);
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
            return "role: " + role + ", state: " + getState()
                    + ", timestamp: " + getStateTimestamp();
        }
    }

    public boolean isAvailable() {
        return (persistentData.state.equals(SERVER_STATE_NORMAL) ||
                persistentData.state.equals(SERVER_STATE_LCONN)) &&
               persistentData.hb.equals(HB_MONITOR_YES);
    }
    
    public RealState getRealState() {        
        try {
            String cmd = "ping";
            String reply = executeQuery(cmd);
            Logger.debug("CMD=\"" + cmd + 
                    "\", REPLY=\"" + reply + 
                    "\", CLUSTER:" + getClusterName() + 
                    ", PGS:" + getName() + 
                    ", STATE:" + persistentData.state + 
                    ", ROLE:" + persistentData.role);
    
            return convertReplyToState(reply);
        } catch (IOException e) {
            return new RealState(false, SERVER_STATE_FAILURE, PGS_ROLE_NONE,
                    persistentData.stateTimestamp);
        }
    }
    
    public long getNewMasterRewindCseq(LogSequence logSeq) {
        if (persistentData.oldMasterSmrVersion.equals(SMR_VERSION_101)) {
            Logger.info("new master rewind cseq: {}, om: {}",
                    logSeq.getLogCommit(), persistentData.oldMasterSmrVersion);
            return logSeq.getLogCommit();
        } else {
            Logger.info("new master rewind cseq: {}, om: {}",
                    logSeq.getLogCommit(), persistentData.oldMasterSmrVersion);
            return logSeq.getMax();
        }
    }
    
    public long getSlaveJoinSeq(LogSequence logSeq) {
        if (persistentData.oldMasterSmrVersion.equals(SMR_VERSION_101)) {
            if (persistentData.oldRole.equals(PGS_ROLE_MASTER)) {
                final long seq = Math.min(logSeq.getLogCommit(),
                        logSeq.getBeCommit());
                Logger.info("slave join rewind cseq: {}, om: {}, or: {}",
                        new Object[] { seq, persistentData.oldMasterSmrVersion,
                                persistentData.oldRole });
                return seq;
            }
        }

        final long seq = logSeq.getLogCommit();
        Logger.info("slave join rewind cseq: {}, om: {}, or: {}", new Object[] {
                seq, persistentData.oldMasterSmrVersion,
                persistentData.oldRole });
        return seq;
    }
    
    static public RealState convertReplyToState(String reply) {
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
        if (!persistentData.role.equals(PGS_ROLE_LCONN) &&
                !persistentData.role.equals(PGS_ROLE_SLAVE) &&
                !persistentData.role.equals(PGS_ROLE_MASTER)) {
            Logger.error("getseq Logger fail. target pgs if not available" +
                    "CLUSTER=" + getClusterName() + 
                    ", PG=" + persistentData.pgId + 
                    ", PGS=" + getName() +
                    ", IP=" + persistentData.pmIp +
                    ", PORT=" + persistentData.smrBasePort);
            return null;
        }
        
        LogSequence logSeq = new LogSequence(this);
        try {
            logSeq.initialize();
        } catch (IOException e) {
            Logger.error("getseq Logger fail. " +
                    "CLUSTER=" + getClusterName() + 
                    ", PG=" + persistentData.pgId + 
                    ", PGS=" + getName() +
                    ", IP=" + persistentData.pmIp +
                    ", PORT=" + persistentData.smrBasePort, e);
            return null;
        }
        
        return logSeq;
    }
    
    public String smrVersion() throws MgmtSmrCommandException {
        final String cmd = "smrversion";
        
        try {
            // Send 'role master'
            String reply = executeQuery(cmd);
            
            String []toks = reply.split(" ");
            Logger.info("get smrversion success {}, reply: \"{}\"", this, reply);
            if (toks[0].equals(ERROR)) {
                return SMR_VERSION_101;
            } else if (toks[1].equals(SMR_VERSION_201)) {
                return SMR_VERSION_201;
            } else {
                throw new MgmtSmrCommandException("Get smrversion error.");
            }
        } catch (IOException e) {
            Logger.error("get smrversion fail. {}", this);
            throw new MgmtSmrCommandException("Get smrversion error. " + e.getMessage());
        }
    }
    
    private void roleMasterCmdResult(String cmd, String reply, long jobID,
            WorkflowLogger workflowLogger) throws MgmtSmrCommandException {
        // Make information for logging
        String infoFmt = 
                "{\"PGS\":{},\"IP\":\"{}\",\"PORT\":\"{}\",\"CMD\":\"{}\",\"REPLY\":\"{}\"}";
        Object[] infoArgs = new Object[] { getName(), persistentData.pmIp,
                persistentData.smrBasePort, cmd, reply };
        
        if (!reply.equals(S2C_OK)) {
            workflowLogger.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election fail. {}, cmd: \"{}\", reply: \"{}\"", 
                    new Object[]{this, cmd, reply}, 
                    infoFmt, infoArgs);
            throw new MgmtSmrCommandException("Role master fail. "
                    + MessageFormatter.arrayFormat(infoFmt, infoArgs));
        } else {
            workflowLogger.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election success. {}, cmd: \"{}\", reply: \"{}\"", 
                    new Object[]{this, cmd, reply},  
                    infoFmt, infoArgs);
        }
    }
    
    public void roleMaster(String smrVersion, PartitionGroup pg,
            LogSequence logSeq, int quorum, String quorumMembers, long jobID,
            WorkflowLogger workflowLogger) throws MgmtSmrCommandException {
        String cmd;
        if (smrVersion.equals(SMR_VERSION_201)) {
            cmd = "role master " + getName() + " " + quorum + " "
                    + getNewMasterRewindCseq(logSeq) + " " + quorumMembers;
        } else {
            cmd = "role master " + getName() + " " + quorum + " "
                    + getNewMasterRewindCseq(logSeq);
        }
        
        try {
            // Workflow log
            workflowLogger.log(jobID, SEVERITY_MODERATE, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "try master election. cmd: \"" + cmd + "\"");
            
            // Send 'role master'
            String reply = executeQuery(cmd);
            
            // Check result
            roleMasterCmdResult(cmd, reply, jobID, workflowLogger);
        } catch (IOException e) {
            workflowLogger.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election fail. {}, cmd: \"{}\", e: \"{}\"", 
                    new Object[]{this, cmd, e.getMessage()});
            throw new MgmtSmrCommandException("Master election Error. " + e.getMessage());
        }
    }
    
	public class RoleMasterZkResult {
		public final PartitionGroup.PartitionGroupData pgM;
		public final PartitionGroupServerData pgsM;

		public RoleMasterZkResult(PartitionGroup.PartitionGroupData pgM,
				PartitionGroupServerData pgsM) {
			this.pgM = pgM;
			this.pgsM = pgsM;
		}
	}
    
    public RoleMasterZkResult updateZNodeAsMaster(PartitionGroup pg,
            List<PartitionGroupServer> pgsList, LogSequence logSeq, int quorum,
            String masterVersion, long jobID, WorkflowLogger workflowLogger)
            throws MgmtSmrCommandException {
        try {
        	PartitionGroup.PartitionGroupData pgM = pg.clonePersistentData();
        	pgM.addMasterGen(logSeq.getMax());
            
            // For backward compatibility, confmaster adds 1 to currentGen
            // since 1.2 and smaller version of confmaster follow a rule, PG.mGen + 1 = PGS.mGen.
            // Notice that it add 2 to currentGen because pg.persistentData.currentGen has not updated yet.
            // Please see below for more details.
            // +--------------------+---------+----------+
            // |                    | PG.mGen | PGS.mGen |
            // +--------------------+---------+----------+
            // | As-is              | 3       | 4        |
            // | Update Master.mGen | 3       | 5(3+2)   |
            // | Update PG.mGen     | 4(3+1)  | 5        |
            // | To-be              | 4       | 5        |
            // +--------------------+---------+----------+
            PartitionGroupServerData pgsM = (PartitionGroupServerData) persistentData.clone(); 
			pgsM.masterGen = pg.currentGen() + 2;
			pgsM.setRole(PGS_ROLE_MASTER);
			pgsM.oldRole = PGS_ROLE_MASTER;
			pgsM.oldMasterSmrVersion = masterVersion;
			pgsM.color = GREEN;
            
            List<Op> ops = new ArrayList<Op>();
            ops.add(Op.setData(getPath(), pgsM.toBytes(), -1));
            ops.add(Op.setData(pg.getPath(), pgM.toBytes(), -1));

            List<OpResult> results = zk.multi(ops);
            zk.handleResultsOfMulti(results);
            return new RoleMasterZkResult(pgM, pgsM);
        } catch (Exception e) {
            workflowLogger.log(jobID,
                    SEVERITY_MAJOR, "RoleMaster",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "master election success but update zookeeper error. {}, e: \"{}\"", 
                    this, e.getMessage());
            throw new MgmtSmrCommandException("Role master zookeeper Error, " + e.getMessage());
        }
    }
    
    private void roleSlaveCmdResult(String cmd, String reply, Color color, long jobID,
            WorkflowLogger workflowLogger) throws MgmtSmrCommandException {
        // Make information for logging
        String infoFmt = 
                "{\"PGS\":{},\"IP\":\"{}\",\"PORT\":\"{}\",\"CMD\":\"{}\",\"REPLY\":\"{}\"}";
        Object[] infoArgs = new Object[] { getName(), persistentData.pmIp,
                persistentData.smrBasePort, cmd, reply };
        
        // Check result
        if (!reply.equals(S2C_OK)) {
            Logger.info("", 
                    new Object[]{this, cmd, reply});
            workflowLogger.log(jobID,
                    SEVERITY_MAJOR, "RoleSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "slave join fail. {}, cmd: \"{}\", color: {}, reply: \"{}\"",
                    new Object[]{this, cmd, color, reply},
                    infoFmt, infoArgs);
            throw new MgmtSmrCommandException("Slave join Fail. " + 
                    MessageFormatter.arrayFormat(infoFmt, infoArgs));
        } else {
            workflowLogger.log(jobID,
                    SEVERITY_MAJOR, "RoleSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "slave join success. {}, cmd: \"{}\", color: {}, reply: \"{}\"",
                    new Object[]{this, cmd, color, reply},
                    infoFmt, infoArgs);
        }
    }

    public void roleSlave(PartitionGroup pg, LogSequence targetLogSeq,
            PartitionGroupServer master, Color color, long jobID,
            WorkflowLogger workflowLogger) throws MgmtSmrCommandException {
        // Make 'role slave' command
        String cmd = "role slave " + getName() + " "
                + master.persistentData.pmIp + " "
                + master.persistentData.smrBasePort + " "
                + getSlaveJoinSeq(targetLogSeq);
        try {
            workflowLogger.log(jobID,
                    SEVERITY_MODERATE, "RolsSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "try slave join. cmd: \"" + cmd + "\", color: " + color);
            
            // Send 'role slave'
            String reply = executeQuery(cmd);
            
            // Check reply
            roleSlaveCmdResult(cmd, reply, color, jobID, workflowLogger);
        } catch (IOException e) {
            workflowLogger.log(jobID,
                    SEVERITY_MAJOR, "RoleSlave",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "Slave join error. {}, cmd: \"{}\", color: {}, exception: \"{}\"",
                    new Object[]{this, cmd, color, e.getMessage()});
            throw new MgmtSmrCommandException("Slave join Error. " + e.getMessage());
        }
    }

    public PartitionGroupServerData updateZNodeAsSlave(long jobID, int mGen, Color color, String masterVersion,
            WorkflowLogger workflowLogger) throws MgmtSmrCommandException {
		PartitionGroupServerData pgsM = (PartitionGroupServerData) persistentData.clone();
		pgsM.masterGen = mGen;
		pgsM.setRole(PGS_ROLE_SLAVE);
		pgsM.oldRole = PGS_ROLE_SLAVE;
		pgsM.oldMasterSmrVersion = masterVersion;
		pgsM.color = color;
        try {
        	zk.setData(getPath(), pgsM.toBytes(), -1);
            return pgsM;
        } catch (Exception e) {
            workflowLogger.log(jobID,
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

    public PartitionGroupServerData updateZNodeAsLconn(long jobID, Color color, WorkflowLogger workflowLogger) 
                    throws MgmtSmrCommandException {        
        try {
			PartitionGroupServerData pgsM = (PartitionGroupServerData) persistentData.clone();
			pgsM.setRole(PGS_ROLE_LCONN);
			pgsM.color = color;
			zk.setData(getPath(), pgsM.toBytes(), -1);
            return pgsM;
        } catch (Exception e) {
            workflowLogger.log(jobID,
                    SEVERITY_MODERATE, "RoleLconn",
                    LOG_TYPE_WORKFLOW, getClusterName(),
                    "role lconn sucess but update zookeeper error. {}, e: \"{}\"", 
                    this, e.getMessage());
            throw new MgmtSmrCommandException("Role lconn zookeeper Error, " + e.getMessage());
        }
    }

    /*
     * return List<String> containing [quorum, nid1, nid2, ...]
     */
    public List<String> getQuorumV() throws MgmtSmrCommandException {
        final String cmd = "getquorumv";

        try {
            String reply = executeQuery(cmd);
            String []toks = reply.split(" ");
            if (!toks[0].equals(S2C_OK)) {
                final String msg = "getquorumv fail. " + this + ", cmd: " + cmd
                        + ", reply: " + reply;
                Logger.error(msg);
                throw new MgmtSmrCommandException(msg);
            }
            
            List<String> ret = new ArrayList<String>();
            for (int i = 1; i < toks.length; i++) {
                ret.add(toks[i]);
            }
            
            Logger.info("getquorumv success. {}, reply: {}", this, reply);
            return ret;
        } catch (IOException e) {
            final String msg = "getquorumv fail. " + this + ", cmd: " + cmd
                    + ", exception: " + e.getMessage();
            Logger.warn(msg);
            throw new MgmtSmrCommandException(msg);
        }
    }
    
    public int getQuorum() throws MgmtSmrCommandException {
        final String cmd = "getquorum";

        try {
            String reply = executeQuery(cmd);
            
            try {
                final int q = Integer.valueOf(reply);
                Logger.info("getquorum success. {}, reply: {}", this, reply);
                return q;
            } catch (NumberFormatException e) {            
                final String msg = "getquorum fail. " + this + ", cmd: " + cmd
                        + ", reply: " + reply;
                Logger.error(msg);                
                throw new MgmtSmrCommandException(msg);
            }
        } catch (IOException e) {
            final String msg = "getquorum fail. " + this + ", cmd: " + cmd
                    + ", exception: " + e.getMessage();
            Logger.warn(msg);
            throw new MgmtSmrCommandException(msg);
        }
    }
    
    public void setQuorum(int q, String quorumMembers)
            throws MgmtSetquorumException, MgmtSmrCommandException {
        final String v = smrVersion();
        String cmd; 
        if (v.equals(SMR_VERSION_201)) {
            cmd = "setquorum " + q + " " + quorumMembers;
        } else {
            cmd = "setquorum " + q;
        }
        
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
        return fullName(getClusterName(), persistentData.pgId, getName());
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

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public NodeType getNodeType() {
		return NodeType.PGS;
	}

	@Override
	public int compareTo(PartitionGroupServer pgs) {
	    return name.compareTo(pgs.name);
	}
    
    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }
    
    @Override
    public void setZNodeVersion(int v) {
    	this.znodeVersion = v;
    }

    public void turnOnUrgentHeartbeat() {
        hbSession.urgent();
    }

    public void setPersistentData(PartitionGroupServerData d) {
        persistentData = d;
    }

    public void setPersistentData(byte[] d) {
        persistentData = mapper.readValue(d, PartitionGroupServerData.class);
    }
    
    @JsonAutoDetect(
            fieldVisibility=Visibility.ANY, 
            getterVisibility=Visibility.NONE, 
            setterVisibility=Visibility.NONE)
    @JsonIgnoreProperties(
            ignoreUnknown=true)
    @JsonPropertyOrder(
            { "pg_ID", "pm_Name", "pm_IP", "backend_Port_Of_Redis",
            "replicator_Port_Of_SMR", "management_Port_Of_SMR", "state",
            "stateTimestamp", "hb", "smr_Role", "old_SMR_Role", "color", "master_Gen", "old_master_version" })
    public static class PartitionGroupServerData implements Cloneable {
        
        @JsonProperty("pg_ID")
        public int pgId;
        @JsonProperty("pm_Name")
        public String pmName;
        @JsonProperty("pm_IP")
        public String pmIp;
        @JsonProperty("backend_Port_Of_Redis")
        public int redisPort;
        @JsonProperty("replicator_Port_Of_SMR")
        public int smrBasePort;
        @JsonProperty("management_Port_Of_SMR")
        public int smrMgmtPort;
        @JsonProperty("state")
        public String state = SERVER_STATE_FAILURE;
        @JsonProperty("stateTimestamp")
        public long stateTimestamp = 0L;
        @JsonProperty("hb")
        public String hb = HB_MONITOR_NO;
        @JsonProperty("smr_Role")
        private String role = PGS_ROLE_NONE;
        @JsonProperty("old_SMR_Role")
        public String oldRole = PGS_ROLE_NONE;
        @JsonProperty("color")
        public Color color = Color.RED;
        @JsonProperty("master_Gen")
        public int masterGen = -1;
        @JsonProperty("old_master_version")
        public String oldMasterSmrVersion = SMR_VERSION_101;
    
        @JsonIgnore
        private final MemoryObjectMapper mapper = new MemoryObjectMapper();
        
        public PartitionGroupServerData() {}
        
        public PartitionGroupServerData(String pgId, String pmName,
                String pmIp, int basePort, int backendPort) {
            this.pgId = Integer.valueOf(pgId);
            this.pmName = pmName;
            this.pmIp = pmIp;
            this.smrBasePort = basePort;
            this.smrMgmtPort = basePort + 3;
            this.redisPort = backendPort;
        }
        
        public void setRole(String role) {
            this.role = role;

            if (role.equals(PGS_ROLE_NONE)) {
                this.state = SERVER_STATE_FAILURE;
            } else if (role.equals(PGS_ROLE_LCONN)) {
                this.state = SERVER_STATE_LCONN;
            } else if (role.equals(PGS_ROLE_SLAVE)) {
                this.state = SERVER_STATE_NORMAL;
            } else if (role.equals(PGS_ROLE_MASTER)) {
                this.state = SERVER_STATE_NORMAL;
            }
        }

        public String getRole() {
            return role;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof PartitionGroupServerData)) {
                return false;
            }
    
            PartitionGroupServerData rhs = (PartitionGroupServerData) obj;
            if (pgId != rhs.pgId) {
                return false;
            }
            if (!pmName.equals(rhs.pmName)) {
                return false;
            }
            if (!pmIp.equals(rhs.pmIp)) {
                return false;
            }
            if (redisPort != rhs.redisPort) {
                return false;
            }
            if (smrBasePort != rhs.smrBasePort) {
                return false;
            }
            if (smrMgmtPort != rhs.smrMgmtPort) {
                return false;
            }
            if (!state.equals(rhs.state)) {
                return false;
            }
            if (!role.equals(rhs.role)) {
                return false;
            }
            if (masterGen != rhs.masterGen) {
                return false;
            }
            if (!hb.equals(rhs.hb)) {
                return false;
            }
            if (stateTimestamp != rhs.stateTimestamp) {
                return false;
            }
            return true;
        }
        
        @Override
        public int hashCode() {
            assert false : "hashCode not designed";
            return 42; // any arbitrary constant will do
        }
        
        @Override
        public Object clone() {
            try {
                return super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    
        @Override
        public String toString() {
            return mapper.writeValueAsString(this);
        }

        public byte[] toBytes() {
            return mapper.writeValueAsBytes(this);
        }
        
    }

	public int getRedisPort() {
		return persistentData.redisPort;
	}
    
    public Color getColor() {
    	return persistentData.color;
    }

	public String getRole() {
		return persistentData.role;
	}

	public String getState() {
		return persistentData.state;
	}

	public int getPgId() {
		return persistentData.pgId;
	}

	public String getPmIp() {
		return persistentData.pmIp;
	}

	public int getSmrBasePort() {
		return persistentData.smrBasePort;
	}
	
	public int getSmrMgmtPort() {
		return persistentData.smrMgmtPort;
	}

	public String getPmName() {
		return persistentData.pmName;
	}

	public byte[] persistentDataToBytes() {
		return persistentData.toBytes();
	}
	
	public String persistentDataToString() {
		return persistentData.toString();
	}

	public PartitionGroupServerData clonePersistentData() {
		return (PartitionGroupServerData) persistentData.clone();
	}

	public int getMasterGen() {
		return persistentData.masterGen;
	}
}

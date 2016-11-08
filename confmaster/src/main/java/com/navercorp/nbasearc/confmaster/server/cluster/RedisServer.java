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

import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_NO;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBState;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;

public class RedisServer implements HeartbeatTarget, Comparable<RedisServer>, ClusterComponent {
    
    protected BlockingSocket connectionForCommand;
    
    protected HBSession hbSession;

    private Config config;
    private MemoryObjectMapper mapper = new MemoryObjectMapper();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    private Cluster cluster;
    
    private int pgId;
    private String path;
    private String name;
    private RedisServerData persistentData;
    private int znodeVersion;
    
    public RedisServer(ApplicationContext context, byte[] d,
            String clusterName, String pgsId, int pgId, int znodeVersion) {
    	persistentData = mapper.readValue(d, RedisServerData.class);
    	init(context, clusterName, pgsId, pgId, znodeVersion);
    }

    public RedisServer(ApplicationContext context, RedisServerData d,
            String clusterName, String pgsId, int pgId, int znodeVersion) {
        persistentData = d;
        init(context, clusterName, pgsId, pgId, znodeVersion);
    }
    
    public RedisServer(ApplicationContext context, String clusterName,
            String pgsId, String pmName, String pmIp, int backendPort,
            int pgId, int znodeVersion) {
    	persistentData = new RedisServerData(pmName, pmIp, backendPort);
    	init(context, clusterName, pgsId, pgId, znodeVersion);
    }
    
    private void init(ApplicationContext context, String clusterName,
            String pgsId, int pgId, int znodeVersion) {
        this.config = context.getBean(Config.class);
        
        this.znodeVersion = znodeVersion;
        this.path = PathUtil.rsPath(pgsId, clusterName);
        this.name = pgsId;
        this.cluster = context.getBean(ClusterComponentContainer.class).getCluster(clusterName);
        this.pgId = pgId;
        
        connectionForCommand = 
            new BlockingSocketImpl(
                persistentData.pmIp, persistentData.redisPort, 
                config.getClusterPgsTimeout(), Constant.REDIS_PING, 
                config.getDelim(), config.getCharset());
        
        HBState hbRefData = new HBState();
        hbRefData.setZkData(persistentData.state, persistentData.stateTimestamp, znodeVersion)
            .setLastState(persistentData.state)
            .setLastStateTimestamp(persistentData.stateTimestamp)
            .setSubmitMyOpinion(false);
        
        hbSession = new HBSession(context, this, persistentData.pmIp,
                persistentData.redisPort, cluster.getMode(), persistentData.hb,
                Constant.REDIS_PING + "\r\n", hbRefData);
        
    }
    
    public void propagateStateToHeartbeatSession() {
        hbSession.getHeartbeatState().setZkData(persistentData.state, 
        		persistentData.stateTimestamp, znodeVersion);
        hbSession.toggleHearbeat(cluster.getMode(), persistentData.hb);
    }

    @Override
    public void release() {
        try {
            hbSession.toggleHearbeat(cluster.getMode(), Constant.HB_MONITOR_NO);
            connectionForCommand.close();
        } catch (Exception e) {
            Logger.error("stop heartbeat fail. RS:" + getName(), e);
        }

        try {
            hbSession.callbackDelete();
            turnOnUrgentHeartbeat();
        } catch (Exception e) {
            Logger.error("failed while delete rs. " + toString(), e);
        }
    }
    
    public List<String> executeQueryAndMultiReply(String query, int replyCount) throws IOException {
        return connectionForCommand.executeAndMultiReply(query, replyCount);
    }
    
    public void closeConnection() throws IOException {
        connectionForCommand.close();
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
        return persistentData.state;
    }

    @Override
    public void setState(String state, long state_timestamp) {
    	persistentData.state = state;
    	persistentData.stateTimestamp = state_timestamp;
    }

    @Override
    public String getHeartbeat() {
        return persistentData.hb;
    }
    
    @Override
    public boolean isHBCResponseCorrect(String recvedLine) {
        return recvedLine.contains(Constant.REDIS_PONG);
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
        return persistentData.redisPort;
    }

    @Override
    public UsedOpinionSet getUsedOpinions() {
        return null;
    }

    public String executeQuery(String query) throws IOException {
        return connectionForCommand.execute(query);
    }

    public String executeQuery(String query, int retryCount) throws IOException {
        return connectionForCommand.execute(query, retryCount);
    }

    public String replPing() {
        try {
            String reply = executeQuery(Constant.PGS_PING);
            Logger.info("CMD=\"" + Constant.PGS_PING + 
                    "\", REPLY=\"" + reply + 
                    "\", CLUSTER:" + getClusterName() + 
                    ", PGSID:" + getName() + 
                    ", STATE:" + persistentData.state);
            if (reply.equals(Constant.REDIS_PONG)) {
                return Constant.SERVER_STATE_NORMAL;
            } else {
                return Constant.SERVER_STATE_FAILURE;
            }
        } catch (IOException e) {
            return Constant.SERVER_STATE_FAILURE;
        }
    }
    
    @Override
    public String toString() {
        return fullName(getClusterName(), getName());
    }
    
    public static String fullName(String clusterName, String pgsId) {
        return clusterName + "/rs:" + pgsId;
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
        return NodeType.RS;
    }

    @Override
    public int compareTo(RedisServer rs) {
        return name.compareTo(rs.name);
    }
    
    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }
    
    public int getPgId() {
        return pgId;
    }

    public void turnOnUrgentHeartbeat() {
        hbSession.urgent();
    }

    public String persistentDataToString() {
        return persistentData.toString();
    }

    public byte[] persistentDataToBytes() {
        return persistentData.toBytes();
    }

    public RedisServerData clonePersistentData() {
        return (RedisServerData) persistentData.clone();
    }

    public void setPersistentData(RedisServerData d) {
        this.persistentData = d;
    }

    public void setPersistentData(byte []d) {
        this.persistentData = mapper.readValue(d, RedisServerData.class);
    }

	@JsonAutoDetect(
	        fieldVisibility=Visibility.ANY, 
	        getterVisibility=Visibility.NONE, 
	        setterVisibility=Visibility.NONE)
	@JsonIgnoreProperties(
	        ignoreUnknown=true)
	@JsonPropertyOrder(
	        {"pm_Name", "pm_IP", "backend_Port_Of_Redis", "state", "stateTimestamp", "hb"})
	public static class RedisServerData implements Cloneable {
	    
	    @JsonProperty("pm_Name")
	    public String pmName;
	    @JsonProperty("pm_IP")
	    public String pmIp;
	    @JsonProperty("backend_Port_Of_Redis")
	    public int redisPort;
	    @JsonProperty("state")
	    public String state;
	    @JsonProperty("stateTimestamp")
	    public long stateTimestamp;
	    @JsonProperty("hb")
	    public String hb;
	
	    @JsonIgnore
	    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
	    
	    public RedisServerData() {}
	    
	    public RedisServerData(final String pmName,
	                            final String pmIp,
	                            final int redisPort) {
	        this.pmName = pmName;
	        this.pmIp = pmIp;
	        this.redisPort = redisPort;

	        // Use default value
	        this.state = SERVER_STATE_FAILURE;
	        this.stateTimestamp = 0L;
	        this.hb = HB_MONITOR_NO;
	    }
	    
	    @Override
	    public boolean equals(Object obj) {
	        if (obj == null) {
	            return false;
	        }
	        if (obj == this) {
	            return true;
	        }
	        if (!(obj instanceof RedisServerData)) {
	            return false;
	        }
	
	        RedisServerData rhs = (RedisServerData) obj;
	        if (!pmName.equals(rhs.pmName)) {
	            return false;
	        }
	        if (!pmIp.equals(rhs.pmIp)) {
	            return false;
	        }
	        if (redisPort != rhs.redisPort) {
	            return false;
	        }
	        if (!state.equals(rhs.state)) {
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
	        try {
	            return mapper.writeValueAsString(this);
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage());
	        }
	    }
	
		public byte[] toBytes() {
			return mapper.writeValueAsBytes(this);
		}
	}

	@Override
	public void setZNodeVersion(int version) {
		this.znodeVersion = version;
	}
}

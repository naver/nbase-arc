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

import static com.navercorp.nbasearc.confmaster.Constant.HB_MONITOR_YES;
import static com.navercorp.nbasearc.confmaster.Constant.SERVER_STATE_FAILURE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class Gateway implements HeartbeatTarget, Comparable<Gateway>, ClusterComponent {

    protected BlockingSocket connectionForCommand;
    
    private HBSession hbSession;
    
    private Config config;
    private MemoryObjectMapper mapper = new MemoryObjectMapper();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Cluster cluster;
    
    private String name;
    private String path;
    private GatewayData persistentData;
    private int znodeVersion;

	public Gateway(ApplicationContext context, String clusterName, String gwId,
			String pmName, String pmIp, int port, int znodeVersion) {
    	persistentData = new GatewayData(pmName, pmIp, port);
    	init(context, clusterName, gwId, znodeVersion);
    }
    
    public Gateway(ApplicationContext context, String clusterName, String gwId,
            byte[] d, int znodeVersion) {
        persistentData = mapper.readValue(d, GatewayData.class);
        init(context, clusterName, gwId, znodeVersion);
    }
    
    public Gateway(ApplicationContext context, String clusterName, String gwId,
            GatewayData d, int znodeVersion) {
        persistentData = d;
        init(context, clusterName, gwId, znodeVersion);
    }
    
    public void init(ApplicationContext context, String clusterName, String gwId, int znodeVersion) {
        this.config = context.getBean(Config.class);
        
        this.path = PathUtil.gwPath(gwId, clusterName);
        this.name = gwId;
        this.cluster = context.getBean(ClusterComponentContainer.class).getCluster(clusterName);
        
        connectionForCommand = 
            new BlockingSocketImpl(
                persistentData.pmIp, persistentData.port + 1, 
                config.getClusterGwTimeout(), Constant.GW_PING,
                config.getDelim(), config.getCharset());
        
        HBState hbRefData = new HBState();
        hbRefData.setZkData(persistentData.state, persistentData.stateTimestamp, znodeVersion)
                  .setLastState(persistentData.state)
                  .setLastStateTimestamp(persistentData.stateTimestamp)
                  .setSubmitMyOpinion(false);

        hbSession = new HBSession(context, this, persistentData.pmIp,
                persistentData.port, cluster.getMode(), persistentData.hb,
                Constant.GW_PING + "\r\n", hbRefData);
    }
    
    @Override
    public void release() {
        try {
            connectionForCommand.close();
            hbSession.callbackDelete();
        } catch (Exception e) {
            Logger.error("failed while delete gw. " + toString(), e);
        }
    }

    /*
     * return a string of Gateway Affinity in Run Length Encoding Format.
     * Affinity Type : A(All), W(Write), R(Read), N(None)
     * Run Length Encoding Format : <Affinity Type><Slot Size><Affinity Type><Slot Size>...
     *                              ex) A2048R2048N4096
     */
    public String getAffinity(ClusterComponentContainer cache) {
        return getAffinity(getClusterName(), persistentData.pmName, cache);
    }
    
    public static String getAffinity(String clusterName, String pmName, ClusterComponentContainer container) {
        final Cluster cluster = container.getCluster(clusterName);
        final List<Integer> pnPgMap = cluster.getPnPgMap();
        final PhysicalMachineCluster machineInfo = container.getPmc(pmName, clusterName);
        final List<Integer> localPgsIdList;
        if (machineInfo != null) {
            localPgsIdList = machineInfo.getPgsIdList();
        } else {
            /*
             * When machineInfo is null, there are no PGS in the machine,
             * so that set localPgsIdList with empty list, ArrayList<Integer>().
             */
            localPgsIdList = new ArrayList<Integer>();
        }
        Character[] keySpace = new Character[Constant.KEY_SPACE_SIZE];
        Arrays.fill(keySpace, Constant.AFFINITY_TYPE_NONE);

        for (Integer pgsID : localPgsIdList) {
            PartitionGroupServer pgs = container.getPgs(clusterName, String.valueOf(pgsID));

            // Get an affinity type that the gateway should use for the PGS.
            char affinityType = Constant.AFFINITY_TYPE_NONE;
            if (pgs.getRole().equals(Constant.PGS_ROLE_MASTER)) {
                affinityType = Constant.AFFINITY_TYPE_ALL;
            } else if (pgs.getRole().equals(Constant.PGS_ROLE_SLAVE)) {
                if (pgs.isMasterInSameMachine(container)) {
                    continue;
                }
                affinityType = Constant.AFFINITY_TYPE_READ;
            } else {
                continue;
            }

            // Set affinity type for the keyspace.
            for (int slot = 0; slot < Constant.KEY_SPACE_SIZE; slot++) {
                if (pgs.getPgId() == pnPgMap.get(slot)) {
                    keySpace[slot] = affinityType;
                }
            }
        }

        return RunLengthEncoder.convertToRLE(keySpace);
    }

    @Override
    public String getClusterName() {
        return cluster.getName();
    }
    
    @Override
    public void setState(String state, long state_timestamp) {
        persistentData.state = state;
        persistentData.stateTimestamp = state_timestamp;
    }
    
    @Override
    public boolean isHBCResponseCorrect(String recvedLine) {
        return recvedLine.contains(Constant.GW_PONG);
    }
    
    @Override
    public HBState getHeartbeatState() {
        return hbSession.getHeartbeatState();
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

    static class RunLengthEncoder {
        public static <T> String convertToRLE(T[] rawData) {
            StringBuilder sb = new StringBuilder();
            int slotStart = 0;

            /* slot pg mapping(Run Length Encoding) */
            for (int i = 1; i < rawData.length; i++) {
                if (rawData[slotStart] != rawData[i]) {
                    sb.append("" + rawData[slotStart] + (i - slotStart));
                    slotStart = i;
                }
            }
            sb.append("" + rawData[slotStart] + (rawData.length - slotStart));
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        return fullName(getClusterName(), getName());
    }
    
    public static String fullName(String clusterName, String gwId) {
        return clusterName + "/gw:" + gwId;
    }

    @Override
    public String getFullName() {
        return toString();
    }

	@Override
	public int compareTo(Gateway o) {
		return name.compareTo(o.name);
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
		return NodeType.GW;
	}
	
	public int getZNodeVersion() {
		return znodeVersion;
	}

    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

    public void propagateStateToHeartbeatSession(int mode) {
        hbSession.getHeartbeatState().setZkData(getState(),
                persistentData.stateTimestamp, getZNodeVersion());
        hbSession.toggleHearbeat(mode, getHeartbeat());
    }
    
    public void turnOnUrgentHeartbeat() {
        hbSession.urgent();
    }

    public byte[] persistentDataToBytes() {
        return persistentData.toBytes();
    }
    
    public String persistentDataToString() {
        return persistentData.toString();
    }
    
    public GatewayData clonePersistentData() {
        return (GatewayData) persistentData.clone();
    }

    public void setPersistentData(GatewayData d) {
        persistentData = d;
    }
    
    public void setPersistentData(byte[] data) {
        persistentData = mapper.readValue(data, GatewayData.class);
    }

    @JsonAutoDetect(
            fieldVisibility=Visibility.ANY, 
            getterVisibility=Visibility.NONE, 
            setterVisibility=Visibility.NONE)
    @JsonIgnoreProperties(
            ignoreUnknown=true)
    @JsonPropertyOrder(
            {"pm_Name", "pm_IP", "port", "state", "stateTimestamp", "hb"})
    public static class GatewayData implements Cloneable {
        
        @JsonProperty("pm_Name")
        public String pmName;
        @JsonProperty("pm_IP")
        public String pmIp;
        @JsonProperty("port")
        public int port;
        @JsonProperty("state")    
        public String state;
        @JsonProperty("stateTimestamp")
        public long stateTimestamp;
        @JsonProperty("hb")
        public String hb;
    
        @JsonIgnore
        private final MemoryObjectMapper mapper = new MemoryObjectMapper();

        public GatewayData() {}
        
        public GatewayData(String pmName, String pmIp, int port) {
            this.pmName = pmName;
            this.pmIp = pmIp;
            this.port = port;
            
            // Use default value
            this.stateTimestamp = 0;
            this.state = SERVER_STATE_FAILURE;
            this.hb = HB_MONITOR_YES;
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
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof GatewayData)) {
                return false;
            }
    
            GatewayData rhs = (GatewayData) obj;
            if (!pmName.equals(rhs.pmName)) {
                return false;
            }
            if (!pmIp.equals(rhs.pmIp)) {
                return false;
            }
            if (port != rhs.port) {
                return false;
            }
            if (!state.equals(rhs.state)) {
                return false;
            }
            if (!hb.equals(rhs.hb)) {
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
        public String toString() {
            return mapper.writeValueAsString(this);
        }
        
        public byte[] toBytes() {
            return mapper.writeValueAsBytes(this);
        }
    
    }

    public String getPmName() {
        return persistentData.pmName;
    }

    public String getState() {
        return persistentData.state;
    }

    public String getPmIp() {
        return persistentData.pmIp;
    }
    
    @Override
    public String getHeartbeat() {
        return persistentData.hb;
    }

    @Override
    public String getView() {
        return persistentData.state;
    }

    @Override
    public void setZNodeVersion(int version) {
        znodeVersion = version;
    }

    @Override
    public String getIP() {
        return persistentData.pmIp;
    }

    @Override
    public int getPort() {
        return persistentData.port;
    }
}

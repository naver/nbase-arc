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
import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.io.MultipleGatewayInvocator;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.server.cluster.GatewayLookup.GatewayAffinityData;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

public class Cluster implements Comparable<Cluster>, ClusterComponent {
    
    private final ReentrantReadWriteLock pgRWLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock gwRWLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock pgsListRWLock = new ReentrantReadWriteLock();
    
    private final AtomicInteger gwAffVer = new AtomicInteger();
    
    private ClusterComponentContainer clusterComponentContainer;
    private WorkflowExecutor wfExecutor;

    private MemoryObjectMapper mapper = new MemoryObjectMapper();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    private String path;
    private String name;
    private ClusterData persistentData;

    public Cluster(ApplicationContext context, String clusterName,
			List<Integer> quorumPolicyAsList, List<Integer> pnPgMap,
			int clusterOn) {
    	persistentData = new ClusterData(quorumPolicyAsList, pnPgMap, clusterOn);
    	init(context, clusterName);
	}

	public Cluster(ApplicationContext context, String name, byte[] d) {
		persistentData = mapper.readValue(d, ClusterData.class);
		init(context, name);
	}

	public Cluster(ApplicationContext context, ClusterData d, String name) {
        persistentData = d;
        init(context, name);
    }

    public void init(ApplicationContext context, String clusterName) {
        path = PathUtil.clusterPath(clusterName);
        name = clusterName;
        
        clusterComponentContainer = context.getBean(ClusterComponentContainer.class);
        wfExecutor = context.getBean(WorkflowExecutor.class);
    }
    
    public Integer getQuorum(Integer closePgsCount) {
        List<Integer> qpList = persistentData.quorumPolicy;
        if (qpList.size() < closePgsCount + 1) {
            return qpList.get(qpList.size() - 1);
        } else {
            return qpList.get(closePgsCount);
        }
    }
    
    public String getGatewayAffinity(ApplicationContext context) {
        List<GatewayAffinityData> gwAffinityList = getGatewayAffinityAsList(context);
        return mapper.writeValueAsString(gwAffinityList);
    }

    public List<GatewayAffinityData> getGatewayAffinityAsList(ApplicationContext context) {
        List<Gateway> gwList = clusterComponentContainer.getGwList(name);
        List<GatewayAffinityData> gwAffinityList = new ArrayList<GatewayAffinityData>(); 
        
        for (Gateway gw : gwList) {
            String gatewayAffinity = gw.getAffinity(clusterComponentContainer);
            
            GatewayAffinityData gwAffinity = new GatewayAffinityData(gw.getName(), gatewayAffinity);
            gwAffinityList.add(gwAffinity);
        }
        
        return gwAffinityList;
    }
    
    public void performUpdateGwAff() {
        wfExecutor.perform(UPDATE_GATEWAY_AFFINITY, this, gwAffVer.incrementAndGet());
    }
    
    public int getGwAffVer() {
        return gwAffVer.get();
    }

    /**
     * @return if succeeded then return null, otherwise return an error message 
     */
    public String isGatewaysAlive() {
        List<Gateway> gwList = clusterComponentContainer.getGwList(name);
        
        Iterator<Gateway> iter = gwList.iterator();
        List<Gateway> failedGatewayList = new ArrayList<Gateway>();
        
        while (iter.hasNext()) {
            Gateway gw = iter.next();
            if (gw.getHeartbeat().equals(Constant.HB_MONITOR_YES)
                    && !gw.getState().equals(Constant.SERVER_STATE_NORMAL)) {
                failedGatewayList.add(gw);
            }
        }

        if (!failedGatewayList.isEmpty()) {
            StringBuffer reply = new StringBuffer("-ERR unavailable gateways list=");
            reply.append(MultipleGatewayInvocator.makeGatewayListString(failedGatewayList));
            return reply.toString();
        }
        
        return null;
    }
    
    public Lock pgListReadLock() {
        return pgRWLock.readLock();
    }

    public Lock pgListWriteLock() {
        return pgRWLock.writeLock();
    }

    public Lock pgsListReadLock() {
        return pgsListRWLock.readLock();
    }

    public Lock pgsListWriteLock() {
        return pgsListRWLock.writeLock();
    }

    public Lock gwListReadLock() {
        return gwRWLock.readLock();
    }

    public Lock gwListWriteLock() {
        return gwRWLock.writeLock();
    }
    
    @Override
    public String toString() {
        return fullName(name);
    }
    
    public static String fullName(String clusterName) {
        return clusterName;
    }

    public void propagateModeToHeartbeatSession() {
        List<PartitionGroupServer> pgsList = clusterComponentContainer.getPgsList(name);
        for (PartitionGroupServer pgs : pgsList) {
            pgs.propagateStateToHeartbeatSession();
        }
        List<RedisServer> rsList = clusterComponentContainer.getRsList(name);
        for (RedisServer rs : rsList) {
            rs.propagateStateToHeartbeatSession();
        }
        List<Gateway> gwList = clusterComponentContainer.getGwList(name);
        for (Gateway gw : gwList) {
            gw.propagateStateToHeartbeatSession(persistentData.mode);
        }
    }
    
    public static void loadClusterFromZooKeeper(ApplicationContext context, String name)
            throws MgmtZooKeeperException, NoNodeException {
        ZooKeeperHolder zk = context.getBean(ZooKeeperHolder.class);
        ClusterComponentContainer container = context.getBean(ClusterComponentContainer.class);

        final String path = PathUtil.clusterPath(name);

        // Build Cluster
        zk.registerChangedEventWatcher(path);
        zk.registerChildEventWatcher(path);

        byte[] d = zk.getData(path, null);
        Cluster cluster = new Cluster(context, name, d);
        container.put(cluster.getPath(), cluster);

        // Load PartitionGroup in this Cluster
        List<String> pgList = zk.getChildren(PathUtil.pgRootPath(name), true);
        for (String pgName : pgList) {
            final String pgPath = PathUtil.pgPath(pgName, name);
            byte[] data = zk.getData(pgPath, null, true);
            PartitionGroup pg = (PartitionGroup) container.get(pgPath);
            if (pg != null) {
                pg.setPersistentData(data);
            } else {
                pg = new PartitionGroup(context, pgPath, pgName, name, data);
                container.put(pg.getPath(), pg);
            }
            zk.registerChildEventWatcher(pgPath);
        }

        // Load PartitionGroupServers in this Cluster
        List<String> pgsList = zk.getChildren(PathUtil.pgsRootPath(name), true);
        for (String pgsName : pgsList) {
            Stat stat = new Stat();
            String pgsPath = PathUtil.pgsPath(pgsName, cluster.getName());
            d = context.getBean(ZooKeeperHolder.class).getData(pgsPath, stat, true);
            PartitionGroupServer pgs = (PartitionGroupServer) container.get(pgsPath); 
            if (pgs != null) {
                pgs.setPersistentData(d);
                pgs.setZNodeVersion(stat.getVersion());
            } else {
                pgs = new PartitionGroupServer(context, d, cluster.getName(), pgsName, stat.getVersion());
                container.put(pgs.getPath(), pgs);
            }
            zk.registerChildEventWatcher(pgsPath);

            stat = new Stat();
            String rsPath = PathUtil.rsPath(pgsName, cluster.getName());
            d = zk.getData(rsPath, stat, true);
            RedisServer rs = (RedisServer) container.get(rsPath);
            if (rs != null) {
                rs.setPersistentData(d);
                rs.setZNodeVersion(stat.getVersion());
            } else {
                rs = new RedisServer(context, d, cluster.getName(), pgsName, pgs.getPgId(), stat.getVersion());
                container.put(rs.getPath(), rs);
            }
            zk.registerChildEventWatcher(rsPath);
        }

        // Load Gateways in this Cluster
        List<String> gwInThisCluster = zk.getChildren(PathUtil.gwRootPath(name), true);
        for (String gwName : gwInThisCluster) {
            Stat stat = new Stat();
            String gwPath = PathUtil.gwPath(gwName, cluster.getName());
            byte[] data = zk.getData(gwPath, stat, true);
            Gateway gw = (Gateway) container.get(gwPath);
            if (gw != null) {
                gw.setPersistentData(data);
                gw.setZNodeVersion(stat.getVersion());
            } else {
                gw = new Gateway(context, cluster.getName(), gwName, data, stat.getVersion());
                container.put(gw.getPath(), gw);
            }
            zk.registerChildEventWatcher(gwPath);
        }
    }

    @Override
    public int compareTo(Cluster o) {
        return name.compareTo(o.name);
    }

    public String getName() {
        return name;
    }
    
    public String getPath() {
        return path;
    }

    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

    @Override
    public void release() {
    }

    public void setPersistentData(ClusterData clusterM) {
        persistentData = clusterM;
    }

    public void setPersistentData(byte[] data) {
        persistentData = mapper.readValue(data, ClusterData.class);
    }

    public byte[] persistentDataToBytes() {
        return persistentData.toBytes();
    }
    
    public String persistentDataToString() {
        return persistentData.toString();
    }

    public ClusterData clonePersistentData() {
        return (ClusterData) persistentData.clone();
    }

	@JsonAutoDetect(
	        fieldVisibility=Visibility.ANY, 
	        getterVisibility=Visibility.NONE, 
	        setterVisibility=Visibility.NONE)
	@JsonIgnoreProperties(
	        ignoreUnknown=true)
	@JsonPropertyOrder(
	        { "key_Space_Size", "quorum_Policy", "pn_PG_Map", "phase", "mode" })
	public static class ClusterData implements Cloneable {
	    
	    @JsonProperty("key_Space_Size")
	    public int keySpaceSize;
	    @JsonProperty("quorum_Policy")
	    private List<Integer> quorumPolicy = new ArrayList<Integer>();
	    @JsonProperty("pn_PG_Map")
	    private List<Integer> pnPgMap = new ArrayList<Integer>();
	    @JsonProperty("phase")
	    public String phase = CLUSTER_PHASE_INIT;
	    @JsonProperty("mode")
	    public int mode = CLUSTER_ON;
	    
	    @JsonIgnore
	    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
	    
	    public ClusterData() {}
	    
	    public ClusterData(List<Integer> quorumPolicy,
				List<Integer> pnPgMap, int mode) {
	    	this.keySpaceSize = KEY_SPACE_SIZE;
	    	this.quorumPolicy = quorumPolicy;
	    	this.pnPgMap = pnPgMap;
	    	this.mode = mode;
		}

		public List<Integer> getQuorumPolicy() {
	        return quorumPolicy;
	    }
	
	    public void setQuorumPolicy(List<Integer> quorum_Policy) {
	        quorumPolicy = quorum_Policy;
	    }
	
	    public List<Integer> getPnPgMap() {
	        return pnPgMap;
	    }
	
	    public String pNPgMapToRLS() {
	        StringBuilder sb = new StringBuilder();
	        int slotStart = 0;
	
	        /* slot pg mapping(Run Length Encoding) */
	        for (int i = 1; i < this.pnPgMap.size(); i++) {
	            if (!this.getPnPgMap().get(slotStart).equals(this.pnPgMap.get(i))) {
	                sb.append(String.format("%d %d ", this.pnPgMap.get(slotStart), i - slotStart));
	                slotStart = i;
	            }
	        }
	        sb.append(String.format("%d %d", this.pnPgMap.get(slotStart),
	                this.pnPgMap.size() - slotStart));
	        return sb.toString();
	    }
	
	    public void setPnPgMap(List<Integer> pN_PG_Map) {
	        pnPgMap = pN_PG_Map;
	    }
	
	    @Override
	    public Object clone() {
	        try {
	        	ClusterData obj = (ClusterData) super.clone();
		        obj.quorumPolicy = new ArrayList<Integer>(this.quorumPolicy);
		        obj.pnPgMap = new ArrayList<Integer>(this.pnPgMap);
		        return obj;
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
	        if (!(obj instanceof ClusterData)) {
	            return false;
	        }
	
	        ClusterData rhs = (ClusterData) obj;
	        if (keySpaceSize != rhs.keySpaceSize) {
	            return false;
	        }
	        if (!quorumPolicy.equals(rhs.quorumPolicy)) {
	            return false;
	        }
	        if (!pnPgMap.equals(rhs.pnPgMap)) {
	            return false;
	        }
	        if (!phase.equals(rhs.phase)) {
	            return false;
	        }
	        if (mode != rhs.mode) {
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

	public int getMode() {
		return persistentData.mode;
	}

	public List<Integer> getPnPgMap() {
		return persistentData.getPnPgMap();
	}

	public int getKeySpaceSize() {
		return persistentData.keySpaceSize;
	}

	public List<Integer> getQuorumPolicy() {
		return persistentData.quorumPolicy;
	}

	public String pNPgMapToRLS() {
		return persistentData.pNPgMapToRLS();
	}
}

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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;

public class PartitionGroup implements Comparable<PartitionGroup>, ClusterComponent {
    
    private String clusterName;
    
    private AtomicLong wfEpoch;
    private AtomicLong wfCnt;
    
    private String path;
    private String name;
    
    private PartitionGroupData persistentData;
    private final MemoryObjectMapper mapper = new MemoryObjectMapper();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public PartitionGroup(ApplicationContext context, String path, String pgId,
            String clusterName, byte[] data) {
        persistentData = mapper.readValue(data, PartitionGroupData.class);
        init(context, clusterName, pgId);
    }
    
    public PartitionGroup(ApplicationContext context, String pgId,
            String clusterName, PartitionGroupData d) {
        persistentData = d;
        init(context, clusterName, pgId);
    }

    public PartitionGroup(ApplicationContext context, String clusterName, String pgId) {
    	persistentData = new PartitionGroupData();
    	init(context, clusterName, pgId);
    }
    
    public void init(ApplicationContext context, String clusterName, String pgId) {
        this.path = PathUtil.pgPath(pgId, clusterName);
        this.name = pgId;
        this.clusterName = clusterName;
        
        wfEpoch = new AtomicLong();
        wfCnt = new AtomicLong();
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    public List<PartitionGroupServer> getSlaves(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> slaveList = new ArrayList<PartitionGroupServer>(); 
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getRole().equals(PGS_ROLE_SLAVE)
                    && pgs.getColor() == GREEN) {
                slaveList.add(pgs);
            }
        }
        return slaveList;
    }
    
    public PartitionGroupServer getMaster(List<PartitionGroupServer> pgsList) {
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getRole().equals(PGS_ROLE_MASTER)
                    && pgs.getColor() == GREEN) {
                return pgs;
            }
        }
        return null;
    }

    public List<PartitionGroupServer> getJoinedPgsList(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> joinedPgsList = new ArrayList<PartitionGroupServer>();
        
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getHeartbeat().equals(HB_MONITOR_YES)) {
                joinedPgsList.add(pgs);
            }
        }
        
        return joinedPgsList;
    }
    
    public List<PartitionGroupServer> getAlivePgsList(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> availablePgsList = new ArrayList<PartitionGroupServer>();
        
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getHeartbeat().equals(HB_MONITOR_YES)
                    && (pgs.getState().equals(SERVER_STATE_LCONN) 
                            || pgs.getState().equals(SERVER_STATE_NORMAL))) {
                availablePgsList.add(pgs);
            }
        }
        
        return availablePgsList;
    } 

    public List<PartitionGroupServer> getAvailablePgsList(List<PartitionGroupServer> pgsList) {
        List<PartitionGroupServer> availablePgsList = new ArrayList<PartitionGroupServer>();
        
        for (PartitionGroupServer pgs : pgsList) {
            if (pgs.getHeartbeat().equals(HB_MONITOR_YES) && 
                    pgs.getState().equals(SERVER_STATE_NORMAL)) {
                availablePgsList.add(pgs);
            }
        }
        
        return availablePgsList;
    }
    
    public List<String> getClosePgsIds(PartitionGroupServer master, 
            Map<String, LogSequence> logSeqMap) {
        /* 
         * The number of SMRs which are live and 
         * log sequences of them are not less than master`s log sequence - max-Logger-seq.
         */
        List<String> closePgsList = new ArrayList<String>();
        LogSequence masterLogSeq = logSeqMap.get(master.getName());
        Set<String> keys = logSeqMap.keySet();
        Iterator<String> iter = keys.iterator();
        
        while (iter.hasNext()) {
            String pgsid = iter.next();
            if (pgsid.equals(master.getName())) {
                continue;
            }
            
            LogSequence logSeq = logSeqMap.get(pgsid);
            if (logSeq == null) {
                continue;
            }
            
            long diff = masterLogSeq.getMax() - logSeq.getMax();
            
            if (diff < SETQUORUM_ADMITTABLE_RANGE) {
                closePgsList.add(pgsid);
            }
        }
        
        return closePgsList;
    }

    public int getD(List<PartitionGroupServer> pgsList) {
        int d = 0;
        for (PartitionGroupServer pgs : pgsList) {
            Color c = pgs.getColor();
            if (c != GREEN && c != BLUE) {
                d++;
            }
        }
        
        return d;
    }

    public List<String> getQuorumMembers(PartitionGroupServer master,
            List<PartitionGroupServer> joinedPgsList) {
        List<String> quorumMembers = new ArrayList<String>();

        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs == master) {
                continue;
            }
            
            if (pgs.getColor() == GREEN
                    || pgs.getColor() == BLUE) {
                quorumMembers.add(pgs.getName());
            }
        }
        
        return quorumMembers;
    }
    
    public String getQuorumMembersString(PartitionGroupServer master,
            List<PartitionGroupServer> joinedPgsList) {
        StringBuilder sb = new StringBuilder();
        for (PartitionGroupServer pgs : joinedPgsList) {
            if (pgs == master) {
                continue;
            }
            
            if (pgs.getColor() == GREEN
                    || pgs.getColor() == BLUE) {
                sb.append(pgs.getName()).append(" ");
            }
        }
        
        if (sb.length() > 0) {
            return sb.subSequence(0, sb.length() - 1).toString();
        } else {
            return sb.toString();
        }
    }
    
    public SortedLogSeqSet getLogSeq(List<PartitionGroupServer> pgsList)
            throws IOException {
        SortedLogSeqSet logSeqSet = new SortedLogSeqSet();

        for (PartitionGroupServer pgs : pgsList) {
            LogSequence logSeq = new LogSequence(pgs);
            logSeq.initialize();
            logSeqSet.add(logSeq);
        }

        return logSeqSet;
    }
    
    public boolean isMasterCandidate(PartitionGroupServer pgs,
            SortedLogSeqSet logSeqMap, List<PartitionGroupServer> pgsList) {
        final int rank = logSeqMap.stdCompetitionRank(pgs) - 1;
        if (rank < 0) {
            return false;
        }
        return rank <= persistentData.quorum - getD(pgsList);
    }

    /**
     * @brief choose a master among candidates
     * @return return a master
     */
    public PartitionGroupServer chooseMasterRandomly(
            final SortedLogSeqSet logSeqMap,
            final List<PartitionGroupServer> joinedPgsList) {
        final int d = getD(joinedPgsList);
        final int r = new Random(System.currentTimeMillis()).nextInt(
                persistentData.quorum - d + 1);
        return logSeqMap.get(r);
    }
    
    @Override
    public String toString() {
        return fullName(getClusterName(), name);
    }
    
    public String info() {
        StringBuilder sb = new StringBuilder(persistentData.toString());
        sb.insert(sb.length() - 1, ",\"wf\":");
        sb.insert(sb.length() - 1, getWfCnt());
        return sb.toString();
    }
    
    public static String fullName(String clusterName, String pgId) {
        return clusterName + "/pg:" + pgId;
    }
    
    public long nextWfEpoch() {
        return wfEpoch.incrementAndGet();
    }
    
    public long getLastWfEpoch() {
        return wfEpoch.get();
    }
    
    public void incWfCnt() {
        wfCnt.incrementAndGet();
    }
    
    public void decWfCnt() {
        wfCnt.decrementAndGet();
    }
    
    public long getWfCnt() {
        return wfCnt.get();
    }
    

	@JsonAutoDetect(
	        fieldVisibility=Visibility.ANY, 
	        getterVisibility=Visibility.NONE, 
	        setterVisibility=Visibility.NONE)
	@JsonIgnoreProperties(ignoreUnknown=true)
	@JsonPropertyOrder({"pgs_ID_List", "master_Gen_Map", "copy", "quorum"})
	public static class PartitionGroupData implements Cloneable {
	    
	    @JsonProperty("pgs_ID_List")
	    private List<Integer> pgsIdList;
	    @JsonProperty("master_Gen_Map")
	    private ConcurrentSkipListMap<Integer, Long> masterGenMap;
	    @JsonProperty("copy")
	    public Integer copy;
	    @JsonProperty("quorum")
	    public Integer quorum;
	
	    @JsonIgnore
	    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
	    
	    public PartitionGroupData() {
	    	// Use default value
		    pgsIdList = new ArrayList<Integer>();
		    masterGenMap = new ConcurrentSkipListMap<Integer, Long>();
		    copy = 0;
		    quorum = 0;
	    }
	    
	    public void addPgsId(Integer id) {
	        pgsIdList.add(id);
	    }
	    
	    public void deletePgsId(Integer id) {
	        pgsIdList.remove(id);
	    }
	    
	    public void deletePgsId(String id) {
	        pgsIdList.remove(Integer.valueOf(id));
	    }
	    
	    public void cleanPgsId() {
	        pgsIdList.clear();
	    }
	
	    @SuppressWarnings("unchecked")
	    public List<Integer> getPgsIdList() {
	        return (List<Integer>)((ArrayList<Integer>)pgsIdList).clone();
	    }
	
	    public void setPgsIdList(List<Integer> pgsIdList) {
	        this.pgsIdList = pgsIdList;
	    }
	
	    public int addMasterGen(Long lastCommitSeq) {
	        int gen;
	        if (masterGenMap.isEmpty()) {
	            gen = 0;
	        } else {
	            gen = masterGenMap.lastKey() + 1;
	        }
	        masterGenMap.put(gen, lastCommitSeq);
	        return gen;
	    }
	
	    public Long commitSeq(Integer gen) {
	        /*
	         * Example of commit sequence:
	         *   PGS MASTER GEN : 7
	         *   PG MASTER GEN  : "master_Gen_Map":{ "0":0,
	         *                                       "1":0,
	         *                                       "2":9390275105,
	         *                                       "3":13419624657,
	         *                                       "4":16486317143,
	         *                                       "5":19760989962,
	         *                                       "6":25515008609}
	         */
	        if (gen == -1) {
	            return 0L;
	        }
	        return masterGenMap.get(gen);
	    }
	    
	    public long minMaxLogSeq(Integer fromGen) {
	        if (fromGen < masterGenMap.firstKey()) {
	            return 0L;
	        }
	        
	        return Collections.min(masterGenMap.tailMap(fromGen).values());
	    }
	    
	    public Long currentSeq() {
	        return commitSeq(currentGen());
	    }
	
	    public int currentGen() {
	        if (masterGenMap.isEmpty()) {
	            return -1;
	        } else {
	            return masterGenMap.lastKey();
	        }
	    }
	    
	    public int nextGen() {
	        return currentGen() + 1;
	    }
	
	    @SuppressWarnings("unchecked")
	    public ConcurrentSkipListMap<Integer, Long> getMasterGenMap() {
	        return (ConcurrentSkipListMap<Integer, Long>)((ConcurrentSkipListMap<Integer, Long>)masterGenMap).clone();
	    }
	
	    protected void setMaster_Gen_Map(ConcurrentSkipListMap<Integer, Long> map) {
	        this.masterGenMap = map;
	    }

	    public void cleanMGen(int mgenHistorySize) {
	        final int lastKey = masterGenMap.lastKey();
	        for (Map.Entry<Integer, Long> e : masterGenMap.entrySet()) {
	            if (e.getKey() <= lastKey - mgenHistorySize) {
	                masterGenMap.remove(e.getKey());
	            }
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
	        if (!(obj instanceof PartitionGroupData)) {
	            return false;
	        }
	
	        PartitionGroupData rhs = (PartitionGroupData) obj;
	        if (!getPgsIdList().equals(rhs.getPgsIdList())) {
	            return false;
	        }
	        if (!getMasterGenMap().equals(rhs.getMasterGenMap())) {
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
	        	PartitionGroupData obj = (PartitionGroupData) super.clone();
		        obj.pgsIdList = new ArrayList<Integer>(this.pgsIdList);
		        obj.masterGenMap = new ConcurrentSkipListMap<Integer, Long>(this.masterGenMap);
		        return obj;
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

	public PartitionGroupData clonePersistentData() {
		return (PartitionGroupData) persistentData.clone();
	}

	public void setPersistentData(PartitionGroupData d) {
		persistentData = d;
	}
	
	public void setPersistentData(byte []d) {
		persistentData = mapper.readValue(d, PartitionGroupData.class);
	}

	public byte[] persistentDataToBytes() {
		return persistentData.toBytes();
	}
	
	public String persistentDataToString() {
		return persistentData.toString();
	}
	
	public int currentGen() {
		return persistentData.currentGen();
	}

	public String getPath() {
		return path;
	}

	public String getName() {
		return name;
	}

	public List<Integer> getPgsIdList() {
		return persistentData.getPgsIdList();
	}

	public int getQuorum() {
		return persistentData.quorum;
	}

	public int getCopy() {
		return persistentData.copy;
	}

	@Override
	public int compareTo(PartitionGroup o) {
		return name.compareTo(o.name);
	}
	
    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

	public ConcurrentSkipListMap<Integer, Long> getMasterGenMap() {
		return persistentData.getMasterGenMap();
	}

    @Override
    public void release() {
    }
    
}

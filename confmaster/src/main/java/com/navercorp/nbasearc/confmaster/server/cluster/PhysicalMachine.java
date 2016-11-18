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

import java.util.ArrayList;
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

import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;

public class PhysicalMachine implements Comparable<PhysicalMachine>, ClusterComponent {

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    private String path;
    private String name;
    private PhysicalMachineData persistentData;
    
    public PhysicalMachine(String pmName, String pmIp) {
    	persistentData = new PhysicalMachineData(pmIp);
    	init(pmName);
    }

    public PhysicalMachine(byte[] d, String pmName) {
    	persistentData = mapper.readValue(d, PhysicalMachineData.class);
    	init(pmName);
    }

    public PhysicalMachine(ApplicationContext context2, PhysicalMachineData d, String pmName) {
        persistentData = d;
        init(pmName);
    }

    public void init(String pmName) {
    	path = PathUtil.pmPath(pmName);
    	name = pmName;
    }

    public String getClusterListString(ClusterComponentContainer container) {
        List<PhysicalMachineCluster> clusterList = container.getPmcList(name);
        StringBuilder ret = new StringBuilder();
        
        ret.append("[");
        
        for (PhysicalMachineCluster cluster : clusterList) {
            ret.append("{\"").append(cluster.getName()).append("\":");
            ret.append(cluster.persistentDataToString()).append("}, ");
        }

        if (!clusterList.isEmpty()) {
            ret.delete(ret.length() - 2, ret.length());
        }
        
        ret.append("]");
        
        return ret.toString();
    }
    
    @Override
    public String toString() {
        return fullName(getName());
    }
    
    public static String fullName(String pmName) { 
        return "pm:" + pmName;
    }
    
    public String getPath() {
        return path;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int compareTo(PhysicalMachine o) {
        return name.compareTo(o.name);
    }

    @Override
    public void release() {
    }

    public byte[] persistentDataToBytes() {
        return persistentData.toBytes();
    }
    
    public String persistentDataToString() {
        return persistentData.toString();
    }
    
    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

    public void setPersistentData(byte[] d) {
        persistentData = mapper.readValue(d, PhysicalMachineData.class);
    }

	@JsonAutoDetect(
	        fieldVisibility=Visibility.ANY, 
	        getterVisibility=Visibility.NONE, 
	        setterVisibility=Visibility.NONE)
	@JsonIgnoreProperties(ignoreUnknown=true)
	@JsonPropertyOrder({"ip"})
	public static class PhysicalMachineData {
	
	    @JsonProperty("ip")
	    public String ip;
	
	    @JsonIgnore
	    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
	
	    public PhysicalMachineData() {}
	    
	    public PhysicalMachineData(String ip) {
	        this.ip = ip;
	    }
	
	    @Override
	    public boolean equals(Object obj) {
	        if (obj == null) {
	            return false;
	        }
	        if (obj == this) {
	            return true;
	        }
	        if (!(obj instanceof PhysicalMachineData)) {
	            return false;
	        }
	
	        PhysicalMachineData rhs = (PhysicalMachineData) obj;
	        if (!ip.equals(rhs.ip)) {
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
	
	    public static class ClusterInPm {
	
	        private String name;
	
	        private List<Integer> pgsList = new ArrayList<Integer>();
	        private List<Integer> gwList = new ArrayList<Integer>();
	
	        public void addPgs(Integer id) {
	            pgsList.add(id);
	        }
	
	        public void deletePgs(Integer id) {
	            pgsList.remove(id);
	        }
	
	        public void addGw(Integer id) {
	            gwList.add(id);
	        }
	
	        public void deleteGw(Integer id) {
	            gwList.remove(id);
	        }
	
	        public List<Integer> getPgs_list() {
	            return pgsList;
	        }
	
	        public void setPgs_list(List<Integer> pgsList) {
	            this.pgsList = pgsList;
	        }
	
	        public List<Integer> getGw_list() {
	            return gwList;
	        }
	
	        public void setGw_list(List<Integer> gwList) {
	            this.gwList = gwList;
	        }
	
	        public String getName() {
	            return name;
	        }
	
	        public void setName(String name) {
	            this.name = name;
	        }
	
	    }
	
	}

	public String getIp() {
		return persistentData.ip;
	}
}

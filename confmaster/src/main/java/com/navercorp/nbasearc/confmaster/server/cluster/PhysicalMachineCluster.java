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

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;

import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;

public class PhysicalMachineCluster implements
        Comparable<PhysicalMachineCluster>, ClusterComponent {

    private MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    private String clusterName;
    private String pmName;
    private String path;
    private PmClusterData persistentData;

    public PhysicalMachineCluster(byte []d, String clusterName, String pmName) {
    	persistentData = mapper.readValue(d, PmClusterData.class);
    	init(clusterName, pmName);
    }
    
    public PhysicalMachineCluster(String clusterName, String pmName) {
    	persistentData = new PmClusterData();
    	init(clusterName, pmName);
    }
    
    public void init(String clusterName, String pmName) {
    	this.pmName = pmName;
    	this.clusterName = clusterName;
        
        path = PathUtil.pmClusterPath(clusterName, pmName);
        setPmName(pmName);
    }
    
    public String getPmName() {
        return pmName;
    }

    public void setPmName(String pmName) {
        this.pmName = pmName;
    }
    
    public boolean isEmpty() {
        return persistentData.getPgsIdList().isEmpty() && persistentData.getGwIdList().isEmpty();
    }
    
    @Override
    public String toString() {
        return fullName(pmName, clusterName);
    }
    
    public static String fullName(String pmName, String clusterName) {
        return "pm:" + pmName + "/cluster:" + clusterName;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String getName() {
        return clusterName;
    }

    @Override
    public void release() {
    }
    
    @Override
    public int compareTo(PhysicalMachineCluster o) {
        return pmName.compareTo(o.pmName);
    }

    public byte[] persistentDataToBytes() {
        return persistentData.toBytes();
    }
    
    public String persistentDataToString() {
        return persistentData.toString();
    }

    public PmClusterData clonePersistentData() {
        return (PmClusterData) persistentData.clone();
    }

	@JsonAutoDetect(
	        fieldVisibility=Visibility.ANY, 
	        getterVisibility=Visibility.NONE, 
	        setterVisibility=Visibility.NONE)
	@JsonIgnoreProperties(ignoreUnknown=true)
	@JsonPropertyOrder({"pgs_ID_List", "gw_ID_List"})
	public static class PmClusterData implements Cloneable {
	    
	    @JsonProperty("pgs_ID_List")
	    private List<Integer> pgsIdList;
	    @JsonProperty("gw_ID_List")
	    private List<Integer> gwIdList;
	
	    @JsonIgnore
	    private final MemoryObjectMapper mapper = new MemoryObjectMapper();
	
	    public PmClusterData() {
		    pgsIdList = new ArrayList<Integer>();
		    gwIdList = new ArrayList<Integer>();
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
	
	    public void addGwId(Integer id) {
	        gwIdList.add(id);
	    }
	    
	    public void deleteGwId(Integer id) {
	        gwIdList.remove(id);
	    }
	    
	    public void deleteGwId(String id) {
	        gwIdList.remove(Integer.valueOf(id));
	    }
	
	    public List<Integer> getPgsIdList() {
	        return pgsIdList;
	    }
	
	    public void setPgsIdList(List<Integer> pgsIdList) {
	        this.pgsIdList = pgsIdList;
	    }
	
	    public List<Integer> getGwIdList() {
	        return gwIdList;
	    }
	
	    public void setGwIdList(List<Integer> gwIdList) {
	        this.gwIdList = gwIdList;
	    }
	    
	    @Override
	    public boolean equals(Object obj) {
	        if (obj == null) {
	            return false;
	        }
	        if (obj == this) {
	            return true;
	        }
	        if (!(obj instanceof PmClusterData)) {
	            return false;
	        }
	
	        PmClusterData rhs = (PmClusterData) obj;
	        if (!pgsIdList.equals(rhs.pgsIdList)) {
	            return false;
	        }
	        if (!gwIdList.equals(rhs.gwIdList)) {
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
	            super.clone(); 
	        } catch (CloneNotSupportedException e) {
	            throw new RuntimeException(e);
	        }
	        PmClusterData data = new PmClusterData();
	        data.pgsIdList = new ArrayList<Integer>(this.pgsIdList);
	        data.gwIdList = new ArrayList<Integer>(this.gwIdList);
	        return data;
	    }
	
	    @Override
	    public String toString() {
            return mapper.writeValueAsString(this);
	    }
	
	    public byte[] toBytes() {
            return mapper.writeValueAsBytes(this);
	    }
	    
	}

	public List<Integer> getPgsIdList() {
		return persistentData.getPgsIdList();
	}

	public List<Integer> getGwIdList() {
		return persistentData.getGwIdList();
	}

	public void addGwId(Integer gwId) {
		persistentData.addGwId(gwId);
	}

	public void deleteGwId(String gwId) {
		persistentData.deleteGwId(gwId);
	}

	public void addPgsId(Integer pgsId) {
		persistentData.addPgsId(pgsId);	
	}
	
	public void deletePgsId(String pgsId) {
		persistentData.deletePgsId(pgsId);
	}
}

package com.navercorp.nbasearc.confmaster.repository.znode;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.ObjectMapper;

@JsonAutoDetect(
        fieldVisibility=Visibility.ANY, 
        getterVisibility=Visibility.NONE, 
        setterVisibility=Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonPropertyOrder({"pgs_ID_List", "gw_ID_List"})
public class PmClusterData implements Cloneable {
    
    @JsonProperty("pgs_ID_List")
    private List<Integer> pgsIdList = new ArrayList<Integer>();
    @JsonProperty("gw_ID_List")
    private List<Integer> gwIdList = new ArrayList<Integer>();

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();

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
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
    
}

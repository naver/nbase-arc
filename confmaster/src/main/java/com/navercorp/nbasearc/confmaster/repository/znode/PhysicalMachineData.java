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
@JsonPropertyOrder({"ip"})
public class PhysicalMachineData {

    @JsonProperty("ip")
    private String ip;

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();

    public PhysicalMachineData() {
    }

    public PhysicalMachineData(String ip) {
        this.ip = ip;
    }

    public void setIp(final String IP, final boolean update) {
        this.ip = IP;
    }

    public void setIp(final String IP) {
        this.ip = IP;
    }

    public String getIp() {
        return this.ip;
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
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public class ClusterInPm {

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

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
@JsonIgnoreProperties(
        ignoreUnknown=true)
@JsonPropertyOrder(
        { "key_Space_Size", "quorum_Policy", "pn_PG_Map", "phase" })
public class ClusterData {
    
    @JsonProperty("key_Space_Size")
    private int Key_Space_Size;
    @JsonProperty("quorum_Policy")
    private List<Integer> Quorum_Policy = new ArrayList<Integer>();
    @JsonProperty("pn_PG_Map")
    private List<Integer> PN_PG_Map = new ArrayList<Integer>();
    @JsonProperty("phase")
    private String Phase;
    
    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();

    public int getKeySpaceSize() {
        return Key_Space_Size;
    }

    public void setKeySpaceSize(int key_Space_Size) {
        Key_Space_Size = key_Space_Size;
    }

    public List<Integer> getQuorumPolicy() {
        return Quorum_Policy;
    }

    public void setQuorumPolicy(List<Integer> quorum_Policy) {
        Quorum_Policy = quorum_Policy;
    }

    public List<Integer> getPbPgMap() {
        return PN_PG_Map;
    }

    public String pNPgMapToRLS() {
        StringBuilder sb = new StringBuilder();
        int slotStart = 0;

        /* slot pg mapping(Run Length Encoding) */
        for (int i = 1; i < this.PN_PG_Map.size(); i++) {
            if (!this.getPbPgMap().get(slotStart).equals(this.PN_PG_Map.get(i))) {
                sb.append(String.format("%d %d ", this.PN_PG_Map.get(slotStart), i - slotStart));
                slotStart = i;
            }
        }
        sb.append(String.format("%d %d", this.PN_PG_Map.get(slotStart),
                this.PN_PG_Map.size() - slotStart));
        return sb.toString();
    }

    public void setPnPgMap(List<Integer> pN_PG_Map) {
        PN_PG_Map = pN_PG_Map;
    }

    public String getPhase() {
        return Phase;
    }

    public void setPhase(String phase) {
        Phase = phase;
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
        if (Key_Space_Size != rhs.Key_Space_Size) {
            return false;
        }
        if (!Quorum_Policy.equals(rhs.Quorum_Policy)) {
            return false;
        }
        if (!PN_PG_Map.equals(rhs.PN_PG_Map)) {
            return false;
        }
        if (!Phase.equals(rhs.Phase)) {
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
    
}

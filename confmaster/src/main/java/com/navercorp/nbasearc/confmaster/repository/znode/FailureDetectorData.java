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
@JsonPropertyOrder({ "quorum_Policy" })
public class FailureDetectorData {
    
    @JsonProperty("quorum_Policy")
    private List<Integer> quorumPolicy = new ArrayList<Integer>();

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();

    public List<Integer> getQuorumPolicy() {
        return quorumPolicy;
    }

    public void setQuorumPolicy(List<Integer> quorumPolicy) {
        this.quorumPolicy = quorumPolicy;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof FailureDetectorData)) {
            return false;
        }

        FailureDetectorData rhs = (FailureDetectorData) obj;
        if (!quorumPolicy.equals(rhs.quorumPolicy)) {
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

package com.navercorp.nbasearc.confmaster.repository.znode;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

@JsonAutoDetect(
        fieldVisibility=Visibility.ANY, 
        getterVisibility=Visibility.NONE, 
        setterVisibility=Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonPropertyOrder({"name", "opinion", "version", "state_timestamp"})
public class OpinionData {
    
    @JsonProperty("name")
    private String name;
    @JsonProperty("opinion")
    private String opinion;
    @JsonProperty("version")
    private int version;
    @JsonProperty("state_timestamp")
    private long stateTimestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getOpinion() {
        return opinion;
    }
    
    public void setOpinion(String opinion) {
        this.opinion = opinion;
    }
    
    public int getVersion() {
        return version;
    }
    
    public void setVersion(int version) {
        this.version = version;
    }

    public long getStatetimestamp() {
        return stateTimestamp;
    }

    public void setStatetimestamp(long stateTimestamp) {
        this.stateTimestamp = stateTimestamp;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof OpinionData)) {
            return false;
        }

        OpinionData rhs = (OpinionData) obj;
        if (!getName().equals(rhs.getName())) {
            return false;
        }
        if (!opinion.equals(rhs.opinion)) {
            return false;
        }
        if (stateTimestamp != rhs.stateTimestamp) {
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
        StringBuilder sb = new StringBuilder();
        sb.append("Opinion(NAME=").append(getName()).append(", STATE=")
                .append(getOpinion()).append(", VERSION=")
                .append(getStatetimestamp()).append(")");
        return sb.toString();
    }

}

package com.navercorp.nbasearc.confmaster.repository.znode;

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
        {"pm_Name", "pm_IP", "port", "state", "stateTimestamp", "hb"})
public class GatewayData implements Cloneable {
    
    @JsonProperty("pm_Name")
    private String pmName;
    @JsonProperty("pm_IP")
    private String pmIp;
    @JsonProperty("port")
    private int port;
    @JsonProperty("state")    
    private String state;
    @JsonProperty("stateTimestamp")
    private long stateTimestamp;
    @JsonProperty("hb")
    private String hb;

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();
    
    public void initialize(final String pmName,
                            final String pmIp,
                            final int port,
                            final String state,
                            final String hb) {
        this.pmName = pmName;
        this.pmIp = pmIp;
        this.port = port;
        this.state = state;
        this.stateTimestamp = 0;
        this.hb = hb;
    }

    public String getPmName() {
        return pmName;
    }

    public void setPmName(String pmName) {
        this.pmName = pmName;
    }

    public String getPmIp() {
        return pmIp;
    }

    public void setPmIp(String pmIp) {
        this.pmIp = pmIp;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getHB() {
        return hb;
    }

    public void setHB(String hB) {
        hb = hB;
    }

    public long getStateTimestamp() {
        return stateTimestamp;
    }
    
    public void setStateTimestamp(long stateTimeStamp) {
        this.stateTimestamp = stateTimeStamp;
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
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

}

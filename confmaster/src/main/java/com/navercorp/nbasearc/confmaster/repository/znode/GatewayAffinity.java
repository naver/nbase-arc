package com.navercorp.nbasearc.confmaster.repository.znode;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;

@JsonAutoDetect(
        fieldVisibility=Visibility.ANY, 
        getterVisibility=Visibility.NONE, 
        setterVisibility=Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonPropertyOrder({"affinity", "gw_id"})
public class GatewayAffinity {
    
    @JsonProperty("gw_id")
    private Integer gwId;
    @JsonProperty("affinity")
    private String affinity;
    
    public GatewayAffinity(String gw_id, String affinity) {
        setGwId(Integer.valueOf(gw_id));
        setAffinity(affinity);
    }

    public Integer getGwId() {
        return gwId;
    }

    public void setGwId(Integer gw_id) {
        this.gwId = gw_id;
    }

    public String getAffinity() {
        return affinity;
    }

    public void setAffinity(String affinity) {
        this.affinity = affinity;
    }
    
}
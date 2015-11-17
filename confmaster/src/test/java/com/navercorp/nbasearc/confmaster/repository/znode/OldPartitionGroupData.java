package com.navercorp.nbasearc.confmaster.repository.znode;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

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
@JsonPropertyOrder({"pgs_ID_List", "master_Gen_Map"})
@JsonIgnoreProperties(ignoreUnknown=true)
public class OldPartitionGroupData {
    
    @JsonProperty("pgs_ID_List")
    private List<Integer> pgsIdList = new ArrayList<Integer>();
    @JsonProperty("master_Gen_Map")
    private SortedMap<Integer, Long> masterGenMap = new TreeMap<Integer, Long>();

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();

    public List<Integer> getPgsIdList() {
        return pgsIdList;
    }

    public SortedMap<Integer, Long> getMasterGenMap() {
        return masterGenMap;
    }
    
}

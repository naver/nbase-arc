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

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
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonPropertyOrder({"pgs_ID_List", "master_Gen_Map", "copy", "quorum"})
public class PartitionGroupData implements Cloneable {
    
    @JsonProperty("pgs_ID_List")
    private List<Integer> pgsIdList = new ArrayList<Integer>();
    @JsonProperty("master_Gen_Map")
    private SortedMap<Integer, Long> masterGenMap = new TreeMap<Integer, Long>();
    @JsonProperty("copy")
    private Integer copy = 0;
    @JsonProperty("quorum")
    private Integer quorum = 0;

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();
    
    public static PgDataBuilder builder() {
        return new PgDataBuilder();
    }

    protected  void addPgsId(Integer id) {
        pgsIdList.add(id);
    }
    
    protected  void deletePgsId(Integer id) {
        pgsIdList.remove(id);
    }
    
    protected  void deletePgsId(String id) {
        pgsIdList.remove(Integer.valueOf(id));
    }

    @SuppressWarnings("unchecked")
    public List<Integer> getPgsIdList() {
        return (List<Integer>)((ArrayList<Integer>)pgsIdList).clone();
    }

    protected  void setPgsIdList(List<Integer> pgsIdList) {
        this.pgsIdList = pgsIdList;
    }

    protected  int addMasterGen(Long lastCommitSeq) {
        int gen;
        if (masterGenMap.isEmpty()) {
            gen = 0;
        } else {
            gen = masterGenMap.lastKey() + 1;
        }
        masterGenMap.put(gen, lastCommitSeq);
        return gen;
    }

    public Long commitSeq(Integer gen) {
        /*
         * Example of commit sequence:
         *   PGS MASTER GEN : 7
         *   PG MASTER GEN  : "master_Gen_Map":{ "0":0,
         *                                       "1":0,
         *                                       "2":9390275105,
         *                                       "3":13419624657,
         *                                       "4":16486317143,
         *                                       "5":19760989962,
         *                                       "6":25515008609}
         */
        if (gen == -1) {
            return 0L;
        }
        return masterGenMap.get(gen);
    }
    
    public Long currentSeq() {
        return commitSeq(currentGen() - 1);
    }

    public int currentGen() {
        if (masterGenMap.isEmpty()) {
            return 0;
        } else {
            return masterGenMap.lastKey() + 1;
        }
    }

    @SuppressWarnings("unchecked")
    public SortedMap<Integer, Long> getMasterGenMap() {
        return (SortedMap<Integer, Long>)((TreeMap<Integer, Long>)masterGenMap).clone();
    }

    protected void setMaster_Gen_Map(SortedMap<Integer, Long> map) {
        this.masterGenMap = map;
    }

    public Integer getCopy() {
        return copy;
    }

    public void setCopy(Integer copy) {
        this.copy = copy;
    }

    public Integer getQuorum() {
        return quorum;
    }

    public void setQuorum(Integer quorum) {
        this.quorum = quorum;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof PartitionGroupData)) {
            return false;
        }

        PartitionGroupData rhs = (PartitionGroupData) obj;
        if (!getPgsIdList().equals(rhs.getPgsIdList())) {
            return false;
        }
        if (!getMasterGenMap().equals(rhs.getMasterGenMap())) {
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
        PartitionGroupData obj = new PartitionGroupData();
        obj.pgsIdList = new ArrayList<Integer>(this.pgsIdList);
        obj.masterGenMap = new TreeMap<Integer, Long>(this.masterGenMap);
        return obj;
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

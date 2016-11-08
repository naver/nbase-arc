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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup.PartitionGroupData;


public class PartitionGroupDataTest {

    @Test
    public void equals() {
        PartitionGroupData d1 = new PartitionGroupData();
        assertEquals(d1, d1);
        
        PartitionGroupData d2 = new PartitionGroupData();
        assertEquals(d1, d2);
        
        d1.addPgsId(0);
        d1.addPgsId(1);
        d1.addMasterGen(0L);
        d1.addMasterGen(100L);
        d2.addPgsId(0);
        d2.addPgsId(1);
        d2.addMasterGen(0L);
        d2.addMasterGen(100L);
        assertEquals(d1, d2);

        d2.addMasterGen(200L);
        assertNotEquals(d1, d2);

        PartitionGroupData d3 = new PartitionGroupData();
        d3.addPgsId(0);
        d3.addPgsId(1);
        d3.addMasterGen(0L);
        d3.addMasterGen(100L);
        d3.addPgsId(2);
        assertNotEquals(d1, d3);
        
        assertNotEquals(d1, null);
        assertNotEquals(d1, new Object());
    }
    
    /*
     * For upgrade
     */
    @Test
    public void emptyCopyQuorumData() throws JsonParseException,
            JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();

        // Create PartitionGroupData
        String raw = "{\"master_Gen_Map\":{\"0\":0,\"1\":0,\"2\":95},\"pgs_ID_List\":[1,51]}";
        PartitionGroupData data = mapper.readValue(raw, PartitionGroupData.class);

        SortedMap<Integer, Long> masterGenMap = new TreeMap<Integer, Long>();
        masterGenMap.put(0, 0L);
        masterGenMap.put(1, 0L);
        masterGenMap.put(2, 95L);
        
        assertEquals(data.getMasterGenMap(), masterGenMap);
        assertEquals(data.getPgsIdList(), Arrays.asList(new Integer[]{1, 51}));
        
        // Check nullable fields
        assertEquals(new Integer(0), data.copy);
        assertEquals(new Integer(0), data.quorum);
    }
    
    /* 
     * For downgrade
     */
    @Test
    public void emptyCopyQuorumClass() throws JsonParseException,
            JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        
        String raw = "{\"pgs_ID_List\":[1,51],"
                + "\"master_Gen_Map\":{\"0\":0,\"1\":0,\"2\":95},"
                + "\"copy\":1,\"quorum\":1}";
        OldPartitionGroupData data = mapper.readValue(raw, OldPartitionGroupData.class);

        SortedMap<Integer, Long> masterGenMap = new TreeMap<Integer, Long>();
        masterGenMap.put(0, 0L);
        masterGenMap.put(1, 0L);
        masterGenMap.put(2, 95L);
        
        assertEquals(data.getMasterGenMap(), masterGenMap);
        assertEquals(data.getPgsIdList(), Arrays.asList(new Integer[]{1, 51}));
    }
    
    @Test
    public void minCommitSeq() throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();

        PartitionGroupData sequential = mapper.readValue(
                "{\"master_Gen_Map\":{\"0\":0,\"1\":100,\"2\":200,\"3\":300}}",
                PartitionGroupData.class);
        assertEquals(0, sequential.minMaxLogSeq(0));
        assertEquals(100, sequential.minMaxLogSeq(1));
        assertEquals(200, sequential.minMaxLogSeq(2));
        assertEquals(300, sequential.minMaxLogSeq(3));
        
        PartitionGroupData prominent = mapper.readValue(
                "{\"master_Gen_Map\":{\"0\":0,\"1\":100,\"2\":3000,\"3\":2500}}",
                PartitionGroupData.class);
        assertEquals(0, prominent.minMaxLogSeq(0));
        assertEquals(100, prominent.minMaxLogSeq(1));
        assertEquals(2500, prominent.minMaxLogSeq(2));
        assertEquals(2500, prominent.minMaxLogSeq(3));
    }
    
    @Test
    public void partitionGroupData() throws Exception {
        PartitionGroupData data = new PartitionGroupData();
        MemoryObjectMapper mapper = new MemoryObjectMapper();
        mapper.writeValueAsString(data);
    }
}

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

package com.navercorp.nbasearc.confmaster.repository;

import static org.junit.Assert.assertEquals;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.navercorp.nbasearc.confmaster.repository.znode.ClusterBackupScheduleData;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.FailureDetectorData;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayAffinity;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.repository.znode.OpinionData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;
import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;
import com.navercorp.nbasearc.confmaster.repository.znode.PmClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZkWorkflowLog;

public class DataJsonMappingTest {
    
    ObjectMapper mapper = new ObjectMapper();
  
    @Test
    public void failureDetectorData() throws Exception { 
        FailureDetectorData data = new FailureDetectorData();
        String json = mapper.writeValueAsString(data);
        assertEquals(json, "{\"quorum_Policy\":[]}");
    }
    
    @Test
    public void clusterBackupScheduleData() throws Exception { 
        ClusterBackupScheduleData data = new ClusterBackupScheduleData();
        int backup_id = 1;
        data.setBackup_id(backup_id);
        String base_time = "02:00:00";
        data.setBase_time(base_time);
        String cluster_name = "arc_dev";
        data.setCluster_name(cluster_name);
        int daemon_id = 1;
        data.setDaemon_id(daemon_id);
        int holding_period = 3;
        data.setHolding_period(holding_period);
        int net_limit = 70;
        data.setNet_limit(net_limit);
        String output_format = "base32hex";
        data.setOutput_format(output_format);
        String period = "0 2 * * * *";
        data.setPeriod(period);
        String service_url = "rsync -az {FILE_PATH} 192.168.0.1::TEST/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.json";
        data.setService_url(service_url);
        String type = "backup";
        data.setType(type);
        int version = 1;
        data.setVersion(version);
        String json = mapper.writeValueAsString(data);
        assertEquals(json, "{\"cluster_name\":\"arc_dev\",\"type\":\"backup\",\"backup_id\":1,\"daemon_id\":1,\"period\":\"0 2 * * * *\",\"base_time\":\"02:00:00\",\"holding_period\":3,\"net_limit\":70,\"output_format\":\"base32hex\",\"service_url\":\"rsync -az {FILE_PATH} 192.168.0.1::TEST/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.json\",\"version\":1}");
    }
    
    @Test
    public void clusterData() throws Exception {
        ClusterData data = new ClusterData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void gatewayAffinity() throws Exception {
        GatewayAffinity data = new GatewayAffinity("1", "A8192");
        String json = mapper.writeValueAsString(data);
        assertEquals(json, "{\"affinity\":\"A8192\",\"gw_id\":1}");
    }
    
    @Test
    public void gatewayData() throws Exception {
        GatewayData data = new GatewayData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void opinionData() throws Exception {
        OpinionData data = new OpinionData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void partitionGroupData() throws Exception {
        PartitionGroupData data = new PartitionGroupData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void partitionGroupServerData() throws Exception {
        PartitionGroupServerData data = new PartitionGroupServerData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void physicalMachineData() throws Exception {
        PhysicalMachineData data = new PhysicalMachineData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void pmClusterData() throws Exception {
        PmClusterData data = new PmClusterData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void RedisServerData() throws Exception { 
        RedisServerData data = new RedisServerData();
        mapper.writeValueAsString(data);
    }
    
    @Test
    public void zkWorkflowLog() throws Exception {
        ZkWorkflowLog data = new ZkWorkflowLog();
        mapper.writeValueAsString(data);
    }
    
}

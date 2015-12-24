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

package com.navercorp.nbasearc.confmaster.repository.dao.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.nbasearc.confmaster.ConfMasterException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.repository.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.repository.PathUtil;
import com.navercorp.nbasearc.confmaster.repository.ZooKeeperHolder;
import com.navercorp.nbasearc.confmaster.repository.dao.ClusterBackupScheduleDao;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterBackupScheduleData;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterBackupSchedule;

@Repository
public class ZkClusterBackupScheduleDao implements ClusterBackupScheduleDao {
    
    @Autowired
    private ZooKeeperHolder zookeeper;

    private final MemoryObjectMapper mapper = new MemoryObjectMapper();

    @Override
    public String createAppData(String clusterName, ClusterBackupSchedule data)
            throws NodeExistsException, MgmtZooKeeperException {
        String path = PathUtil.clusterBackupSchedulePath(clusterName);
        byte rawData[];
        
        rawData = mapper.writeValueAsBytes(data);
        return zookeeper.createPersistentZNode(path, rawData);
    }

    @Override
    public void setAppData(String clusterName, ClusterBackupSchedule data)
            throws MgmtZooKeeperException {
        String path = PathUtil.clusterBackupSchedulePath(clusterName);
        
        byte rawData[] = mapper.writeValueAsBytes(data);
        zookeeper.setData(path, rawData, -1);
    }
    
    @Override
    public ClusterBackupSchedule loadClusterBackupSchedule(String clusterName)
            throws NoNodeException, MgmtZooKeeperException,
            ConfMasterException {
        ClusterBackupSchedule schedule = new ClusterBackupSchedule();
        ClusterBackupSchedule data = mapper.readValue(load(clusterName),
                ClusterBackupSchedule.class);

        for (ClusterBackupScheduleData ScheduleData : data.getBackupSchedules()
                .values()) {
            schedule.addBackupSchedule(ScheduleData);
        }

        return schedule;
    }
    
    @Override
    public byte[] load(String clusterName) throws NoNodeException,
            MgmtZooKeeperException {
        return zookeeper.getData(
                PathUtil.clusterBackupSchedulePath(clusterName), null, null);
    }
}

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

import java.util.List;

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;

public class PhysicalMachine extends ZNode<PhysicalMachineData> {
    
    public PhysicalMachine(ApplicationContext context, String path,
            String name, byte[] data) {
        super(context);
        
        setTypeRef(new TypeReference<PhysicalMachineData>(){});
        setPath(path);
        setName(name);
        setNodeType(NodeType.PM);
        setData(data);
    }

    public String getClusterListString(PhysicalMachineClusterImo pmClusterImo) {
        List<PhysicalMachineCluster> clusterList = pmClusterImo.getList(getName());
        StringBuilder ret = new StringBuilder();
        
        ret.append("[");
        
        for (PhysicalMachineCluster cluster : clusterList) {
            ret.append("{\"").append(cluster.getName()).append("\":");
            ret.append(cluster.getData().toString()).append("}, ");
        }

        if (!clusterList.isEmpty()) {
            ret.delete(ret.length() - 2, ret.length());
        }
        
        ret.append("]");
        
        return ret.toString();
    }
    
    @Override
    public String toString() {
        return fullName(getName());
    }
    
    public static String fullName(String pmName) { 
        return "pm:" + pmName;
    }

}

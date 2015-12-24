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

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.PmClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;

public class PhysicalMachineCluster extends ZNode<PmClusterData> {
    
    private String pmName;
    
    public PhysicalMachineCluster(ApplicationContext context, String path,
            String name, String pmName, byte[] data) {
        super(context);
        
        setTypeRef(new TypeReference<PmClusterData>(){});
        setPath(path);
        setName(name);
        setPmName(pmName);
        setNodeType(NodeType.CLUSTER_IN_PM);
        setData(data);
    }
    
    public String getPmName() {
        return pmName;
    }

    public void setPmName(String pmName) {
        this.pmName = pmName;
    }
    
    public boolean isEmpty() {
        return getData().getPgsIdList().isEmpty() && getData().getGwIdList().isEmpty();
    }
    
    @Override
    public String toString() {
        return fullName(getPmName(), getName());
    }
    
    public static String fullName(String pmName, String clusterName) {
        return "pm:" + pmName + "/cluster:" + clusterName;
    }

}

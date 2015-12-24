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

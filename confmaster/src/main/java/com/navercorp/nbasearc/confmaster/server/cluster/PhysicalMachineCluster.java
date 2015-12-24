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

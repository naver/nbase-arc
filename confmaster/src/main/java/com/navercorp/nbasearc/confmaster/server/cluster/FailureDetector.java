package com.navercorp.nbasearc.confmaster.server.cluster;

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.repository.znode.FailureDetectorData;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;

public class FailureDetector extends ZNode<FailureDetectorData> {
    
    private int numberOfHeartbeatCheckers = 0;
    
    public FailureDetector(ApplicationContext context, String path, String name, byte[] data) {
        super(context);
        
        setTypeRef(new TypeReference<FailureDetectorData>(){});
    
        setPath(path);
        setName(name);
        setNodeType(NodeType.FD);
        setData(data);
    }
    
    public int getMajority() {
        return getNumberOfHeartbeatCheckers() / 2 + 1;
    }

    public int getNumberOfHeartbeatCheckers() {
        return numberOfHeartbeatCheckers;
    }

    public void setNumberOfHeartbeatCheckers(int numberOfHeartbeatChecker) {
        this.numberOfHeartbeatCheckers = numberOfHeartbeatChecker;
    }

}

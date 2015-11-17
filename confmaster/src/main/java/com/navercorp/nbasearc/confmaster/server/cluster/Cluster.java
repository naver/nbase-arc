package com.navercorp.nbasearc.confmaster.server.cluster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.io.MultipleGatewayInvocator;
import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayAffinity;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.GatewayImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;

public class Cluster extends ZNode<ClusterData> {
    
    private final ReentrantReadWriteLock pgRWLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock gwRWLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock pgsListRWLock = new ReentrantReadWriteLock();
    
    public Cluster(ApplicationContext context, String path, String name,
            byte[] data) {
        super(context);
        
        setTypeRef(new TypeReference<ClusterData>(){});

        setPath(path);
        setName(name);
        setNodeType(NodeType.CLUSTER);
        setData(data);
    }
    
    public Integer getQuorum(Integer closePgsCount) {
        List<Integer> qpList = this.getData().getQuorumPolicy();
        if (qpList.size() < closePgsCount + 1) {
            return qpList.get(qpList.size() - 1);
        } else {
            return qpList.get(closePgsCount);
        }
    }
    
    public String getGatewayAffinity(ApplicationContext context) {
        List<GatewayAffinity> gwAffinityList = getGatewayAffinityAsList(context);
        return mapper.writeValueAsString(gwAffinityList);
    }

    public List<GatewayAffinity> getGatewayAffinityAsList(ApplicationContext context) {
        ClusterImo clusterImo = context.getBean(ClusterImo.class);
        PhysicalMachineClusterImo pmClusterImo = context.getBean(PhysicalMachineClusterImo.class); 
        PartitionGroupServerImo pgsImo = context.getBean(PartitionGroupServerImo.class); 
        GatewayImo gwImo = context.getBean(GatewayImo.class);
        
        List<Gateway> gwList = gwImo.getList(getName());
        List<GatewayAffinity> gwAffinityList = new ArrayList<GatewayAffinity>(); 
        
        for (Gateway gw : gwList) {
            String gatewayAffinity = gw.getAffinity(clusterImo, pmClusterImo, pgsImo);
            
            GatewayAffinity gwAffinity = new GatewayAffinity(gw.getName(), gatewayAffinity);
            gwAffinityList.add(gwAffinity);
        }
        
        return gwAffinityList;
    }

    /**
     * @return if succeeded then return null, otherwise return an error message 
     */
    public String isGatewaysAlive(GatewayImo gwImo) {
        List<Gateway> gwList = gwImo.getList(getName());
        
        Iterator<Gateway> iter = gwList.iterator();
        List<Gateway> failedGatewayList = new ArrayList<Gateway>();
        
        while (iter.hasNext()) {
            Gateway gw = iter.next();
            if (gw.getData().getHB().equals(Constant.HB_MONITOR_YES)
                    && !gw.getData().getState().equals(Constant.SERVER_STATE_NORMAL)) {
                failedGatewayList.add(gw);
            }
        }

        if (!failedGatewayList.isEmpty()) {
            StringBuffer reply = new StringBuffer("-ERR unavailable gateways list=");
            reply.append(MultipleGatewayInvocator.makeGatewayListString(failedGatewayList));
            return reply.toString();
        }
        
        return null;
    }
    
    public Lock pgListReadLock() {
        return pgRWLock.readLock();
    }

    public Lock pgListWriteLock() {
        return pgRWLock.writeLock();
    }

    public Lock pgsListReadLock() {
        return pgsListRWLock.readLock();
    }

    public Lock pgsListWriteLock() {
        return pgsListRWLock.writeLock();
    }

    public Lock gwListReadLock() {
        return gwRWLock.readLock();
    }

    public Lock gwListWriteLock() {
        return gwRWLock.writeLock();
    }
    
    @Override
    public String toString() {
        return fullName(getName());
    }
    
    public static String fullName(String clusterName) {
        return clusterName;
    }

}

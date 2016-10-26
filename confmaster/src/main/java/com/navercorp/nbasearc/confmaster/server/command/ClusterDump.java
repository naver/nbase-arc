package com.navercorp.nbasearc.confmaster.server.command;

import java.util.List;

import com.navercorp.nbasearc.confmaster.repository.znode.ClusterData;

public class ClusterDump {
    private String clusterName;
    private ClusterData cluster;
    private List<PartitionGroupDump> pgList;
    private List<PartitionGroupServerDump> pgsList;
    private List<GatewayDump> gwList;
    private List<PhysicalMachineDump> pmList;
    
    public String getClusterName() {
        return clusterName;
    }
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    public ClusterData getCluster() {
        return cluster;
    }
    public void setCluster(ClusterData cluster) {
        this.cluster = cluster;
    }
    public List<PartitionGroupDump> getPgList() {
        return pgList;
    }
    public void setPgList(List<PartitionGroupDump> pgList) {
        this.pgList = pgList;
    }
    public List<PartitionGroupServerDump> getPgsList() {
        return pgsList;
    }
    public void setPgsList(List<PartitionGroupServerDump> pgsList) {
        this.pgsList = pgsList;
    }
    public List<GatewayDump> getGwList() {
        return gwList;
    }
    public void setGwList(List<GatewayDump> gwList) {
        this.gwList = gwList;
    }
    public List<PhysicalMachineDump> getPmList() {
        return pmList;
    }
    public void setPmList(List<PhysicalMachineDump> pmList) {
        this.pmList = pmList;
    }
}

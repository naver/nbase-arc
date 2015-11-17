package com.navercorp.nbasearc.confmaster.server.cluster;

import com.navercorp.nbasearc.confmaster.heartbeat.HBRefData;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;

public interface HeartbeatTarget {

    int getVersion();

    String getPath();

    String getTargetOfHeartbeatPath();

    String getName();
    
    String getFullName();

    NodeType getNodeType();

    String getHB();

    String getClusterName();

    void setClusterName(String clusterName);

    String getState();

    void setState(String state, long state_timestamp);

    long getStateTimestamp();

    boolean isHBCResponseCorrect(String recvedLine);

    String getPingMsg();

    HBRefData getRefData();

    String getIP();

    int getPort();

    UsedOpinionSet getUsedOpinions();

    <T> ZNode<T> getZNode();

}

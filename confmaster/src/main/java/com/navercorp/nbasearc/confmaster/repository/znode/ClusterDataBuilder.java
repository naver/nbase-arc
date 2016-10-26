package com.navercorp.nbasearc.confmaster.repository.znode;

import java.util.List;

public class ClusterDataBuilder {

    private int keySpaceSize;
    private List<Integer> quorumPolicy;
    private List<Integer> pnPgMap;
    private int mode;
    
    public ClusterDataBuilder from(ClusterData data) {
        withKeySpaceSize(data.getKeySpaceSize());
        withQuorumPolicy(data.getQuorumPolicy());
        withPnPgMap(data.getPnPgMap());
        withMode(data.getMode());
        return this;
    }
    
    public ClusterDataBuilder withKeySpaceSize(int kss) {
        this.keySpaceSize = kss;
        return this;
    }
    
    public ClusterDataBuilder withQuorumPolicy(List<Integer> qp) {
        this.quorumPolicy = qp;
        return this;
    }

    public ClusterDataBuilder withPnPgMap(List<Integer> ppm) {
        this.pnPgMap = ppm;
        return this;
    }

    public ClusterDataBuilder withMode(int m) {
        this.mode = m;
        return this;
    }
    
    public ClusterData build() {
        ClusterData data = new ClusterData();
        data.setKeySpaceSize(keySpaceSize);
        data.setQuorumPolicy(quorumPolicy);
        data.setPnPgMap(pnPgMap);
        data.setMode(mode);
        return data;
    }

}

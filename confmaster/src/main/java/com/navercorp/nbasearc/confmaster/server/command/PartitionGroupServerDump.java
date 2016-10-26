package com.navercorp.nbasearc.confmaster.server.command;

import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupServerData;

public class PartitionGroupServerDump {
    private String pgsId;
    private PartitionGroupServerData data;
    
    public PartitionGroupServerDump() {
    }
    
    public PartitionGroupServerDump(String id, PartitionGroupServerData d) {
        this.setPgsId(id);
        this.setData(d);
    }

    public String getPgsId() {
        return pgsId;
    }

    public void setPgsId(String pgsId) {
        this.pgsId = pgsId;
    }

    public PartitionGroupServerData getData() {
        return data;
    }

    public void setData(PartitionGroupServerData data) {
        this.data = data;
    }
}

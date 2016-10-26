package com.navercorp.nbasearc.confmaster.server.command;

import com.navercorp.nbasearc.confmaster.repository.znode.PartitionGroupData;

public class PartitionGroupDump {
    private String pgId;
    private PartitionGroupData data;
    
    public PartitionGroupDump() {
    }
    
    public PartitionGroupDump(String id, PartitionGroupData d) {
        this.setPgId(id);
        this.setData(d);
    }

    public String getPgId() {
        return pgId;
    }

    public void setPgId(String pgId) {
        this.pgId = pgId;
    }

    public PartitionGroupData getData() {
        return data;
    }

    public void setData(PartitionGroupData data) {
        this.data = data;
    }
}

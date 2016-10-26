
package com.navercorp.nbasearc.confmaster.server.command;

import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;

public class GatewayDump {
    private String gwId;
    private GatewayData data;
    
    public GatewayDump() {
    }
    
    public GatewayDump(String id, GatewayData d) {
        this.setGwId(id);
        this.setData(d);
    }

    public String getGwId() {
        return gwId;
    }

    public void setGwId(String gwId) {
        this.gwId = gwId;
    }

    public GatewayData getData() {
        return data;
    }

    public void setData(GatewayData data) {
        this.data = data;
    }
}

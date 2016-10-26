package com.navercorp.nbasearc.confmaster.server.command;

import com.navercorp.nbasearc.confmaster.repository.znode.PhysicalMachineData;

public class PhysicalMachineDump {
    private String pmName;
    private PhysicalMachineData data;
    
    public PhysicalMachineDump() {
    }
    
    public PhysicalMachineDump(String n, PhysicalMachineData d) {
        this.setPmName(n);
        this.setData(d);
    }

    public String getPmName() {
        return pmName;
    }

    public void setPmName(String pmName) {
        this.pmName = pmName;
    }

    public PhysicalMachineData getData() {
        return data;
    }

    public void setData(PhysicalMachineData data) {
        this.data = data;
    }
}

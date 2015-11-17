package com.navercorp.nbasearc.confmaster.context;

public enum ContextType {
    CC("CC"),   // Cluster Controller
    SV("SV"),   // Server
    WF("WF"),   // Workflow
    CM("CM"),   // Command
    HB("HB"),   // Heartbeat
    GW("GW"),   // Gateway
    PS("PS"),   // Partition Group Server
    RS("RS");   // Redis Server
    
    private String value;
    
    private ContextType(String value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return value;
    }
}
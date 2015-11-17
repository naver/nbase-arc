package com.navercorp.nbasearc.confmaster.repository.znode;

public enum NodeType {
    
    UNKOWN(0), 
    CLUSTER(1),
    PM(2),
    CLUSTER_IN_PM(3),
    PG(4),
    PGS(5), 
    GW(6),
    FD(7),
    RS(8);

    private final int type;

    private NodeType(int type) {
        this.type = type;
    }

    @Override
    public String toString() { 
        switch (this) {
        case UNKOWN:
            return "UNKOWN";
        case CLUSTER:
            return "CLUSTER";
        case PM:
            return "PM";
        case CLUSTER_IN_PM:
            return "CLUSTER_IN_PM"; 
        case PG:
            return "PG";
        case PGS:
            return "PGS";
        case GW:
            return "GW";
        case FD:
            return "FD";
        case RS:
            return "RS";
        default:
            return "";
        }
    }

}

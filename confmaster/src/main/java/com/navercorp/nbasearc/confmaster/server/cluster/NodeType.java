/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.server.cluster;

public enum NodeType {
    
    UNKOWN(0), 
    CLUSTER(1),
    PM(2),
    CLUSTER_IN_PM(3),
    PG(4),
    PGS(5), 
    GW(6),
    FD(7),
    RS(8),
    CLUSTER_ROOT(9),
    PG_ROOT(10),
    PGS_ROOT(11), 
    GW_ROOT(12),
    RS_ROOT(13);

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

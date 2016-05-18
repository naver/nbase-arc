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

package com.navercorp.nbasearc.confmaster.context;

public enum ContextType {
    CC("CC"),   // Cluster Controller
    SV("SV"),   // Server
    WF("WF"),   // Workflow
    CM("CM"),   // Command
    HB("HB"),   // Heartbeat
    GW("GW"),   // Gateway
    PS("PS"),   // Partition Group Server
    RS("RS"),   // Redis Server
    RA("RA"),   // Role Adjustment
    QA("QA"),   // Quorum Adjustment
    ME("ME"),   // Master Election
    YJ("YJ"),   // Yellow Join
    BJ("BJ"),   // Blue Join
    MG("MG");   // Memgership Grant
    
    private String value;
    
    private ContextType(String value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return value;
    }
}
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

package com.navercorp.nbasearc.confmaster.repository.znode;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.ObjectMapper;

import static com.navercorp.nbasearc.confmaster.Constant.*;

@JsonAutoDetect(
        fieldVisibility=Visibility.ANY, 
        getterVisibility=Visibility.NONE, 
        setterVisibility=Visibility.NONE)
@JsonIgnoreProperties(
        ignoreUnknown=true)
@JsonPropertyOrder(
        { "pg_ID", "pm_Name", "pm_IP", "backend_Port_Of_Redis",
        "replicator_Port_Of_SMR", "management_Port_Of_SMR", "state",
        "stateTimestamp", "hb", "smr_Role", "old_SMR_Role", "color", "master_Gen" })
public class PartitionGroupServerData implements Cloneable {
    
    @JsonProperty("pg_ID")
    private int pgId;
    @JsonProperty("pm_Name")
    private String pmName;
    @JsonProperty("pm_IP")
    private String pmIp;
    @JsonProperty("backend_Port_Of_Redis")
    private int redisPort;
    @JsonProperty("replicator_Port_Of_SMR")
    private int smrBasePort;
    @JsonProperty("management_Port_Of_SMR")
    private int smrMgmtPort;
    @JsonProperty("state")
    private String state;
    @JsonProperty("stateTimestamp")
    private long stateTimestamp;
    @JsonProperty("hb")
    private String hb;
    @JsonProperty("smr_Role")
    private String role;
    @JsonProperty("color")
    private Color color;
    @JsonProperty("master_Gen")
    private int masterGen;

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();
    
    public void initialize(final int pgId,
                            final String pmName,
                            final String pmIp,
                            final int redisPort,
                            final int smrBasePort,
                            final int smrMgmtPort,
                            final String state,
                            final String smrRole,
                            final Color color,
                            final int masterGen,
                            final String hb) {
        this.pgId = pgId;
        this.pmName = pmName;
        this.pmIp = pmIp;
        this.redisPort = redisPort;
        this.smrBasePort = smrBasePort;
        this.smrMgmtPort = smrMgmtPort;
        this.state = state;
        this.stateTimestamp = 0;
        this.role = smrRole;
        this.color = color;
        this.masterGen = masterGen;
        this.hb = hb;
    }
    
    public static PgsDataBuilder builder() {
        return new PgsDataBuilder();
    }

    public int getPgId() {
        return pgId;
    }

    protected void setPgId(int pgId) {
        this.pgId = pgId;
    }

    public String getPmName() {
        return pmName;
    }

    protected void setPmName(String pmName) {
        this.pmName = pmName;
    }

    public String getPmIp() {
        return pmIp;
    }

    protected void setPmIp(String pmIp) {
        this.pmIp = pmIp;
    }

    public int getRedisPort() {
        return redisPort;
    }

    protected void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public int getSmrBasePort() {
        return smrBasePort;
    }

    protected void setSmrBasePort(int smrBasePort) {
        this.smrBasePort = smrBasePort;
    }

    public int getSmrMgmtPort() {
        return smrMgmtPort;
    }

    protected void setSmrMgmtPort(int smrMgmtPort) {
        this.smrMgmtPort = smrMgmtPort;
    }

    public String getState() {
        return state;
    }

    protected void setState(String state) {
        this.state = state;
    }

    public String getRole() {
        return role;
    }

    protected void setRole(String role) {
        this.role = role;
    }

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public int getMasterGen() {
        return masterGen;
    }

    protected void setMasterGen(int masterGen) {
        this.masterGen = masterGen;
    }

    public String getHb() {
        return hb;
    }

    protected void setHb(String hb) {
        this.hb = hb;
    }

    public long getStateTimestamp() {
        return stateTimestamp;
    }

    protected void setStateTimestamp(long stateTimestamp) {
        this.stateTimestamp = stateTimestamp;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof PartitionGroupServerData)) {
            return false;
        }

        PartitionGroupServerData rhs = (PartitionGroupServerData) obj;
        if (pgId != rhs.pgId) {
            return false;
        }
        if (!pmName.equals(rhs.pmName)) {
            return false;
        }
        if (!pmIp.equals(rhs.pmIp)) {
            return false;
        }
        if (redisPort != rhs.redisPort) {
            return false;
        }
        if (smrBasePort != rhs.smrBasePort) {
            return false;
        }
        if (smrMgmtPort != rhs.smrMgmtPort) {
            return false;
        }
        if (!state.equals(rhs.state)) {
            return false;
        }
        if (!role.equals(rhs.role)) {
            return false;
        }
        if (masterGen != rhs.masterGen) {
            return false;
        }
        if (!hb.equals(rhs.hb)) {
            return false;
        }
        if (stateTimestamp != rhs.stateTimestamp) {
            return false;
        }
        return true;
    }
    
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }
    
    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
    
}

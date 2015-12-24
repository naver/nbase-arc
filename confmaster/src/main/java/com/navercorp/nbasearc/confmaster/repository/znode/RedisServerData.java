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

@JsonAutoDetect(
        fieldVisibility=Visibility.ANY, 
        getterVisibility=Visibility.NONE, 
        setterVisibility=Visibility.NONE)
@JsonIgnoreProperties(
        ignoreUnknown=true)
@JsonPropertyOrder(
        {"pm_Name", "pm_IP", "backend_Port_Of_Redis", "state", "stateTimestamp", "hb"})
public class RedisServerData implements Cloneable {
    
    @JsonProperty("pm_Name")
    private String pmName;
    @JsonProperty("pm_IP")
    private String pmIp;
    @JsonProperty("backend_Port_Of_Redis")
    private int redisPort;
    @JsonProperty("state")
    private String state;
    @JsonProperty("stateTimestamp")
    private long stateTimestamp;
    @JsonProperty("hb")
    private String hb;

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();
    
    public void initialize(final String pmName,
                            final String pmIp,
                            final int redisPort,
                            final String state,
                            final String HB) {
        this.pmName = pmName;
        this.pmIp = pmIp;
        this.redisPort = redisPort;
        this.state = state;
        this.stateTimestamp = 0;
        this.hb = HB;
    }
    
    public static RsDataBuilder builder() {
        return new RsDataBuilder();
    }

    public String getPmName() {
        return pmName;
    }

    public void setPmName(String pmName) {
        this.pmName = pmName;
    }

    public String getPmIp() {
        return pmIp;
    }

    public void setPmIp(String pmIp) {
        this.pmIp = pmIp;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getState() {
        return state;
    }

    protected void setState(String state) {
        this.state = state;
    }

    public String getHB() {
        return hb;
    }

    protected void setHB(String hB) {
        this.hb = hB;
    }

    public long getStateTimestamp() {
        return stateTimestamp;
    }

    public void setStateTimestamp(long stateTimestamp) {
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
        if (!(obj instanceof RedisServerData)) {
            return false;
        }

        RedisServerData rhs = (RedisServerData) obj;
        if (!pmName.equals(rhs.pmName)) {
            return false;
        }
        if (!pmIp.equals(rhs.pmIp)) {
            return false;
        }
        if (redisPort != rhs.redisPort) {
            return false;
        }
        if (!state.equals(rhs.state)) {
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

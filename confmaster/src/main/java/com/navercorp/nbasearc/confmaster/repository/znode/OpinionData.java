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
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

@JsonAutoDetect(
        fieldVisibility=Visibility.ANY, 
        getterVisibility=Visibility.NONE, 
        setterVisibility=Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonPropertyOrder({"name", "opinion", "version", "state_timestamp"})
public class OpinionData {
    
    @JsonProperty("name")
    private String name;
    @JsonProperty("opinion")
    private String opinion;
    @JsonProperty("version")
    private int version;
    @JsonProperty("state_timestamp")
    private long stateTimestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getOpinion() {
        return opinion;
    }
    
    public void setOpinion(String opinion) {
        this.opinion = opinion;
    }
    
    public int getVersion() {
        return version;
    }
    
    public void setVersion(int version) {
        this.version = version;
    }

    public long getStatetimestamp() {
        return stateTimestamp;
    }

    public void setStatetimestamp(long stateTimestamp) {
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
        if (!(obj instanceof OpinionData)) {
            return false;
        }

        OpinionData rhs = (OpinionData) obj;
        if (!getName().equals(rhs.getName())) {
            return false;
        }
        if (!opinion.equals(rhs.opinion)) {
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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Opinion(NAME=").append(getName()).append(", STATE=")
                .append(getOpinion()).append(", VERSION=")
                .append(getStatetimestamp()).append(")");
        return sb.toString();
    }

}

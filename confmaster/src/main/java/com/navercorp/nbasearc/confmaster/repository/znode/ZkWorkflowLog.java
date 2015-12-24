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

import java.util.Date;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

@JsonAutoDetect(
        fieldVisibility=Visibility.ANY, 
        getterVisibility=Visibility.NONE, 
        setterVisibility=Visibility.NONE)
@JsonIgnoreProperties(
        ignoreUnknown=true)
@JsonPropertyOrder(
        { "logID", "logTime", "jobID", "type", "name", "severity",
        "msg", "severity", "msg", "clusterName", "arguments" })
public class ZkWorkflowLog {
    
    @JsonProperty("logID")
    private long logID;
    @JsonProperty("logTime")
    private Date logTime;
    @JsonProperty("jobID")
    private long jobID;
    @JsonProperty("type")
    private String type;
    @JsonProperty("name")
    private String name;
    @JsonProperty("severity")
    private String severity;
    @JsonProperty("msg")
    private String msg;
    @JsonProperty("clusterName")
    private String clusterName;
    @JsonProperty("arguments")
    private String arguments;

    public ZkWorkflowLog() {
    }

    public ZkWorkflowLog(final long logID, 
                            final Date logTime,
                            final long jobID,
                            final String type, 
                            final String severity,
                            final String name, 
                            final String mgs, 
                            final String clusterName,
                            final String arguments) {
        this.setLogID(logID);
        this.setLogTime(logTime);
        this.setJobID(jobID);
        this.setType(type);
        this.setSeverity(severity);
        this.setName(name);
        this.setMsg(mgs);
        this.setClusterName(clusterName);
        this.setArguments(arguments);
    }

    public String toJsonString() {
        return String
                .format("{\"logID\":%d,\"logTime\":\"%s\",\"jobID\":%d,\"type\":\"%s\",\"severity\":\"%s\",\"name\":\"%s\",\"msg\":\"%s\",\"clusterName\":\"%s\",\"arguments\":%s}",
                        getLogID(), getLogTime(), getJobID(), getType(),
                        getSeverity(), getName(), getMsg(), getClusterName(),
                        getArguments());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(getSeverity() + " ");

        sb.append("MSG=\"" + getMsg() + "\", ");

        if (getClusterName() != null && getClusterName().length() > 0) {
            sb.append("CLUSTER=" + getClusterName() + ", ");
        }

        if (getArguments() != null && !getArguments().equals("null")
                && getArguments().length() > 0) {
            sb.append("ARG=" + getArguments() + ", ");
        }

        sb.append("LID=" + getLogID() + ", ");

        return sb.toString();
    }
    
    public String toStringWithoutInfo() {
        StringBuilder sb = new StringBuilder();

        sb.append(getSeverity() + " ");

        sb.append("MSG=\"" + getMsg() + "\", ");

        if (getClusterName() != null && getClusterName().length() > 0) {
            sb.append("CLUSTER=" + getClusterName() + ", ");
        }

        sb.append("LID=" + getLogID() + ", ");

        return sb.toString();
    }

    public long getLogID() {
        return logID;
    }

    public void setLogID(long logID) {
        this.logID = logID;
    }

    public Date getLogTime() {
        return logTime;
    }

    public void setLogTime(Date logTime) {
        this.logTime = logTime;
    }

    public long getJobID() {
        return jobID;
    }

    public void setJobID(long jobID) {
        this.jobID = jobID;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getArguments() {
        return arguments;
    }

    public void setArguments(String arguments) {
        this.arguments = arguments;
    }

}

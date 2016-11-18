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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;

import com.navercorp.nbasearc.confmaster.ConfMasterException;

public class ClusterBackupSchedule {
    
    private Map<Integer, ClusterBackupScheduleData> backupSchedules = 
            new HashMap<Integer, ClusterBackupScheduleData>();
    
    public ClusterBackupSchedule() {
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(); 
        
        sb.append("[");
        
        for (Entry<Integer, ClusterBackupScheduleData> entry : getBackupSchedules()
                .entrySet()) {
            sb.append(entry.getValue().toString()).append(",");
        }

        if (getBackupSchedules().size() != 0) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("]");
        
        return sb.toString();
    }

    public void addBackupSchedule(ClusterBackupScheduleData backupSchedule) 
            throws ConfMasterException {
        if (getBackupSchedules().get(backupSchedule.getBackup_id()) != null) {
            throw new ConfMasterException("Backup '"
                    + backupSchedule.getBackup_id() + "' is already exist.");
        }
        
        getBackupSchedules().put(backupSchedule.getBackup_id(), backupSchedule);
    }

    public void updateBackupSchedule(ClusterBackupScheduleData backupSchedule) 
            throws ConfMasterException {
        ClusterBackupScheduleData old = getBackupSchedules().get(backupSchedule.getBackup_id());
        if (old == null) {
            throw new ConfMasterException("Backup '"
                    + backupSchedule.getBackup_id() + "' is not exist.");
        }

        backupSchedule.setVersion(old.getVersion() + 1);
        getBackupSchedules().put(backupSchedule.getBackup_id(), backupSchedule);
    }
    
    public void deleteBackupSchedule(int backupScheduleId) throws ConfMasterException {
        if (getBackupSchedules().remove(backupScheduleId) == null) {
            throw new ConfMasterException("Backup '" + backupScheduleId + "' is not exist.");
        }
    }
    
    public ClusterBackupScheduleData getBackupSchedule(int backupScheduleId) {
        return backupSchedules.get(backupScheduleId);
    }
    
    public boolean existBackupJob(int backupID) {
        return backupSchedules.containsKey(backupID);
    }

    public Map<Integer, ClusterBackupScheduleData> getBackupSchedules() {
        return backupSchedules;
    }

    public void setBackupSchedules(Map<Integer, ClusterBackupScheduleData> backupSchedules) {
        this.backupSchedules = backupSchedules;
    }

    @JsonAutoDetect(
            fieldVisibility=Visibility.ANY, 
            getterVisibility=Visibility.NONE, 
            setterVisibility=Visibility.NONE)
    @JsonIgnoreProperties(
            ignoreUnknown=true)
    @JsonPropertyOrder(
            { "cluster_name", "type", "backup_id", "daemon_id",
            "period", "base_time", "holding_period", "net_limit", "output_format",
            "service_url", "version" })
    public static class ClusterBackupScheduleData {
        
        @JsonProperty("cluster_name")
        private String clusterName;
        @JsonProperty("type")
        private String type;
        @JsonProperty("backup_id")
        private int backupId;
        @JsonProperty("daemon_id")
        private int daemonId;
        @JsonProperty("period")
        private String period;
        @JsonProperty("base_time")
        private String baseTime;
        @JsonProperty("holding_period")
        private int holdingPeriod; 
        @JsonProperty("net_limit")
        private int netLimit;
        @JsonProperty("output_format")
        private String outputFormat;
        @JsonProperty("service_url")
        private String serviceUrl;
        @JsonProperty("version")
        private int version = 1;
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            sb.append("{");
            sb.append("\"cluster_name\":\"").append(getCluster_name()).append("\",");
            sb.append("\"type\":\"").append(getType()).append("\",");
            sb.append("\"backup_id\":").append(getBackup_id()).append(",");
            sb.append("\"daemon_id\":").append(getDaemon_id()).append(",");
            sb.append("\"period\":\"").append(getPeriod()).append("\",");
            sb.append("\"base_time\":\"").append(getBase_time()).append("\",");
            sb.append("\"holding_period\":\"").append(getHolding_period()).append("\",");
            sb.append("\"net_limit\":\"").append(getNet_limit()).append("\",");
            sb.append("\"output_format\":\"").append(getOutput_format()).append("\",");
            sb.append("\"service_url\":\"").append(getService_url().replace("\"", "\\\"")).append("\",");
            sb.append("\"version\":").append(getVersion()).append("");
            sb.append("}");
            
            return sb.toString();
        }
        
        public void increaseVersion() {
            this.setVersion(this.getVersion() + 1);
        }

        public String getCluster_name() {
            return clusterName;
        }

        public void setCluster_name(String cluster_name) {
            this.clusterName = cluster_name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public int getBackup_id() {
            return backupId;
        }

        public void setBackup_id(int backup_id) {
            this.backupId = backup_id;
        }

        public int getDaemon_id() {
            return daemonId;
        }

        public void setDaemon_id(int daemon_id) {
            this.daemonId = daemon_id;
        }

        public String getPeriod() {
            return period;
        }

        public void setPeriod(String period) {
            this.period = period;
        }

        public String getBase_time() {
            return baseTime;
        }

        public void setBase_time(String base_time) {
            this.baseTime = base_time;
        }

        public int getHolding_period() {
            return holdingPeriod;
        }

        public void setHolding_period(int holding_period) {
            this.holdingPeriod = holding_period;
        }

        public int getNet_limit() {
            return netLimit;
        }

        public void setNet_limit(int net_limit) {
            this.netLimit = net_limit;
        }

        public String getService_url() {
            return serviceUrl;
        }

        public void setService_url(String service_url) {
            this.serviceUrl = service_url;
        }

        public String getOutput_format() {
            return outputFormat;
        }

        public void setOutput_format(String output_format) {
            this.outputFormat = output_format;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

    }

}

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
@JsonIgnoreProperties(
        ignoreUnknown=true)
@JsonPropertyOrder(
        { "cluster_name", "type", "backup_id", "daemon_id",
        "period", "base_time", "holding_period", "net_limit", "output_format",
        "service_url", "version" })
public class ClusterBackupScheduleData {
    
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

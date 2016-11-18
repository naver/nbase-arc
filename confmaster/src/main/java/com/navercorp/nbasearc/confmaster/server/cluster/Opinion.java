package com.navercorp.nbasearc.confmaster.server.cluster;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtZooKeeperException;
import com.navercorp.nbasearc.confmaster.server.MemoryObjectMapper;
import com.navercorp.nbasearc.confmaster.server.ZooKeeperHolder;

@Component
public class Opinion {
    
    @Autowired
    private ZooKeeperHolder zk;
    
    private MemoryObjectMapper mapper = new MemoryObjectMapper();
    
    public OpinionData getOpinion(final String path)
            throws MgmtZooKeeperException, NoNodeException {
        byte data[] = zk.getData(path, null);
        return mapper.readValue(data, OpinionData.class);
    }

    public List<OpinionData> getOpinions(final String path)
            throws MgmtZooKeeperException, NoNodeException {
        List<OpinionData> opinions = new ArrayList<OpinionData>();

        List<String> children;
        children = zk.getChildren(path);
        
        for (String childName : children) {
            StringBuilder builder = new StringBuilder(path);
            builder.append("/").append(childName);
            String childPath = builder.toString();

            byte data[] = zk.getData(childPath, null);
            OpinionData opData = mapper.readValue(data, OpinionData.class);
            opinions.add(opData);
        }
        
        return opinions;
    }
    
    @JsonAutoDetect(
            fieldVisibility=Visibility.ANY, 
            getterVisibility=Visibility.NONE, 
            setterVisibility=Visibility.NONE)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @JsonPropertyOrder({"name", "opinion", "version", "state_timestamp", "creation_time"})
    public static class OpinionData {
        
        @JsonProperty("name")
        private String name;
        @JsonProperty("opinion")
        private String opinion;
        @JsonProperty("version")
        private int version;
        @JsonProperty("state_timestamp")
        private long stateTimestamp;
        @JsonProperty("creation_time")
        private long creationTime;

        public OpinionData() {}
        
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

        public long getCreationTime() {
            return creationTime;
        }

        public void setCreationTime(long creationTime) {
            this.creationTime = creationTime;
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
            if (creationTime != rhs.creationTime) {
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
}

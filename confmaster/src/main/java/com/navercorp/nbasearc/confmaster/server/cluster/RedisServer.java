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

import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBRefData;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.io.BlockingSocket;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.RedisServerData;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;

public class RedisServer extends ZNode<RedisServerData> implements HeartbeatTarget {
    
    private BlockingSocket serverConnection;    
    private String clusterName;
    
    private HBSession hbc;
    private HBRefData hbcRefData;
    
    private final Config config;
    
    public RedisServer(ApplicationContext context, String path, String name,
            String cluster, byte[] data) {
        super(context);
        this.config = context.getBean(Config.class);
        
        setTypeRef(new TypeReference<RedisServerData>(){}); 

        setPath(path);
        setName(name);
        setClusterName(cluster);
        setNodeType(NodeType.RS);
        setData(data);
        
        setServerConnection(
            new BlockingSocket(
                this.getData().getPmIp(), this.getData().getRedisPort(), 
                config.getClusterPgsTimeout(), Constant.REDIS_PING, 
                config.getDelim(), config.getCharset()));
        
        hbcRefData = new HBRefData();
        hbcRefData.setZkData(getData().getState(), getData().getStateTimestamp(), stat.getVersion())
            .setLastState(Constant.SERVER_STATE_UNKNOWN)
            .setLastStateTimestamp(0L)
            .setSubmitMyOpinion(false);
    }
    
    public void updateHBRef() {
        hbcRefData.setZkData(getData().getState(), 
                getData().getStateTimestamp(), stat.getVersion());
        getHbc().updateState(getData().getHB());
    }

    public void release() {
        try {
            getHbc().updateState(Constant.HB_MONITOR_NO);
            getServerConnection().close();
        } catch (Exception e) {
            Logger.error("stop heartbeat fail. RS:" + getName(), e);
        }

        try {
            getHbc().callbackDelete();
            getHbc().urgent();
        } catch (Exception e) {
            Logger.error("failed while delete rs. " + toString(), e);
        }
    }
    
    public List<String> executeQueryAndMultiReply(String query, int replyCount) throws IOException {
        return getServerConnection().executeAndMultiReply(query, replyCount);
    }
    
    public void closeConnection() throws IOException {
        getServerConnection().close();
    }

    @Override
    public int getVersion() {
        return this.stat.getVersion();
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String getState() {
        return this.getData().getState();
    }

    @Override
    public void setState(String state, long state_timestamp) {
        RedisServerData rsModified = 
                RedisServerData.builder().from(getData())
                    .withState(state)
                    .withStateTimestamp(state_timestamp).build();
        setData(rsModified);
    }
    
    @Override
    public long getStateTimestamp() {
        return getData().getStateTimestamp();
    }

    @Override
    public String getTargetOfHeartbeatPath() {
        return this.getPath();
    }
    
    @Override
    public String getHB() {
        return this.getData().getHB();
    }
    
    @Override
    public boolean isHBCResponseCorrect(String recvedLine) {
        return recvedLine.contains(Constant.REDIS_PONG);
    }
    
    @Override
    public String getPingMsg() {
        return Constant.REDIS_PING + "\r\n";
    }
    
    @Override
    public HBRefData getRefData() {
        return hbcRefData;
    }

    @Override
    public String getIP() {
        return getData().getPmIp();
    }

    @Override
    public int getPort() {
        return getData().getRedisPort();
    }

    @Override
    public UsedOpinionSet getUsedOpinions() {
        return null;
    }

    public String executeQuery(String query) throws IOException {
        return getServerConnection().execute(query);
    }

    public String executeQuery(String query, int retryCount) throws IOException {
        return getServerConnection().execute(query, retryCount);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ZNode<RedisServerData> getZNode() {
        return this;
    }

    public HBSession getHbc() {
        return hbc;
    }

    public void setHbc(HBSession hbc) {
        this.hbc = hbc;
    }
    
    public String getRealState() {
        try {
            String reply = executeQuery(Constant.PGS_PING);
            Logger.info("CMD=\"" + Constant.PGS_PING + 
                    "\", REPLY=\"" + reply + 
                    "\", CLUSTER:" + getClusterName() + 
                    ", PGSID:" + getName() + 
                    ", STATE:" + getData().getState());
            if (reply.equals(Constant.REDIS_PONG)) {
                return Constant.SERVER_STATE_NORMAL;
            } else {
                return Constant.SERVER_STATE_FAILURE;
            }
        } catch (IOException e) {
            return Constant.SERVER_STATE_FAILURE;
        }
    }

    public BlockingSocket getServerConnection() {
        return serverConnection;
    }

    public void setServerConnection(BlockingSocket serverConnection) {
        this.serverConnection = serverConnection;
    }
    
    @Override
    public String toString() {
        return fullName(getClusterName(), getName());
    }
    
    public static String fullName(String clusterName, String pgsId) {
        return clusterName + "/rs:" + pgsId;
    }

    @Override
    public String getFullName() {
        return toString();
    }

}

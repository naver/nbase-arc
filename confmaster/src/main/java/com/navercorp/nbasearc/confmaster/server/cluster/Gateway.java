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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.type.TypeReference;
import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.heartbeat.HBRefData;
import com.navercorp.nbasearc.confmaster.heartbeat.HBSession;
import com.navercorp.nbasearc.confmaster.io.BlockingSocketImpl;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.znode.GatewayData;
import com.navercorp.nbasearc.confmaster.repository.znode.NodeType;
import com.navercorp.nbasearc.confmaster.repository.znode.ZNode;
import com.navercorp.nbasearc.confmaster.server.imo.ClusterImo;
import com.navercorp.nbasearc.confmaster.server.imo.PartitionGroupServerImo;
import com.navercorp.nbasearc.confmaster.server.imo.PhysicalMachineClusterImo;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandler;
import com.navercorp.nbasearc.confmaster.server.watcher.WatchEventHandlerGw;

public class Gateway extends ZNode<GatewayData> implements HeartbeatTarget {
    
    private BlockingSocketImpl serverConnection;
    private Cluster cluster;
    
    private HBSession hbc;
    private HBRefData hbcRefData;
    
    private WatchEventHandlerGw watcher;
    
    private final Config config;
    
    public Gateway(ApplicationContext context, String path, String name,
            Cluster cluster, byte[] data) {
        super(context);
        
        this.config = context.getBean(Config.class);
        
        setTypeRef(new TypeReference<GatewayData>(){});
        setPath(path);
        setName(name);
        this.cluster = cluster;
        setNodeType(NodeType.GW);
        setData(data);
        
        setServerConnection(
            new BlockingSocketImpl(
                getData().getPmIp(), getData().getPort() + 1, 
                config.getClusterGwTimeout(), Constant.GW_PING,
                config.getDelim(), config.getCharset()));
        
        hbcRefData = new HBRefData();
        hbcRefData.setZkData(getData().getState(), getData().getStateTimestamp(), stat.getVersion())
                  .setLastState(getData().getState())
                  .setLastStateTimestamp(getData().getStateTimestamp())
                  .setSubmitMyOpinion(false);
    }
    
    public void release() {
        try {
            getServerConnection().close();
            getHbc().callbackDelete();
        } catch (Exception e) {
            Logger.error("failed while delete gw. " + toString(), e);
        }
    }

    /*
     * return a string of Gateway Affinity in Run Length Encoding Format.
     * Affinity Type : A(All), W(Write), R(Read), N(None)
     * Run Length Encoding Format : <Affinity Type><Slot Size><Affinity Type><Slot Size>...
     *                              ex) A2048R2048N4096
     */
    public String getAffinity(ClusterImo clusterImo,
            PhysicalMachineClusterImo pmClusterImo, PartitionGroupServerImo pgsImo) {
        return getAffinity(getClusterName(), getData().getPmName(),
                clusterImo, pmClusterImo, pgsImo);
    }
    
    public static String getAffinity(String clusterName, String pmName, 
            ClusterImo clusterImo, PhysicalMachineClusterImo pmClusterImo,
            PartitionGroupServerImo pgsImo) {
        final Cluster cluster = clusterImo.get(clusterName);
        final List<Integer> pnPgMap = cluster.getData().getPnPgMap();
        final PhysicalMachineCluster machineInfo = 
                pmClusterImo.get(clusterName, pmName);
        final List<Integer> localPgsIdList;
        if (machineInfo != null) {
            localPgsIdList = machineInfo.getData().getPgsIdList();
        } else {
            /*
             * When machineInfo is null, there are no PGS in the machine,
             * so that set localPgsIdList with empty list, ArrayList<Integer>().
             */
            localPgsIdList = new ArrayList<Integer>();
        }
        Character[] keySpace = new Character[Constant.KEY_SPACE_SIZE];
        Arrays.fill(keySpace, Constant.AFFINITY_TYPE_NONE);

        for (Integer pgsID : localPgsIdList) {
            PartitionGroupServer pgs = pgsImo.get(String.valueOf(pgsID), clusterName);

            // Get an affinity type that the gateway should use for the PGS.
            char affinityType = Constant.AFFINITY_TYPE_NONE;
            if (pgs.getData().getRole().equals(Constant.PGS_ROLE_MASTER)) {
                affinityType = Constant.AFFINITY_TYPE_ALL;
            } else if (pgs.getData().getRole().equals(Constant.PGS_ROLE_SLAVE)) {
                if (pgs.isMasterInSameMachine(pmClusterImo, pgsImo)) {
                    continue;
                }
                affinityType = Constant.AFFINITY_TYPE_READ;
            } else {
                continue;
            }

            // Set affinity type for the keyspace.
            for (int slot = 0; slot < Constant.KEY_SPACE_SIZE; slot++) {
                if (pgs.getData().getPgId() == pnPgMap.get(slot)) {
                    keySpace[slot] = affinityType;
                }
            }
        }

        return RunLengthEncoder.convertToRLE(keySpace);
    }

    @Override
    public int getVersion() {
        return this.stat.getVersion();
    }

    @Override
    public String getClusterName() {
        return cluster.getName();
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
    public String getView() {
        return this.getData().getState();
    }
    
    @Override
    public void setState(String state, long state_timestamp) {
        this.getData().setState(state);
        this.getData().setStateTimestamp(state_timestamp);
    }
    
    @Override
    public long getStateTimestamp() {
        return getData().getStateTimestamp();
    }
    
    @Override
    public boolean isHBCResponseCorrect(String recvedLine) {
        return recvedLine.contains(Constant.GW_PONG);
    }
    
    @Override
    public String getPingMsg() {
        return Constant.GW_PING + "\r\n";
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
        return getData().getPort();
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
    public ZNode<GatewayData> getZNode() {
        return this;
    }

    public WatchEventHandler getWatcher() {
        return watcher;
    }

    public void setWatcher(WatchEventHandlerGw watcher) {
        this.watcher = watcher;
    }
    
    public BlockingSocketImpl getServerConnection() {
        return serverConnection;
    }

    public void setServerConnection(BlockingSocketImpl serverConnection) {
        this.serverConnection = serverConnection;
    }

    public HBSession getHbc() {
        return hbc;
    }

    public void setHbc(HBSession hbc) {
        this.hbc = hbc;
    }

    static class RunLengthEncoder {
        public static <T> String convertToRLE(T[] rawData) {
            StringBuilder sb = new StringBuilder();
            int slotStart = 0;

            /* slot pg mapping(Run Length Encoding) */
            for (int i = 1; i < rawData.length; i++) {
                if (rawData[slotStart] != rawData[i]) {
                    sb.append("" + rawData[slotStart] + (i - slotStart));
                    slotStart = i;
                }
            }
            sb.append("" + rawData[slotStart] + (rawData.length - slotStart));
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        return fullName(getClusterName(), getName());
    }
    
    public static String fullName(String clusterName, String gwId) {
        return clusterName + "/gw:" + gwId;
    }

    @Override
    public String getFullName() {
        return toString();
    }
    
}

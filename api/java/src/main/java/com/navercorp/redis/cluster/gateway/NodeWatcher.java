/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.navercorp.redis.cluster.gateway;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author seongminwoo
 * @author jaehong.kim
 */
public class NodeWatcher implements Watcher {
    private static final String ZK_CLUSTER_PATH = "/RC/NOTIFICATION/CLUSTER";
    private static final String ZK_GATEWAY = "/GW";
    private static final String ZK_AFFINITY = "/AFFINITY";
    private static final String KEY_IP = "ip";
    private static final String KEY_PORT = "port";
    private static final String KEY_GW_ID = "gw_id";
    private static final String KEY_AFFINITY = "affinity";

    private final Logger log = LoggerFactory.getLogger(NodeWatcher.class);

    final CountDownLatch connLatcher = new CountDownLatch(1);

    private ZooKeeper zk;
    private GatewayConfig config;
    private GatewayServerData gatewayServerData;
    private ObjectMapper objectMapper = new ObjectMapper();


    public NodeWatcher(GatewayServerData gatewayServerData) {
        this.gatewayServerData = gatewayServerData;
    }

    public void setConfig(GatewayConfig config) {
        this.config = config;
    }

    public void start() throws IOException, InterruptedException, KeeperException {
        log.info("[NodeWatcher] Starting {} {}", config.getZkAddress(), buildGatewayPath());

        this.zk = new ZooKeeper(config.getZkAddress(), config.getZkSessionTimeout(), this);
        this.connLatcher.await(config.getZkConnectTimeout(), TimeUnit.MILLISECONDS); // if not yet connected to ZooKeeper, wait
    }

    String buildGatewayPath() {
        StringBuilder sb = new StringBuilder();
        sb.append(ZK_CLUSTER_PATH).append("/").append(config.getClusterName()).append(ZK_GATEWAY);
        return sb.toString();
    }

    String buildAffinityPath() {
        StringBuilder sb = new StringBuilder();
        sb.append(ZK_CLUSTER_PATH).append("/").append(config.getClusterName()).append(ZK_AFFINITY);
        return sb.toString();
    }

    public List<GatewayAddress> getGatewayAddress() throws KeeperException, InterruptedException, IOException {
        final List<GatewayAddress> addresses = new ArrayList<GatewayAddress>();
        final List<String> children = zk.getChildren(buildGatewayPath(), this);

        for (String node : children) {
            final int id = Integer.parseInt(node);
            final String path = buildGatewayPath() + "/" + node;
            final byte[] data = zk.getData(path, this, null);
            if (data == null) {
                continue;
            }
            log.debug("[NodeWatcher] Gateway info from CM. {path={}, data={}}", path, new String(data));

            try {
                Map keyMap = objectMapper.readValue(data, Map.class);
                if(keyMap.get(KEY_IP) != null && keyMap.get(KEY_PORT) != null) {
                    String ip = (String) keyMap.get(KEY_IP);
                    int port = (Integer)keyMap.get(KEY_PORT);
                    addresses.add(new GatewayAddress(id, ip + ":" + port));
                } else {
                    log.error("[NodeWatcher] Invalid gateway address. data={}", new String(data));
                }
            } catch (Exception e) {
                log.error("[NodeWatcher] Invalid gateway address format. data={}", new String(data));
            }
        }

        return addresses;
    }

    public GatewayAffinity getGatewayAffinity() throws KeeperException, InterruptedException, IOException {
        final GatewayAffinity gatewayAffinity = new GatewayAffinity();
        final String path = buildAffinityPath();
        if (zk.exists(path, false) == null) {
            log.info("[NodeWatcher] Not found affinity info {path={}}", path);
            return gatewayAffinity;
        }

        final byte[] data = zk.getData(path, this, null);
        if (data == null) {
            return gatewayAffinity;
        }
        log.debug("[NodeWatcher] Gateway affinity from CM. {path={}, data={}}", path, new String(data));

        try {
            List list = objectMapper.readValue(data, List.class);
            for (Object value : list) {
                Map jsonObject = (Map) value;
                final int id = (Integer) jsonObject.get(KEY_GW_ID);
                final String affinity = (String) jsonObject.get(KEY_AFFINITY);
                gatewayAffinity.put(id, affinity);
            }
        } catch (Exception e) {
            log.error("[NodeWatcher] Invalid gateway address format. data=" + new String(data), e);
        }

        return gatewayAffinity;
    }

    public void stop() {
        log.info("[NodeWatcher] Stop");

        if (zk == null) {
            return;
        }

        try {
            zk.close();
        } catch (InterruptedException e) {
            log.error("[NodeWatcher] Failed to zookeeper close", e);
        }
    }

    /**
     * @param event
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    public void process(WatchedEvent event) {
        log.debug("[NodeWatcher] Zookeeper watched");
        log.info("[NodeWatcher] Event {type=" + event.getType() + ", state=" + event.getState() + ", path="
                + event.getPath() + "}");

        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    log.debug("[NodeWatcher] SyncConnected");
                    this.connLatcher.countDown();
                    break;
                default:

            }
        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
            log.debug("[NodeWatcher] NodeChildrenChanged");
            reloadGatewaylist();
            reloadGatewayAffinity();
        } else if (event.getType() == Event.EventType.NodeDataChanged) {
            log.debug("[NodeWatcher] NodeDataChanged");
            reloadGatewayAffinity();
        } else if (event.getType() == Event.EventType.NodeDeleted) {
            log.debug("[NodeWatcher] NodeDeleted");
        } else if (event.getType() == Event.EventType.NodeCreated) {
            log.debug("[NodeWatcher] NodeCreated");
        }
    }

    private void reloadGatewaylist() {
        log.info("[NodeWatcher] Reload gateway list");

        try {
            this.gatewayServerData.reload(getGatewayAddress());
        } catch (Exception e) {
            log.error("[NodeWatcher] Failed to reload", e);
        }
    }

    private void reloadGatewayAffinity() {
        log.info("[NodeWatcher] Reload gateway affinity");

        try {
            this.gatewayServerData.reload(getGatewayAffinity());
        } catch (Exception e) {
            log.error("[NodeWatcher] Failed to reload", e);
        }
    }
}

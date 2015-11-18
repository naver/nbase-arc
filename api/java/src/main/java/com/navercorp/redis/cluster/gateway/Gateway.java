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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author seongminwoo
 * @author jaehong.kim
 */
public class Gateway implements GatewayServerData {

    /**
     * The log.
     */
    private final Logger log = LoggerFactory.getLogger(Gateway.class);

    /**
     * The servers.
     */
    private Map<String, GatewayServer> servers = new ConcurrentHashMap<String, GatewayServer>();
    /**
     * id : server
     **/
    private Map<Integer, GatewayServer> index = new HashMap<Integer, GatewayServer>();

    /**
     * The config.
     */
    private GatewayConfig config;

    /**
     * The health checker.
     */
    private HealthCheckManager healthChecker;

    /**
     * The selector.
     */
    private GatewayServerSelector selector;
    private NodeWatcher nodeWatcher;
    private GatewayServerClear clear;
    private GatewayAffinity affinity = new GatewayAffinity();

    /**
     * Instantiates a new gateway.
     *
     * @param config the config
     */
    public Gateway(GatewayConfig config) {
        this.config = config;
        log.info("[Gateway] Starting " + config);

        this.clear = new GatewayServerClear(1);

        List<GatewayAddress> addresses = null;
        if (config.getDomainAddress() != null) {
            // domain
            addresses = GatewayAddress.asListFromDomain(config.getDomainAddress());
        } else if (config.getIpAddress() != null) {
            // IP
            addresses = GatewayAddress.asList(config.getIpAddress());
        } else if (config.isZkUsed()) {
            // zookeeper
            try {
                nodeWatcher = new NodeWatcher(this);
                nodeWatcher.setConfig(config);
                nodeWatcher.start();
                addresses = nodeWatcher.getGatewayAddress();
                affinity = nodeWatcher.getGatewayAffinity();
            } catch (Exception e) {
                log.error("[Gateway] Failed to connect CM. " + config.getZkAddress() + " - " + config.getClusterName(),
                        e);
            }
        }

        if (addresses == null || addresses.size() == 0) {
            destroy();
            throw new IllegalArgumentException("not found address " + config);
        }

        init(addresses);

        if (config.isHealthCheckUsed()) {
            this.healthChecker = new HealthCheckManager(this, config.getHealthCheckPeriodSeconds(),
                    config.getHealthCheckThreadSize());
        }

        this.selector = new GatewayServerSelector(config.getGatewaySelectorMethod());
    }

    public void reload(List<GatewayAddress> addresses) {
        log.info("[Gateway] Reloading {}", addresses);

        synchronized (this.servers) {
            // add
            addServers(addresses);

            // del
            delServers(addresses);
            buildIndex();
        }

        log.info("[Gateway] Reloaded {}", servers);
    }

    private void addServers(List<GatewayAddress> addresses) {
        for (GatewayAddress address : addresses) {
            try {
                if (this.servers.containsKey(address.getName())) {
                    final GatewayServer gatewayServer = this.servers.get(address.getName());
                    gatewayServer.getAddress().setId(address.getId());
                    gatewayServer.setExist(true);
                    log.info("[Gateway] Reuse gateway server " + gatewayServer);
                } else {
                    final GatewayServer gatewayServer = new GatewayServer(address, config.getPoolConfig(),
                            config.getTimeoutMillisec(), config.getKeyspace());
                    final int count = gatewayServer.preload(config.getClientSyncTimeUnitMillis(),
                            config.getConnectPerDelayMillis(), config.getPoolConfig());
                    gatewayServer.setValid(count > 0);
                    gatewayServer.setExist(true);
                    this.servers.put(address.getName(), gatewayServer);
                    log.info("[Gateway] Add gateway server " + gatewayServer);
                }
            } catch (Exception ex) {
                log.error("[Gateway] Failed to reload " + address, ex);
            }
        }
    }

    private void delServers(List<GatewayAddress> addresses) {
        final List<GatewayServer> deletedServers = new ArrayList<GatewayServer>();
        for (final GatewayServer server : servers.values()) {
            String currentGatewayName = server.getAddress().getName();
            boolean isRemoved = true;

            for (GatewayAddress address : addresses) {
                if (currentGatewayName.equals(address.getName())) {
                    isRemoved = false;
                }
            }

            if (isRemoved) {
                deletedServers.add(server);
            }
        }

        for (final GatewayServer server : deletedServers) {
            server.setExist(false);
            server.setValid(false); // set valid flag to false if gateway is removed or not valid
            server.close();
            if (this.servers.remove(server.getAddress().getName()) == null) {
                log.error("[Gateway] Not found deleted gateway server " + server + ", list=" + this.servers);
            }
            this.clear.add(server);
            log.info("[Gateway] Delete gateway server " + server);
        }
    }

    @Override
    public void reload(GatewayAffinity affinity) {
        this.affinity = affinity;
    }

    /**
     * Inits the.
     *
     * @param redisGatewayList the redis gateway list
     * @param addressValidMap
     */
    private void init(List<GatewayAddress> redisGatewayList) {
        for (GatewayAddress redisGateway : redisGatewayList) {
            final GatewayServer server = new GatewayServer(redisGateway, config.getPoolConfig(),
                    config.getTimeoutMillisec(), config.getKeyspace());
            final int count = server.preload(0, config.getConnectPerDelayMillis(), config.getPoolConfig());
            server.setValid(count > 0);

            this.servers.put(redisGateway.getName(), server);
        }
        buildIndex();

        log.info("[Gateway] Initialized " + servers);
    }

    private void buildIndex() {
        final Map<Integer, GatewayServer> index = new HashMap<Integer, GatewayServer>();
        for (GatewayServer server : servers.values()) {
            index.put(server.getAddress().getId(), server);
        }
        this.index = index;
    }

    /**
     * redisClusterPoolMap의 전체 redisClusterPoo을 제거한다.
     */
    public void destroy() {
        log.info("[Gateway] Destroying " + servers);

        if (nodeWatcher != null) {
            nodeWatcher.stop();
        }

        if (this.healthChecker != null) {
            this.healthChecker.shutdown();
        }

        for (GatewayServer server : this.servers.values()) {
            server.destroy();
        }
        this.servers.clear();

        if (this.clear != null) {
            this.clear.shutdown();
        }
    }

    /**
     *
     * @return the server
     * @throws GatewayException the gateway exception
     */
    public GatewayServer getServer(final int partitionNumber, final AffinityState state) throws GatewayException {
        if (servers.size() == 0) {
            throw new GatewayException("not found gateway");
        }

        List<GatewayServer> list = new ArrayList<GatewayServer>();
        if (config.isAffinityUsed() && partitionNumber > 0 && state != null) {
            list = getAffinityServers(partitionNumber, state);
            log.debug("[Gateway] Find affinity servers {}", list);
        }

        if (!config.isAffinityUsed() || list.size() == 0) {
            // filtering.
            for (GatewayServer server : servers.values()) {
                if (server.isExist() && server.isValid()) {
                    list.add(server);
                }
            }
        }

        if (list.size() == 0) {
            // reset state.
            for (GatewayServer server : servers.values()) {
                if (server.isExist()) {
                    server.setValid(true);
                    list.add(server);
                }
            }
        }

        return selector.get(list);
    }

    /*
     * @see GatewayServerData#getServers()
     */
    public Collection<GatewayServer> getServers() {
        return servers.values();
    }

    private List<GatewayServer> getAffinityServers(final int partitionNumber, final AffinityState state) {
        final List<GatewayServer> list = new ArrayList<GatewayServer>();
        final List<Integer> ids = affinity.get(partitionNumber, state);

        if (ids.size() == 0) {
            log.debug("Not found affinity server. {partitionNumber={}, state={}}", partitionNumber, state);
        }

        for (Integer id : ids) {
            GatewayServer server = index.get(id);
            if (server == null || !server.isExist() || !server.isValid() || server.isFullConnection()) {
                if (log.isDebugEnabled()) {
                    log.debug("Not matched affinity server. {id={}, server={}}", id, server == null ? "server is null"
                            : server);
                }
                continue;
            }
            list.add(server);
        }

        return list;
    }
}
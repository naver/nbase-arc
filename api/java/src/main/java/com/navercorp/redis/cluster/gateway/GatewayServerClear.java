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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.redis.cluster.util.DaemonThreadFactory;

/**
 * @author jaehong.kim
 */
public class GatewayServerClear implements Runnable {
    private static final long LONG_WAITING_CHECK_PERIOD = 1000 * 60 * 60; // 1 hour
    private final Logger log = LoggerFactory.getLogger(GatewayServerClear.class);

    private ScheduledExecutorService scheduler;
    private CopyOnWriteArrayList<WaitingServer> list = new CopyOnWriteArrayList<WaitingServer>();
    private long lastRunTime = System.currentTimeMillis();

    public GatewayServerClear(final int periodSeconds) {
        this.scheduler = Executors.newScheduledThreadPool(1,
                new DaemonThreadFactory("nbase-arc-gateway-server-clearer-", true));
        this.scheduler.scheduleAtFixedRate(this, periodSeconds, periodSeconds, TimeUnit.SECONDS);
    }

    public void add(final GatewayServer server) {
        this.list.add(new WaitingServer(server));
    }

    public void run() {
        boolean checkLongWaiting = false;
        final long currentTime = System.currentTimeMillis();
        if (currentTime - this.lastRunTime > LONG_WAITING_CHECK_PERIOD) {
            this.lastRunTime = System.currentTimeMillis();
            checkLongWaiting = true;
        }

        for (final WaitingServer server : list) {
            log.debug("[GatewayServerClear] Check state {}", server);
            final boolean isLongWaiting = checkLongWaiting && server.getElapseTime() > LONG_WAITING_CHECK_PERIOD;
            if (server.isReady() || isLongWaiting) {
                if (isLongWaiting) {
                    log.warn("[GatewayServerClear] Find long waiting server {}", server);
                }

                list.remove(server);
                try {
                    server.destroy();
                    log.info("[GatewayServerClear] Destroy gateway server {}", server);
                } catch (Exception e) {
                    log.warn("[GatewayServerClear] Failed to destroy gateway server. " + server, e);
                }
            }
        }
    }

    /**
     * Shutdown.
     */
    public void shutdown() {
        try {
            this.scheduler.shutdownNow();
        } catch (Exception ignored) {
        }

        for (final WaitingServer server : list) {
            try {
                server.destroy();
                log.info("[GatewayServerClear] Destroy gateway server {}", server);
            } catch (Exception ignored) {
            }
        }
        list.clear();
    }

    class WaitingServer {
        long time;
        GatewayServer server;

        public WaitingServer(final GatewayServer server) {
            this.server = server;
            this.time = System.currentTimeMillis();
        }

        public boolean isReady() {
            this.server.flush();
            if (this.server.hasActiveConnection()) {
                return false;
            }

            return getElapseTime() > this.server.getMaxWait() + (this.server.getTimeout() * 2);
        }

        public void destroy() {
            try {
                this.server.destroy();
            } catch (Exception ignored) {
            }
        }

        public long getElapseTime() {
            return System.currentTimeMillis() - time;
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("elapse=").append(getElapseTime()).append(", ");
            sb.append("server=").append(server).append(", ");
            sb.append("}");
            return sb.toString();
        }
    }
}
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.redis.cluster.util.DaemonThreadFactory;

/**
 * The Class HealthCheckManager.
 *
 * @author seongminwoo
 */
public class HealthCheckManager implements Runnable {

    /**
     * The log.
     */
    private final Logger log = LoggerFactory.getLogger(HealthCheckManager.class);

    /**
     * The scheduler.
     */
    private ScheduledExecutorService scheduler;

    /**
     * The thread pool.
     */
    private ExecutorService threadPool;

    /**
     * The server data.
     */
    private final GatewayServerData serverData;

    /**
     * Instantiates a new health check manager.
     *
     * @param serverData    the server data
     * @param periodSeconds the period seconds
     * @param threadSize    the thread size
     */
    public HealthCheckManager(final GatewayServerData serverData, final int periodSeconds, final int threadSize) {
        this.serverData = serverData;
        this.threadPool = Executors.newFixedThreadPool(threadSize,
                new DaemonThreadFactory("nbase-arc-gateway-healthchecker-", true));
        this.scheduler = Executors.newScheduledThreadPool(1,
                new DaemonThreadFactory("nbase-arc-gateway-healthcheck-scheduler-", true));
        this.scheduler.scheduleAtFixedRate(this, periodSeconds, periodSeconds, TimeUnit.SECONDS);
    }

    /**
     * Shutdown.
     */
    public void shutdown() {
        try {
            this.scheduler.shutdownNow();
        } catch (Exception ignored) {
        }

        try {
            this.threadPool.shutdownNow();
        } catch (Exception ignored) {
        }
    }

    /*
     * @see java.lang.Runnable#run()
     */
    public void run() {
        for (final GatewayServer server : this.serverData.getServers()) {
            if (!server.isExist()) {
                continue;
            }

            Runnable handler = new Runnable() {
                public void run() {
                    if (!server.isExist()) {
                        return;
                    }

                    final boolean result = server.ping();
                    server.setValid(result);
                    log.debug("[HealthCheck] Result {}", server);
                }
            };
            this.threadPool.execute(handler);
        }
    }
}
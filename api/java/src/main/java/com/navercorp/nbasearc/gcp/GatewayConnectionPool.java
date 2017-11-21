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

package com.navercorp.nbasearc.gcp;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.navercorp.redis.cluster.gateway.AffinityState;
import com.navercorp.redis.cluster.gateway.GatewayAffinity;

import io.netty.util.concurrent.ScheduledFuture;

public class GatewayConnectionPool {
    private static final Logger log = LoggerFactory.getLogger(GatewayConnectionPool.class);
    
    private final SingleThreadEventLoopTrunk eventLoopTrunk;
    private final ConcurrentMap<Integer /* gwid */, Gateway> gwMap;
    /**
     * vcConcurrentSet is accessed from multiple thraeds.
     */
    private final Set<VirtualConnection> vcConcurrentSet;
    private final AtomicBoolean closed;

    private volatile SettableFuture<?> closeFuture;

    private final AtomicReference<List<Gateway>> gatewayList;
    private final Affinity affinity;
    
    private final Random random;
    private final AtomicInteger roundGatewayIndex;

    public GatewayConnectionPool(int threadPoolSize, boolean healthCheckUsed) {
        this.eventLoopTrunk = new SingleThreadEventLoopTrunk(threadPoolSize);
        this.gwMap = new ConcurrentHashMap<Integer, Gateway>();
        this.vcConcurrentSet = Sets.newConcurrentHashSet();
        this.closed = new AtomicBoolean();
        this.affinity = new Affinity();
        this.roundGatewayIndex = new AtomicInteger(0);
        this.gatewayList = new AtomicReference<List<Gateway>>();
        this.random = new Random(System.currentTimeMillis());
        
        if (healthCheckUsed) {
            this.eventLoopTrunk.roundrobinEventLoop().getEventLoopGroup().scheduleAtFixedRate(
                    healthCheckJob, 1, 1,TimeUnit.SECONDS);
        }
    }
    
    private final Runnable healthCheckJob = new Runnable() {
        @Override
        public void run() {
            List<Gateway> gwList = gatewayList.get();
            for (Gateway gw : gwList) {
                gw.healthCheck();
            }
        }
    };

    public ListenableFuture<?> close() {
        // Check whether this instance is already closed
        if (closed.getAndSet(true)) {
            return closeFuture;
        }

        // Close VirtualConnections
        SettableFuture<?> vcFuture = closeVirtualConnections();

        // Close Gateways
        SettableFuture<?> gwFuture = closeGateways(vcFuture);

        // Close EventLoops && Set closeFuture
        closeFuture = closeEventLoops(gwFuture);

        return closeFuture;
    }

    private SettableFuture<?> closeVirtualConnections() {
        final SettableFuture<?> future = SettableFuture.create();

        if (vcConcurrentSet.isEmpty()) {
            future.set(null);
        } else {
            for (VirtualConnection vc : vcConcurrentSet) {
                vc.close().addListener(new Runnable() {
                    @Override
                    public void run() {
                        if (vcConcurrentSet.isEmpty()) {
                            future.set(null);
                        }
                    }
                }, MoreExecutors.directExecutor());
            }
        }

        return future;
    }

    private SettableFuture<?> closeGateways(SettableFuture<?> previousCloseJob) {
        final SettableFuture<?> future = SettableFuture.create();
        previousCloseJob.addListener(new Runnable() {
            @Override
            public void run() {
                final AtomicInteger closeGwCnt = new AtomicInteger(gwMap.size());

                if (gwMap.isEmpty()) {
                    future.set(null);
                    return;
                }
                
                for (Integer gwid : gwMap.keySet()) {
                    delGw(gwid).addListener(new Runnable() {
                        @Override
                        public void run() {
                            if (closeGwCnt.decrementAndGet() == 0) {
                                future.set(null);
                            }
                        }
                    }, MoreExecutors.directExecutor());
                }
            }
        }, MoreExecutors.directExecutor());

        return future;
    }

    private SettableFuture<?> closeEventLoops(SettableFuture<?> previousCloseJob) {
        final SettableFuture<?> future = SettableFuture.create();
        previousCloseJob.addListener(new Runnable() {
            @Override
            public void run() {
                eventLoopTrunk.close().addListener(new Runnable() {
                    @Override
                    public void run() {
                        future.set(null);
                    }
                }, MoreExecutors.directExecutor());
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    public ListenableFuture<?> addGw(final Integer gwid, final String ip, final int port, final int concnt,
            final int reconnectInterval) {
        Gateway gw = new Gateway(gwid, ip, port, concnt);
        if (gwMap.putIfAbsent(gwid, gw) != null) {
            throw new IllegalArgumentException("Gateway " + gwid + " is already exist.");
        }

        updateGatewayList();

        return gw.init(eventLoopTrunk, reconnectInterval);
    }

    public ListenableFuture<?> delGw(final Integer gwid) {
        Gateway gw = gwMap.remove(gwid);
        if (gw != null) {
            return gw.unuse(vcConcurrentSet);
        }

        updateGatewayList();

        SettableFuture<?> sf = SettableFuture.create();
        sf.set(null);
        return sf;
    }
    
    private void updateGatewayList() {
        List<Gateway> newGatewayList = new ArrayList<Gateway>();
        for (Gateway gw : gwMap.values()) {
            newGatewayList.add(gw);
        }
        gatewayList.set(newGatewayList);
    }

    public VirtualConnection newVc(int qSize) {
        VirtualConnection vc = new VirtualConnection(this, qSize);
        vcConcurrentSet.add(vc);
        return vc;
    }

    void delVc(VirtualConnection vc) {
        vcConcurrentSet.remove(vc);
    }
    
    public void updateAffinity(ConcurrentHashMap<Integer, GatewayAffinity.AffinityInfo> affinityInfos) {
        affinity.reload(affinityInfos);
    }

    private PhysicalConnection _bestCon(int hash, AffinityState affinityState) {
        PhysicalConnection bestPc = null;
        long bestCost = Long.MAX_VALUE;

        // Affinity first
        List<Integer> affinityGateways = affinity.affinityGateway(hash, affinityState);
        if (affinityGateways != null && affinityGateways.size() != 0) {
            Gateway gw = gwMap.get(affinityGateways.get(random.nextInt(affinityGateways.size())));
            if (gw != null && gw.getActive() > 0) {
                PhysicalConnection pc = gw.bestPc();
                if (pc != null) {
                    return pc;
                }
            }
        }
        
        // Random selection
        List<Gateway> candidates = gatewayList.get();
        int index = roundGatewayIndex.incrementAndGet() % candidates.size();
        if (index < 0) {
            index = index + candidates.size();
        }
        for (int i = 0; i < Math.min(3, candidates.size()); i++) {
            index = (index + 1) % candidates.size();
            
            Gateway gw = candidates.get(index);
            if (gw.getActive() > 0) {
                PhysicalConnection pc = gw.bestPc();
                if (pc == null) {
                    continue;
                }
                
                final long cost = pc.busyCost();
                if (bestCost > cost) {
                    bestCost = cost;
                    bestPc = pc;
                } else if (bestPc != null && bestCost == cost && bestPc.getReferenceCount() > pc.getReferenceCount()) {
                    bestCost = cost;
                    bestPc = pc;
                }
            }
        }

        return bestPc;
    }
    
    PhysicalConnection bestCon(int hash, AffinityState affinityState) {
        while (true) {
            PhysicalConnection pc = _bestCon(hash, affinityState);
            if (pc == null) {
                // No available connection
                return null;
            }
            
            synchronized (pc) {
                if (pc.getState() == PhysicalConnection.State.CONNECTED) {
                    pc.increaseReferenceCount();
                    return pc;
                }
            }
        }
    }

    ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long delay, long period, TimeUnit tu) {
        return eventLoopTrunk.roundrobinEventLoop().getEventLoopGroup().scheduleAtFixedRate(runnable, delay, period,
                tu);
    }

}

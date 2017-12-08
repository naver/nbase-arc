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

import static com.navercorp.nbasearc.gcp.Gateway.State.*;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

class Gateway {

    /**
     * USED --(deleted)--> UNUSED
     * 
     * USED --unuse--> UNUSED
     */
    enum State {
        USED, UNUSED
    }

    private static final Logger log = LoggerFactory.getLogger(Gateway.class);

    private Integer id;
    private final String ip;
    private final int port;
    private final int concnt;
    private final PhysicalConnection[] cons;

    private State state;
    private AtomicInteger active; // active connection count

    Gateway(Integer id, String ip, int port, int concnt) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.concnt = concnt;
        this.cons = new PhysicalConnection[concnt];

        this.state = USED;
        this.active = new AtomicInteger();
    }

    ListenableFuture<?> init(SingleThreadEventLoopTrunk eventLoopTrunk, int reconnectInterval) {
        final SettableFuture<?> sf = SettableFuture.create();

        for (int i = 0; i < concnt; i++) {
            cons[i] = PhysicalConnection.create(ip, port, eventLoopTrunk.roundrobinEventLoop(), this,
                    reconnectInterval);

            final ListenableFuture<?> conFuture = cons[i].connect();
            conFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        conFuture.get();
                        sf.set(null);
                    } catch (InterruptedException e) {
                        log.error("Exception occured while connecting to {}", Gateway.this, e);
                    } catch (ExecutionException e) {
                        log.error("Exception occured while connecting to {}", Gateway.this, e);
                    }
                }
            }, MoreExecutors.directExecutor());
        }

        return sf;
    }

    ListenableFuture<?> unuse(Set<VirtualConnection> vcConcurrentSet) {
        state = UNUSED;

        final SettableFuture<?> sf = SettableFuture.create();
        final AtomicInteger closedConCnt = new AtomicInteger(cons.length); 
        for (PhysicalConnection con : cons) {
            con.unuse(vcConcurrentSet).addListener(new Runnable() {
                @Override
                public void run() {
                    if (closedConCnt.decrementAndGet() == 0) {
                        sf.set(null);
                    }
                }
            }, MoreExecutors.directExecutor());
        }
        
        return sf;
    }

    boolean isSafeToAbandone() {
        for (PhysicalConnection con : cons) {
            if (con.getState() != PhysicalConnection.State.UNUSED) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return new StringBuilder()
            .append("[")
            .append("gwid: ").append(id)
            .append(", ip: ").append(ip)
            .append(", port: ").append(port)
            .append(", concnt: ").append(concnt)
            .append(", state: ").append(getState())
            .append(", active: ").append(active)
            .append("]").toString();
    }
    
    void setId(Integer id) {
        this.id = id;
    }
    
    Integer getId() {
        return id;
    }
    
    String getIp() {
        return ip;
    }
    
    Integer getPort() {
        return port;
    }

    PhysicalConnection bestPc() {
        PhysicalConnection bestPc = null;
        long bestCost = Long.MAX_VALUE;
        
        for (PhysicalConnection pc : cons) {
            if (pc.getState() == PhysicalConnection.State.CONNECTED) {
                long cost = pc.busyCost();
                
                if (bestCost > cost) {
                    bestPc = pc;
                    bestCost = cost;
                }
            }
        }

        return bestPc;
    }

    void decreaseActive() {
        active.getAndDecrement();
        log.info("decrease active {}, {}", active, this);
        assert active.get() >= 0 : this + " active is zero or larger, active: " + active;
    }

    void increaseActive() {
        active.incrementAndGet();
        log.info("increase active {}, {}", active, this);
        assert active.get() <= concnt : 
            this + " active is smaller than concnt, concnt: " + concnt + ", active: " + active;
    }

    int getActive() {
        return active.get();
    }

    State getState() {
        return state;
    }

    private static byte[] PING = "ping\r\n".getBytes();
    void healthCheck() {
        for (PhysicalConnection pc : cons) {
            if (pc.getState() == PhysicalConnection.State.CONNECTED) {
                pc.execute(Request.systemRequest(PING, pc));
            }
        }
    }

}

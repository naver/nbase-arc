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

import static com.navercorp.nbasearc.gcp.ErrorCode.*;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.navercorp.redis.cluster.gateway.AffinityState;

import io.netty.util.concurrent.ScheduledFuture;

public class VirtualConnection {

    private static final Logger log = LoggerFactory.getLogger(VirtualConnection.class);

    private final GatewayConnectionPool gcp;
    private final int qSize;
    private final Semaphore qSpace;
    private final AtomicBoolean closed;
    private final CloseJob job;

    private int hash;
    private AffinityState affinity;
    private Connection con;
    private boolean pipelineMode;

    VirtualConnection(GatewayConnectionPool gcp, int qSize) {
        this.gcp = gcp;
        this.qSize = qSize;
        this.qSpace = new Semaphore(qSize);
        this.closed = new AtomicBoolean();
        this.job = new CloseJob();
        this.con = new Connection(qSize);
    }
    
    public void allocPc(int hash, AffinityState affinity, boolean pipelineMode) {
        this.hash = hash;
        this.affinity = affinity;
        synchronized (con) {
            this.con.pc = gcp.bestCon(hash, affinity);
        }
        this.pipelineMode = pipelineMode;
    }

    public ListenableFuture<?> close() {
        if (closed.getAndSet(true)) {
            return job.sf;
        }
        job.self = gcp.scheduleAtFixedRate(job, 10, 10, TimeUnit.MILLISECONDS);
        return job.sf;
    }

    private class CloseJob implements Runnable {
        private volatile ScheduledFuture<?> self;
        private final SettableFuture<?> sf = SettableFuture.create();

        @Override
        public void run() {
            synchronized (con) {
                if (!con.hasPendingResponse()) {
                    if (con.pc != null) {
                        synchronized (con.pc) {
                            con.pc.decreaseReferenceCount();
                        }
                        con.pc = null;
                    }
                    
                    self.cancel(false);
                    VirtualConnection.this.destroy();
                    sf.set(null);
                }
            }
        }
    }

    private void destroy() {
        gcp.delVc(VirtualConnection.this);
        con.destroy();
    }
    
    public void freePc() {
        synchronized (con) {
            if (con.pc != null) {
                synchronized (con.pc) {
                    con.pc.decreaseReferenceCount();
                }
                con.pc = null;
            }
        }
    }
    
    void reallocIdlePc(PhysicalConnection pc) {
        synchronized (con) {
            if (con.pc != null && con.pc == pc && con.hasPendingResponse() == false
                    && con.hasPendingRequest() == false) {
                if (con.pc != null) {
                    synchronized (con.pc) {
                        con.pc.decreaseReferenceCount();
                    }
                    con.pc = null;
                }
                con.pc = gcp.bestCon(hash, affinity);
            }
        }
    }

    public void request(final byte[] cmd, final int timeout, final RequestCallback requestCallback) {
        final Request rqst = Request.userRequest(cmd, timeout, requestCallback, this);

        if (pipelineMode) {
            requestPipelineMode(rqst);
        } else {
            con.execute(rqst);
        }
    }

    public boolean isConnected() {
        PhysicalConnection pc = con.pc;
        if (pc == null) {
            return false;
        } else {
            return pc.getState() == PhysicalConnection.State.CONNECTED;
        }
    }
    
    private void requestPipelineMode(final Request rqst) {
        // Acquire queue space
        try {
            qSpace.acquire();
        } catch (InterruptedException e) {
            log.error("Interrupted while acquiring qSpace.", e);
            rqst.callback.onResponse(null, INTERNAL_ERROR);
            qSpace.release();
            return;
        }

        synchronized (con) {
            if (con.getFirstError() != null) { // No more requests are permitted if there an error.
                if (con.hasPendingResponse()) {
                    con.addPendingRequest(rqst);
                } else {
                    con.flushPendigRequests(con.getFirstError());
                    rqst.callback.onResponse(null, con.getFirstError());
                    qSpace.release();
                }
                return;
            }

            if (con.hasPendingRequest()) {
                con.addPendingRequest(rqst);
                return;
            }
            
            boolean pending = false;
            if ((con.pc == null || con.pc.getState() != PhysicalConnection.State.CONNECTED)
                    && con.hasPendingResponse()) {
                con.addPendingRequest(rqst);
                pending = true;
            }
            
            if (con.checkAndReallocPc()) {
                con.flushPendigRequests(ErrorCode.OK);
                
                if (con.pc == null && pending == false) {
                    rqst.callback.onResponse(null, ErrorCode.NO_AVAILABLE_CONNECTION);
                    qSpace.release();
                    return;
                }
            }
            
            if (pending == false) {
                con.incrementWaitRespCnt(1);
                con.execute(rqst);
            }
        }
    }

    void onResponse(Request rqst, byte[] response, ErrorCode errCode) {
        if (pipelineMode) {
            onResponsePipelineMode(rqst, response, errCode);
        } else {
            rqst.callback.onResponse(response, errCode);
        }
    }
    
    private void onResponsePipelineMode(Request rqst, byte[] response, ErrorCode errCode) {
        // Check and reallocate physical connection
        synchronized (con) {
            con.decrementWaitRespCnt();
            
            if (errCode.isError() && con.getFirstError() == null) {
                con.setFirstError(errCode);
            }
            
            if (con.checkAndReallocPc()) {
                con.flushPendigRequests(ErrorCode.OK);
            }
        }

        // Response callback
        rqst.callback.onResponse(response, errCode);
        qSpace.release();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[hash: ").append(hash).append(", addr: ").append(con.pc.toString()).append(", wait: ")
                .append(con.waitRespCnt.get()).append("]").toString();
        return sb.toString();
    }

    /**
     * All accesses to this class must be performed in a synchronized(Connection) block.
     *
     */
    private class Connection {
        private PhysicalConnection pc;
        private AtomicLong waitRespCnt;
        private Queue<Request> pending;
        private final int qSize;
        private ErrorCode firstError;

        private Connection(int qSize) {
            this.pc = null;
            this.waitRespCnt = new AtomicLong(0);
            this.pending = new ArrayDeque<Request>(qSize);
            this.qSize = qSize;
            this.setFirstError(null);
        }

        public void destroy() {
            final int p = pending.size();
            assert hasPendingResponse() == false && pending.isEmpty(): 
                "Connection is still used. waitRespCnt: " + waitRespCnt.get() + ", pending: " + p;
            pending.clear();
        }

        private void execute(Request rqst) {
            rqst.setPc(pc);
            
            if (rqst.getState() != Request.State.QUEUING) {
                log.error("{} of request cannot be put into pipeline.", rqst.getState());
                VirtualConnection.this.onResponse(rqst, null, INTERNAL_ERROR);
                return;
            }
            
            if (pc == null) {
                VirtualConnection.this.onResponse(rqst, null, NO_AVAILABLE_CONNECTION);
                return;
            }

            pc.execute(rqst);
        }

        private boolean checkAndReallocPc() {
            if (needRellocPc()) {
                if (pc != null) {
                    pc.decreaseReferenceCount();
                }
                
                pc = gcp.bestCon(hash, affinity);
                return true;
            }
            return false;
        }

        private boolean needRellocPc() {
            if (pc == null) {
                return true;
            }

            if (pc.getState() != PhysicalConnection.State.CONNECTED && hasPendingResponse() == false) {
                return true;
            }

            return false;
        }

        private void flushPendigRequests(ErrorCode errCode) {
            if (errCode != ErrorCode.OK) {
                while (!pending.isEmpty()) {
                    pending.poll().callback.onResponse(null, errCode);
                    qSpace.release();
                }
                return;
            }
            
            if (pc == null) {
                while (!pending.isEmpty()) {
                    Request rqst = pending.poll();
                    rqst.callback.onResponse(null, ErrorCode.NO_AVAILABLE_CONNECTION);
                    qSpace.release();
                }
                return;
            }

            for (Request rqst : pending) {
                rqst.setPc(pc);
            }

            incrementWaitRespCnt(pending.size());
            
            BulkRequest br = new BulkRequest(pending, pc);
            pc.execute(br);
            pending = new ArrayDeque<Request>(qSize);
        }
        
        private boolean hasPendingResponse() {
            return waitRespCnt.get() != 0;
        }

        private void incrementWaitRespCnt(long delta) {
            waitRespCnt.addAndGet(delta);
        }

        private void decrementWaitRespCnt() {
            final long cnt = waitRespCnt.decrementAndGet();
            assert cnt >= 0 : "Invalid state of VirtualConnection. respWaitCnt: " + cnt;
        }

        private void addPendingRequest(Request rqst) {
            pending.add(rqst);
        }
        
        private boolean hasPendingRequest() {
            return pending.isEmpty() == false;
        }

        private ErrorCode getFirstError() {
            return firstError;
        }

        private void setFirstError(ErrorCode error) {
            this.firstError = error;
        }
    }

}

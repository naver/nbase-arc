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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *    +------------(timeout)--------------------+----------------> isTimeout = true
 *    |                                         |
 *    |                                         |
 * QUEUEING --(sent through TCP connection)--> SENT --(receive)--> DONE
 */
class Request implements Runnable, TimerCallback {

    private static final Logger log = LoggerFactory.getLogger(Request.class);

    enum Type {
        SYSTEM, USER
    }
    
    enum State {
        QUEUING, SENT, DONE
    }

    private final Type type;
    private final byte[] command;
    private final int timeoutMillis; // timeout milliseconds
    private final long timeoutTimestamp;
    private final VirtualConnection virtualConnection;
    
    private PhysicalConnection physicalConnection;
    private boolean timeout;
    private State state;
    private long connTimestamp; // time that this request is bound to the conn

    final RequestCallback callback;
    
    static Request userRequest(byte[] cmd, int timeout, RequestCallback callback, VirtualConnection vc) {
        return new Request(Request.Type.USER, cmd, timeout, callback, vc);
    }

    static Request systemRequest(byte[] cmd, PhysicalConnection pc) {
        Request rqst = new Request(Request.Type.SYSTEM, cmd, 0, null, null);
        rqst.physicalConnection = pc;
        return rqst;
    }

    private Request(Type type, byte[] cmd, int timeout, RequestCallback callback, VirtualConnection vc) {
        this.type = type;
        this.command = cmd;
        this.timeoutMillis = timeout;
        this.timeoutTimestamp = System.currentTimeMillis() + this.timeoutMillis;
        this.callback = callback;
        this.virtualConnection = vc;

        this.state = State.QUEUING;
        this.timeout = false;
    }

    @Override
    public void run() {
        try {
            physicalConnection.request(this);
        } catch (Exception e) {
            log.error("failed to execute request", e);
        }
    }

    @Override
    public void onTimer() {
        assert getState() == Request.State.QUEUING || getState() == Request.State.SENT :
            "onTimer() get a requst with invalid state. " + getState();

        timeout = true;
        if (getState() == Request.State.QUEUING) {
            physicalConnection.removeRequestFromPipeline(this);
        }

        virtualConnection.onResponse(this, null, ErrorCode.TIMEOUT);
    }

    @Override
    public long getTimerTimestamp() {
        return timeoutTimestamp;
    }

    Type getType() {
        return type;
    }

    void setState(State newState) {
        state = newState;
    }

    State getState() {
        return state;
    }

    void setPc(PhysicalConnection pc) {
        this.physicalConnection = pc;
        this.connTimestamp = System.currentTimeMillis();
    }

    boolean isTimeout() {
        return timeout;
    }

    void setTimeout() {
        timeout = true;
    }

    byte[] getCommand() {
        return command;
    }
    
    long getConnTimestamp() {
        return connTimestamp;
    }

    VirtualConnection getVirtualConnection() {
        return virtualConnection;
    }

    @Override
    public String toString() {
        return "[Request cmd: " + new String(command) + ", toMillis: " + timeoutMillis + ", toTimestamp: "
                + timeoutTimestamp + ", isTimeout: " + timeout + ", state: " + state + "]";
    }

}

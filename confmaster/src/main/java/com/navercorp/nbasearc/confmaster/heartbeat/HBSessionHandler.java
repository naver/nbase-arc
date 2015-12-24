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

package com.navercorp.nbasearc.confmaster.heartbeat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.concurrent.atomic.AtomicBoolean;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtHbException;
import com.navercorp.nbasearc.confmaster.io.ClientSession;
import com.navercorp.nbasearc.confmaster.io.LineReader;
import com.navercorp.nbasearc.confmaster.io.Session;
import com.navercorp.nbasearc.confmaster.io.SessionHandler;
import com.navercorp.nbasearc.confmaster.io.SessionState;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.statistics.Statistics;

public class HBSessionHandler implements SessionHandler {
    
    enum HBCSessionState {
        HBC_IN_PROGRESS, HBC_DONE
    }
    
    private ByteBuffer pingBuffer;
    private ByteBuffer recvBuffer;

    private CharsetEncoder encoder;
    private ClientSession session;
    private LineReader reader;

    private long startTime = 0L;
    private long nextTime = 0L;
    private long requestTime = 0L;
    private long requestCompletionTime = 0L;
    private boolean sentRequest = false;
    private AtomicBoolean urgent = new AtomicBoolean(false);
    private HBCSessionState state = HBCSessionState.HBC_DONE;
    
    private long timeout;
    private long interval;
    private long slowHeartbeat;

    private HeartbeatTarget target;
    private HBResultProcessor hbcProc;
    
    public HBSessionHandler(int bufferSize, CharsetEncoder encoder,
            CharsetDecoder decoder, long timeout, long interval,
            HBResultProcessor hbcProc, long slowHeartbeat) {
        this.recvBuffer = ByteBuffer.allocate(bufferSize);
        this.encoder = encoder;
        this.reader = new LineReader(decoder);
        this.timeout = timeout;
        this.interval = interval;
        this.hbcProc = hbcProc;
        this.slowHeartbeat = slowHeartbeat;
    }

    public void setPingMsg(String ping) {
        CharBuffer charBuffer = CharBuffer.wrap(ping);
        try {
            pingBuffer = encoder.encode(charBuffer);
        } catch (CharacterCodingException e) {
            throw new AssertionError(e);
        }
    }

    public boolean isTimeout() {
        final long curTime = System.currentTimeMillis();
        if (curTime - getStartTime() >= timeout) {
            logTimeout(curTime);
            return true;
        }

        return false;
    }
    
    private void logTimeout(final long curTime) {
        Logger.warn(
                "timeout(curTime=" + curTime + ", oldNextTime="
                + getNextTime() + ", startTime=" + getStartTime()
                + ", interval=" + (curTime - getStartTime())
                + ", requestTIme=" + requestTime
                + ", requestCompletionTime=" + requestCompletionTime
                + ", timeout=" + timeout + ", state="
                + getState() + ", sessionState=" + session.getState()
                + ", sessionID=" + session.getID() + ", IP="
                + target.getIP() + ", Port=" + target.getPort());
    }

    public void handleResult(String resultState, String response) {
        if (!target.getIP().equals(session.getRemoteHostIP())
                || target.getPort() != session.getRemoteHostPort()) {
            logInconsistency();
        }
        HBResult result = new HBResult(resultState, target,
                response, session.getRemoteHostIP(),
                session.getRemoteHostPort(), session.getID());

        initializeHBCState(System.currentTimeMillis() + interval);

        try {
            hbcProc.proc(result, false);
        } catch (Exception e) {
            Logger.error("[HEART_BEAT] process heartbeat result fail.", e);
        }
    }
    
    private void logInconsistency() {
        Logger.error(
                "inconsistency of heartbeat target(targetIP="
                + target.getIP() + ", targetPort=" + target.getPort()
                + ", socketIP=" + session.getRemoteHostIP() + ", socketPort="
                + session.getRemoteHostPort());
    }

    public void resetState() {
        setStartTime(System.currentTimeMillis());
        setState(HBCSessionState.HBC_IN_PROGRESS);
    }

    public void initializeHBCState(long nextTime) {
        setStartTime(0L);
        setNextTime(nextTime);
        setSentRequest(false);
        setState(HBCSessionState.HBC_DONE);
    }

    public void sendPing() {
        session.getSelectionKey().interestOps(SelectionKey.OP_WRITE);
        setSentRequest(true);
    }

    private void requestHeartbeat() {
        switch (session.getState()) {
        case DISCONNECTED:
            try {
                session.connect();
            } catch (Exception e) {
                Logger.error("Connect to remote server fail. {}", session);
                session.close();
            }
            break;

        case WAIT_CONNECTION:
            if (isTimeout()) {
                session.close();
            }
            break;

        case CONNECTED:
            if (isTimeout()) {
                session.close();
            }

            if (!isSentRequest()) {
                sendPing();
            }
            break;
        }
    }

    @Override
    public void callbackOnLoop(long timeMillis) {
        if ((getNextTime() >= timeMillis)
                && !(this.getUrgent() && getState() == HBCSessionState.HBC_DONE)) {
            return;
        }

        switch (getState()) {
        case HBC_IN_PROGRESS:
            requestHeartbeat();
            break;

        case HBC_DONE:
            resetState();
            break;
        }
    }

    @Override
    public void callbackConnect(SelectionKey key, long timeMillis) {
    }

    @Override
    public void callbackDisconnected() {
        recvBuffer.clear();
        handleResult(Constant.SERVER_STATE_FAILURE, "");
    }

    @Override
    public void callbackRead(SelectionKey key, long timeMillis) {
        try {
            long recvedLen;
            
            try {
                recvedLen = ((SocketChannel) key.channel()).read(recvBuffer);
            } catch (ClosedChannelException e) {
                Logger.error("Read error. {}", session, e);
                key.cancel();
                session.close();
                return;
            }

            if (-1 == recvedLen || 0 == recvedLen) {
                Logger.error("Read error, {}, recvedLen: {}", session, recvedLen);
                session.close();
                return;
            }

            if (session.getState() != SessionState.CONNECTED) {
                Logger.error(
                        "Invalid state, this state of session must be SessionState.CONNECTED. {}, state: {}", 
                        session, session.getState());
                session.close();
                return;
            }

            if (recvedLen > 0) {
                recvBuffer.flip();
                String[] recvedLines = reader.readLines(recvBuffer);
                recvBuffer.flip();
                if (null == recvedLines)
                    return;

                if (recvedLines.length == 0)
                    return;
                
                recvBuffer.clear();

                boolean result = false;
                String response = "";
                for (String recvedLine : recvedLines) {
                    if (target.isHBCResponseCorrect(recvedLine)) {
                        response = recvedLine;
                        result = true;
                    } else {
                        response = recvedLine;
                        result = false;

                        Logger.warn(
                                "invalid response. {}, target: {} {}:{}, response: {}",
                                new Object[] { session, target, target.getIP(),
                                        target.getPort(), response });
                    }
                }

                if (result) {
                    Statistics.updateMaxPingpongDuration(
                            System.currentTimeMillis() - requestTime,
                            target, session, slowHeartbeat);
                    handleResult(Constant.SERVER_STATE_NORMAL, response);
                } else {
                    session.close();
                }
            }
        } catch (IOException e) {
            Logger.error("Read error. {}, target: {} {}:{}", 
                    new Object[]{session, target, target.getIP(), target.getPort()}, e);
            session.close();
        }
    }

    @Override
    public void callbackWrite(SelectionKey key, long timeMillis) throws MgmtHbException {
        try {
            if (!pingBuffer.hasRemaining()) {
                throw new MgmtHbException("State of this session is incorrect. " + session);
            }
            ((SocketChannel) session.getChannel()).write(pingBuffer);
            if (!pingBuffer.hasRemaining()) {
                requestCompletionTime = System.currentTimeMillis();
                pingBuffer.rewind();
                key.interestOps(SelectionKey.OP_READ);
            }
        } catch (IOException e) {
            Logger.error("Write error. {}, target: {} {}:{}", 
                    new Object[]{session, target, target.getIP(), target.getPort()}, e);
            session.close();
        }
    }
    
    @Override
    public void callbackAccept(SelectionKey key, long timeMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callbackConnectError() {
    }

    @Override
    public void setLastUpdatedTime(long timeMillis) {
    }

    @Override
    public long getLastUpdatedTime() {
        return 0;
    }

    @Override
    public void setSession(Session session) {
        this.session = (ClientSession) session;
    }

    public HeartbeatTarget getTarget() {
        return target;
    }

    public void setTarget(HeartbeatTarget target) {
        this.target = target;
    }

    private long getStartTime() {
        return startTime;
    }

    private void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    private long getNextTime() {
        return this.nextTime;
    }

    private void setNextTime(long nextTime) {
        this.nextTime = nextTime;
    }

    private HBCSessionState getState() {
        return state;
    }

    public void setState(HBCSessionState state) {
        this.state = state;
    }

    public boolean isSentRequest() {
        return sentRequest;
    }

    public void setSentRequest(boolean sentRequest) {
        if (sentRequest) {
            this.requestTime = System.currentTimeMillis();
            this.requestCompletionTime = 0L;
        }
        this.sentRequest = sentRequest;
    }

    public boolean getUrgent() {
        return urgent.getAndSet(false);
    }

    public void setUrgent(boolean urgent) {
        this.urgent.set(urgent);
    }

}

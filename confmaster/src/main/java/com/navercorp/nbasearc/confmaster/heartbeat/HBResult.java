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

import org.slf4j.helpers.MessageFormatter;

import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;

public class HBResult {

    private String state;
    private String response;
    private String remoteIP;
    private int remotePort;
    private int sessionID;
    private HeartbeatTarget target;
    private long hbcRefDataVersion = -1L;

    public HBResult(String result, HeartbeatTarget target, String response,
            String remoteIP, int remotePort, int sessionID) {
        this.setState(result);
        this.setTarget(target);
        this.setResponse(response);
        this.setRemoteIP(remoteIP);
        this.setRemotePort(remotePort);
        this.setSessionID(sessionID);
    }

    public String getState() {
        return state;
    }

    private void setState(String result) {
        this.state = result;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public int getRemotePort() {
        return remotePort;
    }

    public void setRemotePort(int remotePort) {
        this.remotePort = remotePort;
    }

    public int getSessionID() {
        return sessionID;
    }

    public void setSessionID(int publisherSessionID) {
        this.sessionID = publisherSessionID;
    }

    public HeartbeatTarget getTarget() {
        return target;
    }

    public void setTarget(HeartbeatTarget target) {
        this.target = target;
    }

    public long getHbcRefDataVersion() {
        return hbcRefDataVersion;
    }

    public void setHbcRefDataVersion(long hbcRefDataVersion) {
        this.hbcRefDataVersion = hbcRefDataVersion;
    }

    @Override
    public String toString() {
        return MessageFormatter
                .arrayFormat("HeartbeatResult[path: {}, state: {}, response: {}, remote: {}:{}, id: {}, type: {}, lastState: {}, lastTimestamp: {}]",
                        new Object[]{target.getPath(), state, response, remoteIP,
                            remotePort, sessionID, target.getNodeType(), 
                            target.getHeartbeatState().getLastState(), 
                            target.getHeartbeatState().getLastStateTimestamp()});
    }
    
}

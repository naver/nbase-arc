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

package com.navercorp.nbasearc.confmaster.server.cluster;

import com.navercorp.nbasearc.confmaster.heartbeat.HBState;

public interface HeartbeatTarget {

    int getZNodeVersion();

    String getPath();

    String getName();

    String getFullName();

    NodeType getNodeType();

    String getHeartbeat();

    String getClusterName();

    String getView();

    void setState(String state, long state_timestamp);

    void setZNodeVersion(int version);

    boolean isHBCResponseCorrect(String recvedLine);

    HBState getHeartbeatState();

    String getIP();

    int getPort();

    UsedOpinionSet getUsedOpinions();

    byte[] persistentDataToBytes();
}

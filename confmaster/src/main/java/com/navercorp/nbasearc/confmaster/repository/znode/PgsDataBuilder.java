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

package com.navercorp.nbasearc.confmaster.repository.znode;

public class PgsDataBuilder {

    private int pgId;
    private String pmName;
    private String pmIp;
    private int redisPort;
    private int smrBasePort;
    private int smrMgmtPort;
    private String state;
    private long stateTimestamp;
    private String hb;
    private String role;
    private int masterGen;

    public PgsDataBuilder from(PartitionGroupServerData data) {
        withPgId(data.getPgId());
        withPmName(data.getPmName());
        withPmIp(data.getPmIp());
        withRedisPort(data.getRedisPort());
        withSmrBasePort(data.getSmrBasePort());
        withSmrMgmtPort(data.getSmrMgmtPort());
        withState(data.getState());
        withStateTimestamp(data.getStateTimestamp());
        withHb(data.getHb());
        withRole(data.getRole());
        withMasterGen(data.getMasterGen());
        return this;
    }

    public PgsDataBuilder withPgId(int pgId) {
        this.pgId = pgId;
        return this;
    }

    public PgsDataBuilder withPmName(String pmName) {
        this.pmName = pmName;
        return this;
    }

    public PgsDataBuilder withPmIp(String pmIp) {
        this.pmIp = pmIp;
        return this;
    }

    public PgsDataBuilder withRedisPort(int redisPort) {
        this.redisPort = redisPort;
        return this;
    }

    public PgsDataBuilder withSmrBasePort(int smrBasePort) {
        this.smrBasePort = smrBasePort;
        return this;
    }

    public PgsDataBuilder withSmrMgmtPort(int smrMgmtPort) {
        this.smrMgmtPort = smrMgmtPort;
        return this;
    }

    public PgsDataBuilder withState(String state) {
        this.state = state;
        return this;
    }

    public PgsDataBuilder withStateTimestamp(long stateTimestamp) {
        this.stateTimestamp = stateTimestamp;
        return this;
    }

    public PgsDataBuilder withHb(String hb) {
        this.hb = hb;
        return this;
    }

    public PgsDataBuilder withRole(String role) {
        this.role = role;
        return this;
    }

    public PgsDataBuilder withOldRole(String oldRole) {
        return this;
    }

    public PgsDataBuilder withMasterGen(int masterGen) {
        this.masterGen = masterGen;
        return this;
    }

    public PartitionGroupServerData build() {
        PartitionGroupServerData data = new PartitionGroupServerData();
        data.initialize(pgId, pmName, pmIp, redisPort, smrBasePort,
                smrMgmtPort, state, role, masterGen, hb);
        ;
        data.setStateTimestamp(stateTimestamp);
        return data;
    }

}

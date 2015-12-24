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


public class RsDataBuilder {

    private String pmName;
    private String pmIp;
    private int redisPort;
    private String state;
    private long stateTimestamp;
    private String hb;

    public RsDataBuilder from(RedisServerData data) {
        withPmName(data.getPmName());
        withPmIp(data.getPmIp());
        withRedisPort(data.getRedisPort());
        withState(data.getState());
        withHb(data.getHB());
        withStateTimestamp(data.getStateTimestamp());
        return this;
    }
    
    public RsDataBuilder withPmName(String pmName) {
        this.pmName = pmName;
        return this;
    }

    public RsDataBuilder withPmIp(String pmIp) {
        this.pmIp = pmIp;
        return this;
    }

    public RsDataBuilder withRedisPort(int redisPort) {
        this.redisPort = redisPort;
        return this;
    }

    public RsDataBuilder withState(String state) {
        this.state = state;
        return this;
    }

    public RsDataBuilder withHb(String hb) {
        this.hb = hb;
        return this;
    }

    public RsDataBuilder withStateTimestamp(long stateTimestamp) {
        this.stateTimestamp = stateTimestamp;
        return this;
    }
    
    public RedisServerData build() {
        RedisServerData data = new RedisServerData();
        data.setPmName(pmName);
        data.setPmIp(pmIp);
        data.setRedisPort(redisPort);
        data.setState(state);
        data.setStateTimestamp(stateTimestamp);
        data.setHB(hb);
        return data;
    }
}

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

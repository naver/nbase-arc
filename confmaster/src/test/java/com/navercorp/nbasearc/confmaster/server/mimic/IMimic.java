package com.navercorp.nbasearc.confmaster.server.mimic;

public interface IMimic<TResp, TRqst> {
    public void init();
    public TResp execute(TRqst cmd);
}

package com.navercorp.nbasearc.confmaster.server.mimic;

public interface IInjector<TResp, TRqst> {
    public TResp inject(TRqst rqst);
}

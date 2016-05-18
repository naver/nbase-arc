package com.navercorp.nbasearc.confmaster.server.mimic;

public interface IInjectable<TResp, TRqst> {
    public void setInjector(IInjector<TResp, TRqst> injector);
}

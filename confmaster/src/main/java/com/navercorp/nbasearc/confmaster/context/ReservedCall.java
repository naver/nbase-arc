package com.navercorp.nbasearc.confmaster.context;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ReservedCall<T> {
    
    private final Callable<T> call;
    private final long delay;
    private final TimeUnit timeUnit;

    public ReservedCall(Callable<T> call, long delay, TimeUnit timeUnit) {
        this.call = call;
        this.delay = delay;
        this.timeUnit = timeUnit;
    }

    public Callable<T> getCall() {
        return call;
    }

    public long getDelay() {
        return delay;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }
    
}

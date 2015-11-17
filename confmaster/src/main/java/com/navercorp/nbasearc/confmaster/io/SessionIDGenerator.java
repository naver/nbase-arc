package com.navercorp.nbasearc.confmaster.io;

import java.util.concurrent.atomic.AtomicInteger;

public class SessionIDGenerator {
    
    public static final AtomicInteger sessionIDSeed = new AtomicInteger(1);
    
    public static int gen() {
        return sessionIDSeed.getAndIncrement();
    }
    
}

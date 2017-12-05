package com.navercorp.redis.cluster.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DaemonThreadFactory implements ThreadFactory {
    private AtomicInteger count = new AtomicInteger(0);
    private final String threadNamePrefix;
    private final boolean daemon;

    public DaemonThreadFactory(String threadNamePrefix, boolean daemon) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }
    
    public Thread newThread(Runnable runnable) {
        final Thread t = new Thread(runnable, threadNamePrefix + count.getAndIncrement());
        t.setDaemon(daemon);
        return t;
    }
}

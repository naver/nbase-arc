package com.navercorp.nbasearc.confmaster.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.context.Context;

@Component
public class ThreadPool {
    
    private int workerPoolSize;
    private ScheduledThreadPoolExecutor workerPool;
    
    @Autowired
    private Config config;
    
    public ThreadPool() {
    }
    
    public void initialize() {
        workerPoolSize = config.getServerJobThreadMax();
        workerPool = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(workerPoolSize);
    }
    
    public void release() {
        workerPool.shutdown();
    }
    
    public <T> Future<T> perform(Context<T> call) {
        // TODO : set this to Context
        return workerPool.submit(call);
    }
    
    public <T> Future<T> performDelayed(Context<T> call, long delay, TimeUnit unit) {
        return workerPool.schedule(call, delay, unit);
    }
    
    public long getCompletedTaskCount() {
        return workerPool.getCompletedTaskCount();
    }
    
    public int getActiveCount() {
        return workerPool.getActiveCount();
    }
    
    public long getQSize() {
        BlockingQueue<Runnable> q = workerPool.getQueue();
        return q.size();
    }

}

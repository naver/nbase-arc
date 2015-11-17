package com.navercorp.nbasearc.confmaster.context;

import java.util.concurrent.Callable;

import com.navercorp.nbasearc.confmaster.logger.LogHistory;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;

public class ExecutionContext<T> implements Context<T> {
    
    private Callable<T> callable;
    private ContextType type;

    private LogHistory logHistory;
    private ThreadPool pool;
    
    public ExecutionContext(Callable<T> callable, ContextType type, ThreadPool pool) {
        this.setCallable(callable);
        this.type = type;
        this.pool = pool;
    }
    
    @Override
    public T call() {
        if (getLogHistory() != null) {
            Logger.setLogHistory(getLogHistory());
        }
        
        try {
            Logger.setContextType(type);
            return getCallable().call();
        } catch (Exception e) {
            Logger.error("Execute context fail. {}", getCallable(), e);
        } finally {
            // Continue if there is next job.
            if (ContextChain.hasNextJob()) {
                ReservedCall<T> call = ContextChain.pollNextJob();                
                ExecutionContext<T> ec = new ExecutionContext<T>(call.getCall(), type, pool);
                ec.setLogHistory(Logger.popLogHistory());
                pool.performDelayed(ec, call.getDelay(), call.getTimeUnit());
            }
        }
        
        return null;
    }

    private Callable<T> getCallable() {
        return callable;
    }

    private void setCallable(Callable<T> callable) {
        this.callable = callable;
    }
    
    protected void setLogHistory(LogHistory logHistory) {
        this.logHistory = logHistory;
    }
    
    protected LogHistory getLogHistory() {
        return logHistory;
    }

}

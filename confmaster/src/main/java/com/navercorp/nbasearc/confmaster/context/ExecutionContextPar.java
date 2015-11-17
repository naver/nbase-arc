package com.navercorp.nbasearc.confmaster.context;

import java.util.concurrent.Callable;

import com.navercorp.nbasearc.confmaster.logger.LogHistory;
import com.navercorp.nbasearc.confmaster.logger.Logger;

public class ExecutionContextPar<T> implements Context<T> {

    private final Callable<T> callable;
    private final ContextType type;
    
    private final LogHistory prevLogHistory;

    public ExecutionContextPar(Callable<T> callable, ContextType type, LogHistory prevLogHistory) {
        this.callable = callable;
        this.type = type;
        this.prevLogHistory = prevLogHistory;
    }

    @Override
    public T call() {
        try {
            Logger.setContextType(type);
            T result = callable.call();
            return result;
        } catch (Exception e) {
            Logger.error("Execute context-par fail. {}", callable, e);
            return null;
        } finally {
            synchronized (prevLogHistory) {
                prevLogHistory.log(Logger.popLogHistory());
            }
        }
    }

}

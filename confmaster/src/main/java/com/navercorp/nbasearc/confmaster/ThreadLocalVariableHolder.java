package com.navercorp.nbasearc.confmaster;

import com.navercorp.nbasearc.confmaster.context.ReservedCallHolder;
import com.navercorp.nbasearc.confmaster.logger.LogHistoryHolder;

public class ThreadLocalVariableHolder {

    /* LogHistory */
    private static final ThreadLocal<LogHistoryHolder> logHistoryHolder = 
            new ThreadLocal<LogHistoryHolder>() {
        @Override
        protected synchronized LogHistoryHolder initialValue() {
            return new LogHistoryHolder();
        }
    };
    
    public static LogHistoryHolder getLogHistoryHolder() {
        return logHistoryHolder.get();
    }
    
    /* Workflow */
    public static final ThreadLocal<ReservedCallHolder> reservedCall = 
            new ThreadLocal<ReservedCallHolder>() {
        @Override
        protected synchronized ReservedCallHolder initialValue() {
            return new ReservedCallHolder();
        }
    };
    
    public static ReservedCallHolder getReservedCallHolder() {
        return reservedCall.get();
    }

}

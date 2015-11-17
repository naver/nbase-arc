package com.navercorp.nbasearc.confmaster.context;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.navercorp.nbasearc.confmaster.ThreadLocalVariableHolder;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;

public class ContextChain {

    public static <T> void setWorkflow(Callable<T> call)
            throws MgmtDuplicatedReservedCallException {
        ReservedCall<T> rcall = new ReservedCall<T>(call, 0,
                TimeUnit.MILLISECONDS);
        ThreadLocalVariableHolder.getReservedCallHolder().setCall(rcall);
    }

    public static <T> void setNextJob(Callable<T> call, long delay,
            TimeUnit timeUnit) throws MgmtDuplicatedReservedCallException  {
        ReservedCall<T> rcall = new ReservedCall<T>(call, delay, timeUnit);
        ThreadLocalVariableHolder.getReservedCallHolder().setCall(rcall);
    }
    
    public static <T> boolean hasNextJob() {
        return ThreadLocalVariableHolder.getReservedCallHolder().hasNextCall();
    }

    public static <T> ReservedCall<T> pollNextJob() {
        return ThreadLocalVariableHolder.getReservedCallHolder().pollCall();
    }

}

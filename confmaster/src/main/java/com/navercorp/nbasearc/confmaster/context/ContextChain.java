/*
 * Copyright 2015 Naver Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.nbasearc.confmaster.context;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.navercorp.nbasearc.confmaster.ThreadLocalVariableHolder;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;

public class ContextChain {

    public static void setWorkflow(Callable<Object> call)
            throws MgmtDuplicatedReservedCallException {
        ReservedCall rcall = new ReservedCall(call, 0,
                TimeUnit.MILLISECONDS);
        ThreadLocalVariableHolder.getReservedCallHolder().setCall(rcall);
    }

    public static void setNextJob(Callable<Object> call, long delay,
            TimeUnit timeUnit) throws MgmtDuplicatedReservedCallException  {
        ReservedCall rcall = new ReservedCall(call, delay, timeUnit);
        ThreadLocalVariableHolder.getReservedCallHolder().setCall(rcall);
    }
    
    public static boolean hasNextJob() {
        return ThreadLocalVariableHolder.getReservedCallHolder().hasNextCall();
    }

    public static ReservedCall pollNextJob() {
        return ThreadLocalVariableHolder.getReservedCallHolder().pollCall();
    }

}

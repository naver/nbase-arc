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

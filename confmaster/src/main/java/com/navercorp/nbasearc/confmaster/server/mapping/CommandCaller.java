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

package com.navercorp.nbasearc.confmaster.server.mapping;

import java.lang.reflect.Method;
import java.util.Arrays;

public class CommandCaller extends Caller {

    private CommandMapping command;
    
    public CommandCaller(Object service, Method method, ArityType arityType) {
        super(service, method, arityType);
        command = getMethod().getAnnotation(CommandMapping.class);
        checkRequiredMode(command.requiredMode());
    }

    public String getUsage() {
        return command.usage();
    }
    
    public int getRequiredState() {
        return command.requiredState();
    }
    
    public int getRequiredMode() {
        return command.requiredMode();
    }
    
    @Override
    public String toString() {
        return "CommandCaller[name:" + getMethod().getName() + ", args:"
                + Arrays.toString(getParamTypes()) + "]";
    }

}

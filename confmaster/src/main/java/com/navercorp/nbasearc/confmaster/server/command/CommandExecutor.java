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

package com.navercorp.nbasearc.confmaster.server.command;

import static com.navercorp.nbasearc.confmaster.server.mapping.ArityType.ANY;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.context.ContextType;
import com.navercorp.nbasearc.confmaster.context.ExecutionContext;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.LockCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;

@Controller
public class CommandExecutor {
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ThreadPool executor;
    
    @Autowired
    private ConfMaster confMaster;
    
    private Map<String, CommandCaller> commandMethods = new HashMap<String, CommandCaller>();
    private Map<String, LockCaller> lockMethods = new HashMap<String, LockCaller>();
    
    public CommandExecutor() {
    }
    
    public void initialize() {
        Map<String, Object> servies = context.getBeansWithAnnotation(Service.class);
        
        for (Object serviceInstance : servies.values()) {
            Method[] methods = serviceInstance.getClass().getMethods();
            for (Method method : methods) {
                CommandMapping commandMapping = method.getAnnotation(CommandMapping.class);
                if (commandMapping != null) {
                    commandMethods.put(
                        commandMapping.name(), 
                        new CommandCaller(serviceInstance, method, commandMapping.arityType()));
                }
                
                LockMapping lockMapping = method.getAnnotation(LockMapping.class);
                if (lockMapping != null) {
                    lockMethods.put(
                        lockMapping.name(),
                        new LockCaller(serviceInstance, method, ANY));
                }
            }
        }
        
        for (Entry<String, CommandCaller> command: commandMethods.entrySet()) {
            final String commandName = command.getKey();
            if (!lockMethods.containsKey(commandName)) {
                throw new AssertionError(
                        "There is no corresponding lock method for "
                                + commandName);
            }
        }
    }
    
    public String getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type help <command> for command specific information\r\n");
        sb.append("available commands:\r\n");
        List<String> commands = new ArrayList<String>(commandMethods.keySet());
        Collections.sort(commands);
        for (String command : commands) {
            sb.append("\t");
            sb.append(command);
            sb.append("\r\n");
        }
        sb.append("\t");
        sb.append("quit");
        sb.append("\r\n");
        return sb.toString();
    }
    
    public String getUsage(String command) {
        return commandMethods.get(command).getUsage();
    }
    
    public Future<JobResult> perform(final String request, final CommandCallback callback) {
        CommandTemplate ct = new CommandTemplate(
                request, callback, context, commandMethods, lockMethods, confMaster);
        ExecutionContext<JobResult> ec = new ExecutionContext<JobResult>(ct, ContextType.CM, executor);
        return executor.perform(ec);
    }
    
}

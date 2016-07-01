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

import static com.navercorp.nbasearc.confmaster.server.JobResult.CommonKey.END_TIME;
import static com.navercorp.nbasearc.confmaster.server.JobResult.CommonKey.REQUEST;
import static com.navercorp.nbasearc.confmaster.server.JobResult.CommonKey.START_TIME;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.STRING_VARG;
import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.support.DefaultConversionService;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandNotFoundException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtCommandWrongArgumentException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtStateNotSatisfiedException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.JobResult;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mapping.CommandCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.LockCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType;

public class CommandTemplate implements Callable<JobResult> {
    
    private final String request;
    private final CommandCallback callback;
    private final ApplicationContext context;
    private final Map<String, CommandCaller> commandMethods;
    private final Map<String, LockCaller> lockMethods;
    
    private final DefaultConversionService cs = new DefaultConversionService();
    private final ConfMaster confMaster;
    
    public CommandTemplate(String request, CommandCallback callback, 
            ApplicationContext context, Map<String, CommandCaller> commandMethods, 
            Map<String, LockCaller> lockMethods, ConfMaster confMaster) {
        this.request = request;
        this.callback = callback;
        this.context = context;
        this.commandMethods = commandMethods;
        this.lockMethods = lockMethods;
        this.confMaster = confMaster;
    }
    
    @Override
    public JobResult call() {
        HierarchicalLockHelper lockHelper = new HierarchicalLockHelper(context);
        JobResult result = new JobResult();
        Long start = System.currentTimeMillis();
        String reply = null;
        
        try {
            String[] args = request.split(" ");

            // Convert command name to lower-case word.
            args[0] = args[0].toLowerCase();
            
            validRequest(args);
            
            lock(args, lockHelper);
            
            // Execute
            reply = execute(args);
        } catch (InvocationTargetException e) {
            String smaple = request.substring(0, Math.min(1024, request.length()));
            Logger.error("Exception occur while handle request. length: {}, message: \"{}\"...", 
                    request.length(), smaple, e);
            result.addException(e.getTargetException());
            Logger.flush(DEBUG);
        } catch (Exception e) {
            String smaple = request.substring(0, Math.min(1024, request.length()));
            Logger.error("Exception occur while handle request. length: {}, message: \"{}\"...", 
                    request.length(), smaple, e);
            result.addException(e);
            Logger.flush(DEBUG);
        } finally {
            // Release lock
            try {
                releaseLock(lockHelper);
            } catch (Exception e) {
                Logger.error("Exception occur while release lock of command. " + e.getMessage(), e);
            }
            
            // Set reply to client
            result.addMessage(reply);

            // Set statistics information of command execution times.
            result.putValue(START_TIME, start);
            result.putValue(END_TIME, System.currentTimeMillis());

            // Set request information
            result.putValue(REQUEST, request);
            
            // Executor callback of client
            try {
                if (callback != null) {
                    callback.callback(result);
                }
            } catch (Exception e) {
                Logger.error("Exception occur while handle callback from client.", e);
            }

            // Flush logs
            try {
                Logger.flush(INFO);
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).error(
                        "Exception occur while flush logs in CommandTemplate.", e);
            }
        }
        
        return result;
    }
    
    private void validRequest(String[] args) throws MgmtCommandNotFoundException,
            MgmtCommandWrongArgumentException, MgmtStateNotSatisfiedException {
        final String command = args[0];
        
        if (LeaderState.isLeader()) {
            if (!commandMethods.containsKey(command)) {
                Logger.error("Command[" + command + "] not found,");
                throw new MgmtCommandNotFoundException();
            }
        } else {
            Set<String> commandForFollower = new HashSet<String>(
                    Arrays.asList(new String[] {"ping", "pgs_info_all", "pgs_sync"}));
            
            if (!commandForFollower.contains(command)) {
                Logger.error("Command[" + command + "] not found,");
                throw new MgmtCommandNotFoundException();
            }
        }
        
        final CommandCaller caller = commandMethods.get(command);

        final int cmState = confMaster.getState();
        if (cmState < caller.getRequiredState()) {
            throw new MgmtStateNotSatisfiedException("-ERR required state is "
                    + caller.getRequiredState() + ", but " + cmState);
        }
        
        final int arity = args.length - 1;
        
        switch (caller.getArityType()) {
        case EQUAL:
            if (caller.getParamLengthWithoutNullable() != arity) {
                throw new MgmtCommandWrongArgumentException(caller.getUsage());
            }
            break;
        case GREATER:
            if (caller.getParamLengthWithoutNullable() > arity) {
                throw new MgmtCommandWrongArgumentException(caller.getUsage());
            }
            break;
        case LESS:
            if (caller.getParamLengthWithoutNullable() > arity) {
                throw new MgmtCommandWrongArgumentException(caller.getUsage());
            }
            break;
        case ANY:
            // Always pass
            break;
        }
    }
    
    private void lock(String[] args, HierarchicalLockHelper lockHelper)
            throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException {
        LockCaller lock = lockMethods.get(args[0]);
        Object[] params = null;
        
        if (lock.getParamLength() > 0) {
            params = new Object[lock.getParamLength()];
            params[0] = lockHelper;
            
            for (int i = 1; i < lock.getParamLength(); i ++) {
                Class<?> paramType = lock.getParamType(i);
                params[i] = cs.convert(args[i], paramType);
            }
        }
        
        lock.invoke(params);
    }
    
    private String execute(String[] args) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        CommandCaller command = commandMethods.get(args[0]);
        Object[] params = prepareParameters(command, args);
        return (String)command.invoke(params);
    }
    
    private Object[] prepareParameters(CommandCaller command, String[] args) {
        if (command.getParamLength() == 0) {
            return null;
        }

        Object[] params = new Object[command.getParamLength()];
        
        for (int i = 0; i < command.getParamLength(); i ++) {
            Class<?> paramType = command.getParamType(i);
            ArgType type = command.getArgType(paramType);
            
            if (type == null) {
                params[i] = cs.convert(args[i + 1], paramType);
            } else if (type == STRING_VARG) {
                String o[] = new String[args.length - i - 1];
                for (int j = 0; j < args.length - i - 1; j++) {
                    o[j] = args[j + i + 1];
                }
                params[i] = cs.convert(o, paramType);
            } else if (type == NULLABLE) {
                if (args.length > i+1) {
                    params[i] = cs.convert(args[i + 1], paramType);
                } else {
                    params[i] = null;
                }
            }
        }
        
        return params;
    }
    
    private void releaseLock(HierarchicalLockHelper lockHelper) {
        lockHelper.releaseAllLock();
    }
    
}

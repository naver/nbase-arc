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

package com.navercorp.nbasearc.confmaster.server.workflow;

import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.STRING_VARG;
import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.support.DefaultConversionService;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtPrivilegeViolationException;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtWorkflowWrongArgumentException;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.repository.lock.HierarchicalLockHelper;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mapping.LockCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.WorkflowCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType;

public class WorkflowTemplate implements Callable<Object> {
    
    private final String workflow ;
    private final Object[] args;
    private final ApplicationContext context;
    private final Map<String, WorkflowCaller> workflowMethods;
    private final Map<String, LockCaller> lockMethods;
    
    private final DefaultConversionService cs = new DefaultConversionService();
    
    public WorkflowTemplate(String workflow, Object[] args, 
            ApplicationContext context, Map<String, WorkflowCaller> workflowMethods, 
            Map<String, LockCaller> lockMethods) {
        this.workflow  = workflow;
        this.args = args;
        this.context = context;
        this.workflowMethods = workflowMethods;
        this.lockMethods = lockMethods;
    }
    
    @Override
    public Object call() {
        HierarchicalLockHelper lockHelper = new HierarchicalLockHelper(context);
        
        try {
            checkPrivilege();
            validRequest();
            lock(lockHelper);
            execute();
        } catch (Exception e) {
            Logger.error("Exception occur while handle request. message: \"{}\"...", 
                    workflow, e);
            Logger.flush(DEBUG);
        } finally {
            // Release lock
            try {
                releaseLock(lockHelper);
            } catch (Exception e) {
                Logger.error("Exception occur while release lock of Workflow. "
                        + e.getMessage(), e);
            }
            
            // Flush logs
            try {
                Logger.flush(INFO);
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).error(
                        "Exception occur while flush logs in WorkflowTemplate.", e);
            }
        }
        
        return null;
    }
    
    private void checkPrivilege() throws MgmtPrivilegeViolationException {
        final WorkflowCaller method = workflowMethods.get(workflow);
        if (!LeaderState.getPrevilege().isGreaterOrEqual(method.getPrivilege())) {
            throw new MgmtPrivilegeViolationException(
                method + " requires " + method.getPrivilege() + 
                " ,but " + LeaderState.getPrevilege());
        }
    }
    
    private void validRequest() throws MgmtWorkflowWrongArgumentException {
        final WorkflowCaller caller = workflowMethods.get(workflow);
        final int arity = args.length;
        final int required = caller.getParamLengthWithoutNullable();
        
        switch (caller.getArityType()) {
        case EQUAL:
            if (required != arity) {
                Logger.error("{} requires {} argument(s), but {}",
                        new Object[] { caller, required, args }); 
                throw new MgmtWorkflowWrongArgumentException(caller);
            }
            break;
        case GREATER:
            if (required >= arity) {
                Logger.error("{} requires {} argument(s), but {}",
                        new Object[] { caller, required, args }); 
                throw new MgmtWorkflowWrongArgumentException(caller);
            }
            break;
        case LESS:
            if (required > arity) {
                Logger.error("{} requires {} argument(s), but {}",
                        new Object[] { caller, required, args }); 
                throw new MgmtWorkflowWrongArgumentException(caller);
            }
            break;
        case ANY:
            // Always pass
            break;
        }
    }
    
    private void lock(HierarchicalLockHelper lockHelper)
            throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException {
        LockCaller lock = lockMethods.get(workflow);
        Object[] params = null;
        
        if (lock.getParamLength() > 0) {
            params = new Object[lock.getParamLength()];
            params[0] = lockHelper;
            
            for (int i = 1; i < lock.getParamLength(); i ++) {
                Class<?> paramType = lock.getParamType(i);
                params[i] = cs.convert(args[i - 1], paramType);
            }
        }
        
        lock.invoke(params);
    }
    
    private String execute() throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        WorkflowCaller caller = workflowMethods.get(workflow);
        Object[] params = prepareParameters(caller);
        return (String)caller.invoke(params);
    }
    
    private Object[] prepareParameters(WorkflowCaller command) {
        if (command.getParamLength() == 0) {
            return null;
        }

        Object[] params = new Object[command.getParamLength()];
        
        for (int i = 0; i < command.getParamLength(); i ++) {
            Class<?> paramType = command.getParamType(i);
            ArgType type = command.getArgType(paramType);
            
            if (type == null) {
                params[i] = cs.convert(args[i], paramType);
            } else if (type == STRING_VARG) {
                Object o[] = new Object[args.length - i - 1];
                for (int j = 0; j < args.length - i - 1; j++) {
                    o[j] = args[j + i];
                }
                params[i] = o;
            } else if (type == NULLABLE) {
                if (args.length > i + 1) {
                    params[i] = args[i];
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

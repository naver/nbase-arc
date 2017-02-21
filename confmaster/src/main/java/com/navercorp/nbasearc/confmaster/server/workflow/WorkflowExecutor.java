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

import static com.navercorp.nbasearc.confmaster.server.mapping.ArityType.ANY;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;

import com.navercorp.nbasearc.confmaster.ConfMaster;
import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtDuplicatedReservedCallException;
import com.navercorp.nbasearc.confmaster.context.ContextChain;
import com.navercorp.nbasearc.confmaster.context.ContextType;
import com.navercorp.nbasearc.confmaster.context.ExecutionContext;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState;
import com.navercorp.nbasearc.confmaster.server.mapping.LockCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.LockMapping;
import com.navercorp.nbasearc.confmaster.server.mapping.WorkflowCaller;
import com.navercorp.nbasearc.confmaster.server.mapping.WorkflowMapping;

@Controller
public class WorkflowExecutor {
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private ThreadPool executor;
    
    @Autowired
    private ConfMaster confMaster;
    
    private Map<String, WorkflowCaller> workflowMethods = new HashMap<String, WorkflowCaller>();
    private Map<String, LockCaller> lockMethods = new HashMap<String, LockCaller>();
    private Map<String, ContextType> ctMap = new HashMap<String, ContextType>();
    
    public WorkflowExecutor() {
        ctMap.put(COMMON_STATE_DECISION, ContextType.HB);
        ctMap.put(PGS_STATE_DECISION, ContextType.HB);
        ctMap.put(BLUE_JOIN, ContextType.BJ);
        ctMap.put(MASTER_ELECTION, ContextType.ME);
        ctMap.put(MEMBERSHIP_GRANT, ContextType.MG);
        ctMap.put(ROLE_ADJUSTMENT, ContextType.RA);
        ctMap.put(QUORUM_ADJUSTMENT, ContextType.QA);
        ctMap.put(YELLOW_JOIN, ContextType.YJ);
        ctMap.put(OPINION_DISCARD, ContextType.HB);
        ctMap.put(OPINION_PUBLISH, ContextType.HB);
        ctMap.put(TOTAL_INSPECTION, ContextType.HB);
        ctMap.put(UPDATE_GATEWAY_AFFINITY, ContextType.WF);
    }
    
    public static final String COMMON_STATE_DECISION = "CommonStateDecision";
    public static final String PGS_STATE_DECISION = "PGSStateDecision";
    public static final String BLUE_JOIN = "BlueJoin";
    public static final String MASTER_ELECTION = "MasterElection";
    public static final String MEMBERSHIP_GRANT = "MembershipGrant";
    public static final String ROLE_ADJUSTMENT = "RoleAdjustment";
    public static final String QUORUM_ADJUSTMENT = "QuorumAdjustment";
    public static final String YELLOW_JOIN = "YellowJoin";
    public static final String OPINION_DISCARD = "OpinionDiscard";
    public static final String OPINION_PUBLISH = "OpinionPublish";
    public static final String TOTAL_INSPECTION = "TotalInspectionWorkflow";
    public static final String UPDATE_GATEWAY_AFFINITY = "UpdateGatewayAffinity";
    
    public void initialize() {
        Map<String, Object> servies = context.getBeansWithAnnotation(Service.class);
        
        for (Object serviceInstance : servies.values()) {
            Method[] methods = serviceInstance.getClass().getMethods();
            for (Method method : methods) {
                WorkflowMapping workflowMapping = method.getAnnotation(WorkflowMapping.class);
                if (workflowMapping != null) {
                    workflowMethods.put(
                        workflowMapping.name(),
                        new WorkflowCaller(serviceInstance, method, workflowMapping.arityType()));
                    
                }

                LockMapping lockMapping = method.getAnnotation(LockMapping.class);
                if (lockMapping != null) {
                    lockMethods.put(
                        lockMapping.name(),
                        new LockCaller(serviceInstance, method, ANY));
                }
            }
        }
        
        for (Entry<String, WorkflowCaller> command: workflowMethods.entrySet()) {
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
        for (String command : workflowMethods.keySet()) {
            sb.append("\t");
            sb.append(command);
            sb.append("\r\n");
        }
        sb.append("\t");
        sb.append("quit");
        sb.append("\r\n");
        return sb.toString();
    }
    
    public ContextType getContextType(String workflow) {
        return ctMap.get(workflow);
    }
    
    public Future<Object> perform(String workflow, Object ... args) {
        if (!checkPrivilege(workflow)) {
            return null;
        }

        if (confMaster.getState() != ConfMaster.RUNNING) {
            return null;
        }
        
        Object[] objects = new Object[args.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = args[i];
        }
        WorkflowTemplate wf = new WorkflowTemplate(
                workflow, objects, context, workflowMethods, lockMethods);
        ExecutionContext<Object> c = 
                new ExecutionContext<Object>(wf, getContextType(workflow), executor);
        return executor.perform(c);
    }
    
    public Future<Object> performDelayed(String workflow, long delay, TimeUnit timeUnit,
            Object... args) {
        if (!checkPrivilege(workflow)) {
            return null;
        }

        if (confMaster.getState() != ConfMaster.RUNNING) {
            return null;
        }
        
        Object[] objects = new Object[args.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = args[i];
        }
        WorkflowTemplate wf = new WorkflowTemplate(
                workflow, objects, context, workflowMethods, lockMethods);
        ExecutionContext<Object> c = 
                new ExecutionContext<Object>(wf, getContextType(workflow), executor);
        return executor.performDelayed(c, delay, timeUnit);
    }

    public void performContextContinue(String workflow,
            Object... args) throws MgmtDuplicatedReservedCallException {
        if (!checkPrivilege(workflow)) {
            return;
        }
        
        Object[] objects = new Object[args.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = args[i];
        }
        WorkflowTemplate wf = new WorkflowTemplate(
                workflow, objects, context, workflowMethods, lockMethods);
        ContextChain.setNextJob(wf, 0, TimeUnit.MILLISECONDS);
    }

    public void performContextContinueDelayed(String workflow,
            long delay, TimeUnit timeUnit, Object... args)
            throws MgmtDuplicatedReservedCallException {
        if (!checkPrivilege(workflow)) {
            return;
        }
        
        Object[] objects = new Object[args.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = args[i];
        }
        WorkflowTemplate wf = new WorkflowTemplate(
                workflow, objects, context, workflowMethods, lockMethods);
        ContextChain.setNextJob(wf, delay, timeUnit);
    }
    
    public boolean checkPrivilege(String workflow) {
        WorkflowCaller caller = workflowMethods.get(workflow);
        return LeaderState.getPrevilege().isGreaterOrEqual(caller.getPrivilege());
    }

}

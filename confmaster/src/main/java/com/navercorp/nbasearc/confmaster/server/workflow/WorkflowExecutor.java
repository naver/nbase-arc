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
    
    private Map<String, WorkflowCaller> workflowMethods = new HashMap<String, WorkflowCaller>();
    private Map<String, LockCaller> lockMethods = new HashMap<String, LockCaller>();
    
    public WorkflowExecutor() {
    }
    
    public static final String FAILOVER_COMMON = "FailoverCommon";
    public static final String FAILOVER_PGS = "FailoverPgs";
    public static final String OPINION_DISCARD = "OpinionDiscard";
    public static final String OPINION_PUBLISH = "OpinionPublish";
    public static final String SET_QUORUM = "SetQuorum";
    public static final String UPDATE_HEARTBEAT_CHECKER = "UpdateHeartbeatChecker";
        
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
    
    public Future<Object> perform(String workflow, Object ... args) {
        if (!checkPrivilege(workflow)) {
            return null;
        }
        
        Object[] objects = new Object[args.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = args[i];
        }
        WorkflowTemplate wf = new WorkflowTemplate(
                workflow, objects, context, workflowMethods, lockMethods);
        ExecutionContext<Object> c = 
                new ExecutionContext<Object>(wf, ContextType.WF, executor);
        return executor.perform(c);
    }

    public Future<Object> performContextContinue(String workflow,
            Object... args) throws MgmtDuplicatedReservedCallException {
        if (!checkPrivilege(workflow)) {
            return null;
        }
        
        Object[] objects = new Object[args.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = args[i];
        }
        WorkflowTemplate wf = new WorkflowTemplate(
                workflow, objects, context, workflowMethods, lockMethods);
        ContextChain.setNextJob(wf, 0, TimeUnit.MILLISECONDS);
        return null;
    }

    public Future<Object> performContextContinueDelayed(String workflow,
            long delay, TimeUnit timeUnit, Object... args)
            throws MgmtDuplicatedReservedCallException {
        if (!checkPrivilege(workflow)) {
            return null;
        }
        
        Object[] objects = new Object[args.length];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = args[i];
        }
        WorkflowTemplate wf = new WorkflowTemplate(
                workflow, objects, context, workflowMethods, lockMethods);
        ContextChain.setNextJob(wf, delay, timeUnit);
        return null;
    }
    
    public boolean checkPrivilege(String workflow) {
        WorkflowCaller caller = workflowMethods.get(workflow);
        return LeaderState.getPrevilege().isGreaterOrEqual(caller.getPrivilege());
    }

}

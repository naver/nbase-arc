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
                request, callback, context, commandMethods, lockMethods);
        ExecutionContext ec = new ExecutionContext(ct, ContextType.CM, executor);
        return executor.perform(ec);
    }
    
}

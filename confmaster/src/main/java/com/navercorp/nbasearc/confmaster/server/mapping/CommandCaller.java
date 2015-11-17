package com.navercorp.nbasearc.confmaster.server.mapping;

import java.lang.reflect.Method;
import java.util.Arrays;

public class CommandCaller extends Caller {

    public CommandCaller(Object service, Method method, ArityType arityType) {
        super(service, method, arityType);
    }

    public String getUsage() {
        CommandMapping command = getMethod().getAnnotation(CommandMapping.class);
        return command.usage();
    }

    @Override
    public String toString() {
        return "CommandCaller[name:" + getMethod().getName() + ", args:"
                + Arrays.toString(getParamTypes()) + "]";
    }

}

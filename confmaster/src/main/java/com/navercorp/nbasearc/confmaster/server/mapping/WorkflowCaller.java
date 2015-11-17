package com.navercorp.nbasearc.confmaster.server.mapping;

import java.lang.reflect.Method;
import java.util.Arrays;

import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderState.ElectionState;

public class WorkflowCaller extends Caller {

    public WorkflowCaller(Object service, Method method, ArityType arityType) {
        super(service, method, arityType);
    }

    public ElectionState getPrivilege() {
        WorkflowMapping workflow = getMethod().getAnnotation(WorkflowMapping.class);
        return workflow.privilege();
    }

    @Override
    public String toString() {
        return "WorkflowCaller[name:" + getMethod().getName() + ", args:"
                + Arrays.toString(getParamTypes()) + "]";
    }

}

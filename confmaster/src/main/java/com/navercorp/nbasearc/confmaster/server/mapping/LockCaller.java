package com.navercorp.nbasearc.confmaster.server.mapping;

import java.lang.reflect.Method;
import java.util.Arrays;

public class LockCaller extends Caller {

    public LockCaller(Object service, Method method, ArityType arityType) {
        super(service, method, arityType);
    }

    @Override
    public String toString() {
        return "LockCaller[name:" + getMethod().getName() + ", args:"
                + Arrays.toString(getParamTypes()) + "]";
    }

}

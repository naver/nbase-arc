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

import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.STRING_VARG;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType;

public class Caller {

    private Method method;
    private Class<?>[] paramTypes;
    private Object service;
    private ArityType arityType;
    private Map<Class<?>, ArgType> argTypes;

    public Caller(Object service, Method method, ArityType arityType) {
        this.service = service;
        this.method = method;
        this.paramTypes = method.getParameterTypes();
        this.arityType = arityType;
        this.argTypes = getArgTypes(method);
    }

    public Object invoke(Object... args) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        return method.invoke(service, args);
    }

    public int getParamLength() {
        return paramTypes.length;
    }

    public int getParamLengthWithoutNullable() {
        int nullableCount = 0;
        for (Entry<Class<?>, ArgType> argType : argTypes.entrySet()) {
            if (argType.getValue() == NULLABLE) {
                nullableCount++;
            }
        }
        return paramTypes.length - nullableCount;
    }

    public Class<?> getParamType(int i) {
        return paramTypes[i];
    }

    public ArgType getArgType(Class<?> paramType) {
        return argTypes.get(paramType);
    }

    public ArityType getArityType() {
        return arityType;
    }

    protected Method getMethod() {
        return method;
    }

    protected Class<?>[] getParamTypes() {
        return paramTypes;
    }
    
    private Map<Class<?>, ArgType> getArgTypes(Method method) {
        Class<?>[] paramTypes = method.getParameterTypes();
        Annotation[][] paramAnns = method.getParameterAnnotations();
        Map<Class<?>, ArgType> argTypes = new HashMap<Class<?>, ArgType>();
        
        if (paramTypes.length > 0) {
            for (int i = 0; i < paramTypes.length; i++) {
                for (Annotation paramAnn : paramAnns[i]) {
                    if (!Param.class.isInstance(paramAnn)) {
                        continue;
                    }
                    
                    Param commandParam = (Param) paramAnn;
                    validParamAnn(commandParam, paramTypes[i], paramTypes.length, i);
                    argTypes.put(paramTypes[i], commandParam.type());
                }
            }
        }

        return argTypes;
    }

    private void validParamAnn(Param commandParam, Class<?> paramType,
            int paramTypeLen, int position) {
        if (commandParam.type() == STRING_VARG) {
            if (position != paramTypeLen - 1) {
                throw new RuntimeException(
                        "STRING_VARG type of CommandParam must be the last parameter");
            }
            if (paramType != String[].class) {
                throw new RuntimeException(
                        "STRING_VARG type of CommandParam must be the type of String[]");
            }
        } else if (commandParam.type() == NULLABLE
                && position != paramTypeLen - 1) {
            throw new RuntimeException(
                    "NULL type of CommandParam must be the last parameter");
        }
    }

}

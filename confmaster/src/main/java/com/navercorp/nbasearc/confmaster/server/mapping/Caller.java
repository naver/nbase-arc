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

import static com.navercorp.nbasearc.confmaster.Constant.CLUSTER_OFF;
import static com.navercorp.nbasearc.confmaster.Constant.CLUSTER_ON;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.NULLABLE;
import static com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType.STRING_VARG;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.navercorp.nbasearc.confmaster.heartbeat.HBResult;
import com.navercorp.nbasearc.confmaster.server.cluster.ClusterComponentContainer;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.cluster.NodeType;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroup;
import com.navercorp.nbasearc.confmaster.server.cluster.PartitionGroupServer;
import com.navercorp.nbasearc.confmaster.server.mapping.Param.ArgType;

public class Caller {

    private Method method;
    private Class<?>[] paramTypes;
    private Object service;
    private ArityType arityType;
    private Map<Class<?>, ArgType> argTypes;
    
    private int paramIdxClusterHint = -1;
    private Class<?> clusterHint;

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
    
    public String getClusterName(Object []args, int offset) {
        int idx = offset + paramIdxClusterHint;
        if (clusterHint == String.class) {
            return (String)args[idx];
        } else if (clusterHint == PartitionGroup.class) {
            return ((PartitionGroup)args[idx]).getClusterName();
        } else if (clusterHint == HeartbeatTarget.class) {
            return ((HeartbeatTarget)args[idx]).getClusterName();
        } else if (clusterHint == PartitionGroupServer.class) {
            return ((PartitionGroupServer)args[idx]).getClusterName();
        } else if (clusterHint == Gateway.class) {
            return ((Gateway)args[idx]).getClusterName();
        } else if (clusterHint == HBResult.class) {
            return ((HBResult)args[idx]).getTarget().getClusterName();
        }
        
        return null;
    }
    
    public PartitionGroup getPartitionGroup(Object[] args, int offset, ClusterComponentContainer container) {
        if (paramIdxClusterHint == -1) {
            return null;
        }

        final int idx = offset + paramIdxClusterHint;
        if (clusterHint == PartitionGroup.class) {
            return (PartitionGroup) args[idx];
        } else if (clusterHint == PartitionGroupServer.class) {
            PartitionGroupServer pgs = (PartitionGroupServer) args[idx];
            return container.getPg(pgs.getClusterName(), String.valueOf(pgs.getPgId()));
        } else if (clusterHint == HeartbeatTarget.class) {
            return getPgFromHeartbeatTarget((HeartbeatTarget) args[idx], container);
        } else if (clusterHint == HBResult.class) {
            return getPgFromHeartbeatTarget(((HBResult) args[idx]).getTarget(), container);
        }

        return null;
    }

    private PartitionGroup getPgFromHeartbeatTarget(HeartbeatTarget hbt, ClusterComponentContainer container) {
        if (hbt.getNodeType() != NodeType.PGS) {
            return null;
        }
        PartitionGroupServer pgs = (PartitionGroupServer) container.get(hbt.getPath());
        if (pgs == null) {
            return null;
        }
        return container.getPg(pgs.getClusterName(), String.valueOf(pgs.getPgId()));
    }
    
    protected void checkRequiredMode(int requiredMode) {
        if (requiredMode == 0) {
            return;
        }

        if ((requiredMode & (CLUSTER_ON | CLUSTER_OFF)) != 0) {
            boolean hasClusterHint = false;
            for (Annotation[] paramAnns : method.getParameterAnnotations()) {
                for (Annotation paramAnn : paramAnns) {
                    if (ClusterHint.class.isInstance(paramAnn)) {
                        hasClusterHint = true;
                    }
                }
            }

            if (hasClusterHint == false) {
                throw new RuntimeException(
                        "@ParamClusterHint must be annotated to a parameter in "
                                + method.getName());
            }
        }
    }
    
    private Map<Class<?>, ArgType> getArgTypes(Method method) {
        Class<?>[] paramTypes = method.getParameterTypes();
        Annotation[][] paramAnns = method.getParameterAnnotations();
        Map<Class<?>, ArgType> argTypes = new HashMap<Class<?>, ArgType>();
        
        if (paramTypes.length > 0) {
            for (int i = 0; i < paramTypes.length; i++) {
                for (Annotation paramAnn : paramAnns[i]) {
                    if (Param.class.isInstance(paramAnn)) {
                        Param commandParam = (Param) paramAnn;
                        validParamAnn(commandParam, paramTypes[i], paramTypes.length, i);
                        argTypes.put(paramTypes[i], commandParam.type());
                    } else if (ClusterHint.class.isInstance(paramAnn)) {
                        validParamClusterHintAnn();
                        paramIdxClusterHint = i;    // method(command_name, arg1, arg2, ...)
                        clusterHint = paramTypes[i];
                    }
                }
            }
        }

        return argTypes;
    }
    
    private void validParamClusterHintAnn() {
        if (paramIdxClusterHint != -1) {
            throw new RuntimeException(
                    "@ParamClusterHint must be used just once in " + method.getName());
        }
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

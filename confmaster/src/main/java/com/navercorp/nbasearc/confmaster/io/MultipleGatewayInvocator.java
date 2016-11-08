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

package com.navercorp.nbasearc.confmaster.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.helpers.MessageFormatter;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.context.ContextType;
import com.navercorp.nbasearc.confmaster.context.ExecutionContextPar;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.ThreadPool;
import com.navercorp.nbasearc.confmaster.server.cluster.Gateway;

public class MultipleGatewayInvocator {

    /**
     * @return if succeeded then return S2C_OK, otherwise return an error message 
     */
    public String request(String clusterName, List<Gateway> gateways, 
            String cmd, String expectedRes, ThreadPool executor) {
        List<Future<Result>> futures = new ArrayList<Future<Result>>();
        List<Gateway> failedGatewayList = new ArrayList<Gateway>();
        Map<Gateway, Result> results = new HashMap<Gateway, Result>();
        
        for (Gateway gw : gateways) {
            SingleGatewayInvocator job = 
                    new SingleGatewayInvocator(clusterName, gw, cmd, expectedRes);
            ExecutionContextPar<Result> executionContext = 
                    new ExecutionContextPar<Result>(job, ContextType.GW, Logger.getLogHistory());
            
            futures.add(executor.perform(executionContext));
            
            results.put(gw, null);
        }
        
        for (int i = 0; i < gateways.size(); i++) {
            Gateway gw = gateways.get(i);
            Future<Result> future = futures.get(i);
            
            try {
                Result result = future.get();
                results.put(result.getGateway(), result);
            } catch (Exception e) {
                results.put(gw, new Result(e));
                
            }
        }
        
        for (Entry<Gateway, Result> entry : results.entrySet()) {
            Gateway gw = entry.getKey();
            Result result = entry.getValue();
            
            if (result.getException() != null) {
                failedGatewayList.add(result.getGateway());
                
                Logger.error("Send gateway command fail. {} {}:{}, cmd: \"{}\"", 
                         new Object[]{gw, gw.getIP(), gw.getPort(), cmd}, 
                         result.getException());
            } else {
                if (!result.getResult().equals(Constant.S2C_OK)) {
                    failedGatewayList.add(result.getGateway());

                    Logger.error("Send gateway command fail. {} {}:{}, cmd: \"{}\"", 
                             new Object[]{gw, gw.getIP(), gw.getPort(), cmd}, 
                             result.getException());
                } else {
                    Logger.info("Send gateway command success. {} {}:{}, cmd: \"{}\"", 
                            new Object[]{gw, gw.getIP(), gw.getPort(), cmd});
                }
            }
        }
        
        if (!failedGatewayList.isEmpty()) {
            return MessageFormatter.format(
                    "-ERR failed to forward cmd to gateway. cluster:{}, unavailable gateways list:{}", 
                    clusterName, makeGatewayListString(failedGatewayList));
        }
        
        return null;
    }

    public static String makeGatewayListString(List<Gateway> gatewayList) {
        if (gatewayList.isEmpty()) {
            Logger.error("GatewayList have no gateway.");
            return null;
        }
        
        StringBuilder reply = new StringBuilder();
        
        Iterator<Gateway> iterFailedGateways = gatewayList.iterator();
        while (iterFailedGateways.hasNext()) {
            Gateway gw = iterFailedGateways.next();
            reply.append(
                String.format(
                    "[id:%s, state:%s, ip:%s, port:%d], ", 
                    gw.getName(), gw.getState(), 
                    gw.getPmIp(), gw.getPort()));
        }
        
        reply.delete(reply.length() - 2, reply.length());
        return reply.toString();
    }
    
    class SingleGatewayInvocator implements Callable<Result> {
        private String clusterName;
        private Gateway gw;
        private String command;
        private String expectedResponse;
        
        public SingleGatewayInvocator(String clusterName, Gateway gw,
                String command, String expectedResponse) {
            this.clusterName = clusterName;
            this.setGateway(gw);
            this.command = command;
            this.expectedResponse = expectedResponse;
        }
        
        @Override
        public Result call() {
            if (getGateway() == null) {
                Logger.error(
                    "Send gateway command fail. Gateay variable is null. cluster: {}, command: \"{}\"",
                    clusterName, command);
                return new Result(gw, Constant.ERROR);
            }
            
            try {
                String reply = getGateway().executeQuery(command);
                if (!reply.equals(expectedResponse)) {
                    Logger.error("Send gateway command fail. {} {}:{}, cmd: \"{}\", reply: \"{}\"", 
                            new Object[]{gw, gw.getIP(), gw.getPort(), command, reply});
                    return new Result(gw, Constant.ERROR);
                } else {
                    Logger.info("Send gateway command success. {} {}:{}, cmd: \"{}\", reply: \"{}\"", 
                            new Object[]{gw, gw.getIP(), gw.getPort(), command, reply});
                }
            } catch (IOException e) {
                Logger.error("Send gateway command fail. {} {}:{}, cmd: \"{}\"", 
                        new Object[]{gw, gw.getIP(), gw.getPort(), command}, e);
                return new Result(gw, Constant.ERROR);
            }

            return new Result(gw, Constant.S2C_OK);
        }
        
        public Gateway getGateway() {
            return gw;
        }

        public void setGateway(Gateway gw) {
            this.gw = gw;
        }
    }
    
    public class Result {
        private Gateway gateway;
        private String result;
        private Exception exception = null;
        
        public Result(Gateway gateway, String result) {
            this.setGateway(gateway);
            this.setResult(result);
        }
        
        public Result(Exception e) {
            this.exception = e;
        }

        public Gateway getGateway() {
            return gateway;
        }

        public void setGateway(Gateway gateway) {
            this.gateway = gateway;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public Exception getException() {
            return exception;
        }

        public void setException(Exception exception) {
            this.exception = exception;
        }
    }

}

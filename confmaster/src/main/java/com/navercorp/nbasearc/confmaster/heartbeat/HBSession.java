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

package com.navercorp.nbasearc.confmaster.heartbeat;

import static com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor.*;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.Constant;
import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.ClientSession;
import com.navercorp.nbasearc.confmaster.io.EventSelector;
import com.navercorp.nbasearc.confmaster.io.SessionIDGenerator;
import com.navercorp.nbasearc.confmaster.logger.Logger;
import com.navercorp.nbasearc.confmaster.server.cluster.HeartbeatTarget;
import com.navercorp.nbasearc.confmaster.server.workflow.WorkflowExecutor;

public class HBSession {
    
    private ClientSession session;
    private HBSessionHandler handler;
    private String hbState = "";
    
    private final ApplicationContext context;
    private final EventSelector hbProcessor;
    private final WorkflowExecutor workflowExecutor;

    public HBSession(ApplicationContext context, EventSelector hbProcessor,
            HeartbeatTarget target, String ip, int port, String hbState) {
        this.context = context;
        this.workflowExecutor = context.getBean(WorkflowExecutor.class);
        this.hbProcessor = hbProcessor;

        session = createHbcSession(target, ip, port);
        setHandler((HBSessionHandler) session.getHandler());
        
        getHandler().setPingMsg(target.getPingMsg());
        
        updateState(hbState);
    }
    
    private ClientSession createHbcSession(HeartbeatTarget target, String ip, int port) {
        final Config config = context.getBean(Config.class);
        
        ClientSession session = new ClientSession();
        try {
            session.createChannel();
        } catch (IOException e) {
            /*
             * If session failed to create channel, it will retry when it tries
             * to connect to anywhere. So that, ignore the exception.
             */
            Logger.warn("Create channel fail. {}" + session);
        }
        session.setSessionID(SessionIDGenerator.gen());
        
        HBSessionHandler handler = new HBSessionHandler(
                config.getHeartbeatNioSessionBufferSize(),
                Charset.forName(config.getCharset()).newEncoder(),
                Charset.forName(config.getCharset()).newDecoder(),
                config.getHeartbeatTimeout(),
                config.getHeartbeatInterval(), 
                context.getBean(HBResultProcessor.class),
                config.getHeartbeatSlowlog());
        handler.setSession(session);
        handler.setTarget(target);
        
        session.setHandler(handler);
        session.setSelector(hbProcessor.getSelector());
        session.setRemoteHostIP(ip).setRemoteHostPort(port);
        
        return session;
    }
    
    public synchronized void callbackDelete() {
        if (getHbState().equals(Constant.HB_MONITOR_YES)) {
            stop();
        }
    }
    
    public synchronized void updateState(String value)  {
        if (getHbState().equals(value)) {
            return;
        }
        
        setHbState(value);
        if (getHbState().equals(Constant.HB_MONITOR_YES)) {
            start();
        } else if (getHbState().equals(Constant.HB_MONITOR_NO)) {
            stop();
        }
    }

    public synchronized void start() {
        HBSessionHandler handler = (HBSessionHandler) session.getHandler();
        handler.initializeHBCState(System.currentTimeMillis());
        hbProcessor.addSession(session);
    }

    public synchronized void stop() {
        hbProcessor.removeSession(session.getID());
        
        HBSessionHandler handler = (HBSessionHandler) session.getHandler();
        handler.initializeHBCState(0);
        
        session.disconnect();

        workflowExecutor.perform(OPINION_DISCARD, handler.getTarget());
    }

    public String getHbState() {
        return hbState;
    }

    public void setHbState(String hbState) {
        this.hbState = hbState;
    }
    
    public void urgent() {
        this.getHandler().setUrgent(true);
    }

    public HBSessionHandler getHandler() {
        return handler;
    }

    public void setHandler(HBSessionHandler handler) {
        this.handler = handler;
    }
    
    public int getID() {
        return session.getID();
    }
    
}

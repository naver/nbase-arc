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

package com.navercorp.nbasearc.confmaster.server;

import org.springframework.context.ApplicationContext;

import com.navercorp.nbasearc.confmaster.config.Config;
import com.navercorp.nbasearc.confmaster.io.ClientSession;
import com.navercorp.nbasearc.confmaster.io.EventSelector;
import com.navercorp.nbasearc.confmaster.io.Session;
import com.navercorp.nbasearc.confmaster.io.SessionFactory;
import com.navercorp.nbasearc.confmaster.io.SessionHandler;
import com.navercorp.nbasearc.confmaster.io.SessionIDGenerator;
import com.navercorp.nbasearc.confmaster.server.command.CommandExecutor;
import com.navercorp.nbasearc.confmaster.server.leaderelection.LeaderElectionHandler;

public class ClientSessionFactory implements SessionFactory {
    
    private CommandExecutor commandTemplate;
    
    private Config config;
    
    private LeaderElectionHandler leaderElectionHandler;
    
    public ClientSessionFactory(ApplicationContext context) {
        commandTemplate = context.getBean(CommandExecutor.class);
        config = context.getBean(Config.class);
        leaderElectionHandler = context.getBean(LeaderElectionHandler.class);
    }
    
    @Override
    public Session session() {
        ClientSession session = new ClientSession();
        session.setSessionID(SessionIDGenerator.gen());
        return session;
    }
    
    @Override
    public SessionHandler handler(EventSelector eventSelctor) {
        ClientSessionHandler handler = new ClientSessionHandler(
                commandTemplate, leaderElectionHandler, config, eventSelctor);
        return handler;
    }

}
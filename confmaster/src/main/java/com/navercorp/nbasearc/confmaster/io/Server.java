package com.navercorp.nbasearc.confmaster.io;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Collection;

import com.navercorp.nbasearc.confmaster.logger.Logger;

public class Server {

    private final ServerSession session;
    private final SessionFactory sessionFactory;
    private final EventSelector eventSelector;
    
    private final int port;
    private final int maxSessionCnxns;
    private final long maxSessionTimeout; 
    
    public Server(int port, int maxSessionCnxns, long maxSessionTimeout,
            SessionFactory sessionFactory) throws IOException {
        this.port = port;
        this.maxSessionCnxns = maxSessionCnxns;
        this.maxSessionTimeout = maxSessionTimeout;
        this.sessionFactory = sessionFactory;
        this.session = new ServerSession();
        this.eventSelector = new EventSelector(1);
    }
    
    public void initialize(SessionHandler handler) throws IOException {
        this.session.setHandler(handler);
        this.session.createChannel();
        this.session.setSelector(this.eventSelector.getSelector());
        this.session.bind(null, this.port);
        this.eventSelector.register(this.session, SelectionKey.OP_ACCEPT);
    }
    
    public void process() {
        eventSelector.ioProcess();
        eventSelector.loopProcess();
    }
    
    public void close() throws IOException {
        eventSelector.shutdown();
        session.close();
    }
    
    public Integer getSessionCount() {
        return eventSelector.getSessionCount();
    }
    
    public int closeIdleSession() {
        Collection<Session> sessions = eventSelector.getSessions();
        long currentTime = System.currentTimeMillis();
        int closedClient = 0;
        for (Session session : sessions) {
            long time = session.getHandler().getLastUpdatedTime();
            if (time < currentTime - maxSessionTimeout) {
                try {
                    session.close();
                    closedClient++;
                } catch (Exception e) {
                    Logger.error("Close socket fail. {}", session, e);
                }
            }
        }
        return closedClient;
    }

    public int getMaxSessionCnxns() {
        return maxSessionCnxns;
    }

    public EventSelector getEventSelector() {
        return eventSelector;
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

}

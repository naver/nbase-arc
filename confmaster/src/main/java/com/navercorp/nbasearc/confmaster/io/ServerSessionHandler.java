package com.navercorp.nbasearc.confmaster.io;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.navercorp.nbasearc.confmaster.logger.Logger;

public class ServerSessionHandler implements SessionHandler {

    private final EventSelector eventSelector;
    private final SessionFactory factory;
    private final Server server;
    
    public ServerSessionHandler(EventSelector eventSelector,
            SessionFactory factory, Server server) {
        this.eventSelector = eventSelector;
        this.factory = factory;
        this.server = server;
    }

    @Override
    public void callbackAccept(SelectionKey key, long timeMillis) throws IOException {
        SocketChannel channel = ((ServerSocketChannel)key.channel()).accept();
        String remoteIp = channel.socket().getInetAddress().getHostAddress();
        int port = channel.socket().getPort();
        
        if (server.getSessionCount() >= server.getMaxSessionCnxns()) {
            int closedCount = server.closeIdleSession();
            if (closedCount == 0) {
                Logger.warn("Too many connections, close new session. {}:{}", remoteIp, port);
                channel.close();
                return;
            }
        }
        
        ClientSession session = (ClientSession) factory.session();
        
        SessionHandler handler = factory.handler(eventSelector);
        handler.setLastUpdatedTime(timeMillis);
        handler.setSession(session);
        session.setHandler(handler);
        session.setRemoteHostIP(remoteIp).setRemoteHostPort(port);
        
        channel.configureBlocking(false);
        session.setChannel(channel);
        session.setSelector(eventSelector.getSelector());
        
        eventSelector.register(session, SelectionKey.OP_READ);
        eventSelector.addSession(session);
        
        Logger.info("Create new session success. {}", session);
    }

    @Override
    public void callbackOnLoop(long timeMillis) {
        // Do nothing...
    }

    @Override
    public void callbackConnect(SelectionKey key, long timeMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callbackDisconnected() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callbackRead(SelectionKey key, long timeMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callbackWrite(SelectionKey key, long timeMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSession(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callbackConnectError() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLastUpdatedTime(long timeMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastUpdatedTime() {
        throw new UnsupportedOperationException();
    }

}

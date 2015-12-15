package com.navercorp.nbasearc.confmaster.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;

import com.navercorp.nbasearc.confmaster.logger.Logger;

public final class ServerSession extends Session {

    private ServerSocketChannel channel;

    public ServerSession() {
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
    
    @Override
    public void createChannel() throws IOException {
        try {
            channel = ServerSocketChannel.open();
            channel.configureBlocking(false);
            channel.socket().setReuseAddress(true);
            setChannel(channel);
        } catch (IOException e) {
            Logger.error("Create channel of ServerSession fail. {}", this, e);
            throw e;
        }
    }
    
    public void bind(String ip, int port) throws IOException {
        try {
            channel.socket().bind(new InetSocketAddress(ip, port));
        } catch (IOException e) {
            Logger.error("Bind listen socket fail. ip: {}, port: {}", ip, port, e);
            throw e;
        }
    }

    @Override
    public void callbackAccept(SelectionKey key, long timeMillis) throws IOException {
        if (null != getHandler()) {
            getHandler().callbackAccept(key, timeMillis);
        }
    }

}

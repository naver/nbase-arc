package com.navercorp.nbasearc.confmaster.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtHbException;
import com.navercorp.nbasearc.confmaster.logger.Logger;

public class ClientSession extends Session {
    
    private SessionState state;
    private String remoteHostIP;
    private int remoteHostPort;
    
    public ClientSession() {
        setState(SessionState.DISCONNECTED);
    }
    
    @Override
    public void close() {
        if (null != getHandler()) {
            getHandler().callbackDisconnected();
        }
        
        disconnect();
    }
    
    public void disconnect() {
        setState(SessionState.DISCONNECTED);
        
        if (getChannel().isOpen()) {
            try {
                getChannel().close(); 
            } catch(Exception e) {
                Logger.warn("Close channel fail. {}", this, e);
            }
        }
    }
    
    @Override
    public void createChannel() throws IOException {
        SocketChannel channel;
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        setChannel(channel);
    }

    /**
     * @param ip tcp address of a remote host
     * @param port tcp port of a remote host
     * @throws IOException
     */
    public void connect(String ip, int port) throws IOException {
        try {
            boolean ret = ((SocketChannel) getChannel()).connect(new InetSocketAddress(ip, port));
            SelectionKey key = getChannel().register(getSelector(), SelectionKey.OP_CONNECT, this);
            setSelectionKey(key);
            setState(SessionState.WAIT_CONNECTION);
            
            if (ret) {
                this.callbackConnect(key, System.currentTimeMillis());
            }
        } catch (ConnectionPendingException e) {
            Logger.warn(
                    "a non-blocking connection operation is already in progress on this channel. {}",
                    this, e);
            throw e;
        } catch (ClosedChannelException e) {
            Logger.warn(
                    "this channel is closed. {}", 
                    this, e);
            throw e;
        } catch (UnresolvedAddressException e) {
            Logger.warn(
                    "the given remote address is not fully resolved. {}",
                    this, e);
            throw e;
        } catch (UnsupportedAddressTypeException e) {
            Logger.warn(
                    "the type of the given remote address is not supported. {}",
                    this, e);
            throw e;
        } catch (IOException e) {
            Logger.warn(
                    "connect error. {}", 
                    this, e);
            throw e;
        }
    }
    
    /**
     * @throws IOException
     */
    public void connect() throws IOException {
        if (!getChannel().isOpen()) {
            createChannel();
        }
        connect(getRemoteHostIP(), getRemoteHostPort());
    }
    
    @Override
    public void callbackOnLoop(long timeMillis) {
        if (null != getHandler()) {
            getHandler().callbackOnLoop(timeMillis);
        }
    }

    @Override
    public void callbackConnect(SelectionKey key, long timeMillis) {
        //Logger.info("callbackConnect, sessionID=" + getID());

        SocketChannel channel = (SocketChannel) getChannel();
        
        try {
            boolean ret = channel.finishConnect();
            if (!ret) {
                Logger.error("channel is not connected.");
                close();
                return;
            }

            setState(SessionState.CONNECTED);
            Logger.info("Connection established. " + this.toString());
            
            if (null != getHandler()) {
                getHandler().callbackConnect(key, timeMillis);
            }
        } catch (Exception e) {
            try {
                Logger.error("connect error. " + toString());
            } finally {
                close();
            }
            
            if (null != getHandler()) {
                getHandler().callbackConnectError();
            }
        }
    }

    @Override
    public void callbackRead(SelectionKey key, long timeMillis) {
        if (null != getHandler()) {
            getHandler().callbackRead(key, timeMillis);
        }
    }

    @Override
    public void callbackWrite(SelectionKey key, long timeMillis) throws MgmtHbException {
        if (null != getHandler()) {
            getHandler().callbackWrite(key, timeMillis);
        }
    }

    public synchronized SessionState getState() {
        return state;
    }

    /**
     * must be called by handler or itself
     * TODO : replace this funcion with an accesser of a state
     * @param state
     */
    private synchronized void setState(SessionState state) {
        this.state = state;
    }

    public String getRemoteHostIP() {
        return remoteHostIP;
    }

    public ClientSession setRemoteHostIP(String remoteHostIP) {
        this.remoteHostIP = remoteHostIP;
        return this;
    }

    public int getRemoteHostPort() {
        return remoteHostPort;
    }

    public ClientSession setRemoteHostPort(int remoteHostPort) {
        this.remoteHostPort = remoteHostPort;
        return this;
    }
    
    @Override
    public String toString() {
        return "ClientSession[id:" + getID() + ", client:" + getRemoteHostIP()
                + ":" + getRemoteHostPort() + "]";
    }
    
}

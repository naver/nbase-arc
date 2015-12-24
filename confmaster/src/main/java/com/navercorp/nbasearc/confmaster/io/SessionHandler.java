package com.navercorp.nbasearc.confmaster.io;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtHbException;

public interface SessionHandler {

    void setSession(Session session);

    /**
     * this function is called on each loop in NioProcessor.loop
     */
    void callbackOnLoop(long timeMillis);

    /**
     * OP_ACCEPT from java.nio.channels.Selector
     * 
     * @param key
     * @param timeMillis
     * @throws IOException
     */
    void callbackAccept(SelectionKey key, long timeMillis) throws IOException;

    /**
     * OP_CONNECT from java.nio.channels.Selector
     * 
     * @param key
     * @param timeMillis
     */
    void callbackConnect(SelectionKey key, long timeMillis);

    /**
     * callback on disconnect while this channel is connected to remote host
     * successfully.
     */
    void callbackDisconnected();

    /**
     * OP_READ from java.nio.channels.Selector
     * 
     * @param key
     * @param timeMillis
     */
    void callbackRead(SelectionKey key, long timeMillis);

    /**
     * OP_WRITE from java.nio.channels.Selector
     * 
     * @param key
     * @param timeMillis
     * @throws MgmtHbException
     */
    void callbackWrite(SelectionKey key, long timeMillis)
            throws MgmtHbException;

    /**
     * this function is called if an error occurs while trying to connect to a
     * remote host.
     */
    void callbackConnectError();

    void setLastUpdatedTime(long timeMillis);

    long getLastUpdatedTime();

}

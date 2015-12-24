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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;

import com.navercorp.nbasearc.confmaster.ConfMasterException.MgmtHbException;

public abstract class Session {
    
    public static int INVALID_SESSION_ID = -1;
    
    private int sessionID;
    private Selector selector;
    private AbstractSelectableChannel channel;
    private SelectionKey selectionKey;
    private SessionHandler handler;
    
    public Session() {
    }
    
    public abstract void close() throws IOException;
    
    public int getID() {
        return getSessionID();
    }

    public Selector getSelector() {
        return selector;
    }

    public abstract void createChannel() throws IOException;

    public AbstractSelectableChannel getChannel() {
        return channel;
    }

    public void setChannel(AbstractSelectableChannel channel) {
        this.channel = channel;
    }
    
    public void setSelector(Selector selector) {
        this.selector = selector;
    }

    public SessionHandler getHandler() {
        return handler;
    }
    
    public void setHandler(SessionHandler handler) {
        this.handler = handler;
    }
    
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }
    
    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public void callbackOnLoop(long timeMillis) {
    }

    public void callbackAccept(SelectionKey key, long timeMillis)
            throws IOException {
    }

    public void callbackConnect(SelectionKey key, long timeMillis) {
    }

    public void callbackRead(SelectionKey key, long timeMillis) {
    }

    public void callbackWrite(SelectionKey key, long timeMillis)
            throws MgmtHbException {
    }

    public int getSessionID() {
        return sessionID;
    }

    public void setSessionID(int sessionID) {
        this.sessionID = sessionID;
    }
    
}

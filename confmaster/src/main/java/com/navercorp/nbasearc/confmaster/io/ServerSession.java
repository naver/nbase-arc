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

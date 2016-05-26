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
import java.util.List;

public interface BlockingSocket {
    public String execute(String query, int retryCount) throws IOException;

    public String execute(String query) throws IOException;

    public List<String> executeAndMultiReply(String query, int replyCount)
            throws IOException;

    public void send(String query) throws IOException;

    public String recvLine() throws IOException;

    public void close() throws IOException;
}

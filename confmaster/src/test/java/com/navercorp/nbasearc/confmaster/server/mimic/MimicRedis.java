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

package com.navercorp.nbasearc.confmaster.server.mimic;

import static com.navercorp.nbasearc.confmaster.Constant.*;

import java.io.IOException;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

// Leaky-spec of redis, mimicking some behaviors for test.
public class MimicRedis implements Answer<String>, IMimic<String, String> {
    @Override
    public void init() {
    }
    
    @Override
    public String answer(InvocationOnMock invocation) throws IOException {
        return execute((String) invocation.getArguments()[0]);
    }

    @Override
    public String execute(String cmd) {
        String[] args = cmd.split(" ");

        if (args[0].equals(PGS_PING)) {
            return ping();
        } else if (args[0].equals(REDIS_PING)) {
            return bping();
        }
        return "-ERR unknown command " + cmd;
    }

    public String ping() {
        return REDIS_PONG;
    }

    public String bping() {
        return REDIS_PONG;
    }
}
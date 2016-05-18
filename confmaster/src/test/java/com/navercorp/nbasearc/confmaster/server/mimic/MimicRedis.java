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
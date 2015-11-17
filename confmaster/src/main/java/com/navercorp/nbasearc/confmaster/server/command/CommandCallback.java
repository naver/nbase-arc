package com.navercorp.nbasearc.confmaster.server.command;

import com.navercorp.nbasearc.confmaster.server.JobResult;

public interface CommandCallback {

    void callback(JobResult result);
    
}

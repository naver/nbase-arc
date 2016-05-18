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

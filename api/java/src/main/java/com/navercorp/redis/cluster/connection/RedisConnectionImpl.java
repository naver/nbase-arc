package com.navercorp.redis.cluster.connection;

import java.util.List;

import com.navercorp.nbasearc.gcp.VirtualConnection;
import com.navercorp.redis.cluster.connection.RedisProtocol.Command;
import com.navercorp.redis.cluster.gateway.AffinityState;

interface RedisConnectionImpl {
    int getTimeout();

    void setTimeout(final int timeoutMillisec);

    void commitActiveTimeout(final int timeoutMillisec);

    void rollbackActiveTimeout();

    void sendCommand(final Command cmd, final String... args);

    void sendCommand(final Command cmd, final byte[]... args);

    void sendCommand(final Command cmd);
    
    String toStringArgs(final byte[]... args);

    String getHost();

    void setHost(final String host);

    int getPort();

    void setPort(final int port);

    void connect();

    void disconnect();

    void passivate();
    
    boolean isConnected();

    String getStatusCodeReply();

    String getBulkReply();

    byte[] getBinaryBulkReply();

    Long getIntegerReply();

    List<String> getMultiBulkReply();

    List<byte[]> getBinaryMultiBulkReply();

    List<Object> getObjectMultiBulkReply();

    List<Long> getIntegerMultiBulkReply();

    List<Object> getAll();

    List<Object> getAll(int except);
    
    void setVc(VirtualConnection vc);
    
    void allocPc(int hash, AffinityState affinity, boolean pipelineMode);

    String toString();
    
}

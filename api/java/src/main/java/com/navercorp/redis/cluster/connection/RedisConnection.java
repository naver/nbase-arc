/**
 * Copyright (c) 2011 Jonathan Leibiusky
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.navercorp.redis.cluster.connection;

import java.util.List;

import redis.clients.util.SafeEncoder;

import com.navercorp.nbasearc.gcp.VirtualConnection;
import com.navercorp.redis.cluster.connection.RedisProtocol.Command;
import com.navercorp.redis.cluster.gateway.AffinityState;

/**
 * The Class RedisConnection.
 *
 * @author jaehong.kim
 */
public class RedisConnection {

    private RedisConnectionImpl impl;

    /**
     * Gets the timeout.
     *
     * @return the timeout
     */
    public int getTimeout() {
        return impl.getTimeout();
    }

    /**
     * Sets the timeout.
     *
     * @param timeoutMillisec the new timeout
     */
    public void setTimeout(final int timeoutMillisec) {
        impl.setTimeout(timeoutMillisec);
    }

    public void commitActiveTimeout(final int timeoutMillisec) {
        impl.commitActiveTimeout(timeoutMillisec);
    }

    public void rollbackActiveTimeout() {
        impl.rollbackActiveTimeout();
    }
    
    public void setVc(VirtualConnection vc) {
        impl.setVc(vc);
    }
    
    public void allocPc(int hash, AffinityState affinity, boolean pipelineMode) {
        impl.allocPc(hash, affinity, pipelineMode);
    }

    /**
     * Instantiates a new redis connection.
     *
     * @param host the host
     */
    public RedisConnection(final String host) {
        impl = new RedisConnectionSync(host);
    }

    public static RedisConnection createAsync(String host, int port) {
        RedisConnection con = new RedisConnection();
        con.impl = new RedisConnectionAsync(host, port);
        return con;
    }
    
    /**
     * Send command.
     *
     * @param cmd  the cmd
     * @param args the args
     * @return the redis connection
     */
    protected RedisConnection sendCommand(final Command cmd, final String... args) {
        final byte[][] bargs = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            bargs[i] = SafeEncoder.encode(args[i]);
        }
        return sendCommand(cmd, bargs);
    }

    /**
     * Send command.
     *
     * @param cmd  the cmd
     * @param args the args
     * @return the redis connection
     */
    protected RedisConnection sendCommand(final Command cmd, final byte[]... args) {
        impl.sendCommand(cmd, args);
        return this;
    }

    /**
     * Send command.
     *
     * @param cmd the cmd
     * @return the redis connection
     */
    protected RedisConnection sendCommand(final Command cmd) {
        impl.sendCommand(cmd);
        return this;
    }

    /**
     * Instantiates a new redis connection.
     *
     * @param host the host
     * @param port the port
     */
    public RedisConnection(final String host, final int port) {
        impl = new RedisConnectionSync(host, port);
    }

    /**
     * Gets the host.
     *
     * @return the host
     */
    public String getHost() {
        return impl.getHost();
    }

    /**
     * Sets the host.
     *
     * @param host the new host
     */
    public void setHost(final String host) {
        impl.setHost(host);
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort() {
        return impl.getPort();
    }

    /**
     * Sets the port.
     *
     * @param port the new port
     */
    public void setPort(final int port) {
        impl.setPort(port);
    }

    /**
     * Instantiates a new redis connection.
     */
    public RedisConnection() {
        impl = new RedisConnectionSync();
    }

    public RedisConnection(String host, int port, boolean async) {
        impl = new RedisConnectionAsync(host, port);
    }

    /**
     * Connect.
     */
    public void connect() {
        impl.connect();
    }

    /**
     * Disconnect.
     */
    public void disconnect() {
        impl.disconnect();
    }

    public void passivate() {
        impl.passivate();
    }

    /**
     * Checks if is connected.
     *
     * @return true, if is connected
     */
    public boolean isConnected() {
        return impl.isConnected();
    }

    /**
     * Gets the status code reply.
     *
     * @return the status code reply
     */
    public String getStatusCodeReply() {
        return impl.getStatusCodeReply();
    }

    /**
     * Gets the bulk reply.
     *
     * @return the bulk reply
     */
    public String getBulkReply() {
        return impl.getBulkReply();
    }

    /**
     * Gets the binary bulk reply.
     *
     * @return the binary bulk reply
     */
    public byte[] getBinaryBulkReply() {
        return impl.getBinaryBulkReply();
    }

    /**
     * Gets the integer reply.
     *
     * @return the integer reply
     */
    public Long getIntegerReply() {
        return impl.getIntegerReply();
    }

    /**
     * Gets the multi bulk reply.
     *
     * @return the multi bulk reply
     */
    public List<String> getMultiBulkReply() {
        return impl.getMultiBulkReply();
    }

    /**
     * Gets the binary multi bulk reply.
     *
     * @return the binary multi bulk reply
     */
    public List<byte[]> getBinaryMultiBulkReply() {
        return impl.getBinaryMultiBulkReply();
    }

    /**
     * Gets the object multi bulk reply.
     *
     * @return the object multi bulk reply
     */
    public List<Object> getObjectMultiBulkReply() {
        return impl.getObjectMultiBulkReply();
    }

    /**
     * Gets the integer multi bulk reply.
     *
     * @return the integer multi bulk reply
     */
    public List<Long> getIntegerMultiBulkReply() {
        return impl.getIntegerMultiBulkReply();
    }

    public List<Object> getAll() {
        return impl.getAll();
    }

    public List<Object> getAll(int except) {
        return impl.getAll(except);
    }

    /*
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return impl.toString();
    }
}
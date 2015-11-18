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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import com.navercorp.redis.cluster.pipeline.BuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

import com.navercorp.redis.cluster.connection.RedisProtocol.Command;
import com.navercorp.redis.cluster.util.RedisInputStream;
import com.navercorp.redis.cluster.util.RedisOutputStream;

/**
 * The Class RedisConnection.
 *
 * @author jaehong.kim
 */
public class RedisConnection {
    private final Logger log = LoggerFactory.getLogger(RedisConnection.class);

    /**
     * The host.
     */
    private String host;

    /**
     * The port.
     */
    private int port = RedisProtocol.DEFAULT_PORT;

    /**
     * The socket.
     */
    private Socket socket;

    /**
     * The output stream.
     */
    private RedisOutputStream outputStream;

    /**
     * The input stream.
     */
    private RedisInputStream inputStream;

    /**
     * The timeout.
     */
    private int timeout = RedisProtocol.DEFAULT_TIMEOUT;

    private int pipelinedCommands = 0;
    private long connectedTime = 0;
    private long disconnectedTime = 0;

    /**
     * Gets the socket.
     *
     * @return the socket
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * Gets the timeout.
     *
     * @return the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout.
     *
     * @param timeoutMillisec the new timeout
     */
    public void setTimeout(final int timeoutMillisec) {
        this.timeout = timeoutMillisec;
    }

    public void commitActiveTimeout(final int timeoutMillisec) {
        try {
            socket.setSoTimeout(timeoutMillisec);
        } catch (SocketException ex) {
            throw new JedisException(ex);
        }
    }

    public void rollbackActiveTimeout() {
        try {
            socket.setSoTimeout(this.timeout);
        } catch (SocketException ex) {
            throw new JedisException(ex);
        }
    }

    /**
     * Instantiates a new redis connection.
     *
     * @param host the host
     */
    public RedisConnection(final String host) {
        super();
        this.host = host;
    }

    /**
     * Flush.
     */
    protected void flush() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            throw new JedisConnectionException(e);
        }
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
        log.trace("[RedisConnection] Command={} {}", cmd.name(), toStringArgs(args));
        connect();
        RedisProtocol.sendCommand(outputStream, cmd, args);
        this.pipelinedCommands++;
        return this;
    }

    String toStringArgs(final byte[]... args) {
        final StringBuilder sb = new StringBuilder();
        for (byte[] arg : args) {
            if (sb.length() != 0) {
                sb.append(" ");
            }
            sb.append(SafeEncoder.encode(arg));
        }

        return sb.toString();
    }

    /**
     * Send command.
     *
     * @param cmd the cmd
     * @return the redis connection
     */
    protected RedisConnection sendCommand(final Command cmd) {
        log.trace("[RedisConnection] Command={}", cmd.name());
        connect();
        RedisProtocol.sendCommand(outputStream, cmd, new byte[0][]);
        this.pipelinedCommands++;
        return this;
    }

    /**
     * Instantiates a new redis connection.
     *
     * @param host the host
     * @param port the port
     */
    public RedisConnection(final String host, final int port) {
        super();
        this.host = host;
        this.port = port;
    }

    /**
     * Gets the host.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host.
     *
     * @param host the new host
     */
    public void setHost(final String host) {
        this.host = host;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port.
     *
     * @param port the new port
     */
    public void setPort(final int port) {
        this.port = port;
    }

    /**
     * Instantiates a new redis connection.
     */
    public RedisConnection() {

    }

    /**
     * Connect.
     */
    public void connect() {
        if (!isConnected()) {
            try {
                socket = new Socket();
                // ->@wjw_add
                socket.setReuseAddress(true);
                socket.setKeepAlive(true); // Will monitor the TCP connection is
                // valid
                socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to
                // ensure timely delivery of data
                socket.setSoLinger(true, 0); // Control calls close () method,
                // the underlying socket is
                // closed immediately
                // <-@wjw_add

                socket.connect(new InetSocketAddress(host, port), timeout);
                socket.setSoTimeout(timeout);
                outputStream = new RedisOutputStream(socket.getOutputStream());
                inputStream = new RedisInputStream(socket.getInputStream());
                connectedTime = System.currentTimeMillis();
                log.trace("[RedisConnection] Connected {}", toString());
            } catch (IOException ex) {
                throw new JedisConnectionException(ex);
            }
        }
    }

    /**
     * Disconnect.
     */
    public void disconnect() {
        if (isConnected()) {
            try {
                inputStream.close();
                outputStream.close();
                if (!socket.isClosed()) {
                    socket.close();
                }
                disconnectedTime = System.currentTimeMillis();
                log.trace("[RedisConnection] Disconnected {}", toString());
            } catch (IOException ex) {
                throw new JedisConnectionException(ex);
            }
        }
    }

    /**
     * Checks if is connected.
     *
     * @return true, if is connected
     */
    public boolean isConnected() {
        return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
                && !socket.isInputShutdown() && !socket.isOutputShutdown();
    }

    /**
     * Gets the status code reply.
     *
     * @return the status code reply
     */
    public String getStatusCodeReply() {
        flush();
        this.pipelinedCommands--;
        final byte[] resp = (byte[]) RedisProtocol.read(inputStream);
        if (null == resp) {
            return null;
        } else {
            return SafeEncoder.encode(resp);
        }
    }

    /**
     * Gets the bulk reply.
     *
     * @return the bulk reply
     */
    public String getBulkReply() {
        final byte[] result = getBinaryBulkReply();
        if (null != result) {
            return SafeEncoder.encode(result);
        } else {
            return null;
        }
    }

    /**
     * Gets the binary bulk reply.
     *
     * @return the binary bulk reply
     */
    public byte[] getBinaryBulkReply() {
        flush();
        this.pipelinedCommands--;
        return (byte[]) RedisProtocol.read(inputStream);
    }

    /**
     * Gets the integer reply.
     *
     * @return the integer reply
     */
    public Long getIntegerReply() {
        flush();
        this.pipelinedCommands--;
        return (Long) RedisProtocol.read(inputStream);
    }

    /**
     * Gets the multi bulk reply.
     *
     * @return the multi bulk reply
     */
    public List<String> getMultiBulkReply() {
        return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
    }

    /**
     * Gets the binary multi bulk reply.
     *
     * @return the binary multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public List<byte[]> getBinaryMultiBulkReply() {
        flush();
        this.pipelinedCommands--;
        return (List<byte[]>) RedisProtocol.read(inputStream);
    }

    /**
     * Gets the object multi bulk reply.
     *
     * @return the object multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public List<Object> getObjectMultiBulkReply() {
        flush();
        this.pipelinedCommands--;
        return (List<Object>) RedisProtocol.read(inputStream);
    }

    /**
     * Gets the integer multi bulk reply.
     *
     * @return the integer multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public List<Long> getIntegerMultiBulkReply() {
        flush();
        this.pipelinedCommands--;
        return (List<Long>) RedisProtocol.read(inputStream);
    }

    public List<Object> getAll() {
        return getAll(0);
    }

    public List<Object> getAll(int except) {
        List<Object> all = new ArrayList<Object>();
        flush();
        while (pipelinedCommands > except) {
            try {
                all.add(RedisProtocol.read(inputStream));
            } catch (JedisDataException e) {
                all.add(e);
            }
            pipelinedCommands--;
        }
        return all;
    }

    /*
     * @see java.lang.Object#toString()
     */
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("host=").append(host).append(", ");
        sb.append("port=").append(port).append(", ");
        sb.append("timeout=").append(timeout).append(", ");
        sb.append("connected=").append(connectedTime).append(", ");
        sb.append("disconnected=").append(disconnectedTime).append(", ");
        long elapseTime = 0;
        if (connectedTime > 0 && disconnectedTime > 0) {
            elapseTime = disconnectedTime - connectedTime;
        } else if (connectedTime > 0) {
            elapseTime = System.currentTimeMillis() - connectedTime;
        }
        sb.append("elapse=").append(elapseTime);
        sb.append("}");

        return sb.toString();
    }
}
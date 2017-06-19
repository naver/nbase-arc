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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.navercorp.nbasearc.gcp.ErrorCode;
import com.navercorp.nbasearc.gcp.RequestCallback;
import com.navercorp.nbasearc.gcp.VirtualConnection;
import com.navercorp.redis.cluster.connection.RedisProtocol.Command;
import com.navercorp.redis.cluster.gateway.AffinityState;
import com.navercorp.redis.cluster.gateway.GatewayException;
import com.navercorp.redis.cluster.pipeline.BuilderFactory;
import com.navercorp.redis.cluster.util.ByteHashMap;
import com.navercorp.redis.cluster.util.RedisInputStream;
import com.navercorp.redis.cluster.util.RedisOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.JedisByteHashMap;
import redis.clients.util.SafeEncoder;

/**
 * The Class RedisConnection.
 *
 * @author seunghoo.han
 */
class RedisConnectionAsync implements RedisConnectionImpl {
    private final Logger log = LoggerFactory.getLogger(RedisConnectionAsync.class);
    private static final ByteBufAllocator ALLOCATOR = PooledByteBufAllocator.DEFAULT;

    private VirtualConnection vc;
    private RedisOutputStream outputStream;
    private ByteBuf outputBuf;
    
    /**
     * The host.
     */
    private String host;

    /**
     * The port.
     */
    private int port = RedisProtocol.DEFAULT_PORT;

    /**
     * The timeout.
     */
    private int timeout = RedisProtocol.DEFAULT_TIMEOUT;

    private Queue<ListenableFuture<byte[]>> pipelinedFutures = new ArrayDeque<ListenableFuture<byte[]>>();
    private long connectedTime = 0;
    private long disconnectedTime = 0;
    
    RedisConnectionAsync() {
        outputBuf = ALLOCATOR.buffer();
        outputStream = new RedisOutputStream(new ByteBufOutputStream(outputBuf));
    }
    
    RedisConnectionAsync(String host, int port) {
        this();
        this.host = host;
        this.port = port;
    }

    public void setVc(VirtualConnection vc) {
        this.vc = vc;
    }
    
    public void allocPc(int hash, AffinityState affinity, boolean pipelineMode) {
        vc.allocPc(hash, affinity, pipelineMode);
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
    }

    public void rollbackActiveTimeout() {
    }

    /**
     * Send command.
     *
     * @param cmd  the cmd
     * @param args the args
     * @return the redis connection
     */
    public void sendCommand(final Command cmd, final String... args) {
        final byte[][] bargs = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            bargs[i] = SafeEncoder.encode(args[i]);
        }
        sendCommand(cmd, bargs);
    }

    /**
     * Send command.
     *
     * @param cmd  the cmd
     * @param args the args
     * @return the redis connection
     */
    public void sendCommand(final Command cmd, final byte[]... args) {
        log.trace("[RedisConnection] Command={} {}", cmd.name(), toStringArgs(args));
        RedisProtocol.sendCommand(outputStream, cmd, args);
        try {
            outputStream.flush();
        } catch (IOException e) {
            throw new JedisConnectionException(e);
        }
        
        final SettableFuture<byte[]> future = SettableFuture.create();
        
        byte [] cmdBytes = new byte[outputBuf.readableBytes()]; 
        outputBuf.readBytes(cmdBytes);
        outputBuf.discardSomeReadBytes();
        
        vc.request(cmdBytes, timeout, new RequestCallback() {
            @Override
            public void onResponse(byte[] response, ErrorCode errCode) {
                switch (errCode) {
                case OK:
                    future.set(response);
                    break;
                    
                case CONNECTION_ERROR:
                    future.setException(new GatewayException("Connection error"));
                    break;
                case INTERNAL_ERROR:
                    future.setException(new GatewayException("Internal error"));
                    break;
                case NO_AVAILABLE_CONNECTION:
                    future.setException(new GatewayException("no available connection"));
                    break;
                case SHUTDOWN:
                    future.setException(new GatewayException("already closed"));
                    break;
                case TIMEOUT:
                    future.setException(new GatewayException("Timeout"));
                    break;
                }
            }
        });
        
        pipelinedFutures.add(future);
    }

    public String toStringArgs(final byte[]... args) {
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
    public void sendCommand(final Command cmd) {
        log.trace("[RedisConnectionAsync] Command={}", cmd.name());
        sendCommand(cmd, new byte[0][]);
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
     * Connect.
     */
    public void connect() {
        // do nothing
    }

    /**
     * Disconnect.
     */
    public void disconnect() {
        if (vc != null) {
            vc.close();
        }
        outputBuf.release();
    }

    @Override
    public void passivate() {
        vc.freePc();
    }

    /**
     * Checks if is connected.
     *
     * @return true, if is connected
     */
    public boolean isConnected() {
        return vc.isConnected();
    }
    
    private byte[] getResponse() {
        ListenableFuture<byte[]> f = pipelinedFutures.poll();
        try {
            return f.get();
        } catch (InterruptedException e) {
            throw new JedisConnectionException(e);
        } catch (ExecutionException e) {
            throw new JedisConnectionException(e);
        }
    }

    /**
     * Gets the status code reply.
     *
     * @return the status code reply
     */
    public String getStatusCodeReply() {
        return getStatusCodeReply(getResponse());
    }

    /**
     * Gets the bulk reply.
     *
     * @return the bulk reply
     */
    public String getBulkReply() {
        return getBulkReply(getResponse());
    }

    /**
     * Gets the binary bulk reply.
     *
     * @return the binary bulk reply
     */
    public byte[] getBinaryBulkReply() {
        return getBinaryBulkReply(getResponse());
    }

    /**
     * Gets the integer reply.
     *
     * @return the integer reply
     */
    public Long getIntegerReply() {
        return getIntegerReply(getResponse());
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
        return getBinaryMultiBulkReply(getResponse());
    }

    /**
     * Gets the object multi bulk reply.
     *
     * @return the object multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public List<Object> getObjectMultiBulkReply() {
        return getObjectMultiBulkReply(getResponse());
    }

    /**
     * Gets the integer multi bulk reply.
     *
     * @return the integer multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public List<Long> getIntegerMultiBulkReply() {
        return getIntegerMultiBulkReply(getResponse());
    }

    public List<Object> getAll() {
        return getAll(0);
    }

    public List<Object> getAll(int except) {
        List<Object> all = new ArrayList<Object>();
        while (pipelinedFutures.isEmpty() == false) {
            try {
                all.add(RedisProtocol.read(new RedisInputStream(new ByteArrayInputStream(getResponse()))));
            } catch (JedisDataException e) {
                all.add(e);
            }
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
    

    // ---------------------------------------------------------------------------------------
    // Conversion Functions
    // ---------------------------------------------------------------------------------------
    /**
     * Gets the bulk reply.
     *
     * @return the bulk reply
     */
    public static String getBulkReply(byte[] input) {
        final byte[] result = getBinaryBulkReply(input);
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
    public static byte[] getBinaryBulkReply(byte[] input) {
        return (byte[]) RedisProtocol.read(new RedisInputStream(new ByteArrayInputStream(input)));
    }

    /**
     * Gets the integer reply.
     *
     * @return the integer reply
     */
    public static Long getIntegerReply(RedisInputStream inputStream) {
        return (Long) RedisProtocol.read(inputStream);
    }

    /**
     * Gets the binary multi bulk reply.
     *
     * @return the binary multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public static List<byte[]> getBinaryMultiBulkReply(byte[] input) {
        return (List<byte[]>) RedisProtocol.read(
                new RedisInputStream(new ByteArrayInputStream(input)));
    }

    /**
     * Gets the object multi bulk reply.
     *
     * @return the object multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public static List<Object> getObjectMultiBulkReply(byte[] input) {
        return (List<Object>) RedisProtocol.read(
                new RedisInputStream(new ByteArrayInputStream(input)));
    }

    /**
     * Gets the integer multi bulk reply.
     *
     * @return the integer multi bulk reply
     */
    @SuppressWarnings("unchecked")
    public static List<Long> getIntegerMultiBulkReply(byte[] input) {
        return (List<Long>) RedisProtocol.read(
                new RedisInputStream(new ByteArrayInputStream(input)));
    }
    
    public static ListenableFuture<String> STRING(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], String>() {
            @Override
            public String apply(byte[] buf) {
                return getStatusCodeReply(buf);
            }
        });
    }

    /**
     * Gets the status code reply.
     *
     * @return the status code reply
     */
    public static String getStatusCodeReply(byte[] buf) {
        final byte[] resp = (byte[]) RedisProtocol.read(new RedisInputStream(new ByteArrayInputStream(buf)));
        if (null == resp) {
            return null;
        } else {
            return SafeEncoder.encode(resp);
        }
    }
    
    public static ListenableFuture<byte[]> BYTE(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], byte[]>() {
           @Override
           public byte[] apply(byte[] buf) {
               return getBinaryBulkReply(buf);
           }
        });
    }
    
    public static ListenableFuture<Long> LONG(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Long>() {
            @Override
            public Long apply(byte[] buf) {
                return getIntegerReply(buf);
            }
        });
    }

    /**
     * Gets the integer reply.
     *
     * @return the integer reply
     */
    public static Long getIntegerReply(byte[] buf) {
        return (Long) RedisProtocol.read(new RedisInputStream(new ByteArrayInputStream(buf)));
    }

    public static ListenableFuture<Double> DOUBLE(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Double>() {
            @Override
            public Double apply(byte[] buf) {
                return getDoubleReply(buf);
            }
        });
    }
    
    public static Double getDoubleReply(byte[] buf) {
        String reply = getBulkReply(buf);
        return (reply != null ? new Double(reply) : null);
    }
    
    public static ListenableFuture<Boolean> BOOLEAN(ListenableFuture<byte[]> f) {
        return Futures.transform(f,  new Function<byte[], Boolean>() {
            @Override
            public Boolean apply(byte[] input) {
                return getIntegerReply(input) == 1;
            }
        });
    }
    
    public static ListenableFuture<List<byte[]>> LIST_BYTE(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], List<byte[]>>() {
            @Override
            public List<byte[]> apply(byte[] buf) {
                return getListByteReply(buf);
            }
        });
    }
    
    public static List<byte[]> getListByteReply(byte[] buf) {
        List<byte[]> values = getBinaryMultiBulkReply(buf);
        if (values == null) {
            values = new ArrayList<byte[]>();
        }
        return values;
    }
    
    public static ListenableFuture<List<String>> LIST_STRING(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], List<String>>() {
            @Override
            public List<String> apply(byte[] buf) {
                return getListStringReply(buf);
            }
        });
    }

    public static List<String> getMultiBulkReply(byte[] buf) {
        return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply(buf));
    }
    
    public static List<String> getListStringReply(byte[] buf) {
        return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply(buf));
    }
    
    public static ListenableFuture<Map<byte[], List<byte[]>>> MAP_BYTE_LIST_BYTE(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Map<byte[], List<byte[]>>>() {
            @Override
            public Map<byte[], List<byte[]>> apply(byte[] buf) {
                return getMapByteListByteReply(buf);
            }
        });
    }
    
    public static Map<byte[], List<byte[]>> getMapByteListByteReply(byte []buf) {
        List<byte[]> values = getBinaryMultiBulkReply(buf);
        if (values == null) {
            values = new ArrayList<byte[]>();
        }

        final Map<byte[], List<byte[]>> result = new ByteHashMap<List<byte[]>>();
        final Iterator<byte[]> iterator = values.iterator();
        if (iterator == null) {
            return result;
        }

        while (iterator.hasNext()) {
            final byte[] name = iterator.next();
            List<byte[]> list = result.get(name);
            if (list == null) {
                list = new ArrayList<byte[]>();
            }

            if (iterator.hasNext()) {
                final byte[] value = iterator.next();
                if (value != null) {
                    list.add(value);
                }
            }
            result.put(name, list);
        }

        return result;
    }
    
    public static ListenableFuture<Map<String, List<String>>> MAP_STRING_LIST_STRING(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Map<String, List<String>>>() {
            @Override
            public Map<String, List<String>> apply(byte[] buf) {
                return getMapStringListStringReply(buf);
            }
        });
    }

    public static Map<String, List<String>> getMapStringListStringReply(byte[] buf) {
        List<String> values = getMultiBulkReply(buf);
        if (values == null) {
            values = new ArrayList<String>();
        }
        final Map<String, List<String>> result = new HashMap<String, List<String>>();
        final Iterator<String> iterator = values.iterator();
        if (iterator == null) {
            return result;
        }

        while (iterator.hasNext()) {
            final String name = iterator.next();
            List<String> list = result.get(name);
            if (list == null) {
                list = new ArrayList<String>();
            }

            if (iterator.hasNext()) {
                final String value = iterator.next();
                if (value != null) {
                    list.add(value);
                }
            }
            result.put(name, list);
        }

        return result;
    }
    
    public static ListenableFuture<Map<byte[], byte[]>> MAP_BYTE_BYTE(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Map<byte[], byte[]>>() {
            @Override
            public Map<byte[], byte[]> apply(byte[] input) {
                return getMapByteByteReply(input);
            }
        });
    }
    
    public static Map<byte[], byte[]> getMapByteByteReply(byte []buf) {
        final List<byte[]> flatHash = getBinaryMultiBulkReply(buf);
        final Map<byte[], byte[]> hash = new JedisByteHashMap();
        if (flatHash == null) {
            return hash;
        }

        final Iterator<byte[]> iterator = flatHash.iterator();
        if (iterator == null) {
            return hash;
        }

        while (iterator.hasNext()) {
            hash.put(iterator.next(), iterator.next());
        }

        return hash;
    }
    
    public static ListenableFuture<Map<String, String>> MAP_STRING_STRING(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Map<String, String>>() {
            @Override
            public Map<String, String> apply(byte[] input) {
                return BuilderFactory.STRING_MAP.build(getBinaryMultiBulkReply(input));
                
            }
        });
    }
    

    public static ListenableFuture<Map<byte[], Set<byte[]>>> MAP_SET_BYTE(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Map<byte[], Set<byte[]>>>() {
            @Override
            public Map<byte[], Set<byte[]>> apply(byte[] buf) {
                return getMapSetByteReply(buf);
            }
        });
    }
    
    public static Map<byte[], Set<byte[]>> getMapSetByteReply(byte []buf) {
        List<byte[]> values = getBinaryMultiBulkReply(buf);
        if (values == null) {
            values = new ArrayList<byte[]>();
        }

        final Map<byte[], Set<byte[]>> result = new ByteHashMap<Set<byte[]>>();
        final Iterator<byte[]> iterator = values.iterator();
        if (iterator == null) {
            return result;
        }

        while (iterator.hasNext()) {
            final byte[] name = iterator.next();
            Set<byte[]> set = result.get(name);
            if (set == null) {
                set = new HashSet<byte[]>();
            }
            if (iterator.hasNext()) {
                final byte[] value = iterator.next();
                if (value != null) {
                    set.add(value);
                }
            }
            result.put(name, set);
        }

        return result;
    }

    public static ListenableFuture<Map<String, Set<String>>> MAP_SET_STRING(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Map<String, Set<String>>>() {
            @Override
            public Map<String, Set<String>> apply(byte[] buf) {
                return getMapSetStringReply(buf);
            }
        });
    }
    
    public static Map<String, Set<String>> getMapSetStringReply(byte []buf) {
        List<String> values = getMultiBulkReply(buf);
        if (values == null) {
            values = new ArrayList<String>();
        }
        final Map<String, Set<String>> result = new HashMap<String, Set<String>>();
        final Iterator<String> iterator = values.iterator();
        if (iterator == null) {
            return result;
        }

        while (iterator.hasNext()) {
            final String name = iterator.next();
            Set<String> set = result.get(name);
            if (set == null) {
                set = new HashSet<String>();
            }
            if (iterator.hasNext()) {
                final String value = iterator.next();
                if (value != null) {
                    set.add(value);
                }
            }
            result.put(name, set);
        }

        return result;
    }
    
    public static ListenableFuture<Set<byte[]>> SET_BYTE(ListenableFuture<byte[]> f) {
        return Futures.transform(f,  new Function<byte[], Set<byte[]>>() {

            @Override
            public Set<byte[]> apply(byte[] input) {
                return getSetByteReply(input);
            }
            
        });
    }
    
    public static Set<byte[]> getSetByteReply(byte []buf) {
        List<byte[]> values = getBinaryMultiBulkReply(buf);
        if (values == null) {
            return new HashSet<byte[]>();
        }

        return new HashSet<byte[]>(values);
    }
    
    public static ListenableFuture<Set<String>> SET_STRING(ListenableFuture<byte[]> f) {
        return Futures.transform(f,  new Function<byte[], Set<String>>() {
            @Override
            public Set<String> apply(byte[] input) {
                return getSetStringReply(input);
            }
            
        });
    }
    
    public static Set<String> getSetStringReply(byte []buf) {
        List<String> values = getMultiBulkReply(buf);
        if (values == null) {
            return new HashSet<String>();
        }

        return new HashSet<String>(values);
    }
    
    public static ListenableFuture<Set<Tuple>> SET_TUPLE(ListenableFuture<byte[]> f) {
        return Futures.transform(f, new Function<byte[], Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(byte[] input) {
                return getBinaryTupledSet(input);
            }
        });
    }

    /**
     * Gets the binary tupled set.
     *
     * @return the binary tupled set
     */
    public static Set<Tuple> getBinaryTupledSet(byte[] buf) {
        List<byte[]> membersWithScores = getBinaryMultiBulkReply(buf);
        Set<Tuple> set = new LinkedHashSet<Tuple>();
        if (membersWithScores == null) {
            return set;
        }

        Iterator<byte[]> iterator = membersWithScores.iterator();
        if (iterator == null) {
            return set;
        }

        while (iterator.hasNext()) {
            set.add(new Tuple(iterator.next(), Double.valueOf(SafeEncoder.encode(iterator.next()))));
        }
        return set;
    }


    /**
     * To byte arrays.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param expireMillis the expire millis
     * @param keys         the keys
     * @return the byte[][]
     */
    protected byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode,
                                  final long expireMillis, final byte[][] keys) {
        final byte[][] allArgs = new byte[keys.length + 4][];

        int index = 0;
        allArgs[index++] = keyspace;
        allArgs[index++] = uid;
        allArgs[index++] = serviceCode;
        allArgs[index++] = RedisProtocol.toByteArray(expireMillis);

        for (int i = 0; i < keys.length; i++) {
            allArgs[index++] = keys[i];
        }

        return allArgs;
    }

    /**
     * To byte arrays.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param values       the values
     * @return the byte[][]
     */
    protected byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                                  final long expireMillis, final byte[][] values) {
        final byte[][] allArgs = new byte[values.length * 2 + 4][];

        int index = 0;
        allArgs[index++] = keyspace;
        allArgs[index++] = uid;
        allArgs[index++] = serviceCode;
        allArgs[index++] = key;
        byte[] expireTime = RedisProtocol.toByteArray(expireMillis);

        for (int i = 0; i < values.length; i++) {
            allArgs[index++] = values[i];
            allArgs[index++] = expireTime;
        }

        return allArgs;
    }

    /**
     * To byte arrays.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param values      the values
     * @return the byte[][]
     */
    protected byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                                  final byte[][] values) {
        final byte[][] allArgs = new byte[values.length + 4][];
        int index = 0;
        allArgs[index++] = keyspace;
        allArgs[index++] = uid;
        allArgs[index++] = serviceCode;
        allArgs[index++] = key;

        for (int i = 0; i < values.length; i++) {
            allArgs[index++] = values[i];
        }
        return allArgs;
    }

    /**
     * To byte arrays.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @return the byte[][]
     */
    protected byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[][] key) {
        final byte[][] allArgs = new byte[key.length + 3][];
        int index = 0;
        allArgs[index++] = keyspace;
        allArgs[index++] = uid;
        allArgs[index++] = serviceCode;

        for (int i = 0; i < key.length; i++) {
            allArgs[index++] = key[i];
        }
        return allArgs;
    }

}
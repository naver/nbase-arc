/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.navercorp.redis.cluster.triples;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.navercorp.redis.cluster.RedisClusterClient;
import com.navercorp.redis.cluster.connection.RedisConnection;
import com.navercorp.redis.cluster.util.ByteHashMap;

import redis.clients.util.SafeEncoder;

/**
 * The Class BinaryTriplesRedisCluster.
 *
 * @author jaehong.kim
 */
public class BinaryTriplesRedisCluster implements BinaryTriplesRedisClusterCommands {

    /**
     * The Constant DEFAULT_KEYSPACE.
     */
    public static final String DEFAULT_KEYSPACE = "*";

    /**
     * The client.
     */
    protected RedisClusterClient client = null;

    /**
     * The default expire seconds.
     */
    protected long defaultExpireSeconds = 0;

    /**
     * The keyspace.
     */
    protected String keyspace = DEFAULT_KEYSPACE;

    /**
     * Instantiates a new binary triples redis cluster.
     *
     * @param host the host
     */
    public BinaryTriplesRedisCluster(final String host) {
        client = new RedisClusterClient(host);
    }

    /**
     * Instantiates a new binary triples redis cluster.
     *
     * @param host the host
     * @param port the port
     */
    public BinaryTriplesRedisCluster(final String host, final int port) {
        client = new RedisClusterClient(host, port);
    }

    /**
     * Instantiates a new binary triples redis cluster.
     *
     * @param host    the host
     * @param port    the port
     * @param timeout the timeout
     */
    public BinaryTriplesRedisCluster(final String host, final int port, final int timeout) {
        client = new RedisClusterClient(host, port);
        client.setTimeout(timeout);
    }

    /**
     * Instantiates a new binary triples redis cluster.
     *
     * @param host    the host
     * @param port    the port
     * @param timeout the timeout
     * @param async   if async is true, this class uses async network I/O
     */
    public BinaryTriplesRedisCluster(final String host, final int port, final int timeout, boolean async) {
        client = new RedisClusterClient(host, port, async);
        client.setTimeout(timeout);
    }

    public RedisConnection getConnection() {
        return client;
    }

    @Deprecated
    public void setKeyspace(final String keyspace) {
        this.keyspace = keyspace;
    }

    @Deprecated
    public String getKeyspace() {
        return this.keyspace;
    }

    public List<byte[]> slget(byte[] key, byte[] field, byte[] name) {
        client.slget(SafeEncoder.encode(keyspace), key, field, name);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            values = new ArrayList<byte[]>();
        }
        return values;
    }

    public Map<byte[], List<byte[]>> slmget(byte[] key, byte[] field, byte[]... names) {
        client.slmget(SafeEncoder.encode(keyspace), key, field, names);
        List<byte[]> values = client.getBinaryMultiBulkReply();
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

    public Set<byte[]> slkeys(byte[] key) {
        client.slkeys(SafeEncoder.encode(keyspace), key);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            return new HashSet<byte[]>();
        }

        return new HashSet<byte[]>(values);
    }

    public Set<byte[]> slkeys(byte[] key, byte[] field) {
        client.slkeys(SafeEncoder.encode(keyspace), key, field);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            return new HashSet<byte[]>();
        }

        return new HashSet<byte[]>(values);
    }

    public Long sladd(byte[] key, byte[] field, byte[] name, byte[]... values) {
        client.sladd(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds),
                values);
        return client.getIntegerReply();
    }

    public Long sladd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        client.sladd(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long sladdAt(byte[] key, byte[] field, byte[] name, long unixTime, byte[]... values) {
        client.sladdAt(SafeEncoder.encode(keyspace), key, field, name, unixTime, values);
        return client.getIntegerReply();
    }

    public Long slset(byte[] key, byte[] field, byte[] name, byte[]... values) {
        client.slset(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds),
                values);
        return client.getIntegerReply();
    }

    public Long slset(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        client.slset(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long sldel(byte[] key) {
        client.sldel(SafeEncoder.encode(keyspace), key);
        return client.getIntegerReply();
    }

    public Long slrem(byte[] key, byte[] field) {
        client.slrem(SafeEncoder.encode(keyspace), key, field);
        return client.getIntegerReply();
    }

    public Long slrem(byte[] key, byte[] field, byte[] name) {
        client.slrem(SafeEncoder.encode(keyspace), key, field, name);
        return client.getIntegerReply();
    }

    public Long slrem(byte[] key, byte[] field, byte[] name, byte[]... values) {
        client.slrem(SafeEncoder.encode(keyspace), key, field, name, values);
        return client.getIntegerReply();
    }

    public Long slcount(byte[] key) {
        client.slcount(SafeEncoder.encode(keyspace), key);
        return client.getIntegerReply();
    }

    public Long slcount(byte[] key, byte[] field) {
        client.slcount(SafeEncoder.encode(keyspace), key, field);
        return client.getIntegerReply();
    }

    public Long slcount(byte[] key, byte[] field, byte[] name) {
        client.slcount(SafeEncoder.encode(keyspace), key, field, name);
        return client.getIntegerReply();
    }

    public Boolean slexists(byte[] key, byte[] field, byte[] name, byte[] value) {
        client.slexists(SafeEncoder.encode(keyspace), key, field, name, value);
        return client.getIntegerReply() == 1;
    }

    public Long slexpire(byte[] key, long expireSeconds) {
        client.slexpire(SafeEncoder.encode(keyspace), key, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slexpire(byte[] key, byte[] field, long expireSeconds) {
        client.slexpire(SafeEncoder.encode(keyspace), key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slexpire(byte[] key, byte[] field, byte[] name, long expireSeconds) {
        client.slexpire(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slexpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds) {
        client.slexpire(SafeEncoder.encode(keyspace), key, field, name, value, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slttl(byte[] key, byte[] field, byte[] name) {
        client.slttl(SafeEncoder.encode(keyspace), key, field, name);
        return client.getIntegerReply();
    }

    public Long slttl(byte[] key, byte[] field, byte[] name, byte[] value) {
        client.slttl(SafeEncoder.encode(keyspace), key, field, name, value);
        return client.getIntegerReply();
    }

    public List<byte[]> slvals(byte[] key, byte[] field) {
        client.slvals(SafeEncoder.encode(keyspace), key, field);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            return new ArrayList<byte[]>();
        }

        return values;
    }

    public Set<byte[]> ssget(byte[] key, byte[] field, byte[] name) {
        client.ssget(SafeEncoder.encode(keyspace), key, field, name);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            return new HashSet<byte[]>();
        }

        return new HashSet<byte[]>(values);
    }

    public Map<byte[], Set<byte[]>> ssmget(byte[] key, byte[] field, byte[]... names) {
        client.ssmget(SafeEncoder.encode(keyspace), key, field, names);
        List<byte[]> values = client.getBinaryMultiBulkReply();
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

    public Set<byte[]> sskeys(byte[] key) {
        client.sskeys(SafeEncoder.encode(keyspace), key);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            return new HashSet<byte[]>();
        }

        return new HashSet<byte[]>(values);
    }

    public Set<byte[]> sskeys(byte[] key, byte[] field) {
        client.sskeys(SafeEncoder.encode(keyspace), key, field);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            return new HashSet<byte[]>();
        }

        return new HashSet<byte[]>(values);
    }

    public Long ssadd(byte[] key, byte[] field, byte[] name, byte[]... values) {
        client.ssadd(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds),
                values);
        return client.getIntegerReply();
    }

    public Long ssadd(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        client.ssadd(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long ssaddAt(byte[] key, byte[] field, byte[] name, long unixTime, byte[]... values) {
        client.ssaddAt(SafeEncoder.encode(keyspace), key, field, name, unixTime, values);
        return client.getIntegerReply();
    }

    public Long ssset(byte[] key, byte[] field, byte[] name, byte[]... values) {
        client.ssset(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds),
                values);
        return client.getIntegerReply();
    }

    public Long ssset(byte[] key, byte[] field, byte[] name, long expireSeconds, byte[]... values) {
        client.ssset(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long ssdel(byte[] key) {
        client.ssdel(SafeEncoder.encode(keyspace), key);
        return client.getIntegerReply();
    }

    public Long ssrem(byte[] key, byte[] field) {
        client.ssrem(SafeEncoder.encode(keyspace), key, field);
        return client.getIntegerReply();
    }

    public Long ssrem(byte[] key, byte[] field, byte[] name) {
        client.ssrem(SafeEncoder.encode(keyspace), key, field, name);
        return client.getIntegerReply();
    }

    public Long ssrem(byte[] key, byte[] field, byte[] name, byte[]... values) {
        client.ssrem(SafeEncoder.encode(keyspace), key, field, name, values);
        return client.getIntegerReply();
    }

    public Long sscount(byte[] key) {
        client.sscount(SafeEncoder.encode(keyspace), key);
        return client.getIntegerReply();
    }

    public Long sscount(byte[] key, byte[] field) {
        client.sscount(SafeEncoder.encode(keyspace), key, field);
        return client.getIntegerReply();
    }

    public Long sscount(byte[] key, byte[] field, byte[] name) {
        client.sscount(SafeEncoder.encode(keyspace), key, field, name);
        return client.getIntegerReply();
    }

    public Boolean ssexists(byte[] key, byte[] field, byte[] name, byte[] value) {
        client.ssexists(SafeEncoder.encode(keyspace), key, field, name, value);
        return client.getIntegerReply() == 1;
    }

    public Long ssexpire(byte[] key, long expireSeconds) {
        client.ssexpire(SafeEncoder.encode(keyspace), key, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssexpire(byte[] key, byte[] field, long expireSeconds) {
        client.ssexpire(SafeEncoder.encode(keyspace), key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssexpire(byte[] key, byte[] field, byte[] name, long expireSeconds) {
        client.ssexpire(SafeEncoder.encode(keyspace), key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssexpire(byte[] key, byte[] field, byte[] name, byte[] value, long expireSeconds) {
        client.ssexpire(SafeEncoder.encode(keyspace), key, field, name, value, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssttl(byte[] key, byte[] field, byte[] name) {
        client.ssttl(SafeEncoder.encode(keyspace), key, field, name);
        return client.getIntegerReply();
    }

    public Long ssttl(byte[] key, byte[] field, byte[] name, byte[] value) {
        client.ssttl(SafeEncoder.encode(keyspace), key, field, name, value);
        return client.getIntegerReply();
    }

    public List<byte[]> ssvals(byte[] key, byte[] field) {
        client.ssvals(SafeEncoder.encode(keyspace), key, field);
        List<byte[]> values = client.getBinaryMultiBulkReply();
        if (values == null) {
            return new ArrayList<byte[]>();
        }

        return values;
    }

    public String connectInfo() {
        return client.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("client=").append(client).append(", ");
        sb.append("defaultExpireSeconds=").append(defaultExpireSeconds).append(", ");
        sb.append("keyspace=").append(keyspace);
        sb.append("}");

        return sb.toString();
    }
}
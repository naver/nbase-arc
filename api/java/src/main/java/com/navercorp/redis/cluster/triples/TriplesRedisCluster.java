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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The Class TriplesRedisCluster.
 *
 * @author jaehong.kim
 */
public class TriplesRedisCluster extends BinaryTriplesRedisCluster implements TriplesRedisClusterCommands {

    /**
     * Instantiates a new triples redis cluster.
     *
     * @param host the host
     */
    public TriplesRedisCluster(final String host) {
        super(host);
    }

    /**
     * Instantiates a new triples redis cluster.
     *
     * @param host the host
     * @param port the port
     */
    public TriplesRedisCluster(final String host, final int port) {
        super(host, port);
    }

    /**
     * Instantiates a new triples redis cluster.
     *
     * @param host    the host
     * @param port    the port
     * @param timeout the timeout
     */
    public TriplesRedisCluster(final String host, final int port, final int timeout) {
        super(host, port, timeout);
    }

    public List<String> slget(final String key, final String field, final String name) {
        client.slget(keyspace, key, field, name);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            values = new ArrayList<String>();
        }
        return values;
    }

    public Map<String, List<String>> slmget(String key, String field, String... names) {
        client.slmget(keyspace, key, field, names);
        List<String> values = client.getMultiBulkReply();
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

    public Set<String> slkeys(String key) {
        client.slkeys(keyspace, key);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            return new HashSet<String>();
        }
        return new HashSet<String>(values);
    }

    public Set<String> slkeys(String key, String field) {
        client.slkeys(keyspace, key, field);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            return new HashSet<String>();
        }
        return new HashSet<String>(values);
    }

    public Long sladd(String key, String field, String name, String... values) {
        client.sladd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
        return client.getIntegerReply();
    }

    public Long sladd(String key, String field, String name, long expireSeconds, String... values) {
        client.sladd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long sladdAt(String key, String field, String name, long millisecondsTimestamp, String... values) {
        client.sladdAt(keyspace, key, field, name, millisecondsTimestamp, values);
        return client.getIntegerReply();
    }

    public Long slset(String key, String field, String name, String... values) {
        client.slset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
        return client.getIntegerReply();
    }

    public Long slset(String key, String field, String name, long expireSeconds, String... values) {
        client.slset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long sldel(String key) {
        client.sldel(keyspace, key);
        return client.getIntegerReply();
    }

    public Long slrem(String key, String field) {
        client.slrem(keyspace, key, field);
        return client.getIntegerReply();
    }

    public Long slrem(String key, String field, String name) {
        client.slrem(keyspace, key, field, name);
        return client.getIntegerReply();
    }

    public Long slrem(String key, String field, String name, String... values) {
        client.slrem(keyspace, key, field, name, values);
        return client.getIntegerReply();
    }

    public Long slcount(String key) {
        client.slcount(keyspace, key);
        return client.getIntegerReply();
    }

    public Long slcount(String key, String field) {
        client.slcount(keyspace, key, field);
        return client.getIntegerReply();
    }

    public Long slcount(String key, String field, String name) {
        client.slcount(keyspace, key, field, name);
        return client.getIntegerReply();
    }

    public Boolean slexists(String key, String field, String name, String value) {
        client.slexists(keyspace, key, field, name, value);
        return client.getIntegerReply() == 1;
    }

    public Long slexpire(String key, long expireSeconds) {
        client.slexpire(keyspace, key, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slexpire(String key, String field, long expireSeconds) {
        client.slexpire(keyspace, key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slexpire(String key, String field, String name, long expireSeconds) {
        client.slexpire(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slexpire(String key, String field, String name, String value, long expireSeconds) {
        client.slexpire(keyspace, key, field, name, value, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long slttl(String key, String field, String name) {
        client.slttl(keyspace, key, field, name);
        return client.getIntegerReply();
    }

    public Long slttl(String key, String field, String name, String value) {
        client.slttl(keyspace, key, field, name, value);
        return client.getIntegerReply();
    }

    public List<String> slvals(String key, String field) {
        client.slvals(keyspace, key, field);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            return new ArrayList<String>();
        }

        return values;
    }

    public Set<String> ssget(String key, String field, String name) {
        client.ssget(keyspace, key, field, name);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            return new HashSet<String>();
        }

        return new HashSet<String>(values);
    }

    public Map<String, Set<String>> ssmget(String key, String field, String... names) {
        client.ssmget(keyspace, key, field, names);
        List<String> values = client.getMultiBulkReply();
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

    public Set<String> sskeys(String key) {
        client.sskeys(keyspace, key);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            return new HashSet<String>();
        }
        return new HashSet<String>(values);
    }

    public Set<String> sskeys(String key, String field) {
        client.sskeys(keyspace, key, field);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            return new HashSet<String>();
        }

        return new HashSet<String>(values);
    }

    public Long ssadd(String key, String field, String name, String... values) {
        client.ssadd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
        return client.getIntegerReply();
    }

    public Long ssadd(String key, String field, String name, long expireSeconds, String... values) {
        client.ssadd(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long ssaddAt(String key, String field, String name, long unixTime, String... values) {
        client.ssaddAt(keyspace, key, field, name, unixTime, values);
        return client.getIntegerReply();
    }

    public Long ssset(String key, String field, String name, String... values) {
        client.ssset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(defaultExpireSeconds), values);
        return client.getIntegerReply();
    }

    public Long ssset(String key, String field, String name, long expireSeconds, String... values) {
        client.ssset(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds), values);
        return client.getIntegerReply();
    }

    public Long ssdel(String key) {
        client.ssdel(keyspace, key);
        return client.getIntegerReply();
    }

    public Long ssrem(String key, String field) {
        client.ssrem(keyspace, key, field);
        return client.getIntegerReply();
    }

    public Long ssrem(String key, String field, String name) {
        client.ssrem(keyspace, key, field, name);
        return client.getIntegerReply();
    }

    public Long ssrem(String key, String field, String name, String... values) {
        client.ssrem(keyspace, key, field, name, values);
        return client.getIntegerReply();
    }

    public Long sscount(String key) {
        client.sscount(keyspace, key);
        return client.getIntegerReply();
    }

    public Long sscount(String key, String field) {
        client.sscount(keyspace, key, field);
        return client.getIntegerReply();
    }

    public Long sscount(String key, String field, String name) {
        client.sscount(keyspace, key, field, name);
        return client.getIntegerReply();
    }

    public Boolean ssexists(String key, String field, String name, String value) {
        client.ssexists(keyspace, key, field, name, value);
        return client.getIntegerReply() == 1;
    }

    public Long ssexpire(String key, long expireSeconds) {
        client.ssexpire(keyspace, key, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssexpire(String key, String field, long expireSeconds) {
        client.ssexpire(keyspace, key, field, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssexpire(String key, String field, String name, long expireSeconds) {
        client.ssexpire(keyspace, key, field, name, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssexpire(String key, String field, String name, String value, long expireSeconds) {
        client.ssexpire(keyspace, key, field, name, value, TimeUnit.SECONDS.toMillis(expireSeconds));
        return client.getIntegerReply();
    }

    public Long ssttl(String key, String field, String name) {
        client.ssttl(keyspace, key, field, name);
        return client.getIntegerReply();
    }

    public Long ssttl(String key, String field, String name, String value) {
        client.ssttl(keyspace, key, field, name, value);
        return client.getIntegerReply();
    }

    public List<String> ssvals(String key, String field) {
        client.ssvals(keyspace, key, field);
        List<String> values = client.getMultiBulkReply();
        if (values == null) {
            return new ArrayList<String>();
        }

        return values;
    }
}
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

import redis.clients.util.SafeEncoder;

/**
 * The Class TriplesRedisClusterClient.
 *
 * @author jaehong.kim
 */
public class TriplesRedisClusterClient extends BinaryTriplesRedisClusterClient {

    /**
     * Instantiates a new triples redis cluster client.
     *
     * @param host the host
     */
    public TriplesRedisClusterClient(final String host) {
        super(host);
    }

    /**
     * Instantiates a new triples redis cluster client.
     *
     * @param host the host
     * @param port the port
     */
    public TriplesRedisClusterClient(final String host, final int port) {
        super(host, port);
    }

    // Hashes of Lists ////////////////////

    /**
     * Slget.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slget(final String keyspace, final String uid, final String serviceCode, final String key) {
        slget(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Slmget.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param keys        the keys
     */
    public void slmget(final String keyspace, final String uid, final String serviceCode, final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        slmget(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode), bkeys);
    }

    /**
     * Slkeys.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void slkeys(final String keyspace, final String uid) {
        slkeys(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid));
    }

    /**
     * Slkeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slkeys(final String keyspace, final String uid, final String serviceCode) {
        slkeys(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }

    /**
     * Slkeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slkeys(final String keyspace, final String uid, final String serviceCode, final String key) {
        slkeys(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Sladd.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param values       the values
     */
    public void sladd(final String keyspace, final String uid, final String serviceCode, final String key,
                      final long expireMillis, final String... values) {
        final byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        sladd(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), expireMillis, bvalues);
    }

    public void sladdAt(final String keyspace, final String uid, final String serviceCode, final String key,
                        final long unixTime, final String... values) {
        final byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        sladdAt(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), unixTime, bvalues);
    }

    /**
     * Sldel.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void sldel(final String keyspace, final String uid) {
        sldel(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid));
    }

    /**
     * Slrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slrem(final String keyspace, final String uid, final String serviceCode) {
        slrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }

    /**
     * Slrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slrem(final String keyspace, final String uid, final String serviceCode, final String key) {
        slrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Slrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param value       the value
     */
    public void slrem(final String keyspace, final String uid, final String serviceCode, final String key,
                      final String value) {
        slrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    /**
     * Slrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param values      the values
     */
    public void slrem(final String keyspace, final String uid, final String serviceCode, final String key,
                      final String[] values) {
        final byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        slrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), bvalues);
    }

    /**
     * Slmrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param keys        the keys
     */
    public void slmrem(final String keyspace, final String uid, final String serviceCode, final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        slmrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode), bkeys);
    }

    /**
     * Slcount.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void slcount(final String keyspace, final String uid) {
        slcount(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid));
    }

    /**
     * Slcount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slcount(final String keyspace, final String uid, final String serviceCode) {
        slcount(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }

    /**
     * Slcount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slcount(final String keyspace, final String uid, final String serviceCode, final String key) {
        slcount(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Slexists.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slexists(final String keyspace, final String uid, final String serviceCode, final String key) {
        slexists(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Slexists.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param value       the value
     */
    public void slexists(final String keyspace, final String uid, final String serviceCode, final String key,
                         final String value) {
        slexists(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    /**
     * Slexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param expireMillis the expire millis
     */
    public void slexpire(final String keyspace, final String uid, long expireMillis) {
        slexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), expireMillis);
    }

    /**
     * Slexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param expireMillis the expire millis
     */
    public void slexpire(final String keyspace, final String uid, final String serviceCode, long expireMillis) {
        slexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode), expireMillis);
    }

    /**
     * Slexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     */
    public void slexpire(final String keyspace, final String uid, final String serviceCode, final String key,
                         long expireMillis) {
        slexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), expireMillis);
    }

    /**
     * Slexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param value        the value
     * @param expireMillis the expire millis
     */
    public void slexpire(final String keyspace, final String uid, final String serviceCode, final String key,
                         final String value, final long expireMillis) {
        slexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value), expireMillis);
    }

    /**
     * Slmexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param expireMillis the expire millis
     * @param serviceCode  the service code
     * @param keys         the keys
     */
    public void slmexpire(final String keyspace, final String uid, final long expireMillis, final String serviceCode,
                          final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        slmexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), expireMillis, SafeEncoder.encode(serviceCode),
                bkeys);
    }

    /**
     * Slttl.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slttl(final String keyspace, final String uid, final String serviceCode, final String key) {
        slttl(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Slttl.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param value       the value
     */
    public void slttl(final String keyspace, final String uid, final String serviceCode, final String key,
                      final String value) {
        slttl(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    /**
     * Slvals.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slvals(final String keyspace, final String uid, final String serviceCode) {
        slvals(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }

    /**
     * Slset.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param values       the values
     */
    public void slset(final String keyspace, final String uid, final String serviceCode, final String key,
                      final long expireMillis, final String... values) {
        byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        slset(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), expireMillis, bvalues);
    }

    /**
     * Slreplace.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param expireTime  the expire time
     * @param oldValue    the old value
     * @param newValue    the new value
     */
    public void slreplace(final String keyspace, final String uid, final String serviceCode, final String key,
                          final long expireTime, final String oldValue, final String newValue) {
        slreplace(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), expireTime, SafeEncoder.encode(oldValue), SafeEncoder.encode(newValue));
    }

    // Hashes of Sets ///////////////

    /**
     * Ssget.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssget(final String keyspace, final String uid, final String serviceCode, final String key) {
        ssget(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Ssmget.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param keys        the keys
     */
    public void ssmget(final String keyspace, final String uid, final String serviceCode, final String... keys) {
        final byte[][] bkeys = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bkeys[i] = SafeEncoder.encode(keys[i]);
        }
        ssmget(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode), bkeys);
    }

    /**
     * Sskeys.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void sskeys(final String keyspace, final String uid) {
        sskeys(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid));
    }

    /**
     * Sskeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void sskeys(final String keyspace, final String uid, final String serviceCode) {
        sskeys(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }

    /**
     * Sskeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void sskeys(final String keyspace, final String uid, final String serviceCode, final String key) {
        sskeys(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Ssadd.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param values       the values
     */
    public void ssadd(final String keyspace, final String uid, final String serviceCode, final String key,
                      final long expireMillis, final String... values) {
        final byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        ssadd(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), expireMillis, bvalues);
    }

    public void ssaddAt(final String keyspace, final String uid, final String serviceCode, final String key,
                        final long unixTime, final String... values) {
        final byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        ssaddAt(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), unixTime, bvalues);
    }

    /**
     * Ssdel.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void ssdel(final String keyspace, final String uid) {
        ssdel(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid));
    }

    /**
     * Ssrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void ssrem(final String keyspace, final String uid, final String serviceCode) {
        ssrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }

    /**
     * Ssrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssrem(final String keyspace, final String uid, final String serviceCode, final String key) {
        ssrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Ssrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param value       the value
     */
    public void ssrem(final String keyspace, final String uid, final String serviceCode, final String key,
                      final String value) {
        ssrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    /**
     * Ssrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param values      the values
     */
    public void ssrem(final String keyspace, final String uid, final String serviceCode, final String key,
                      final String[] values) {
        final byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        ssrem(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), bvalues);
    }

    /**
     * Sscount.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void sscount(final String keyspace, final String uid) {
        sscount(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid));
    }

    /**
     * Sscount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void sscount(final String keyspace, final String uid, final String serviceCode) {
        sscount(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }

    /**
     * Sscount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void sscount(final String keyspace, final String uid, final String serviceCode, final String key) {
        sscount(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Ssexists.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssexists(final String keyspace, final String uid, final String serviceCode, final String key) {
        ssexists(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Ssexists.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param value       the value
     */
    public void ssexists(final String keyspace, final String uid, final String serviceCode, final String key,
                         final String value) {
        ssexists(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    /**
     * Ssexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param expireMillis the expire seconds
     */
    public void ssexpire(final String keyspace, final String uid, long expireMillis) {
        ssexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), expireMillis);
    }

    /**
     * Ssexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param expireMillis the expire seconds
     */
    public void ssexpire(final String keyspace, final String uid, final String serviceCode, long expireMillis) {
        ssexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode), expireMillis);
    }

    /**
     * Ssexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     */
    public void ssexpire(final String keyspace, final String uid, final String serviceCode, final String key,
                         long expireMillis) {
        ssexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), expireMillis);
    }

    /**
     * Ssexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param value        the value
     * @param expireMillis the expire millis
     */
    public void ssexpire(final String keyspace, final String uid, final String serviceCode, final String key,
                         final String value, final long expireMillis) {
        ssexpire(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value), expireMillis);
    }

    /**
     * Ssttl.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssttl(final String keyspace, final String uid, final String serviceCode, final String key) {
        ssttl(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key));
    }

    /**
     * Ssttl.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     * @param value       the value
     */
    public void ssttl(final String keyspace, final String uid, final String serviceCode, final String key,
                      final String value) {
        ssttl(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    /**
     * Ssset.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param values       the values
     */
    public void ssset(final String keyspace, final String uid, final String serviceCode, final String key,
                      final long expireMillis, final String... values) {
        final byte[][] bvalues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bvalues[i] = SafeEncoder.encode(values[i]);
        }
        ssset(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode),
                SafeEncoder.encode(key), expireMillis, bvalues);
    }

    /**
     * Ssvals.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void ssvals(final String keyspace, final String uid, final String serviceCode) {
        ssvals(SafeEncoder.encode(keyspace), SafeEncoder.encode(uid), SafeEncoder.encode(serviceCode));
    }
}
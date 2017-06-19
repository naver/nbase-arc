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

import com.navercorp.redis.cluster.connection.RedisConnection;
import com.navercorp.redis.cluster.connection.RedisProtocol;
import com.navercorp.redis.cluster.connection.RedisProtocol.Command;

/**
 * The Class BinaryTriplesRedisClusterClient.
 *
 * @author jaehong.kim
 */
public class BinaryTriplesRedisClusterClient extends RedisConnection {

    /**
     * Instantiates a new binary triples redis cluster client.
     *
     * @param host the host
     */
    public BinaryTriplesRedisClusterClient(final String host) {
        super(host);
    }

    /**
     * Instantiates a new binary triples redis cluster client.
     *
     * @param host the host
     * @param port the port
     */
    public BinaryTriplesRedisClusterClient(String host, final int port) {
        super(host, port);
    }
    
    // Hashes of Lists ////////////////////

    public BinaryTriplesRedisClusterClient(String host, int port, boolean async) {
        super(host, port, async);
    }

    /**
     * Slget.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slget(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3LGET, keyspace, uid, serviceCode, key);
    }

    /**
     * Slmget.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slmget(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[]... key) {
        byte[][] params = toByteArrays(keyspace, uid, serviceCode, key);
        sendCommand(Command.S3LMGET, params);
    }

    /**
     * Slkeys.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void slkeys(final byte[] keyspace, final byte[] uid) {
        sendCommand(Command.S3KEYS, keyspace, uid);
    }

    /**
     * Slkeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slkeys(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3LKEYS, keyspace, uid, serviceCode);
    }

    /**
     * Slkeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slkeys(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3LKEYS, keyspace, uid, serviceCode, key);
    }

    /**
     * Sladd.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param value        the value
     */
    public void sladd(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final long expireMillis, final byte[] value) {
        sendCommand(Command.S3LADD, keyspace, uid, serviceCode, key, value, RedisProtocol.toByteArray(expireMillis));
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
    public void sladd(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final long expireMillis, final byte[]... values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, expireMillis, values);
        sendCommand(Command.S3LADD, params);
    }

    public void sladdAt(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                        final long unixTime, final byte[]... values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, unixTime, values);
        sendCommand(Command.S3LADDAT, params);
    }

    /**
     * Sldel.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void sldel(final byte[] keyspace, final byte[] uid) {
        sendCommand(Command.S3REM, keyspace, uid);
    }

    /**
     * Slrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3LREM, keyspace, uid, serviceCode);
    }

    /**
     * Slrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3LREM, keyspace, uid, serviceCode, key);
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
    public void slrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final byte[]... values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, values);
        sendCommand(Command.S3LREM, params);
    }

    /**
     * Slmrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param keys        the keys
     */
    public void slmrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[]... keys) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, keys);
        sendCommand(Command.S3LMREM, params);
    }

    /**
     * Slcount.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void slcount(final byte[] keyspace, final byte[] uid) {
        sendCommand(Command.S3COUNT, keyspace, uid);
    }

    /**
     * Slcount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slcount(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3LCOUNT, keyspace, uid, serviceCode);
    }

    /**
     * Slcount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slcount(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3LCOUNT, keyspace, uid, serviceCode, key);
    }

    /**
     * Slexists.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slexists(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3LEXISTS, keyspace, uid, serviceCode, key);
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
    public void slexists(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                         final byte[] value) {
        sendCommand(Command.S3LEXISTS, keyspace, uid, serviceCode, key, value);
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
    public void slmexpire(final byte[] keyspace, final byte[] uid, long expireMillis, final byte[] serviceCode,
                          final byte[]... keys) {
        byte[][] params = toByteArrays(keyspace, uid, serviceCode, expireMillis, keys);
        sendCommand(Command.S3LMEXPIRE, params);
    }

    /**
     * Slexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param expireMillis the expire millis
     */
    public void slexpire(final byte[] keyspace, final byte[] uid, long expireMillis) {
        sendCommand(Command.S3EXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis));
    }

    /**
     * Slexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param expireMillis the expire millis
     */
    public void slexpire(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, long expireMillis) {
        sendCommand(Command.S3LEXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis), serviceCode);
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
    public void slexpire(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                         long expireMillis) {
        sendCommand(Command.S3LEXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis), serviceCode, key);
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
    public void slexpire(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                         final byte[] value, final long expireMillis) {
        sendCommand(Command.S3LEXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis), serviceCode, key, value);
    }

    /**
     * Slttl.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void slttl(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3LTTL, keyspace, uid, serviceCode, key);
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
    public void slttl(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final byte[] value) {
        sendCommand(Command.S3LTTL, keyspace, uid, serviceCode, key, value);
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
    public void slset(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final long expireMillis, final byte[]... values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, expireMillis, values);
        sendCommand(Command.S3LSET, params);
    }

    /**
     * Slreplace.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param oldValue     the old value
     * @param newValue     the new value
     */
    public void slreplace(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                          final long expireMillis, final byte[] oldValue, final byte[] newValue) {
        sendCommand(Command.S3LREPLACE, keyspace, uid, serviceCode, key, oldValue, newValue,
                RedisProtocol.toByteArray(expireMillis));
    }

    /**
     * Slvals
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void slvals(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3LVALS, keyspace, uid, serviceCode);
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
    public void ssget(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3SGET, keyspace, uid, serviceCode, key);
    }

    /**
     * Ssmget.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssmget(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[]... key) {
        byte[][] params = toByteArrays(keyspace, uid, serviceCode, key);
        sendCommand(Command.S3SMGET, params);
    }

    /**
     * Sskeys.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void sskeys(final byte[] keyspace, final byte[] uid) {
        sendCommand(Command.S3KEYS, keyspace, uid);
    }

    /**
     * Sskeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void sskeys(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3SKEYS, keyspace, uid, serviceCode);
    }

    /**
     * Sskeys.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void sskeys(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3SKEYS, keyspace, uid, serviceCode, key);
    }

    /**
     * Ssadd.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param key          the key
     * @param expireMillis the expire millis
     * @param value        the value
     */
    public void ssadd(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final long expireMillis, final byte[] value) {
        sendCommand(Command.S3SADD, keyspace, uid, serviceCode, key, value, RedisProtocol.toByteArray(expireMillis));
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
    public void ssadd(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final long expireMillis, final byte[][] values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, expireMillis, values);
        sendCommand(Command.S3SADD, params);
    }

    public void ssaddAt(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                        final long unixTime, final byte[][] values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, unixTime, values);
        sendCommand(Command.S3SADDAT, params);
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
    public void ssset(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final long expireMillis, final byte[][] values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, expireMillis, values);
        sendCommand(Command.S3SSET, params);
    }

    /**
     * Ssdel.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void ssdel(final byte[] keyspace, final byte[] uid) {
        sendCommand(Command.S3REM, keyspace, uid);
    }

    /**
     * Ssrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void ssrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3SREM, keyspace, uid, serviceCode);
    }

    /**
     * Ssrem.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3SREM, keyspace, uid, serviceCode, key);
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
    public void ssrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final byte[] value) {
        sendCommand(Command.S3SREM, keyspace, uid, serviceCode, key, value);
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
    public void ssrem(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final byte[][] values) {
        final byte[][] params = toByteArrays(keyspace, uid, serviceCode, key, values);
        sendCommand(Command.S3SREM, params);
    }

    /**
     * Sscount.
     *
     * @param keyspace the keyspace
     * @param uid      the uid
     */
    public void sscount(final byte[] keyspace, final byte[] uid) {
        sendCommand(Command.S3COUNT, keyspace, uid);
    }

    /**
     * Sscount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void sscount(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3SCOUNT, keyspace, uid, serviceCode);
    }

    /**
     * Sscount.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void sscount(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3SCOUNT, keyspace, uid, serviceCode, key);
    }

    /**
     * Ssexists.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssexists(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3SEXISTS, keyspace, uid, serviceCode, key);
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
    public void ssexists(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                         final byte[] value) {
        sendCommand(Command.S3SEXISTS, keyspace, uid, serviceCode, key, value);
    }

    /**
     * Ssexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param expireMillis the expire millis
     */
    public void ssexpire(final byte[] keyspace, final byte[] uid, long expireMillis) {
        sendCommand(Command.S3EXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis));
    }

    /**
     * Ssexpire.
     *
     * @param keyspace     the keyspace
     * @param uid          the uid
     * @param serviceCode  the service code
     * @param expireMillis the expire millis
     */
    public void ssexpire(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, long expireMillis) {
        sendCommand(Command.S3SEXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis), serviceCode);
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
    public void ssexpire(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                         long expireMillis) {
        sendCommand(Command.S3SEXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis), serviceCode, key);
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
    public void ssexpire(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                         final byte[] value, final long expireMillis) {
        sendCommand(Command.S3SEXPIRE, keyspace, uid, RedisProtocol.toByteArray(expireMillis), serviceCode, key, value);
    }

    /**
     * Ssttl.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     * @param key         the key
     */
    public void ssttl(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key) {
        sendCommand(Command.S3STTL, keyspace, uid, serviceCode, key);
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
    public void ssttl(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
                      final byte[] value) {
        sendCommand(Command.S3STTL, keyspace, uid, serviceCode, key, value);
    }

    /**
     * Ssvals.
     *
     * @param keyspace    the keyspace
     * @param uid         the uid
     * @param serviceCode the service code
     */
    public void ssvals(final byte[] keyspace, final byte[] uid, final byte[] serviceCode) {
        sendCommand(Command.S3SVALS, keyspace, uid, serviceCode);
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
    private byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode,
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
    private byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
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
    private byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[] key,
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
    private byte[][] toByteArrays(final byte[] keyspace, final byte[] uid, final byte[] serviceCode, final byte[][] key) {
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
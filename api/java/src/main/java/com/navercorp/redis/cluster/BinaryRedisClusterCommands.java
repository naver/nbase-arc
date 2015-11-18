/*
 * @(#)BinaryRedisClusterCommands.java 2013. 6. 21
 * 
 * Copyright 2013 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.navercorp.redis.cluster;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Tuple;

/**
 * The Interface BinaryRedisClusterCommands.
 *
 * @author jaehong.kim
 */
public interface BinaryRedisClusterCommands {

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Keys

    /**
     * Remove the specified keys. If a given key does not exist no operation is
     * performed for this key. The command returns the number of keys removed.
     * <p>
     * Time complexity: O(1)
     *
     * @param keys the keys
     * @return Integer reply, specifically: an integer greater than 0 if one or
     * more keys were removed 0 if none of the specified key existed
     */
    Long del(byte[]... key);

    /**
     * Test if the specified key exists. The command returns "1" if the key
     * exists, otherwise "0" is returned. Note that even keys set with an empty
     * string as value will return "1".
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return Integer reply, "1" if the key exists, otherwise "0"
     */
    Boolean exists(byte[] key);

    /**
     * Set a timeout on the specified key. After the timeout the key will be
     * automatically deleted by the server. A key with an associated timeout is
     * said to be volatile in Redis terminology.
     * <p>
     * Voltile keys are stored on disk like the other keys, the timeout is
     * persistent too like all the other aspects of the dataset. Saving a
     * dataset containing expires and stopping the server does not stop the flow
     * of time as Redis stores on disk the time when the key will no longer be
     * available as Unix time, and not the remaining seconds.
     * <p>
     * Since Redis 2.1.3 you can update the value of the timeout of a key
     * already having an expire set. It is also possible to undo the expire at
     * all turning the key into a normal key using the {@link #persist(byte[])
     * PERSIST} command.
     * <p>
     * Time complexity: O(1)
     *
     * @param key     the key
     * @param seconds the seconds
     * @return Integer reply, specifically: 1: the timeout was set. 0: the
     * timeout was not set since the key already has an associated
     * timeout (this may happen only in Redis versions < 2.1.3, Redis >=
     * 2.1.3 will happily update the timeout), or the key does not
     * exist.
     * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
     */
    Long expire(byte[] key, int seconds);

    /**
     * EXPIREAT works exctly like {@link #expire(byte[], int) EXPIRE} but
     * instead to get the number of seconds representing the Time To Live of the
     * key as a second argument (that is a relative way of specifing the TTL),
     * it takes an absolute one in the form of a UNIX timestamp (Number of
     * seconds elapsed since 1 Gen 1970).
     * <p>
     * EXPIREAT was introduced in order to implement the Append Only File
     * persistence mode so that EXPIRE commands are automatically translated
     * into EXPIREAT commands for the append only file. Of course EXPIREAT can
     * also used by programmers that need a way to simply specify that a given
     * key should expire at a given time in the future.
     * <p>
     * Since Redis 2.1.3 you can update the value of the timeout of a key
     * already having an expire set. It is also possible to undo the expire at
     * all turning the key into a normal key using the {@link #persist(byte[])
     * PERSIST} command.
     * <p>
     * Time complexity: O(1)
     *
     * @param key      the key
     * @param unixTime the unix time
     * @return Integer reply, specifically: 1: the timeout was set. 0: the
     * timeout was not set since the key already has an associated
     * timeout (this may happen only in Redis versions < 2.1.3, Redis >=
     * 2.1.3 will happily update the timeout), or the key does not
     * exist.
     * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
     */
    Long expireAt(byte[] key, long unixTime);

    /**
     * This command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param milliseconds
     * @return Integer reply, specifically:
     * 1 if the timeout was set.
     * 0 if key does not exist or the timeout could not be set.
     */
    Long pexpire(byte[] key, long milliseconds);

    /**
     * PEXPIREAT has the same effect and semantic as EXPIREAT, but the Unix time at which the key will expire is specified in milliseconds instead of seconds.
     * Return value
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param millisecondsTimestamp
     * @return Integer reply, specifically:
     * 1 if the timeout was set.
     * 0 if key does not exist or the timeout could not be set (see: EXPIRE).
     */
    Long pexpireAt(byte[] key, long millisecondsTimestamp);

    /**
     * OBJECT REFCOUNT <key> returns the number of references of the value associated with the specified key.
     * This command is mainly useful for debugging.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return the long
     */
    Long objectRefcount(byte[] key);

    /**
     * OBJECT ENCODING <key> returns the kind of internal representation used in order to store the value associated with a key.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return the byte[]
     */
    byte[] objectEncoding(byte[] key);

    /**
     * OBJECT IDLETIME <key> returns the number of seconds since the object stored at the specified key is idle (not requested by read or write operations).
     * While the value is returned in seconds the actual resolution of this timer is 10 seconds, but may vary in future implementations.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return the long
     */
    Long objectIdletime(byte[] key);


    /**
     * The TTL command returns the remaining time to live in seconds of a key
     * that has an {@link #expire(byte[], int) EXPIRE} set. This introspection
     * capability allows a Redis client to check how many seconds a given key
     * will continue to be part of the dataset.
     *
     * @param key the key
     * @return Integer reply, returns the remaining time to live in seconds of a
     * key that has an EXPIRE. If the Key does not exists or does not
     * have an associated expire, -1 is returned.
     */
    Long ttl(byte[] key);

    /**
     * Like TTL this command returns the remaining time to live of a key that has an expire set, with the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds.
     * In Redis 2.6 or older the command returns -1 if the key does not exist or if the key exist but has no associated expire.
     * Starting with Redis 2.8 the return value in case of error changed:
     * The command returns -2 if the key does not exist.
     * The command returns -1 if the key exists but has no associated expire.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @return Integer reply: TTL in milliseconds, or a negative value in order to signal an error (see the description above).
     */
    Long pttl(byte[] key);

    /**
     * Return the type of the value stored at key in form of a string. The type
     * can be one of "none", "string", "list", "set". "none" is returned if the
     * key does not exist.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return Status code reply, specifically: "none" if the key does not exist
     * "string" if the key contains a String value "list" if the key
     * contains a List value "set" if the key contains a Set value
     * "zset" if the key contains a Sorted Set value "hash" if the key
     * contains a Hash value
     */
    String type(byte[] key);

    /**
     * Remove the existing timeout on key, turning the key from volatile (a key with an expire set) to persistent (a key that will never expire as no timeout is associated).
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @return Integer reply, specifically:
     * 1 if the timeout was removed.
     * 0 if key does not exist or does not have an associated timeout.
     */
    Long persist(byte[] key);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Strings

    /**
     * If the key already exists and is a string, this command appends the
     * provided value at the end of the string. If the key does not exist it is
     * created and set as an empty string, so APPEND will be very similar to SET
     * in this special case.
     * <p>
     * Time complexity: O(1). The amortized time complexity is O(1) assuming the
     * appended value is small and the already present value is of any size,
     * since the dynamic string library used by Redis will double the free space
     * available on every reallocation.
     *
     * @param key   the key
     * @param value the value
     * @return Integer reply, specifically the total length of the string after
     * the append operation.
     */
    Long append(byte[] key, byte[] value);

    /**
     * Decrement the number stored at key by one. If the key does not exist or
     * contains a value of a wrong type, set the key to the value of "0" before
     * to perform the decrement operation.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are
     * not "integer" types. Simply the string stored at the key is parsed as a
     * base 10 64 bit signed integer, incremented, and then converted back as a
     * string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return Integer reply, this commands will reply with the new value of key
     * after the increment.
     * @see #incr(byte[])
     * @see #incrBy(byte[], long)
     * @see #decrBy(byte[], long)
     */
    Long decr(byte[] key);

    /**
     * IDECRBY work just like {@link #decr(String) INCR} but instead to
     * decrement by 1 the decrement is integer.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are
     * not "integer" types. Simply the string stored at the key is parsed as a
     * base 10 64 bit signed integer, incremented, and then converted back as a
     * string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key     the key
     * @param integer the integer
     * @return Integer reply, this commands will reply with the new value of key
     * after the increment.
     * @see #incr(byte[])
     * @see #decr(byte[])
     * @see #incrBy(byte[], long)
     */
    Long decrBy(byte[] key, long integer);

    /**
     * Get the value of the specified key. If the key does not exist the special
     * value 'nil' is returned. If the value stored at key is not a string an
     * error is returned because GET can only handle string values.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return Bulk reply
     */
    byte[] get(byte[] key);

    /**
     * Returns the bit value at offset in the string value stored at key.
     *
     * @param key    the key
     * @param offset the offset
     * @return the bit
     */
    Boolean getbit(byte[] key, long offset);

    /**
     * Returns the substring of the string value stored at key, determined by the offsets start and end (both are inclusive).
     * Negative offsets can be used in order to provide an offset starting from the end of the string.
     * So -1 means the last character, -2 the penultimate and so forth.
     * <p>
     * The function handles out of range requests by limiting the resulting range to the actual length of the string.
     * <p>
     * Time complexity: O(N) where N is the length of the returned string.
     * The complexity is ultimately determined by the returned length,
     * but because creating a substring from an existing string is very cheap,
     * it can be considered O(1) for small strings
     *
     * @param key         the key
     * @param startOffset the start offset
     * @param endOffset   the end offset
     * @return Bulk reply
     */
    byte[] getrange(byte[] key, long startOffset, long endOffset);

    /**
     * Return a subset of the string from offset start to offset end (both
     * offsets are inclusive). Negative offsets can be used in order to provide
     * an offset starting from the end of the string. So -1 means the last char,
     * -2 the penultimate and so forth.
     * <p>
     * The function handles out of range requests without raising an error, but
     * just limiting the resulting range to the actual length of the string.
     * <p>
     * Time complexity: O(start+n) (with start being the start index and n the
     * total length of the requested range). Note that the lookup part of this
     * command is O(1) so for small strings this is actually an O(1) command.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return Bulk reply
     */
    byte[] substr(byte[] key, int start, int end);

    /**
     * GETSET is an atomic set this value and return the old value command. Set
     * key to the string value and return the old value stored at key. The
     * string can't be longer than 1073741824 bytes (1 GB).
     * <p>
     * Time complexity: O(1)
     *
     * @param key   the key
     * @param value the value
     * @return Bulk reply
     */
    byte[] getSet(byte[] key, byte[] value);

    /**
     * Increment the number stored at key by one. If the key does not exist or
     * contains a value of a wrong type, set the key to the value of "0" before
     * to perform the increment operation.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are
     * not "integer" types. Simply the string stored at the key is parsed as a
     * base 10 64 bit signed integer, incremented, and then converted back as a
     * string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return Integer reply, this commands will reply with the new value of key
     * after the increment.
     * @see #incrBy(byte[], long)
     * @see #decr(byte[])
     * @see #decrBy(byte[], long)
     */
    Long incr(byte[] key);

    /**
     * INCRBY work just like {@link #incr(byte[]) INCR} but instead to increment
     * by 1 the increment is integer.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are
     * not "integer" types. Simply the string stored at the key is parsed as a
     * base 10 64 bit signed integer, incremented, and then converted back as a
     * string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key     the key
     * @param integer the integer
     * @return Integer reply, this commands will reply with the new value of key
     * after the increment.
     * @see #incr(byte[])
     * @see #decr(byte[])
     * @see #decrBy(byte[], long)
     */
    Long incrBy(byte[] key, long integer);

    /**
     * Increment the string representing a floating point number stored at key by the specified increment. If the key does not exist, it is set to 0 before performing the operation. An error is returned if one of the following conditions occur:
     * The key contains a value of the wrong type (not a string).
     * The current key content or the specified increment are not parsable as a double precision floating point number.
     * If the command is successful the new incremented value is stored as the new value of the key (replacing the old one), and returned to the caller as a string.
     * Both the value already contained in the string key and the increment argument can be optionally provided in exponential notation, however the value computed after the increment is stored consistently in the same format, that is, an integer number followed (if needed) by a dot, and a variable number of digits representing the decimal part of the number. Trailing zeroes are always removed.
     * The precision of the output is fixed at 17 digits after the decimal point regardless of the actual internal precision of the computation.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param increment
     * @return
     */
    Double incrByFloat(byte[] key, double increment);

    /**
     * Set the string value as value of the key. The string can't be longer than
     * 1073741824 bytes (1 GB).
     * <p>
     * Time complexity: O(1)
     *
     * @param key   the key
     * @param value the value
     * @return Status code reply
     */
    String set(byte[] key, byte[] value);

    /**
     * Sets or clears the bit at offset in the string value stored at key.
     *
     * @param key    the key
     * @param offset the offset
     * @param value  the value
     * @return the boolean
     */
    Boolean setbit(byte[] key, long offset, byte[] value);

    /**
     * The command is exactly equivalent to the following group of commands:.
     *
     * @param key     the key
     * @param seconds the seconds
     * @param value   the value
     * @return Status code reply
     * {@link #set(byte[], byte[]) SET} + {@link #expire(byte[], int) EXPIRE}.
     * The operation is atomic.
     * <p>
     * Time complexity: O(1)
     */
    String setex(byte[] key, int seconds, byte[] value);

    /**
     * SETNX works exactly like {@link #set(byte[], byte[]) SET} with the only
     * difference that if the key already exists no operation is performed.
     * SETNX actually means "SET if Not eXists".
     * <p>
     * Time complexity: O(1)
     *
     * @param key   the key
     * @param value the value
     * @return Integer reply, specifically: 1 if the key was set 0 if the key
     * was not set
     */
    Long setnx(byte[] key, byte[] value);

    /**
     * Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value.
     * If the offset is larger than the current length of the string at key, the string is padded with zero-bytes to make offset fit.
     * Non-existing keys are considered as empty strings, so this command will make sure it holds a string large enough to be able to set value at offset.
     * Note that the maximum offset that you can set is 229 -1 (536870911), as Redis Strings are limited to 512 megabytes. If you need to grow beyond this size, you can use multiple keys
     * <p>
     * Time complexity: O(1) not counting the time taken to copy the new string in place.
     * Usually, this string is very small so the amortized complexity is O(1).
     * Otherwise, complexity is O(M) with M being the length of the value argument.
     *
     * @param key    the key
     * @param offset the offset
     * @param value  the value
     * @return Integer reply, the length of the string after it was modified by the command.
     */
    Long setrange(byte[] key, long offset, byte[] value);

    /**
     * PSETEX works exactly like SETEX with the sole difference that the expire time is specified in milliseconds instead of seconds.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param milliseconds
     * @param value
     * @return
     */
    String psetex(byte[] key, long milliseconds, byte[] value);

    /**
     * Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return Integer reply, the length of the string at key, or 0 when key does not exist.
     */
    Long strlen(byte[] key);

    /**
     * Get the values of all the specified keys. If one or more keys dont exist
     * or is not of type String, a 'nil' value is returned instead of the value
     * of the specified key, but the operation never fails.
     * <p>
     * Time complexity: O(1) for every key
     *
     * @param keys
     * @return Multi bulk reply. Error Code will return if parts of keys fail in cluster due to server down or network problem.
     */
    List<byte[]> mget(final byte[]... keys);

    /**
     * Set the the respective keys to the respective values. MSET will replace
     * old values with new values, while {@link #msetnx(String...) MSETNX} will
     * not perform any operation at all even if just a single key already
     * exists.
     * <p>
     * Because of this semantic MSETNX can be used in order to set different
     * keys representing different fields of an unique logic object in a way
     * that ensures that either all the fields or none at all are set.
     * <p>
     * Both MSET and MSETNX are atomic operations. This means that for instance
     * if the keys A and B are modified, another client talking to Redis can
     * either see the changes to both A and B at once, or no modification at
     * all.
     *
     * @param keysvalues
     * @return Status code reply. Error Code will return if parts of keys fail in cluster due to server down or network problem.
     * @see #msetnx(String...)
     */
    String mset(byte[]... keysvalues);

    /**
     * Count the number of set bits (population counting) in a string.
     * By default all the bytes contained in the string are examined. It is possible to specify the counting operation only in an interval passing the additional arguments start and end.
     * Like for the GETRANGE command start and end can contain negative values in order to index bytes starting from the end of the string, where -1 is the last byte, -2 is the penultimate, and so forth.
     * Non-existent keys are treated as empty strings, so the command will return zero.
     * <p>
     * Time complexity: O(N)
     *
     * @param key
     * @return
     */
    Long bitcount(byte[] key);

    /**
     * Count the number of set bits (population counting) in a string.
     * By default all the bytes contained in the string are examined. It is possible to specify the counting operation only in an interval passing the additional arguments start and end.
     * Like for the GETRANGE command start and end can contain negative values in order to index bytes starting from the end of the string, where -1 is the last byte, -2 is the penultimate, and so forth.
     * Non-existent keys are treated as empty strings, so the command will return zero.
     * <p>
     * Time complexity: O(N)
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    Long bitcount(byte[] key, long start, long end);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Hashes

    /**
     * Remove the specified field from an hash stored at key.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key    the key
     * @param fields the fields
     * @return If the field was present in the hash it is deleted and 1 is
     * returned, otherwise 0 is returned and no operation is performed.
     */
    Long hdel(byte[] key, byte[]... fields);

    /**
     * Test for existence of a specified field in a hash.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key   the key
     * @param field the field
     * @return Return 1 if the hash stored at key contains the specified field.
     * Return 0 if the key is not found or the field is not present.
     */
    Boolean hexists(byte[] key, byte[] field);

    /**
     * If key holds a hash, retrieve the value associated to the specified
     * field.
     * <p>
     * If the field is not found or the key does not exist, a special 'nil'
     * value is returned.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key   the key
     * @param field the field
     * @return Bulk reply
     */
    byte[] hget(byte[] key, byte[] field);

    /**
     * Return all the fields and associated values in a hash.
     * <p>
     * <b>Time complexity:</b> O(N), where N is the total number of entries
     *
     * @param key the key
     * @return All the fields and values contained into a hash.
     */
    Map<byte[], byte[]> hgetAll(byte[] key);

    /**
     * Increment the number stored at field in the hash at key by value. If key
     * does not exist, a new key holding a hash is created. If field does not
     * exist or holds a string, the value is set to 0 before applying the
     * operation. Since the value argument is signed you can use this command to
     * perform both increments and decrements.
     * <p>
     * The range of values supported by HINCRBY is limited to 64 bit signed
     * integers.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     * @return Integer reply The new value at field after the increment
     * operation.
     */
    Long hincrBy(byte[] key, byte[] field, long value);

    /**
     * Increment the specified field of an hash stored at key, and representing a floating point number, by the specified increment. If the field does not exist, it is set to 0 before performing the operation. An error is returned if one of the following conditions occur:
     * The field contains a value of the wrong type (not a string).
     * The current field content or the specified increment are not parsable as a double precision floating point number.
     * The exact behavior of this command is identical to the one of the INCRBYFLOAT command, please refer to the documentation of INCRBYFLOAT for further information.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param field
     * @param increment
     * @return
     */
    Double hincrByFloat(byte[] key, byte[] field, double increment);

    /**
     * Return all the fields in a hash.
     * <p>
     * <b>Time complexity:</b> O(N), where N is the total number of entries
     *
     * @param key the key
     * @return All the fields names contained into a hash.
     */
    Set<byte[]> hkeys(byte[] key);

    /**
     * Return the number of items in a hash.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key the key
     * @return The number of entries (fields) contained in the hash stored at
     * key. If the specified key does not exist, 0 is returned assuming
     * an empty hash.
     */
    Long hlen(byte[] key);

    /**
     * Retrieve the values associated to the specified fields.
     * <p>
     * If some of the specified fields do not exist, nil values are returned.
     * Non existing keys are considered like empty hashes.
     * <p>
     * <b>Time complexity:</b> O(N) (with N being the number of fields)
     *
     * @param key    the key
     * @param fields the fields
     * @return Multi Bulk Reply specifically a list of all the values associated
     * with the specified fields, in the same order of the request.
     */
    List<byte[]> hmget(byte[] key, byte[]... fields);

    /**
     * Set the respective fields to the respective values. HMSET replaces old
     * values with new values.
     * <p>
     * If key does not exist, a new key holding a hash is created.
     * <p>
     * <b>Time complexity:</b> O(N) (with N being the number of fields)
     *
     * @param key  the key
     * @param hash the hash
     * @return Always OK because HMSET can't fail
     */
    String hmset(byte[] key, Map<byte[], byte[]> hash);

    /**
     * Set the specified hash field to the specified value.
     * <p>
     * If key does not exist, a new key holding a hash is created.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     * @return If the field already exists, and the HSET just produced an update
     * of the value, 0 is returned, otherwise if a new field is created
     * 1 is returned.
     */
    Long hset(byte[] key, byte[] field, byte[] value);

    /**
     * Set the specified hash field to the specified value if the field not
     * exists. <b>Time complexity:</b> O(1)
     *
     * @param key   the key
     * @param field the field
     * @param value the value
     * @return If the field already exists, 0 is returned, otherwise if a new
     * field is created 1 is returned.
     */
    Long hsetnx(byte[] key, byte[] field, byte[] value);

    /**
     * Return all the values in a hash.
     * <p>
     * <b>Time complexity:</b> O(N), where N is the total number of entries
     *
     * @param key the key
     * @return All the fields values contained into a hash.
     */
    List<byte[]> hvals(byte[] key);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Lists

    /**
     * Return the specified element of the list stored at the specified key. 0
     * is the first element, 1 the second and so on. Negative indexes are
     * supported, for example -1 is the last element, -2 the penultimate and so
     * on.
     * <p>
     * If the value stored at key is not of list type an error is returned. If
     * the index is out of range a 'nil' reply is returned.
     * <p>
     * Note that even if the average time complexity is O(n) asking for the
     * first or the last element of the list is O(1).
     * <p>
     * Time complexity: O(n) (with n being the length of the list)
     *
     * @param key   the key
     * @param index the index
     * @return Bulk reply, specifically the requested element
     */
    byte[] lindex(byte[] key, long index);

    /**
     * Inserts value in the list stored at key either before or after the reference value pivot.
     * When key does not exist, it is considered an empty list and no operation is performed.
     * An error is returned when key exists but does not hold a list value.
     *
     * @param key   the key
     * @param where the where
     * @param pivot the pivot
     * @param value the value
     * @return the long
     */
    Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value);

    /**
     * Return the length of the list stored at the specified key. If the key
     * does not exist zero is returned (the same behaviour as for empty lists).
     * If the value stored at key is not a list an error is returned.
     * <p>
     * Time complexity: O(1)
     *
     * @param key the key
     * @return The length of the list.
     */
    Long llen(byte[] key);

    /**
     * Atomically return and remove the first (LPOP) or last (RPOP) element of
     * the list. For example if the list contains the elements "a","b","c" LPOP
     * will return "a" and the list will become "b","c".
     * <p>
     * If the key does not exist or the list is already empty the special value
     * 'nil' is returned.
     *
     * @param key the key
     * @return Bulk reply
     * @see #rpop(byte[])
     */
    byte[] lpop(byte[] key);

    /**
     * Add the string value to the head (LPUSH) or tail (RPUSH) of the list
     * stored at key. If the key does not exist an empty list is created just
     * before the append operation. If the key exists but is not a List an error
     * is returned.
     * <p>
     * Time complexity: O(1)
     *
     * @param key     the key
     * @param strings the strings
     * @return Integer reply, specifically, the number of elements inside the
     * list after the push operation.
     * @see BinaryJedis#rpush(byte[], byte[]...)
     */
    Long lpush(byte[] key, byte[]... strings);

    /**
     * Inserts value at the head of the list stored at key, only if key already exists and holds a list.
     * In contrary to LPUSH, no operation will be performed when key does not yet exist.
     * <p>
     * Time complexity: O(1)
     *
     * @param key    the key
     * @param string the string
     * @return integer reply, the length of the list after the push operation.
     */
    Long lpushx(byte[] key, byte[] string);

    /**
     * Return the specified elements of the list stored at the specified key.
     * Start and end are zero-based indexes. 0 is the first element of the list
     * (the list head), 1 the next element and so on.
     * <p>
     * For example LRANGE foobar 0 2 will return the first three elements of the
     * list.
     * <p>
     * start and end can also be negative numbers indicating offsets from the
     * end of the list. For example -1 is the last element of the list, -2 the
     * penultimate element and so on.
     * <p>
     * <b>Consistency with range functions in various programming languages</b>
     * <p>
     * Note that if you have a list of numbers from 0 to 100, LRANGE 0 10 will
     * return 11 elements, that is, rightmost item is included. This may or may
     * not be consistent with behavior of range-related functions in your
     * programming language of choice (think Ruby's Range.new, Array#slice or
     * Python's range() function).
     * <p>
     * LRANGE behavior is consistent with one of Tcl.
     * <p>
     * <b>Out-of-range indexes</b>
     * <p>
     * Indexes out of range will not produce an error: if start is over the end
     * of the list, or start > end, an empty list is returned. If end is over
     * the end of the list Redis will threat it just like the last element of
     * the list.
     * <p>
     * Time complexity: O(start+n) (with n being the length of the range and
     * start being the start offset)
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return Multi bulk reply, specifically a list of elements in the
     * specified range.
     */
    List<byte[]> lrange(byte[] key, long start, long end);

    /**
     * Remove the first count occurrences of the value element from the list. If
     * count is zero all the elements are removed. If count is negative elements
     * are removed from tail to head, instead to go from head to tail that is
     * the normal behaviour. So for example LREM with count -2 and hello as
     * value to remove against the list (a,b,c,hello,x,hello,hello) will have
     * the list (a,b,c,hello,x). The number of removed elements is returned as
     * an integer, see below for more information about the returned value. Note
     * that non existing keys are considered like empty lists by LREM, so LREM
     * against non existing keys will always return 0.
     * <p>
     * Time complexity: O(N) (with N being the length of the list)
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     * @return Integer Reply, specifically: The number of removed elements if
     * the operation succeeded
     */
    Long lrem(byte[] key, long count, byte[] value);

    /**
     * Set a new value as the element at index position of the List at key.
     * <p>
     * Out of range indexes will generate an error.
     * <p>
     * Similarly to other list commands accepting indexes, the index can be
     * negative to access elements starting from the end of the list. So -1 is
     * the last element, -2 is the penultimate, and so forth.
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(N) (with N being the length of the list), setting the first or last
     * elements of the list is O(1).
     *
     * @param key   the key
     * @param index the index
     * @param value the value
     * @return Status code reply
     * @see #lindex(byte[], int)
     */
    String lset(byte[] key, long index, byte[] value);

    /**
     * Trim an existing list so that it will contain only the specified range of
     * elements specified. Start and end are zero-based indexes. 0 is the first
     * element of the list (the list head), 1 the next element and so on.
     * <p>
     * For example LTRIM foobar 0 2 will modify the list stored at foobar key so
     * that only the first three elements of the list will remain.
     * <p>
     * start and end can also be negative numbers indicating offsets from the
     * end of the list. For example -1 is the last element of the list, -2 the
     * penultimate element and so on.
     * <p>
     * Indexes out of range will not produce an error: if start is over the end
     * of the list, or start > end, an empty list is left as value. If end over
     * the end of the list Redis will threat it just like the last element of
     * the list.
     * <p>
     * Hint: the obvious use of LTRIM is together with LPUSH/RPUSH. For example:
     * <p>
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return Status code reply
     * {@code lpush("mylist", "someelement"); ltrim("mylist", 0, 99); * }
     * <p>
     * The above two commands will push elements in the list taking care that
     * the list will not grow without limits. This is very useful when using
     * Redis to store logs for example. It is important to note that when used
     * in this way LTRIM is an O(1) operation because in the average case just
     * one element is removed from the tail of the list.
     * <p>
     * Time complexity: O(n) (with n being len of list - len of range)
     */
    String ltrim(byte[] key, long start, long end);

    /**
     * Atomically return and remove the first (LPOP) or last (RPOP) element of
     * the list. For example if the list contains the elements "a","b","c" LPOP
     * will return "a" and the list will become "b","c".
     * <p>
     * If the key does not exist or the list is already empty the special value
     * 'nil' is returned.
     *
     * @param key the key
     * @return Bulk reply
     * @see #lpop(byte[])
     */
    byte[] rpop(byte[] key);

    /**
     * Add the string value to the head (LPUSH) or tail (RPUSH) of the list
     * stored at key. If the key does not exist an empty list is created just
     * before the append operation. If the key exists but is not a List an error
     * is returned.
     * <p>
     * Time complexity: O(1)
     *
     * @param key     the key
     * @param strings the strings
     * @return Integer reply, specifically, the number of elements inside the
     * list after the push operation.
     * @see BinaryJedis#rpush(byte[], byte[]...)
     */
    Long rpush(byte[] key, byte[]... strings);

    /**
     * Inserts value at the tail of the list stored at key, only if key already exists and holds a list.
     * In contrary to RPUSH, no operation will be performed when key does not yet exist.
     * <p>
     * Time complexity: O(1)
     *
     * @param key    the key
     * @param string the string
     * @return Integer reply, the length of the list after the push operation.
     */
    Long rpushx(byte[] key, byte[] string);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sets

    /**
     * Add the specified member to the set value stored at key. If member is
     * already a member of the set no operation is performed. If key does not
     * exist a new set with the specified member as sole member is created. If
     * the key exists but does not hold a set value an error is returned.
     * <p>
     * Time complexity O(1)
     *
     * @param key     the key
     * @param members the members
     * @return Integer reply, specifically: 1 if the new element was added 0 if
     * the element was already a member of the set
     */
    Long sadd(byte[] key, byte[]... members);

    /**
     * Return the set cardinality (number of elements). If the key does not
     * exist 0 is returned, like for empty sets.
     *
     * @param key the key
     * @return Integer reply, specifically: the cardinality (number of elements)
     * of the set as an integer.
     */
    Long scard(byte[] key);

    /**
     * Return 1 if member is a member of the set stored at key, otherwise 0 is
     * returned.
     * <p>
     * Time complexity O(1)
     *
     * @param key    the key
     * @param member the member
     * @return Integer reply, specifically: 1 if the element is a member of the
     * set 0 if the element is not a member of the set OR if the key
     * does not exist
     */
    Boolean sismember(byte[] key, byte[] member);

    /**
     * Return all the members (elements) of the set value stored at key. This is
     * just syntax glue for {@link #sinter(String...) SINTER}.
     * <p>
     * Time complexity O(N)
     *
     * @param key the key
     * @return Multi bulk reply
     */
    Set<byte[]> smembers(byte[] key);

    /**
     * Return a random element from a Set, without removing the element. If the
     * Set is empty or the key does not exist, a nil object is returned.
     * <p>
     * The SPOP command does a similar work but the returned element is popped
     * (removed) from the Set.
     * <p>
     * Time complexity O(1)
     *
     * @param key the key
     * @return Bulk reply
     */
    byte[] srandmember(byte[] key);

    /**
     * Return a random element from a Set, without removing the element. If the
     * Set is empty or the key does not exist, a nil object is returned.
     * <p>
     * The SPOP command does a similar work but the returned element is popped
     * (removed) from the Set.
     * <p>
     * Time complexity O(1)
     *
     * @param key the key
     * @return Bulk reply
     */
    List<byte[]> srandmember(byte[] key, int count);

    /**
     * Remove the specified member from the set value stored at key. If member
     * was not a member of the set no operation is performed. If key does not
     * hold a set value an error is returned.
     * <p>
     * Time complexity O(1)
     *
     * @param key    the key
     * @param member the member
     * @return Integer reply, specifically: 1 if the new element was removed 0
     * if the new element was not a member of the set
     */
    Long srem(byte[] key, byte[]... member);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sorted Sets

    /**
     * Add the specified member having the specifeid score to the sorted set
     * stored at key. If member is already a member of the sorted set the score
     * is updated, and the element reinserted in the right position to ensure
     * sorting. If key does not exist a new sorted set with the specified member
     * as sole member is crated. If the key exists but does not hold a sorted
     * set value an error is returned.
     * <p>
     * The score value can be the string representation of a double precision
     * floating point number.
     * <p>
     * Time complexity O(log(N)) with N being the number of elements in the
     * sorted set
     *
     * @param key    the key
     * @param score  the score
     * @param member the member
     * @return Integer reply, specifically: 1 if the new element was added 0 if
     * the element was already a member of the sorted set and the score
     * was updated
     */
    Long zadd(byte[] key, double score, byte[] member);

    /**
     * Zadd.
     *
     * @param key          the key
     * @param scoreMembers the score members
     * @return the long
     */
    Long zadd(byte[] key, Map<Double, byte[]> scoreMembers);

    /**
     * Return the sorted set cardinality (number of elements). If the key does
     * not exist 0 is returned, like for empty sorted sets.
     * <p>
     * Time complexity O(1)
     *
     * @param key the key
     * @return the cardinality (number of elements) of the set as an integer.
     */
    Long zcard(byte[] key);

    /**
     * Zcount.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     * @return the long
     */
    Long zcount(byte[] key, double min, double max);

    /**
     * Zcount.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     * @return the long
     */
    Long zcount(byte[] key, byte[] min, byte[] max);

    /**
     * If member already exists in the sorted set adds the increment to its
     * score and updates the position of the element in the sorted set
     * accordingly. If member does not already exist in the sorted set it is
     * added with increment as score (that is, like if the previous score was
     * virtually zero). If key does not exist a new sorted set with the
     * specified member as sole member is crated. If the key exists but does not
     * hold a sorted set value an error is returned.
     * <p>
     * The score value can be the string representation of a double precision
     * floating point number. It's possible to provide a negative value to
     * perform a decrement.
     * <p>
     * For an introduction to sorted sets check the Introduction to Redis data
     * types page.
     * <p>
     * Time complexity O(log(N)) with N being the number of elements in the
     * sorted set
     *
     * @param key    the key
     * @param score  the score
     * @param member the member
     * @return The new score
     */
    Double zincrby(byte[] key, double score, byte[] member);

    /**
     * Zrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return the sets the
     */
    Set<byte[]> zrange(byte[] key, long start, long end);

    /**
     * Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     * <p>
     * The elements having the same score are returned sorted lexicographically
     * as ASCII strings (this follows from a property of Redis sorted sets and
     * does not involve further computation).
     * <p>
     * Using the optional
     *
     * @param key    the key
     * @param min    the min
     * @param max    the max
     * @param offset the offset
     * @param count  the count
     * @return Multi bulk reply specifically a list of elements in the specified
     * score range.
     * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
     * possible to get only a range of the matching elements in an SQL-alike
     * way. Note that if offset is large the commands needs to traverse the list
     * for offset elements and this adds up to the O(M) figure.
     * <p>
     * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
     * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
     * of returning the actual elements in the specified interval, it just
     * returns the number of matching elements.
     * <p>
     * <b>Exclusive intervals and infinity</b>
     * <p>
     * min and max can be -inf and +inf, so that you are not required to know
     * what's the greatest or smallest element in order to take, for instance,
     * elements "up to a given value".
     * <p>
     * Also while the interval is for default closed (inclusive) it's possible
     * to specify open intervals prefixing the score with a "(" character, so
     * for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (1.3 5}
     * <p>
     * Will return all the values with score > 1.3 and <= 5, while for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (5 (10}
     * <p>
     * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and
     * M the number of elements returned by the command, so if M is constant
     * (for instance you always ask for the first ten elements with LIMIT) you
     * can consider it O(log(N))
     * @see #zrangeByScore(byte[], double, double)
     * @see #zrangeByScore(byte[], double, double, int, int)
     * @see #zrangeByScoreWithScores(byte[], double, double)
     * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
     * @see #zcount(byte[], double, double)
     */
    Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count);

    /**
     * Zrange by score.
     *
     * @param key    the key
     * @param min    the min
     * @param max    the max
     * @param offset the offset
     * @param count  the count
     * @return the sets the
     */
    Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count);

    /**
     * Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     * <p>
     * The elements having the same score are returned sorted lexicographically
     * as ASCII strings (this follows from a property of Redis sorted sets and
     * does not involve further computation).
     * <p>
     * Using the optional
     *
     * @param key the key
     * @param min the min
     * @param max the max
     * @return Multi bulk reply specifically a list of elements in the specified
     * score range.
     * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
     * possible to get only a range of the matching elements in an SQL-alike
     * way. Note that if offset is large the commands needs to traverse the list
     * for offset elements and this adds up to the O(M) figure.
     * <p>
     * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
     * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
     * of returning the actual elements in the specified interval, it just
     * returns the number of matching elements.
     * <p>
     * <b>Exclusive intervals and infinity</b>
     * <p>
     * min and max can be -inf and +inf, so that you are not required to know
     * what's the greatest or smallest element in order to take, for instance,
     * elements "up to a given value".
     * <p>
     * Also while the interval is for default closed (inclusive) it's possible
     * to specify open intervals prefixing the score with a "(" character, so
     * for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (1.3 5}
     * <p>
     * Will return all the values with score > 1.3 and <= 5, while for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (5 (10}
     * <p>
     * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and
     * M the number of elements returned by the command, so if M is constant
     * (for instance you always ask for the first ten elements with LIMIT) you
     * can consider it O(log(N))
     * @see #zrangeByScore(byte[], double, double)
     * @see #zrangeByScore(byte[], double, double, int, int)
     * @see #zrangeByScoreWithScores(byte[], double, double)
     * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
     * @see #zcount(byte[], double, double)
     */
    Set<byte[]> zrangeByScore(byte[] key, double min, double max);

    /**
     * Zrange by score.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     * @return the sets the
     */
    Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max);

    /**
     * Zrange with scores.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return the sets the
     */
    Set<Tuple> zrangeWithScores(byte[] key, long start, long end);

    /**
     * Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     * <p>
     * The elements having the same score are returned sorted lexicographically
     * as ASCII strings (this follows from a property of Redis sorted sets and
     * does not involve further computation).
     * <p>
     * Using the optional
     *
     * @param key the key
     * @param min the min
     * @param max the max
     * @return Multi bulk reply specifically a list of elements in the specified
     * score range.
     * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
     * possible to get only a range of the matching elements in an SQL-alike
     * way. Note that if offset is large the commands needs to traverse the list
     * for offset elements and this adds up to the O(M) figure.
     * <p>
     * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
     * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
     * of returning the actual elements in the specified interval, it just
     * returns the number of matching elements.
     * <p>
     * <b>Exclusive intervals and infinity</b>
     * <p>
     * min and max can be -inf and +inf, so that you are not required to know
     * what's the greatest or smallest element in order to take, for instance,
     * elements "up to a given value".
     * <p>
     * Also while the interval is for default closed (inclusive) it's possible
     * to specify open intervals prefixing the score with a "(" character, so
     * for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (1.3 5}
     * <p>
     * Will return all the values with score > 1.3 and <= 5, while for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (5 (10}
     * <p>
     * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and
     * M the number of elements returned by the command, so if M is constant
     * (for instance you always ask for the first ten elements with LIMIT) you
     * can consider it O(log(N))
     * @see #zrangeByScore(byte[], double, double)
     * @see #zrangeByScore(byte[], double, double, int, int)
     * @see #zrangeByScoreWithScores(byte[], double, double)
     * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
     * @see #zcount(byte[], double, double)
     */
    Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max);

    /**
     * Zrange by score with scores.
     *
     * @param key the key
     * @param min the min
     * @param max the max
     * @return the sets the
     */
    Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max);

    /**
     * Return the all the elements in the sorted set at key with a score between
     * min and max (including elements with score equal to min or max).
     * <p>
     * The elements having the same score are returned sorted lexicographically
     * as ASCII strings (this follows from a property of Redis sorted sets and
     * does not involve further computation).
     * <p>
     * Using the optional
     *
     * @param key    the key
     * @param min    the min
     * @param max    the max
     * @param offset the offset
     * @param count  the count
     * @return Multi bulk reply specifically a list of elements in the specified
     * score range.
     * {@link #zrangeByScore(byte[], double, double, int, int) LIMIT} it's
     * possible to get only a range of the matching elements in an SQL-alike
     * way. Note that if offset is large the commands needs to traverse the list
     * for offset elements and this adds up to the O(M) figure.
     * <p>
     * The {@link #zcount(byte[], double, double) ZCOUNT} command is similar to
     * {@link #zrangeByScore(byte[], double, double) ZRANGEBYSCORE} but instead
     * of returning the actual elements in the specified interval, it just
     * returns the number of matching elements.
     * <p>
     * <b>Exclusive intervals and infinity</b>
     * <p>
     * min and max can be -inf and +inf, so that you are not required to know
     * what's the greatest or smallest element in order to take, for instance,
     * elements "up to a given value".
     * <p>
     * Also while the interval is for default closed (inclusive) it's possible
     * to specify open intervals prefixing the score with a "(" character, so
     * for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (1.3 5}
     * <p>
     * Will return all the values with score > 1.3 and <= 5, while for instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (5 (10}
     * <p>
     * Will return all the values with score > 5 and < 10 (5 and 10 excluded).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and
     * M the number of elements returned by the command, so if M is constant
     * (for instance you always ask for the first ten elements with LIMIT) you
     * can consider it O(log(N))
     * @see #zrangeByScore(byte[], double, double)
     * @see #zrangeByScore(byte[], double, double, int, int)
     * @see #zrangeByScoreWithScores(byte[], double, double)
     * @see #zrangeByScoreWithScores(byte[], double, double, int, int)
     * @see #zcount(byte[], double, double)
     */
    Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count);

    /**
     * Zrange by score with scores.
     *
     * @param key    the key
     * @param min    the min
     * @param max    the max
     * @param offset the offset
     * @param count  the count
     * @return the sets the
     */
    Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count);

    /**
     * Return the rank (or index) or member in the sorted set at key, with
     * scores being ordered from low to high.
     * <p>
     * When the given member does not exist in the sorted set, the special value
     * 'nil' is returned. The returned rank (or index) of the member is 0-based
     * for both commands.
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))
     *
     * @param key    the key
     * @param member the member
     * @return Integer reply or a nil bulk reply, specifically: the rank of the
     * element as an integer reply if the element exists. A nil bulk
     * reply if there is no such element.
     * @see #zrevrank(byte[], byte[])
     */
    Long zrank(byte[] key, byte[] member);

    /**
     * Remove the specified member from the sorted set value stored at key. If
     * member was not a member of the set no operation is performed. If key does
     * not not hold a set value an error is returned.
     * <p>
     * Time complexity O(log(N)) with N being the number of elements in the
     * sorted set
     *
     * @param key     the key
     * @param members the members
     * @return Integer reply, specifically: 1 if the new element was removed 0
     * if the new element was not a member of the set
     */
    Long zrem(byte[] key, byte[]... members);

    /**
     * Remove all elements in the sorted set at key with rank between start and
     * end. Start and end are 0-based with rank 0 being the element with the
     * lowest score. Both start and end can be negative numbers, where they
     * indicate offsets starting at the element with the highest rank. For
     * example: -1 is the element with the highest score, -2 the element with
     * the second highest score and so forth.
     * <p>
     * <b>Time complexity:</b> O(log(N))+O(M) with N being the number of
     * elements in the sorted set and M the number of elements removed by the
     * operation
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return the long
     */
    Long zremrangeByRank(byte[] key, long start, long end);

    /**
     * Remove all the elements in the sorted set at key with a score between min
     * and max (including elements with score equal to min or max).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and
     * M the number of elements removed by the operation
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return Integer reply, specifically the number of elements removed.
     */
    Long zremrangeByScore(byte[] key, double start, double end);

    /**
     * Zremrange by score.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return the long
     */
    Long zremrangeByScore(byte[] key, byte[] start, byte[] end);

    /**
     * Zrevrange.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return the sets the
     */
    Set<byte[]> zrevrange(byte[] key, long start, long end);

    /**
     * Zrevrange by score.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     * @return the sets the
     */
    Set<byte[]> zrevrangeByScore(byte[] key, double max, double min);

    /**
     * Zrevrange by score.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     * @return the sets the
     */
    Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min);

    /**
     * Zrevrange by score.
     *
     * @param key    the key
     * @param max    the max
     * @param min    the min
     * @param offset the offset
     * @param count  the count
     * @return the sets the
     */
    Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count);

    /**
     * Zrevrange by score.
     *
     * @param key    the key
     * @param max    the max
     * @param min    the min
     * @param offset the offset
     * @param count  the count
     * @return the sets the
     */
    Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count);

    /**
     * Zrevrange by score with scores.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     * @return the sets the
     */
    Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min);

    /**
     * Zrevrange by score with scores.
     *
     * @param key    the key
     * @param max    the max
     * @param min    the min
     * @param offset the offset
     * @param count  the count
     * @return the sets the
     */
    Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count);

    /**
     * Zrevrange by score with scores.
     *
     * @param key the key
     * @param max the max
     * @param min the min
     * @return the sets the
     */
    Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min);

    /**
     * Zrevrange by score with scores.
     *
     * @param key    the key
     * @param max    the max
     * @param min    the min
     * @param offset the offset
     * @param count  the count
     * @return the sets the
     */
    Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count);

    /**
     * Zrevrange with scores.
     *
     * @param key   the key
     * @param start the start
     * @param end   the end
     * @return the sets the
     */
    Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end);

    /**
     * Return the rank (or index) or member in the sorted set at key, with
     * scores being ordered from high to low.
     * <p>
     * When the given member does not exist in the sorted set, the special value
     * 'nil' is returned. The returned rank (or index) of the member is 0-based
     * for both commands.
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))
     *
     * @param key    the key
     * @param member the member
     * @return Integer reply or a nil bulk reply, specifically: the rank of the
     * element as an integer reply if the element exists. A nil bulk
     * reply if there is no such element.
     * @see #zrank(byte[], byte[])
     */
    Long zrevrank(byte[] key, byte[] member);

    /**
     * Return the score of the specified element of the sorted set at key. If
     * the specified element does not exist in the sorted set, or the key does
     * not exist at all, a special 'nil' value is returned.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key    the key
     * @param member the member
     * @return the score
     */
    Double zscore(byte[] key, byte[] member);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Connection

    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Server

    /**
     * Serialize the value stored at key in a Redis-specific format and return it to the user. The returned value can be synthesized back into a Redis key using the RESTORE command.
     * The serialization format is opaque and non-standard, however it has a few semantical characteristics:
     * It contains a 64-bit checksum that is used to make sure errors will be detected. The RESTORE command makes sure to check the checksum before synthesizing a key using the serialized value.
     * Values are encoded in the same format used by RDB.
     * An RDB version is encoded inside the serialized value, so that different Redis versions with incompatible RDB formats will refuse to process the serialized value.
     * The serialized value does NOT contain expire information. In order to capture the time to live of the current value the PTTL command should be used.
     * If key does not exist a nil bulk reply is returned.
     * <p>
     * Time complexity: O(1) to access the key and additional O(N*M) to serialized it, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1).
     *
     * @param key
     * @return Bulk reply: the serialized value.
     */
    byte[] dump(byte[] key);

    /**
     * Create a key associated with a value that is obtained by deserializing the provided serialized value (obtained via DUMP).
     * If ttl is 0 the key is created without any expire, otherwise the specified expire time (in milliseconds) is set.
     * RESTORE checks the RDB version and data checksum. If they don't match an error is returned.
     * <p>
     * Time complexity: O(1) to create the new key and additional O(N*M) to recostruct the serialized value, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1). However for sorted set values the complexity is O(N*M*log(N)) because inserting values into sorted sets is O(log(N)).
     *
     * @param key
     * @param ttl
     * @param serializedValue
     * @return Status code reply: The command returns OK on success.
     */
    String restore(byte[] key, long ttl, byte[] serializedValue);

}
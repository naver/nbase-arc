import redis

def do_commands(conn, GW=True):
    r = conn.do_generic_request
    assert_equal = redis.rr_assert_equal
    assert_subs = redis.rr_assert_substring
    key = 'commands_genericxxxxxxxkey'
    key2 = 'commands_genericxxxxxxkey2'
    dest = 'commands_generic_xxxxxdest'
    try:
        # |    APPEND           | O | |
        r('del', key)
        r('APPEND', key, '1')
        r('APPEND', key, '2')
        assert_equal('12', r('get', key))

        # |    ASKING           | X | |
        resp = r('ASKING')
        assert(resp.startswith('ERR Unsupported'))

        # |    AUTH             | X | |
        resp = r('AUTH', 'passwd')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('ERR Client sent'))

        # |    BGREWRITEAOF     | X | |
        resp = r('BGREWRITEAOF')
        assert(resp.startswith('ERR Unsupported'))

        # |    BGSAVE           | X | |
        resp = r('BGSAVE')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('Background'))

        # |    BITCOUNT         | O | |
        r('set', key, 'foobar')
        resp = r('BITCOUNT', key)
        assert(resp == 26)

        # |    BITFIELD         | O | Available since 1.4 |
        r('del', key)
        resp = r('BITFIELD', key, 'incrby', 'i5', 100, 1, 'get', 'u4', 0)
        assert(resp == [1,0])

        # |    BITOP            | X | |
        r('set', key, 'foobar')
        r('set', key2, 'abcdef')
        resp = r('BITOP', 'AND', dest, key, key2)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 6)

        # |    BITPOS           | O | |
        r('set', key, '\xff\xf0\x00')
        resp = r('BITPOS', key, 0)
        assert(resp == 12)

        # |    BLPOP            | X | |
        resp = r('BLPOP', key, 0)
        assert(resp.startswith('ERR Unsupported'))

        # |    BRPOP            | X | |
        resp = r('BRPOP', key, 0)
        assert(resp.startswith('ERR Unsupported'))

        # |    BRPOPLPUSH       | X | |
        resp = r('BRPOPLPUSH', key, dest, 0)
        assert(resp.startswith('ERR Unsupported'))

        # |    CLIENT           | X | |
        resp = r('CLIENT', 'LIST')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('id='))

        # |    CLUSTER          | X | |
        resp = r('CLUSTER', 'info')
        assert(resp.startswith('ERR Unsupported'))

        # |    COMMAND          | X | |
        resp = r('COMMAND')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) > 0)

        # |    CONFIG           | X | |
        resp = r('CONFIG', 'get', 'save')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 2) # ['save', '']

        # |    CRC16            | O | |
        r('del', key)
        resp = r('CRC16', key, 'abcd')
        assert(resp == 43062)

        # |    DBSIZE           | O | returns aggregated dbsize |
        r('set', key, '1')
        resp = r('DBSIZE')
        assert(resp >= 1)

        # |    DEBUG            | X | |
        resp = r('DEBUG', 'object', key)
        assert(resp.startswith('ERR Unsupported'))

        # |    DECR             | O | |
        r('set', key, 10)
        resp = r('DECR', key)
        assert(resp == 9)

        # |    DECRBY           | O | |
        r('set', key, 10)
        resp = r('DECRBY', key, 2)
        assert(resp == 8)

        # |    DEL              | O | |
        r('set', key, 'babe')
        r('DEL', key)
        resp = r('exists', key)
        assert(resp == 0)

        # |    DISCARD          | X | |
        resp = r('DISCARD')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('ERR DISCARD'))

        # |    DUMP             | O | |
        r('set', key, 1)
        dump = r('DUMP', key)
        resp = r('del', key)
        assert(resp == 1)
        r('restore', key, 0, dump)
        resp = r('get', key)
        assert(resp == '1')

        # |    ECHO             | X | |
        resp = r('ECHO', 'hihi')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 'hihi')

        # |    EVAL             | X | |
        resp = r('EVAL', 'return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}', 2,  key,  key2, 'first', 'second')
        assert(resp.startswith('ERR Unsupported'))

        # |    EVALSHA          | X | |
        resp = r('EVALSHA', 'fd758d1589d044dd850a6f05d52f2eefd27f033f', 1, key)
        assert(resp.startswith('ERR Unsupported'))

        # |    EXEC             | X | |
        resp = r('EXEC')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('ERR EXEC'))

        # |    EXISTS           | O | |
        r('del', key)
        r('set', key, 1)
        resp = r('EXISTS', key)
        assert(resp == 1)

        # |    EXPIRE           | O | |
        r('set', key, 1)
        resp = r('EXPIRE', key, 1)
        assert(resp == 1)

        # |    EXPIREAT         | O | |
        r('set', key, 1)
        resp = r('EXPIREAT', key, 1293840000)
        assert(resp == 1)

        # |    FLUSHALL         | X | |
        resp = r('FLUSHALL')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('OK'))

        # |    FLUSHDB          | X | |
        resp = r('FLUSHDB')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('OK'))

        # |    GEOADD           | O | Available since 1.4 |
        # |    GEODIST          | O | Available since 1.4 |
        # |    GEOHASH          | O | Available since 1.4 |
        # |    GEOPOS           | O | Available since 1.4 |
        # |    GEORADIUS        | O | Available since 1.4 (no store option) |
        # |   GEORADIUSBYMEMBER | O | Available since 1.4 (no store option) |
        r('del', key)
        resp = r('GEOADD', key, 13.361389, 38.115556, 'Palermo', 15.087269, 37.502669, 'Catania')
        assert(resp == 2)

        resp = r('GEODIST', key, 'Palermo', 'Catania')
        assert(float(resp) > 166274 and float(resp) < 166275) # 66274.1516

        resp = r('GEOHASH', key, 'Palermo', 'Catania')
        assert(len(resp) == 2)

        resp = r('GEOPOS', key, 'Palermo', 'Catania', 'NonExisting')
        assert(len(resp) == 3)

        resp = r('GEORADIUS', key, 15, 37, 200, 'km', 'WITHDIST')
        assert(len(resp) == 2)

        resp = r('GEORADIUS', key, 15, 37, 200, 'km', 'WITHDIST', 'STORE', key2)
        assert(resp.startswith('ERR STORE'))

        resp = r('GEORADIUSBYMEMBER', key, 'Palermo', 1000, 'km')
        assert(len(resp) == 2)

        resp = r('GEORADIUSBYMEMBER', key, 'Palermo', 200, 'km', 'STORE', key2)
        assert(resp.startswith('ERR STORE'))

        # |    GET              | O | |
        r('set', key, 'gg')
        resp = r('GET', key)
        assert(resp == 'gg')

        # |    GETBIT           | O | |
        r('setbit', key, 7, 1)
        resp = r('GETBIT', key, 7)
        assert(resp == 1)

        # |    GETRANGE         | O | |
        r('set', key, "This is a string")
        resp = r('GETRANGE', key, 0, 3)
        assert(resp == "This")

        # |    GETSET           | O | |
        r('set', key, 'oldval')
        resp = r('GETSET', key, 'newval')
        assert(resp == 'oldval')

        # |    HDEL             | O | |
        # |    HEXISTS          | O | |
        # |    HGET             | O | |
        # |    HGETALL          | O | |
        # |    HINCRBY          | O | |
        # |    HINCRBYFLOAT     | O | |
        # |    HKEYS            | O | |
        # |    HLEN             | O | |
        # |    HMGET            | O | |
        # |    HMSET            | O | |
        r('del', key)
        resp = r('HSET', key, 'k1', 'v1')
        assert(resp == 1)
        resp = r('HGET', key, 'k1')
        assert(resp == 'v1')
        resp = r('HGETALL', key)
        assert(len(resp) == 2)
        resp = r('HEXISTS', key, 'kkk')
        assert(resp == 0)
        r('hset', key, 'count', 100)
        resp = r('HINCRBY', key, 'count', 2)
        assert(resp == 102)
        resp = r('HINCRBYFLOAT', key, 'count', 2.0)
        assert(float(resp) == 104.0)
        resp = r('HKEYS', key)
        assert(len(resp) == 2)
        resp = r('HLEN', key)
        assert(resp == 2)
        resp = r('HMGET', key, 'k1', 'k2')
        assert(len(resp) == 2)
        resp = r('HMSET', key, 'kk1', 'vv1', 'kk2', 'vv2')
        assert(resp == 'OK')

        # |    HOST:            | X | |
        # skip

        # |    HSCAN            | O | Available since 1.4 |
        # |    HSET             | O | |
        # |    HSETNX           | O | |
        # |    HSTRLEN          | O | Available since 1.4 |
        # |    HVALS            | O | |
        r('del', key)
        resp = r('HSET', key, 'k1', 'v11')
        assert(resp == 1)
        resp = r('HSCAN', key, 0)
        assert(len(resp) == 2)
        resp = r('HSETNX', key, 'k1', 'v2')
        assert(resp == 0)
        resp = r('HSTRLEN', key, 'k1')
        assert(resp == 3)
        resp = r('HVALS', key)
        assert(len(resp) == 1)

        # |    INCR             | O | |
        # |    INCRBY           | O | |
        # |    INCRBYFLOAT      | O | |
        r('set', key, 100)
        resp = r('INCR', key)
        assert(resp == 101)
        resp = r('INCRBY', key, 1000)
        assert(resp == 1101)
        resp = r('INCRBYFLOAT', key, 0.12)
        assert(float(resp) == 1101.12)

        # |    INFO             | O | returns cluster info |
        resp = r('INFO')
        assert(len(resp) > 500)

        # |    KEYS             | X | |
        resp = r('KEYS', 'nosuchkey_may_be.really.ok???')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 0)

        # |    LASTSAVE         | X | |
        resp = r('LASTSAVE')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp > 1500000000)

        # |    LATENCY          | X | |
        resp = r('LATENCY')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp.startswith('ERR wrong number'))

        # |    LINDEX           | O | |
        # |    LINSERT          | O | |
        # |    LLEN             | O | |
        # |    LPOP             | O | |
        # |    LPUSH            | O | |
        # |    LPUSHX           | O | |
        # |    LRANGE           | O | |
        # |    LREM             | O | |
        # |    LSET             | O | |
        # |    LTRIM            | O | |
        r('del', key)
        resp = r('LPUSH', key, 'v2')
        assert(resp == 1)
        resp = r('LPUSHX', key, 'v1')
        assert(resp == 2)
        resp = r('LINDEX', key, 1)
        assert(resp == 'v2')
        resp = r('LINSERT', key, 'BEFORE', 'v2', 'mid')
        assert(resp == 3)
        resp = r('LLEN', key)
        assert(resp == 3)
        resp = r('LRANGE', key, 0, 0)
        assert(len(resp) == 1 and resp[0] == 'v1')
        resp = r('LREM', key, 0, 'v1')
        assert(resp == 1)
        resp = r('LSET', key, 1, 'MID')
        assert(resp == 'OK')
        resp = r('LTRIM', key, 1, -1)
        assert(resp == 'OK')
        resp = r('LPOP', key)
        assert(resp == 'MID')

        # |    MGET             | O | |
        r('set', key, 1)
        r('set', key2, 2)
        resp = r('MGET', key, key2)
        assert(len(resp) == 2)

        # |    MIGRATE          | X | |
        resp = r('MIGRATE', 'localhost', '7009', key)
        assert(resp.startswith('ERR Unsupported'))

        # |    MONITOR          | X | |
        # skip

        # |    MOVE             | X | |
        resp = r('MOVE', key, 1)
        assert(resp.startswith('ERR Unsupported'))

        # |    MSET             | O | |
        resp = r('MSET', key, 1, key2, 2)
        assert(resp == 'OK')

        # |    MSETNX           | X | |
        r('del', key)
        r('del', key2)
        resp = r('MSETNX', key, 1, key2, 2)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 1) # all keys are set

        # |    MULTI            | X | |
        resp = r('MULTI')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 'OK')
            resp = r('discard')
            assert(resp == 'OK')

        # |    OBJECT           | O | |
        r('set', key, 'this is test expected to be uni..dque')
        resp = r('OBJECT', 'REFCOUNT', key)
        assert(resp == 1)

        # |    PERSIST          | O | |
        r('set', key, 100)
        resp = r('PERSIST', key)
        assert(resp == 0) # has no associated ttl

        # |    PEXPIRE          | O | |
        r('set', key, 100)
        resp = r('PEXPIRE', key, 10000)
        assert(resp == 1)

        # |    PEXPIREAT        | O | |
        r('set', key, 200)
        resp = r('PEXPIREAT', key, 1000000)
        assert(resp == 1)

        # |    PFADD            | O | Available since 1.4 |
        # |    PFCOUNT          | O | Available since 1.4 (single key only) |
        r('del', key)
        resp = r('PFADD', key, 1, 2, 3, 4, 5, 6)
        assert (resp == 1)
        resp = r('PFCOUNT', key)
        assert (resp == 6)


        # |    PFDEBUG          | X | |
        resp = r('PFDEBUG', key)
        assert(resp.startswith('ERR Unsupported'))

        # |    PFMERGE          | X | |
        r('del', key)
        r('del', key2)
        r('pfdadd', key, 1, 2, 3, 4, 5)
        resp = r('PFMERGE', key2, key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 'OK')

        # |    PFSELFTEST       | X | |
        resp = r('PFSELFTEST', 'xbac') # bad arg for purpose
        assert(resp.startswith('ERR Unsupported'))

        # |    PING             | O | gateway ping |
        resp = r('PING')
        assert(resp == 'PONG')

        # |    POST             | X | |
        # skip

        # |    PSETEX           | O | |
        r('del', key)
        resp = r('PSETEX', key, 10000, 'val')
        assert(resp == 'OK')

        # |    PSUBSCRIBE       | X | |
        resp = r('PSUBSCRIBE', 'h?llo')
        assert(resp.startswith('ERR Unsupported'))

        # |    PSYNC            | X | |
        resp = r('PSYNC', 'runid', 1000)
        assert(resp.startswith('ERR Unsupported'))

        # |    PTTL             | O | |
        r('set', key, 1)
        resp = r('PTTL', key)
        assert(resp == -1)

        # |    PUBLISH          | X | |
        resp = r('PUBLISH', 'chan', 'message')
        assert(resp.startswith('ERR Unsupported'))

        # |    PUBSUB           | X | |
        resp = r('PUBSUB', 'CHANNELS')
        assert(resp.startswith('ERR Unsupported'))

        # |    PUNSUBSCRIBE     | X | |
        resp = r('PUNSUBSCRIBE')
        assert(resp.startswith('ERR Unsupported'))

        # |    QUIT             | O | |
        # skip

        # |    RANDOMKEY        | X | |
        r('set', key, 100)
        resp = r('RANDOMKEY')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) > 0)

        # |    READONLY         | X | |
        resp = r('READONLY')
        assert(resp.startswith('ERR Unsupported'))

        # |    READWRITE        | X | |
        resp = r('READWRITE')
        assert(resp.startswith('ERR Unsupported'))

        # |    RENAME           | X | |
        r('set', key, 1)
        r('del', key2)
        resp = r('RENAME', key, key2)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 'OK')

        # |    RENAMENX         | X | |
        r('set', key, 1)
        r('del', key2)
        resp = r('RENAMENX', key, key2)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 1)

        # |    REPLCONF         | X | |
        resp = r('REPLCONF', 'option', 'value')
        assert(resp.startswith('ERR Unsupported'))

        # |    RESTORE          | O | |
        r('del', key)
        resp = r('RESTORE', key, 0, '\n\x17\x17\x00\x00\x00\x12\x00\x00\x00\x03\x00\x00\xc0\x01\x00\x04\xc0\x02\x00\x04\xc0\x03\x00\xff\x04\x00u#<\xc0;.\xe9\xdd')
        assert(resp == 'OK')
        resp = r('type', key)
        assert(resp == 'list')
        # |    RESTORE-ASKING   | X | |
        r('del', key)
        resp = r('RESTORE-ASKING', key, 0, '\n\x17\x17\x00\x00\x00\x12\x00\x00\x00\x03\x00\x00\xc0\x01\x00\x04\xc0\x02\x00\x04\xc0\x03\x00\xff\x04\x00u#<\xc0;.\xe9\xdd')
        assert(resp.startswith('ERR Unsupported'))

        # |    ROLE             | X | |
        resp = r('ROLE')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 3)

        # |    RPOP             | O | |
        r('del', key)
        r('rpush', key, 'v1')
        r('rpush', key, 'v2')
        resp = r('RPOP', key)
        assert(resp == 'v2')

        # |    RPOPLPUSH        | X | |
        r('del', key)
        r('del', key2)
        resp = r('RPOPLPUSH', key, key2)
        if GW:
            assert (resp.startswith('ERR Unsupported'))
        else:
            assert (resp == None)

        # |    RPUSH            | O | |
        r('del', key)
        resp = r('RPUSH', key, 'v')
        assert(resp == 1)

        # |    RPUSHX           | O | |
        r('del', key)
        r('rpush', key, 'v1')
        resp = r('RPUSHX', key, 'v2')
        assert(resp == 2)

        # |    SADD             | O | |
        r('del', key)
        resp = r('SADD', key, 'v1')
        assert(resp == 1)

        # |    SAVE             | X | |
        resp = r('SAVE')
        assert(resp.startswith('ERR Unsupported'))

        # |    SCAN             | O | Available since 1.4 |
        resp = r('SCAN', 0)
        assert(len(resp) == 2)

        # |    SCARD            | O | |
        r('del', key)
        r('sadd', key, 'v')
        resp = r('SCARD', key)
        assert(resp == 1)

        # |    SCRIPT           | X | |
        resp = r('SCRIPT', 'exists')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 0)

        # |    SDIFF            | X | |
        r('del', key)
        r('del', key2)
        r('sadd', key, 'a', 'b', 'c')
        r('sadd', key2, 'c', 'd')
        resp = r('SDIFF', key, key2)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 2)

        # |    SDIFFSTORE       | X | |
        r('del', key)
        r('del', key2)
        r('sadd', key, 'a', 'b', 'c')
        r('sadd', key2, 'c', 'd')
        resp = r('SDIFFSTORE', key2, key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 3)

        # |    SELECT           | X | |
        resp = r('SELECT', 0)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 'OK')

        # |    SET              | O | |
        resp = r('SET', key, 100)
        assert(resp == 'OK')

        # |    SETBIT           | O | |
        r('set', key, 7)
        resp = r('SETBIT', key, 3, 0)
        assert(resp == 1)

        # |    SETEX            | O | |
        resp = r('SETEX', key, 10, "hello")
        assert(resp == 'OK')

        # |    SETNX            | O | |
        r('del', key)
        resp = r('SETNX', key, 100)
        assert(resp == 1)

        # |    SETRANGE         | O | |
        r('set', key, 'Hello World')
        resp = r('SETRANGE', key, 6, 'Redis')
        assert(resp == 11)

        # |    SHUTDOWN         | X | |
        resp = r('SHUTDOWN')
        assert(resp.startswith('ERR Unsupported'))

        # |    SINTER           | X | |
        r('del', key)
        r('del', key2)
        r('sadd', key, 'a', 'b', 'c')
        r('sadd', key2, 'b', 'c')
        resp = r('SINTER', key, key2)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 2)

        # |    SINTERSTORE      | X | |
        r('del', key)
        r('del', key2)
        r('sadd', key, 'a', 'b', 'c')
        resp = r('SINTERSTORE', key2, key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 3)

        # |    SISMEMBER        | O | |
        r('del', key)
        r('sadd', key, 'a', 'b', 'c')
        resp = r('SISMEMBER',key, 'c')
        assert(resp == 1)

        # |    SLAVEOF          | X | |
        resp = r('SLAVEOF', 'localhost', 1234)
        assert(resp.startswith('ERR Unsupported'))

        # |    SLOWLOG          | X | |
        resp = r('SLOWLOG', 'get', 1)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(not str(resp).startswith('ERR'))

        # |    SMEMBERS         | O | |
        r('del', key)
        r('sadd', key, 'a', 'b', 'c')
        resp = r('SMEMBERS', key)
        assert(len(resp) == 3)

        # |    SMOVE            | X | |
        r('del', key)
        r('del', key2)
        r('sadd', key, 'a', 'b', 'c')
        r('sadd', key2, 'b', 'c')
        resp = r('SMOVE', key, key2, 'a')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 1)

        # |    SORT             | X | |
        r('del', key)
        r('sadd', key, 10, 9, 8)
        resp = r('SORT', key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 3 and resp[0] == '8')

        # |    SPOP             | X | |
        r('del', key)
        r('sadd', key, 10, 9, 8)
        resp = r('SPOP', key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == '10' or resp == '9' or resp == '8')

        # |    SRANDMEMBER      | O | |
        r('del', key)
        r('sadd', key, 10, 9, 8)
        resp = r('SRANDMEMBER', key)
        assert(resp == '10' or resp == '9' or resp == '8')

        # |    SREM             | O | |
        r('del', key)
        r('sadd', key, 10, 9, 8)
        resp = r('SREM', key, 10, 9)
        assert(resp == 2)

        # |    SSCAN            | O | Available since 1.4 |
        r('del', key)
        r('sadd', key, 10)
        resp = r('SSCAN', key, 0)
        assert(len(resp) == 2)

        # |    STRLEN           | O | |
        r('set', key, '01234')
        resp = r('STRLEN', key)
        assert(resp == 5)

        # |    SUBSCRIBE        | X | |
        resp = r('SUBSCRIBE', 'channel')
        assert(resp.startswith('ERR Unsupported'))

        # |    SUBSTR           | O | |
        r('set', key, "This is a string")
        resp = r('SUBSTR', key, 0, 3)
        assert(resp == "This")

        # |    SUNION           | X | |
        r('del', key)
        r('sadd', key, 'a', 'b', 'c')
        resp = r('SUNION', key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 3)

        # |    SUNIONSTORE      | X | |
        r('del', key)
        r('del', key2)
        r('sadd', key, 'a', 'b', 'c')
        resp = r('SUNIONSTORE', key2, key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 3)

        # |    SYNC             | X | |
        resp = r('SYNC')
        assert(resp.startswith('ERR Unsupported'))

        # |    TIME             | X | |
        resp = r('TIME')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(len(resp) == 2)

        # |    TOUCH            | O | Available since 1.4 (multi key) |
        r('del', key)
        r('del', key2)
        resp = r('TOUCH', key, key2)
        assert(resp == 0)

        # |    TTL              | O | |
        r('set', key, 100)
        resp = r('TTL', key)
        assert(resp == -1)

        # |    TYPE             | O | |
        r('set', key, 100)
        resp = r('TYPE', key)
        assert(resp == 'string')

        # |    UNSUBSCRIBE      | X | |
        resp = r('UNSUBSCRIBE')
        assert(resp.startswith('ERR Unsupported'))

        # |    UNWATCH          | X | |
        # |    WATCH            | X | |
        resp = r('WATCH', key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 'OK')

        resp = r('UNWATCH')
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 'OK')

        # |    WAIT             | X | |
        resp = r('WAIT', 1, 10000)
        assert(resp.startswith('ERR Unsupported'))

        # |    ZADD             | O | |
        # |    ZCARD            | O | |
        # |    ZCOUNT           | O | |
        # |    ZINCRBY          | O | |
        # |    ZINTERSTORE      | X | |
        # |    ZLEXCOUNT        | O | Available since 1.4 |
        # |    ZRANGE           | O | |
        # |    ZRANGEBYLEX      | O | Available since 1.4 |
        # |    ZRANGEBYSCORE    | O | |
        # |    ZRANK            | O | |
        # |    ZREM             | O | |
        # |    ZREMRANGEBYLEX   | O | Available since 1.4 |
        # |    ZREMRANGEBYRANK  | O | |
        # |    ZREMRANGEBYSCORE | O | |
        # |    ZREVRANGE        | O | |
        # |    ZREVRANGEBYLEX   | O | Available since 1.4 |
        # |    ZREVRANGEBYSCORE | O | |
        # |    ZREVRANK         | O | |
        # |    ZSCAN            | O | Available since 1.4 |
        # |    ZSCORE           | O | |
        # |    ZUNIONSTORE      | X | |
        r('del', key)
        resp = r('ZADD', key, 1.0, 'v1')
        assert(resp == 1)
        resp = r('ZCARD', key)
        assert(resp == 1)

        resp = r('ZCOUNT', key, 0.9, 1.1)
        assert(resp == 1)

        resp = r('ZINCRBY', key, 1.0, 'v1')
        assert(float(resp) == 2.0)

        resp = r('ZINTERSTORE', dest, 1, key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 1)

        r('del', key)
        r('zadd',key, 0, 'a', 0, 'b', 0, 'c', 0, 'd', 0, 'e', 0, 'f', 0, 'g')
        resp = r('ZLEXCOUNT', key, '-', '+')
        assert(resp == 7)

        r('del', key)
        r('zadd', key, 0, "zero", 1, "one", 2, "two")
        resp = r('ZRANGE', key, 0, -1)
        assert(len(resp) == 3)

        r('del', key)
        r('zadd',key, 0, 'a', 0, 'b', 0, 'c', 0, 'd', 0, 'e', 0, 'f', 0, 'g')
        resp = r('ZRANGEBYLEX', key, '-', '[c')
        assert(len(resp) == 3)

        r('del', key)
        r('zadd', key, 0, "zero", 1, "one", 2, "two")
        resp = r('ZRANGEBYSCORE', key, 1, 2)
        assert(len(resp) == 2)

        resp = r('ZRANK', key, "one")
        assert(resp == 1)

        resp = r('ZREM', key, "two")
        assert(resp == 1)

        r('del', key)
        r('zadd',key, 0, 'a', 0, 'b', 0, 'c', 0, 'd', 0, 'e', 0, 'f', 0, 'g')
        resp = r('ZREMRANGEBYLEX', key, '[a', '[e')
        assert(resp == 5)

        r('del', key)
        r('zadd',key, 0, 'a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', 6, 'g')
        resp = r('ZREMRANGEBYRANK', key, 0, 1)
        assert(resp == 2)

        r('del', key)
        r('zadd',key, 0, 'a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', 6, 'g')
        resp = r('ZREMRANGEBYSCORE', key, 0, 3)
        assert(resp == 4)

        r('del', key)
        r('zadd', key, 0, "zero", 1, "one", 2, "two")
        resp = r('ZREVRANGE', key, 0, -1)
        assert(len(resp) == 3 and resp[0] == 'two')

        r('del', key)
        r('zadd',key, 0, 'a', 0, 'b', 0, 'c', 0, 'd', 0, 'e', 0, 'f', 0, 'g')
        resp = r('ZREVRANGEBYLEX', key, '[c', '-')
        assert(len(resp) == 3)

        r('del', key)
        r('zadd',key, 0, 'a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', 6, 'g')
        resp = r('ZREVRANGEBYSCORE', key, '(3', '(0')
        assert(len(resp) == 2)

        r('del', key)
        r('zadd', key, 0, "zero", 1, "one", 2, "two")
        resp = r('ZREVRANK', key, "zero")
        assert(resp == 2)

        r('del', key)
        r('zadd', key, 0, "zero")
        resp = r('ZSCAN', key, 0)
        assert(len(resp) == 2 and resp[0] == '0')

        r('del', key)
        r('zadd',key, 0, 'a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', 6, 'g')
        resp = r('ZSCORE', key, 'c')
        assert(float(resp) == 2)

        r('del', key)
        r('del', key2)
        r('zadd',key, 0, 'a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', 6, 'g')
        resp = r('ZUNIONSTORE', key2, 1, key)
        if GW:
            assert(resp.startswith('ERR Unsupported'))
        else:
            assert(resp == 7)

    finally:
        r('del', key)
        r('del', key2)
        r('del', dest)

### Command support.
Nbasearc currently uses modified version of Redis 3.2.9. 
Some of the Redis commands are not supported and some are modified in the context of the cluster environment. See also [s3 commands](/doc/s3-commands.md)

Following table summarizes the supported Redis commands and differences supported by the nbase-arc gateways.

| Commands | Supported | Note |
| ------------ | ------------ | ----- |
|    APPEND           | O | |
|    ASKING           | X | TBD |
|    AUTH             | X | |
|    BGREWRITEAOF     | X | |
|    BGSAVE           | X | |
|    BITCOUNT         | O | |
|    BITFIELD         | O | TBD |
|    BITOP            | X | |
|    BITPOS           | O | |
|    BLPOP            | X | |
|    BRPOP            | X | |
|    BRPOPLPUSH       | X | |
|    CLIENT           | X | |
|    CLUSTER          | X | TBD |
|    COMMAND          | X | TBD |
|    CONFIG           | X | |
|    CRC16            | O | |
|    DBSIZE           | O | returns aggregated dbsize |
|    DEBUG            | X | |
|    DECR             | O | |
|    DECRBY           | O | |
|    DEL              | O | |
|    DISCARD          | X | |
|    DUMP             | O | |
|    ECHO             | X | |
|    EVAL             | X | |
|    EVALSHA          | X | |
|    EXEC             | X | |
|    EXISTS           | O | |
|    EXPIRE           | O | |
|    EXPIREAT         | O | |
|    FLUSHALL         | X | |
|    FLUSHDB          | X | |
|    GEOADD           | O | TBD |
|    GEODIST          | O | TBD |
|    GEOHASH          | O | TBD |
|    GEOPOS           | O | TBD |
|    GEORADIUS        | O | TBD |
|   GEORADIUSBYMEMBER | O | TBD |
|    GET              | O | |
|    GETBIT           | O | |
|    GETRANGE         | O | |
|    GETSET           | O | |
|    HDEL             | O | |
|    HEXISTS          | O | |
|    HGET             | O | |
|    HGETALL          | O | |
|    HINCRBY          | O | |
|    HINCRBYFLOAT     | O | |
|    HKEYS            | O | |
|    HLEN             | O | |
|    HMGET            | O | |
|    HMSET            | O | |
|    HOST:            | X | TBD |
|    HSCAN            | O | TBD |
|    HSET             | O | |
|    HSETNX           | O | |
|    HSTRLEN          | O | TBD |
|    HVALS            | O | |
|    INCR             | O | |
|    INCRBY           | O | |
|    INCRBYFLOAT      | O | |
|    INFO             | O | returns cluster info |
|    KEYS             | X | |
|    LASTSAVE         | X | |
|    LATENCY          | X | TBD |
|    LINDEX           | O | |
|    LINSERT          | O | |
|    LLEN             | O | |
|    LPOP             | O | |
|    LPUSH            | O | |
|    LPUSHX           | O | |
|    LRANGE           | O | |
|    LREM             | O | |
|    LSET             | O | |
|    LTRIM            | O | |
|    MGET             | O | |
|    MIGRATE          | X | |
|    MONITOR          | X | |
|    MOVE             | X | |
|    MSET             | O | |
|    MSETNX           | X | |
|    MULTI            | X | |
|    OBJECT           | O | |
|    PERSIST          | O | |
|    PEXPIRE          | O | |
|    PEXPIREAT        | O | |
|    PFADD            | O | TBD |
|    PFCOUNT          | O | TBD multi key |
|    PFDEBUG          | X | TBD expose? |
|    PFMERGE          | X | TBD |
|    PFSELFTEST       | X | TBD |
|    PING             | O | gateway ping |
|    POST             | X | TBD |
|    PSETEX           | O | |
|    PSUBSCRIBE       | X | |
|    PSYNC            | X | |
|    PTTL             | O | |
|    PUBLISH          | X | |
|    PUBSUB           | X | |
|    PUNSUBSCRIBE     | X | |
|    QUIT             | O | |
|    RANDOMKEY        | X | |
|    READONLY         | X | TBD |
|    READWRITE        | X | TBD |
|    RENAME           | X | |
|    RENAMENX         | X | |
|    REPLCONF         | X | |
|    RESTORE          | O | |
|    RESTORE-ASKING   | X | TBD |
|    ROLE             | X | TBD |
|    RPOP             | O | |
|    RPOPLPUSH        | X | |
|    RPUSH            | O | |
|    RPUSHX           | O | |
|    SADD             | O | |
|    SAVE             | X | |
|    SCAN             | O | TBD |
|    SCARD            | O | |
|    SCRIPT           | X | |
|    SDIFF            | X | |
|    SDIFFSTORE       | X | |
|    SELECT           | X | |
|    SET              | O | |
|    SETBIT           | O | |
|    SETEX            | O | |
|    SETNX            | O | |
|    SETRANGE         | O | |
|    SHUTDOWN         | X | |
|    SINTER           | X | |
|    SINTERSTORE      | X | |
|    SISMEMBER        | O | |
|    SLAVEOF          | X | |
|    SLOWLOG          | X | |
|    SMEMBERS         | O | |
|    SMOVE            | X | |
|    SORT             | X | |
|    SPOP             | X | |
|    SRANDMEMBER      | O | |
|    SREM             | O | |
|    SSCAN            | O | TBD |
|    STRLEN           | O | |
|    SUBSCRIBE        | X | |
|    SUBSTR           | O | |
|    SUNION           | X | |
|    SUNIONSTORE      | X | |
|    SYNC             | X | |
|    TIME             | X | |
|    TOUCH            | O | TBD multi key |
|    TTL              | O | |
|    TYPE             | O | |
|    UNSUBSCRIBE      | X | |
|    UNWATCH          | X | |
|    WAIT             | X | TBD |
|    WATCH            | X | |
|    ZADD             | O | |
|    ZCARD            | O | |
|    ZCOUNT           | O | |
|    ZINCRBY          | O | |
|    ZINTERSTORE      | X | |
|    ZLEXCOUNT        | O | TBD |
|    ZRANGE           | O | |
|    ZRANGEBYLEX      | O | TBD |
|    ZRANGEBYSCORE    | O | |
|    ZRANK            | O | |
|    ZREM             | O | |
|    ZREMRANGEBYLEX   | O | TBD |
|    ZREMRANGEBYRANK  | O | |
|    ZREMRANGEBYSCORE | O | |
|    ZREVRANGE        | O | |
|    ZREVRANGEBYLEX   | O | TBD |
|    ZREVRANGEBYSCORE | O | |
|    ZREVRANK         | O | |
|    ZSCAN            | O | TBD |
|    ZSCORE           | O | |
|    ZUNIONSTORE      | X | |

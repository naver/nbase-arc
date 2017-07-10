### Command support.
Nbasearc currently uses modified version of Redis 3.2.9. 
Some of the Redis commands are not supported and some are modified in the context of the cluster environment. See also [s3 commands](/doc/s3-commands.md), [cluster scan commands](/doc/cluster-scan-commands.md)

Following table summarizes the supported Redis commands and differences supported by the nbase-arc gateways.

| Commands | Supported | Note |
| ------------ | ------------ | ----- |
|    APPEND           | O | |
|    ASKING           | X | |
|    AUTH             | X | |
|    BGREWRITEAOF     | X | |
|    BGSAVE           | X | |
|    BITCOUNT         | O | |
|    BITFIELD         | O | Available since 1.4 |
|    BITOP            | X | |
|    BITPOS           | O | |
|    BLPOP            | X | |
|    BRPOP            | X | |
|    BRPOPLPUSH       | X | |
|    CLIENT           | X | |
|    CLUSTER          | X | |
|    COMMAND          | X | |
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
|    GEOADD           | O | Available since 1.4 |
|    GEODIST          | O | Available since 1.4 |
|    GEOHASH          | O | Available since 1.4 |
|    GEOPOS           | O | Available since 1.4 |
|    GEORADIUS        | O | Available since 1.4 (no store option) |
|   GEORADIUSBYMEMBER | O | Available since 1.4 (no store option) |
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
|    HOST:            | X | |
|    HSCAN            | O | Available since 1.4 |
|    HSET             | O | |
|    HSETNX           | O | |
|    HSTRLEN          | O | Available since 1.4 |
|    HVALS            | O | |
|    INCR             | O | |
|    INCRBY           | O | |
|    INCRBYFLOAT      | O | |
|    INFO             | O | returns cluster info |
|    KEYS             | X | |
|    LASTSAVE         | X | |
|    LATENCY          | X | |
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
|    PFADD            | O | Available since 1.4 |
|    PFCOUNT          | O | Available since 1.4 (single key only) |
|    PFDEBUG          | X | |
|    PFMERGE          | X | |
|    PFSELFTEST       | X | |
|    PING             | O | gateway ping |
|    POST             | X | |
|    PSETEX           | O | |
|    PSUBSCRIBE       | X | |
|    PSYNC            | X | |
|    PTTL             | O | |
|    PUBLISH          | X | |
|    PUBSUB           | X | |
|    PUNSUBSCRIBE     | X | |
|    QUIT             | O | |
|    RANDOMKEY        | X | |
|    READONLY         | X | |
|    READWRITE        | X | |
|    RENAME           | X | |
|    RENAMENX         | X | |
|    REPLCONF         | X | |
|    RESTORE          | O | |
|    RESTORE-ASKING   | X | |
|    ROLE             | X | |
|    RPOP             | O | |
|    RPOPLPUSH        | X | |
|    RPUSH            | O | |
|    RPUSHX           | O | |
|    SADD             | O | |
|    SAVE             | X | |
|    SCAN             | O | Available since 1.4 |
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
|    SSCAN            | O | Available since 1.4 |
|    STRLEN           | O | |
|    SUBSCRIBE        | X | |
|    SUBSTR           | O | |
|    SUNION           | X | |
|    SUNIONSTORE      | X | |
|    SYNC             | X | |
|    TIME             | X | |
|    TOUCH            | O | Available since 1.4 (multi key) |
|    TTL              | O | |
|    TYPE             | O | |
|    UNSUBSCRIBE      | X | |
|    UNWATCH          | X | |
|    WAIT             | X | |
|    WATCH            | X | |
|    ZADD             | O | |
|    ZCARD            | O | |
|    ZCOUNT           | O | |
|    ZINCRBY          | O | |
|    ZINTERSTORE      | X | |
|    ZLEXCOUNT        | O | Available since 1.4 |
|    ZRANGE           | O | |
|    ZRANGEBYLEX      | O | Available since 1.4 |
|    ZRANGEBYSCORE    | O | |
|    ZRANK            | O | |
|    ZREM             | O | |
|    ZREMRANGEBYLEX   | O | Available since 1.4 |
|    ZREMRANGEBYRANK  | O | |
|    ZREMRANGEBYSCORE | O | |
|    ZREVRANGE        | O | |
|    ZREVRANGEBYLEX   | O | Available since 1.4 |
|    ZREVRANGEBYSCORE | O | |
|    ZREVRANK         | O | |
|    ZSCAN            | O | Available since 1.4 |
|    ZSCORE           | O | |
|    ZUNIONSTORE      | X | |

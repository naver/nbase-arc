# Redis configuration file template

REDIS_CONFIG =  (
    # <available versions> <key> <value>
    [[1.1, 1.2], "smr-local-port", "-1"],
    [[1.1, 1.2], "port", "-1"],
    # nBase-ARC
    [[1.1, 1.2], "cronsave", [[0,3]]],

    ################################ GENERAL ######################################
    [[1.1, 1.2], "daemonize", "yes"],
    [[1.1, 1.2], "pidfile", "redis.pid"],
    [[1.1, 1.2], "timeout", "0"],
    [[1.2], "tcp-keepalive", "60"],
    [[1.1, 1.2], "loglevel", "notice"],
    [[1.1, 1.2], "logfile", "redis.log"],
    [[1.1, 1.2], "databases", "1"],

    ################################ SNAPSHOTTING #################################
    # nBase-ARC default disabled
    [[1.1, 1.2], "save", "\"\""],
    # nBase-ARC
    [[1.1, 1.2], "seqsave", "500"],
    [[1.1, 1.2], "stop-writes-on-bgsave-error", "no"],
    [[1.1, 1.2], "rdbcompression", "yes"],
    [[1.1, 1.2], "rdbchecksum", "yes"],
    [[1.1, 1.2], "dbfilename", "dump.rdb"],
    # nBase-ARC the total number of rdb backups
    [[1.2], "number-of-rdb-backups", "0"],
    [[1.1, 1.2], "dir", "./"],

    ################################# REPLICATION #################################
    [[1.1, 1.2], "slave-serve-stale-data", "yes"],
    [[1.1, 1.2], "slave-read-only", "yes"],
    [[1.2], "repl-disable-tcp-nodelay", "no"],
    [[1.1, 1.2], "slave-priority", "100"],

    ############################## APPEND ONLY MODE ###############################
    [[1.1, 1.2], "appendonly", "no"],
    [[1.2], "appendfilename", '"appendonly.aof"'],
    [[1.1, 1.2], "appendfsync", "everysec"],
    [[1.1, 1.2], "no-appendfsync-on-rewrite", "no"],
    [[1.1, 1.2], "auto-aof-rewrite-percentage", "100"],
    [[1.1, 1.2], "auto-aof-rewrite-min-size", "64mb"],

    ################################ LUA SCRIPTING ################################
    [[1.1, 1.2], "lua-time-limit", "5000"],

    ################################## SLOW LOG ###################################
    [[1.1, 1.2], "slowlog-log-slower-than", "10000"],
    [[1.1, 1.2], "slowlog-max-len", "128"],

    ############################# Event notification ##############################
    [[1.2], "notify-keyspace-events", "\"\""],

    ############################### ADVANCED CONFIG ###############################
    [[1.1, 1.2], "hash-max-ziplist-entries", "512"],
    [[1.1, 1.2], "hash-max-ziplist-value", "64"],
    [[1.1, 1.2], "list-max-ziplist-entries", "512"],
    [[1.1, 1.2], "list-max-ziplist-value", "64"],
    [[1.1, 1.2], "set-max-intset-entries", "512"],
    [[1.1, 1.2], "zset-max-ziplist-entries", "128"],
    [[1.1, 1.2], "zset-max-ziplist-value", "64"],
    [[1.1, 1.2], "activerehashing", "yes"],
    [[1.1, 1.2], "client-output-buffer-limit", ["normal 0 0 0", "pubsub 32mb 8mb 60", "slave 256mb 64mb 60"]],
    [[1.2], "hz", "10"],
    [[1.2], "aof-rewrite-incremental-fsync", "yes"]
)

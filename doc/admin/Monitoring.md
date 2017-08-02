# Monitoring
* [Fault Detection](#fault-detection)	 
* [Cluster Statistics](#cluster-statistics)	
* [Individual Process Log](#individual-process-log)	

## Fault Detection
Configuration Master monitors all the clusters and logs faults if it detects any. This log file is used to monitor status change of nbase-arc. If you detect any status changes that correspond to the circumstances described in this chapter, follow the procedures in [Fault Handling](FaultHandling.md).

### Configuration Master Log Detection
A log file is created in the following path of the machine where the Configuration Master process is running. You should monitor the logs of all machines where the Configuration Master process is installed. It is generally installed in three machines.
* `$HOME/nbase-arc/confmaster/output.log`

Check this file to detect failures that might occur in components of nbase-arc.

### Configuration Master Log by Status
There are the following cases of status changes in the Configuration Master log. If a status change is made, take a proper action for it.

#### New leader was elected.
This log message is shown in the following three cases:
* When you run the Configuration Master for the first time on the newly set up management node.
* When a fault occurs in the Configuration Master.
* When you upgrade the Configuration Master on the management node.

This message can normally be shown while you install or upgrade the Configuration Master. However, if it is shown in other cases, follow the procedures described in [Configuration Master Repair](FaultHandling.md#configuration-master-repair).

#### Master election Success
This log message is shown in the following three cases:
* When you set up a new nbase-arc cluster.
* When a fault occurs in a PGS of nbase-arc cluster.
* When you upgrade the nbase-arc cluster.

This message can normally be shown while you set up or upgrade the nbase-arc cluster. However, if it is shown in other cases, follow the procedures described in [Cluster Auto Repair](FaultHandling.md#cluster-auto-repair). Before using the cluster auto repair, please be sure to check if there is any hardware issue in the server where a fault occurred. Otherwise, the fault may occur again even after the repair.

#### Slave join Success
This log message is shown in the following three cases:
* When you set up a new nbase-arc cluster.
* When a fault occurs in a PGS of nbase-arc cluster and then is repaired.
* When you upgrade the nbase-arc cluster.

#### State change
This log message is shown in the following three cases:
* When you set up a new nbase-arc cluster.
* When a fault occurs in a PGS of nbase-arc cluster.
* When you upgrade the nbase-arc cluster.

This message can normally be shown while you set up or upgrade the nbase-arc cluster. However, if it is shown in other cases, follow the procedures described in [Cluster Auto Repair](FaultHandling.md#cluster-auto-repair). Before using the cluster auto repair, please be sure to check if there is any hardware issue in the server where a fault occurred. Otherwise, the fault may occur again even after the repair.

## Cluster Statistics
### Statistics Collection Using nbase-arc CLI Tool
You can collect nbase-arc cluster statistics data by using nbase-arc CLI tool.
nbase-arc CLI tool has two statistics commands which are `stat <interval sec>` and `latency <interval sec>`.
```
$ arc-cli
    _   ______  ___   _____ ______   ___    ____  ______   ________    ____
   / | / / __ )/   | / ___// ____/  /   |  / __ \/ ____/  / ____/ /   /  _/
  /  |/ / __  / /| | \__ \/ __/    / /| | / /_/ / /      / /   / /    / /
 / /|  / /_/ / ___ |___/ / /___   / ___ |/ _, _/ /___   / /___/ /____/ /
/_/ |_/_____/_/  |_/____/_____/  /_/  |_/_/ |_|\____/   \____/_____/___/

Usage: arc-cli -z <zk addr> -c <cluster name>
   -z <zookeeper address>      Zookeeper address (ex: zookeeper.nbasearc.com:2181)
   -c <cluster name>           Cluster name (ex: dev_test)

Special Commands
   STAT [<interval sec>]       Print stats of cluster
   LATENCY [<interval sec>]    Print latencies of cluster
```
```
$ arc-cli -z zookeeper.address.com:2181 -c your_cluster
    _   ______  ___   _____ ______   ___    ____  ______   ________    ____
   / | / / __ )/   | / ___// ____/  /   |  / __ \/ ____/  / ____/ /   /  _/
  /  |/ / __  / /| | \__ \/ __/    / /| | / /_/ / /      / /   / /    / /
 / /|  / /_/ / ___ |___/ / /___   / ___ |/ _, _/ /___   / /___/ /____/ /
/_/ |_/_____/_/  |_/____/_____/  /_/  |_/_/ |_|\____/   \____/_____/___/

your_cluster> stat 1
+------------------------------------------------------------------------------------------------------+
|  2015-12-07 17:12:30, CLUSTER:your_cluster                                                           |
+------------------------------------------------------------------------------------------------------+
|  Time | Redis |  PG  | Connection |    Mem    |   OPS   |   Hits   |  Misses  |   Keys   |  Expires  |
+------------------------------------------------------------------------------------------------------+
| 12:30 |   128 |   64 |       1388 |   1.02 TB |616.21 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
| 12:31 |   128 |   64 |       1388 |   1.02 TB |580.53 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
| 12:32 |   128 |   64 |       1388 |   1.02 TB |556.90 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
| 12:33 |   128 |   64 |       1388 |   1.02 TB |565.91 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
^Cyour_cluster> latency 1
+-------------------------------------------------------------------------------------------------------------------------------+
|  2015-12-07 17:12:35, CLUSTER:your_cluster                                                                                    |
+-------------------------------------------------------------------------------------------------------------------------------+
|  Time |  <= 1ms |  <= 2ms |  <= 4ms |  <= 8ms | <= 16ms | <= 32ms | <= 64ms |  <= 128 |  <= 256 |  <= 512 | <= 1024 |  > 1024 |
+-------------------------------------------------------------------------------------------------------------------------------+
| 12:35 |  6.79 K |  4.56 K | 11.56 K |  8.85 K |  1.70 K |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
| 12:36 |  8.46 K |  3.86 K | 14.43 K |  6.84 K |      31 |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
| 12:37 |  9.53 K |  8.22 K |  9.47 K |  3.05 K |     383 |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
| 12:38 |  8.07 K |  5.94 K | 10.13 K |  2.00 K |      84 |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
^Cyour_cluster>
```

### Statistics Collection Using INFO Command
#### INFO Command Execution
nbase-arc supports INFO command which is similar to that of Redis, but not the same. Because nbase-arc is composed of multiple Redis processes, its INFO command shows integrated information of those processes.
The following example shows a result of executing INFO command of nbase-arc cluster.
```
$ ./arc-cli -z zookeeper.address.com:2181 -c your_cluster
    _   ______  ___   _____ ______   ___    ____  ______   ________    ____
   / | / / __ )/   | / ___// ____/  /   |  / __ \/ ____/  / ____/ /   /  _/
  /  |/ / __  / /| | \__ \/ __/    / /| | / /_/ / /      / /   / /    / /
 / /|  / /_/ / ___ |___/ / /___   / ___ |/ _, _/ /___   / /___/ /____/ /
/_/ |_/_____/_/  |_/____/_____/  /_/  |_/_/ |_|\____/   \____/_____/___/

your_cluster> info all
# Cluster
cluster_name:your_cluster
total_redis_instances:4
redis_instances_available:4
total_partition_groups:2
... ...

```

#### Information by Section
The result of executing INFO command is composed of 8 sections. You can get the information by section, as shown in the following.
##### Cluster
```
# Cluster
cluster_name:your_cluster
total_redis_instances:4
redis_instances_available:4
total_partition_groups:2
partition_groups_available:2
cluster_slot_size:8192
pg_map_rle:0 4096 1 4096
```
* cluster_name: nbase-arc cluster's name
* total_redis_instances: Total number of Redis processes in the cluster
* redis_instances_available: The number of available Redis processes in the cluster
* total_partition_groups: The number of PGs in the cluster
* partition_groups_available: The number of available PGs in the cluster
* cluster_slot_size: Hash mapping slot size of the cluster
* pg_map_rle: Hash mapping information of each PG. Displayed in run length encoding (RLE).

##### REDIS
```
# REDIS
redis_id_list:0 1 50 51
redis_0:addr=100.100.100.100 port=7009 pg_id=0 max_latency=4
redis_1:addr=100.100.100.100 port=7019 pg_id=1 max_latency=4
redis_50:addr=100.100.100.101 port=7009 pg_id=0 max_latency=4
redis_51:addr=100.100.100.101 port=7019 pg_id=1 max_latency=5
```
* redis_id_list: A list of Redis processes' IDs in the cluster
* redis_<ID>: Detailed information of the Redis process that is specified with ID
  * addr = machine's IP address, port = access port, pg_id = PG(Partition Group) ID
  * max_latency = Maximum latency of the commands processed within last 1 sec

##### Gateway
```
# Gateway
gateway_process_id:1596
gateway_tcp_port:6000
gateway_total_commands_processed:0
gateway_instantaneous_ops_per_sec:0
gateway_total_commands_lcon:0
gateway_instantaneous_lcon_ops_per_sec:0
gateway_connected_clients:1
gateway_prefer_domain_socket:0
gateway_disconnected_redis:0
gateway_delay_filter_rle:0 8192
```
* gateway_process_id: Gateway's process ID
* gateway_tcp_port: Gateway's access Port
* gateway_total_commands_processed: Total number of commands processed by the gateway
* gateway_instantaneous_ops_per_sec: The number of commands processed per second by the gateway
* gateway_total_commands_lcon: Total number of commands processed through local connection by the gateway
* gateway_instantaneous_lcon_ops_per_sec: The number of commands processed per second through local connection by the gateway
* gateway_connected_clients: The number of clients connected to the gateway
* gateway_prefer_domain_socket: Gateway's option for domain socket connection
* gateway_disconnected_reids: The number of Redis processes disconnected with the gateway
* gateway_delay_filter_rle: Slot information that is delayed during migration, in the form of RLE.

##### Latency
```
# Latency
less_than_or_equal_to_1ms:0
less_than_or_equal_to_2ms:0
less_than_or_equal_to_4ms:3
less_than_or_equal_to_8ms:1
less_than_or_equal_to_16ms:0
less_than_or_equal_to_32ms:0
less_than_or_equal_to_64ms:0
less_than_or_equal_to_128ms:0
less_than_or_equal_to_256ms:0
less_than_or_equal_to_512ms:0
less_than_or_equal_to_1024ms:0
more_than_1024ms:0
```
* Latency Histogram: Latency histogram of the commands processed during 1 sec in the gateway. Each item shows the number of commands within the latency, processed during 1 sec.

>Note  
>Gateway and Latency section shows information of the gateway that the current client is connected with.


##### Clients
```
# Clients
connected_clients:60
```
* connected_clients: The number of connections established between the gateway and Redis in the entire cluster.

##### Memory
```
# Memory
used_memory:3405861296
used_memory_human:3.17G
used_memory_rss:3699404800
used_memory_peak:13703934144
used_memory_peak_human:12.76G
mem_fragmentation_ratio:1.09
```
* used_memory: Memory usage of the entire cluster
* used_memory_human: used_memory in human readable form
* used_memory_rss: Resident set size (RSS) usage of the entire cluster
* used_memory_peak: Peak memory usage of the entire cluster
* used_memory_peak_human: used_memory_peak in human readable form
* mem_fragmentation_ratio: Fragmentation ratio of memory allocation represented by the ratio of used_memory and used_memory_rss

##### Stats
```
# Stats
total_connections_received:14075
total_commands_processed:432225655
instantaneous_ops_per_sec:0
total_commands_lcon:21979488
instantaneous_lcon_ops_per_sec:0
rejected_connections:0
expired_keys:60096
evicted_keys:0
keyspace_hits:217587743
keyspace_misses:4479
```
* total_connections_received: Total number of connections made by the cluster to the Redis server
* total_commands_processed: Total number of commands processed by the entire cluster
* instantaneous_ops_per_sec: The number of commands processed per second by the entire cluster
* total_commands_processed: Total number of commands processed through local connection by the entire cluster
* instantaneous_ops_per_sec: The number of commands processed per second through local connection by the entire cluster
* rejected_connections: The number of connections rejected, by the entire cluster to the Redis server
* expired_keys: The number of keys that expires in the entire cluster
* evicted_keys: The number of keys evicted in the entire cluster (always 0 because nbase-arc does not support eviction)
* keyspace_hits: The number of keyspace hits in the entire cluster
* keyspace_misses: The number of keyspace misses in the entire cluster

##### Keyspace
```
# Keyspace
db0:keys=348566,expires=12,avg_ttl=312571694971
```
* keys: The number of keys in the entire cluster
* expires: The number of keys on which a timeout is set, in the entire cluster
* avg_ttl: Average TTL of the keys on which a timeout is set

## Individual Process Log
* REDIS Log path: ```$NBASE_ARC_HOME/pgs/[PORT]/redis/redis.log```
* SMR Log path: ```$NBASE_ARC_HOME/pgs/[PORT]/smr/smrlog-date-time```
* Gateway Log path: ```$NBASE_ARC_HOME/gw/[PORT]/gwlog-date-time.log```
* Conf Master Log path: ```$NBASE_ARC_HOME/confmaster/output.log```
* Zookeeper Log: Please refer to the ZooKeeper's guide in http://zookeeper.apache.org/ for ZooKeeper log.

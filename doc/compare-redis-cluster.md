### Compare with Redis Cluster (RC, in short)
nbase-arc and RC has different approaches in many ways. The following table summarizes the differences between nbase-arc and RC.

|       | nbase-arc | RC |
|-------|-----------|----|
| *Key distribution* | crc16(key) % 8192 | crc16(key) % 16384 |
| *Replication* | asynchronous | consensus-based |
| *Data persistence* | RDB/AOF | RDB+LOG |
| *Client connection* | Redis | Gateway |
| *Migration* | key based | partition based |
| *Fault-detection* | gossip | multiple heartbeat |
| *fail-over* | master election protocol | configuration master |


#### Key distribution model
nbase-arc uses a partitioning scheme based on the hash value of the key. 

* Hash number calculation method is same as the that of the RC except that the modulo divisor is 8192 in nbase-arc.
* key space is partitioned into 8192 partitions and a key is mapped to a partition number (PN)
* Each partition belongs to a partition group (PG)
* Keys belongs to a partition group is stored in a group of synchronously replicated Redis servers called partition group servers (PGSs)

#### Replication
nbase-arc implemented consensus-based synchronous replication instead of using Redis asynchronous replication. 

See [replication](/doc/state-machine-replicator.md) for a detailed explanation of nbase-arc replication.

#### Data Persistence
In Redis, RDB or AOF can be used for data persistence. RDB is a point in time snapshot of the dataset and AOF is a log of write operations for rebuilding the up to date dataset from the empty one. Each method has its own advantages and disadvantages, but both RDB and AOF cannot be used in a unified way to compensate each others disadvantages.

nbasedarc unified RDB and replication log. Additionally, the log sequence number at the time of the checkpoint is held in RDB file. After the RDB file is loaded, only the logs after the checkpoint can be used to recover the date dataset.

### Client connection
Clients in RC directly connect to Redis nodes of the cluster. Because all operations must be handled at master node even during cluster live configuration, clients must have to handle MOVED, ASK redirection messages to get to the proper master. Such cluster information should normally cached on the client side for better performance.

In nbase-arc, clients connect to any gateways to perform operation and there is no redirection. Clients also can be hinted about the cluster configuration by the configuration master for the request to be routed to the most proper gateway.

See [gateway](/doc/gateway.md) for a more information about gateway
See [C API](/doc/c-api.md) for a more information about C API

#### Migration
In RC, Migration of a slot is performed by MIGRATE command  key by key.
In nbase-arc, migration of a slot (a partition) is performed by dumping whole partition data, load the data, catch-up log after the dump, and cluster reconfiguration.
See [migration](/doc/migration.md) for a detailed explanation of nbase-arc migration.


#### Fault-detection and fail-over
In RC, Each node's view of another nodes is propagated to each other via gossip protocol and a failure of a node is eventually determined independently by a node itself. When a slave believes that its master failed it starts the slave election procedure and the only one of the campaigning slaves is promoted to master with the help of majority number of masters.

In nbase-arc, node failure is determined by the centralized authority called configuration master. Configuration master stores the status of every cluster node on Zookeeper ensemble. Each configuration master instance (leader or follower) continuously checks the status of the components via heartbeat message. When a configuration master detects status mismatch (i.e. from dead to alive, or vice versa) it votes the change of the status. The leader configuration master decides the final status of the component and does failover action if needed.

See [fail-over](/doc/failure-detection-and-failover.md) for a detailed procedure of the nbase-arc fail-over 

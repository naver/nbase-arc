# Overview

nbase-arc provides dynamic scale-in/out of a cluster. To scale out a cluster, a new partition group (PG) is created and all data of some partitions in an source PG are moved to the new PG. Cluster scale-in is essentially the same process.

Migration task is processed as in following steps:
  1. Dump and load
  2. Log catch-up
  3. Migration 2PC (two phase commit)
  4. Clean-up

The following notations are used to describe a migration task.
  * PG: partition group.
  * Source PG : PG that contains data to be migrated.
  * Target PG : PG that migration data will be imported to

### Dump and load
![dump and load](/doc/images/mig1.png)

In the dump step, a checkpoint file that contains only the data to be migrated is created. 
The dump file also contains the log sequence number (LSN) which represents the last command at the time of the checkpoint. In the load step, all data in the dump file are loaded into the target PG. This is done by a utility program and the program actually acts like a normal Redis client.

### Log catch-up
![log catch-up](/doc/images/mig2.png)

Write operations that are routed to source PG after the checkpoint must be also applied to target PG.
These operations are sent from source PG to target PG by replaying replication log on the source PG side.


### Migration 2PC (two phase commit protocol)
![mig 2pc](/doc/images/mig3.png)

When log catch-up between source PG and target PG is almost done, PN-PG mapping must be changed transparently. This process is performed by distributed transaction, 2PC (two phase commit) between the configuration master and gateways.

In the first phase, the configuration master blocks operations in gateways belongs to partitions in migration. When a gateway returns OK to this command, it is assured that all previous queries belongs to migrating partitions are replicated and committed in replication layer.

In the second phase, the configuration master changes partition number (PN) to partition group (PG) mapping with redirect command.  After redirect, operations blocked in the gateways are sent to the target PG as in normal.

### Cleanup
After migration, migrated data in source PG are deleted. This work is performed independently with the handling of client requests.

# Overview
Configuration master manages all of the cluster information in a Zookeeper Ensemble. It consists of multiple instances of configuration master process, whose role is leader or a follower. Each instance monitors the cluster components via a application level heart-beat messaging. The leader instance determines the component state and does failover if needed.

Configuration master also provides the gateway information of each cluster to C and Java clients. Each client watches the Zookeeper node and got notified when there is a change on any gateway.

![Configuration master](/doc/images/cm1.png)

### Cluster configuration management
* It manages cluster configuration information which is stored in Zookeeper ensemble. Administrator interacts with leader instance to make new clusters, to modify clusters, and to check the state of processes in clusters.
* When a new Partition Group Server is added to a cluster, configuration master sets up the role of the new Partition Group Server automatically.

### Monitoring and failover
* Configuration master monitors all of the cluster components and notifies faults if it detects any. This notification is used to decide the status of components in nbase-arc. When configuration master detects faults, it performs a failover automatically depdends on the component type.
  - Partition Group Server : configuration master changes the state of the SMR replicator and store it in ZooKeeper. When there is the master SMR replicator fault, it will elect a new master replicator among slaves.
  - Gateway : configuration master changes the state of the gateway and store it in ZooKeeper. And it changes the affinity information of the gateway and informs it to nbase-arc clients (C, Java) in order to reduce the network bandwidth usage.

See [Failure detection and failover](doc/failure-detection-and-failover.md) for a detailed explanation.

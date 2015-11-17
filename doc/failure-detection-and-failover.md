## Failure detection and failover
From the view of automatic failover, each configuration master instance monitors all the SMR replicators via application level heart beat messaging and does a failover action detemined based on the each other's view.

### Failure detection of the SMR replicator
* Each heartbeat is an exchange of ping/pong message between a configuration master and a SMR replicator.
* A pong response contains a version information of the SMR replicator. The version information is increased whenever a state of the replicator is changed.
* When a reply message is not receved within a specified amount of time. The SMR component is considered to be in dead state.
* When a configuration master instance detects state mismatch, this different view of a cluster component is notified to the leader instance by adding a special ephemeral z-node below the z-node of the component.
![Failure detection](/doc/images/fd1.png)

### State decision by the leader
The leader configuration master watches all the z-nodes of PGSes. When a new child z-node are added, the leader configuration master can get the states supposed by other configuration master instances. The leader decides the final state of the node by the majority rule. 
![Failure detection decision](/doc/images/fd2.png)

### Failover
After the decision is made, the leader reconfigures the states of the SMR replicators.
* master election
  - If a master SMR replicator is unavailable, it elects a new master among slaves.
* quorum adjustment
  - If a slave SMR replicator is unavailable, it changes a quorum of the master in order not to wait responses from the unavailable slaves.
* slave join
  - If a SMR is available again, it joins the replicator to an existing group of SMR replicators.


## Failure detection and failover
Each configuration master instance monitors all the cluster components and does a proper action based on the component state change. The type of the monitored component is either a gateway or a PGS (An SMR replicator process and a Redis instance in tandem).


### Failure detection
The monitoring method is a continuous exchange of heartbeat message.
* Each heartbeat is an exchange of ping/pong message between a configuration master and a component.
  * A pong response of the SMR replicator contains a version information which is increased whenever a state of the replicator is changed.
* When a reply message is not received within a specified amount of time. The component is considered to be in a dead state.
* When a configuration master instance detects state mismatch, this different view of a cluster component is notified to the leader instance by adding a special ephemeral z-node below the z-node of the component.
![Failure detection](/doc/images/fd1.png)

### State decision by the leader
The leader configuration master watches all the z-nodes of PGSes. When a new child z-node are added, the leader configuration master can get the states supposed by other configuration master instances. The leader decides the final state of the node by the majority rule.
![Failure detection decision](/doc/images/fd2.png)

### Failover
After the decision is made, the leader reconfigures the states of the component.  
If the type of the component is a gateway, the reader changes the state of the z-node which represents the gateway.
If the type of the component is an SMR replicator, a proper action is taken w.r.t. the role of the replicator.
  * master election
    - If a master SMR replicator is dead, it elects a new master among slaves.
  * quorum adjustment
    - If a slave SMR replicator is dead, it changes a quorum of the master in order not to wait responses from the unavailable slaves.
  * slave join
    - If an SMR is available again, it joins the replicator as a slave to an existing group of SMR replicators.

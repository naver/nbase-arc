## Introduction

nbase-arc (n-base-ARC) is an open source distributed memory store based on Redis. It provides a zone of clusters where each cluster is composed of synchronously replicated Redis server groups which can scale-in/out without service interruption.
![Overviw](/doc/images/overview.png)

## Features
* Multi-cluster zone
  - You can make multiple clusters within a single zone. Each cluster is distinguished by name
  - Configuration master manages all cluster information safely. Configuration master also does failure detection and automatic fail-over of the cluster components
* A Cluster is a single big Redis server
  - Clients access Redis servers indirectly via gateways. A cluster acts like a single big Redis server instance with multiple access point. 
  - Gateway is a Redis proxy that accepts Redis request from client.
* High availability and consistency
  - Unlike Redis replication which is asynchronous and can lost changes when a master crashes, nbase-arc implements synchronous replication layer that supports both high availability and consistency. Changes replied to clients are durable even the master side of the replication is crashed
* Service without interruption
  - All cluster management operations can be performed without service interruption. You can even upgrade gateways (client access directly) transparently if you use nbase-arc C/Java client libraries

## Quick start
* [Build](doc/quick-start.md#build)
* [Setup nbase-arc zone](doc/quick-start.md#setup-nbase-arc-zone)
* [Create nbase-arc cluster](doc/quick-start.md#create-nbase-arc-cluster)

## Documents
* Components
  - [Configuration master](doc/configuration-master.md)
  - [Gateway](doc/gateway.md)
  - [State machine replicator](doc/state-machine-replicator.md)
* Client libraries
  - [C API](api/arcci/README.md)
  - [Java API](api/java/README.md)
* Tools
  - [Administration Guide](doc/admin/AdminGuide.md)
  - [CLI](doc/arc-cli.md)
* More 
  - [Migration](doc/migration.md)
  - [Failure detection and fail-over](doc/failure-detection-and-failover.md)
  - [Supported commands](doc/supported-commands-2.8.8.md)
  - [Compare with Redis Cluster](doc/compare-redis-cluster.md)

## Discussions
* User group: https://groups.google.com/forum/#!forum/nbase-arc-user


## License


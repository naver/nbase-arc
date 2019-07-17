## Introduction

nbase-arc (n-base-ARC) is an open source distributed memory store based on Redis. It provides a zone of clusters where each cluster is composed of synchronously replicated Redis server groups that can scale-in/out without service interruption.
![Overview](/doc/images/overview.png)

## Features
* Multi-cluster zone
  - You can make multiple clusters within a single zone. Each cluster is distinguished by its name.
  - Configuration master manages all cluster information safely. Configuration master also does failure detection and automatic fail-over of the cluster components
* A Cluster is a single big Redis server
  - Clients access Redis servers indirectly via gateways. A cluster acts like a single big Redis server instance with multiple access points. 
  - Gateway is a Redis proxy that accepts Redis requests from client.
* High availability and consistency
  - Unlike Redis replication which is asynchronous and can lose changes when a master crashes, nbase-arc implements synchronous replication layer that supports both high availability and consistency. Changes replied to clients are durable even when the master side of the replication is crashed.
* Service without interruption
  - All cluster management operations can be performed without service interruption. You can even upgrade gateways (client access directly) transparently if you use nbase-arc C/Java client libraries.

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
  - [Supported commands](doc/supported-commands.md)
  - [Compare with Redis Cluster](doc/compare-redis-cluster.md)

## Discussions
* User group: https://groups.google.com/forum/#!forum/nbase-arc-user

## Related Projects
* nbase-arc spring boot starter: https://github.com/smallmiro/nbasearc-spring-boot-starter

## License

```
Copyright 2015 Naver Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

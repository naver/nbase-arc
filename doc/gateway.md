### Overview
Gateway is located between clients and Redis processes and provides a transparent query routing functionality.  Gateway acts like a big Redis server from a client side of view by supporting RESP(Redis Serialization Protocol). Gateway hides composition of the cluster data from the clients.

The nbase-arc gateway has following notable features.

### High performance
The performance of the gateway is equal or higher than other similar open source products. This is due to zero-copy buffer management, scatter/gather I/O, reducing malloc overhead by using fixed size buffer with memory pool, and smart pipelining of multiple client quries. See below benchmark test.

### Seamless integration with the configuration master and tools
Gateway provides administrative commands such as adding/removing Redis processes and change the key distribution information of a cluster. These commands along with configuration master allow the reconfigurations of a cluster can be performed seamlessly on production environment. The reconfiguration of a cluster includes moving processes between servers, upgrading components, scaling in/out cluster, and etc. 

### Structure
Gateway is composed of multiple worker threads and single master thread. Each thread runs non-blocking eventloop. Master thread allocates client connections to the worker threads and excutes admin commands. Each worker thread runs indepedently and processes queries from allocated clients.

### Benchmark

#### Test Settings
* Hardware
    Xeon L5640 @ 2.27GHz X 2 sockets
    48 GB RAM
    Local loopback connection

* Software
    Redis without replication X 8 processes in single machine
    Gateway X 1 process(1/4/8 worker threads) in same machine
    Memtier benchmark X 1 process(1/4 threads) in same machine

* Test options
```
./memtier_benchmark -p $target -t $nthread -c $nclient --ratio=1:1 --test-time 30 -d 100 --key-pattern=S:S --pipeline=$npipeline
```

    $nclient varies through each test.
    $nthread is 1 if $nclient is less than 4, otherwise $nthread is 4.
    $npipeline is 1 or 100.


#### Test comparing twemproxy
* Without pipelining, 1 Core (1 Worker thread)
![Without pipelining](/doc/images/arc_twemproxy_wo_pipeline.png)

* Pipeline 100, 1 Core (1 Worker thread)
![Pipeline 100](/doc/images/arc_twemproxy_pipeline.png)

#### Test comparing codis
* Pipeline 100, 4/8 Core nbase-arc, 8 Core codis
![Pipeline 100](/doc/images/arc_codis.png)

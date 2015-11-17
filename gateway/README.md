## nbase-arc gateway
Gateway is located between clients and Redis processes and provides a transparent query routing functionality.
See [gateway](doc/gateway.md) for more information.

### Installation
1. Copy gateway binary.
2. Execute binary with command arguments.
   Address of conf master and name of connecting cluster are mandatory arguments.

```
$ ./redis-gateway
Usage: ./redis-gateway <options>
option:
    -p: gateway port
    -w: the number of workers
    -t: TCP listen backlog (min = 512)
    -D: daemonize (default false)
    -l: logfile prefix (default gateway)
    -c: conf master addr
    -b: conf master port
    -d: prefer domain socket
    -n: cluster name
```

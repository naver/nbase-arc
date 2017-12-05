## Overview

ARC-CLI is CLI(Command Line Interface) for nbase-arc cluster. By using this tool, you can connect to nbase-arc cluster with two parameters, "zookeeper address" and "cluster name". ARC-CLI supports executing redis commands and monitoring some statistics.

## Usage
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

## Redis commands
<pre>
$ arc-cli -z zookeeper.address.com:2181 -c your_cluster
    _   ______  ___   _____ ______   ___    ____  ______   ________    ____
   / | / / __ )/   | / ___// ____/  /   |  / __ \/ ____/  / ____/ /   /  _/
  /  |/ / __  / /| | \__ \/ __/    / /| | / /_/ / /      / /   / /    / /
 / /|  / /_/ / ___ |___/ / /___   / ___ |/ _, _/ /___   / /___/ /____/ /
/_/ |_/_____/_/  |_/____/_____/  /_/  |_/_/ |_|\____/   \____/_____/___/

your_cluster> <b>set testkey testval</b>
+OK
your_cluster> <b>get testkey</b>
"testval"
your_cluster>
</pre>

## Statictics commands
#### stat command
Stat command collects general statistics from gateway's "info" command and shows it in a table format.
You can type `STAT [<interval sec>]` in CLI prompt. Default interval is 5 sec.

<pre>
your_cluster> <b>stat 1</b>
+------------------------------------------------------------------------------------------------------+
|  2015-12-07 17:12:30, CLUSTER:your_cluster                                                           |
+------------------------------------------------------------------------------------------------------+
|  Time | Redis |  PG  | Connection |    Mem    |   OPS   |   Hits   |  Misses  |   Keys   |  Expires  |
+------------------------------------------------------------------------------------------------------+
| 12:30 |   128 |   64 |       1388 |   1.02 TB |616.21 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
| 12:31 |   128 |   64 |       1388 |   1.02 TB |580.53 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
| 12:32 |   128 |   64 |       1388 |   1.02 TB |556.90 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
| 12:33 |   128 |   64 |       1388 |   1.02 TB |565.91 K |   1.66 T | 555.41 G |   1.50 G |  838.07 M |
... ...
</pre>

#### latency command
Latency command collects latency information from gateway's "info latency" command and shows it in a histogram format.
You can type `LATENCY [<interval sec>]` in CLI prompt. Default interval is 5 sec.

<pre>
your_cluster> <b>latency 1</b>
+-------------------------------------------------------------------------------------------------------------------------------+
|  2015-12-07 17:12:35, CLUSTER:your_cluster                                                                                    |
+-------------------------------------------------------------------------------------------------------------------------------+
|  Time |  <= 1ms |  <= 2ms |  <= 4ms |  <= 8ms | <= 16ms | <= 32ms | <= 64ms |  <= 128 |  <= 256 |  <= 512 | <= 1024 |  > 1024 |
+-------------------------------------------------------------------------------------------------------------------------------+
| 12:35 |  6.79 K |  4.56 K | 11.56 K |  8.85 K |  1.70 K |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
| 12:36 |  8.46 K |  3.86 K | 14.43 K |  6.84 K |      31 |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
| 12:37 |  9.53 K |  8.22 K |  9.47 K |  3.05 K |     383 |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
| 12:38 |  8.07 K |  5.94 K | 10.13 K |  2.00 K |      84 |       0 |       0 |       0 |       0 |       0 |       0 |       0 |
... ...
</pre>

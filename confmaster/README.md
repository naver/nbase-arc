# Installation

### Requirements
* JDK 6 or 7 installed
* Maven 3.2.x+ installed
* ZooKeeper Ensemble Configuration
Please refer to the following URL and install ZooKeeper. It is recommended to use at least 3 ZooKeeper instances that compose an ensemble, for availability.
http://zookeeper.apache.org/doc/r3.4.6/zookeeperStarted.html

### Set and Execute Configuration Master
Access each and every management node, change the configuration of Configuration master and execute it.
* Edit the cc.properties file in $NBASE_ARC_HOME/confmaster directory, as follows.
```
confmaster.zookeeper.address=<zookeeper ensemble address>
confmaster.ip=<IP address of the connected management node>
```
* Enter the ZooKeeper ensemble address in the form of connection string, as shown below. The connection string format is "IP:port,IP:port,IP:port" where the IP is the IP address of each management node in which ZooKeeper is run, and the port is the basic port of ZooKeeper, 2181. 
```
confmaster.zookeeper.address=10.20.30.40:2181,10.20.30.41:2181,10.20.30.42:2181
```
* Enter the IP address of the machine (management node) currently connected, for server_ip.
```
confmaster.ip=10.20.30.40
```
* Use the default settings for the following fields of cc.properties.
```
confmaster.port=1122                  Client listen port of Configuration master
confmaster.server.thread.max=64       The no. of threads required to process mgmt-cc
confmaster.heartbeat.timeout=4000     Heartbeat response timeout. If the timeout expires, it is regarded as terminated.
confmaster.heartbeat.interval=1000    Heartbeat interval
```
* Use the following command to execute Configuration master.
```
./confmaster-<version>.sh
```
When you execute Configuration master, the nBase-ARC management process is started. Run the process on every management node.

# Quick start
This guide demonstrates single machine setup of nbase-arc cluster. Please refer to [Admin Guide](doc/admin/AdminGuide.md) for how to install multi-machine, fault-tolerant cluster.

## Build
#### Build Requirements
Install these dependencies prior to building nbase-arc.
* JAVA JDK 1.7 or 1.8
* Apache Maven : https://maven.apache.org/

#### Build & Copy release binaries
```shell
# Change directory to your workspace.
$ cd $WORKSPACE

$ git clone https://github.com/naver/nbase-arc.git
$ cd nbase-arc

# Build nbase-arc release binaries.
$ make release
# ... ...

# Copy release binaries to $HOME directory.
$ cp -r release/nbase-arc $HOME
```

## Setup nbase-arc zone
#### Install Configuration Master
Install & Run Apache Zookeeper 3.4.5 or 3.4.6
* https://zookeeper.apache.org/

Set properties of Configuration Master
* In properties, do not use `{machine IP}` as "localhost" or "127.0.0.1". Instead, use machine's actual IP.
```shell
$ cd $HOME/nbase-arc/confmaster

# Open cc.properties file and modify following properties
confmaster.ip={machine IP}
confmaster.zookeeper.address={zookeeper address}
```

Run Configuration master
```shell
$ cd $HOME/nbase-arc/confamster
$ ./confmaster-<version>.sh

# Send cm_start command to Configuration master
$ echo cm_start | nc {machine IP} 1122
{"state":"success","msg":"+OK"}
```

#### Install mgmt tool
Install Python & Fabric library
* Python 2.7 : https://www.python.org/
* Fabric 1.10 : http://www.fabfile.org/

Set properties of mgmt tool
* In properties, do not use `{machine IP}` as "localhost" or "127.0.0.1". Instead, use machine's actual IP.
* `{username}` is an account where your cluster will be installed. For quick start, use current user.
```shell
$ cd $HOME/nbase-arc/mgmt/config

# Open conf_mnode.py file and modify following properties
CONF_MASTER_IP = "{machine IP}"
CONF_MASTER_PORT = 1122
CONF_MASTER_MGMT_CONS = 1
USERNAME = "{username}"
```

Specify deploy binaries
* Nbase-arc's binaries are postfixed with version, like `redis-arc-<version>`.
```shell
$ cd $HOME/nbase-arc/mgmt/config

# Open conf_dnode.py file and modify following properties
REDIS_VERSION = "<version>"
GW_VERSION = "<version>"
SMR_VERSION = "<version>"
```

## Create nbase-arc cluster
#### Set up ssh connection
* Mgmt tool deploys clusters via SSH. For convenience, you can set up ssh authorization by public key.
```shell
# Generate SSH key if it doesn't exist already.
$ ssh-keygen
# ... ...

# Append generated public key to SSH's authorized_keys file
$ cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
```

#### Execute mgmt tool
```shell
# Execute mgmt tool
$ cd $HOME/nbase-arc/mgmt
$ fab main

=======================================
1. Show cluster information
2. Upgrade PGS
3. Upgrade GW
4. Install Cluster
5. Install PGS
6. Install GW
7. Uninstall Cluster
8. Uninstall PGS
9. Uninstall GW
10. Add replication
11. Leave replication
12. Migration
13. Repair Cluster (Cluster already installed)
14. Deploy bash_profile
x. Exit
=======================================
>>
```

#### Deploy shell profile
<pre>
# Select "Deploy bash_profile". Then, input machine's actual IP
>> <b>14</b>
PM information('ip list' or all) <b>{machine IP}</b>
... ...
</pre>

#### Create nbase-arc cluster
* PGS Physical Machine list is list of replica groups where a replica group is list of machines. Since we use only one machine in this guide, input format is like [["{machine name} {machine ip}"]].  
`Ex) [["machine1 11.22.33.44"]]`
* Gateway Physical Machine list is list of machines where gateway will be installed. Usually, Gateway and PGS share same machine. In this guide, input format is like ["{machine name} {machine ip}"].  
`Ex) ["machine1 11.22.33.44"]`

<pre>
# Select "Install Cluster".
>> <b>4</b>
Cluster name : <b>test_cluster</b>
PG count : <b>4</b>
Replication number : <b>1</b>
PGS Physical Machine list([["PM_NAME PM_IP", "PM_NAME PM_IP"], ["PM_NAME PM_IP", "PM_NAME PM_IP"], ...]) <b>[["{machine name} {machine IP}"]]</b>
Gateway Physical Machine list([PM_NAME PM_IP, PM_NAME PM_IP, ...]) <b>["{machine name} {machine IP}"]</b>
Cronsave number <b>1</b>
Print script? [Y/n] <b>Y</b>
Print configuration? [Y/n] <b>Y</b>
Create PGS, Continue? [Y/n] <b>Y</b>
Confirm Mode? [Y/n] <b>n</b>
... ...
</pre>

#### Test cluster using arc-cli
```shell
$ cd $HOME/nbase-arc/bin

$ ./arc-cli-<version> -z {zookeeper address} -c test_cluster
    _   ______  ___   _____ ______   ___    ____  ______   ________    ____
   / | / / __ )/   | / ___// ____/  /   |  / __ \/ ____/  / ____/ /   /  _/
  /  |/ / __  / /| | \__ \/ __/    / /| | / /_/ / /      / /   / /    / /
 / /|  / /_/ / ___ |___/ / /___   / ___ |/ _, _/ /___   / /___/ /____/ /
/_/ |_/_____/_/  |_/____/_____/  /_/  |_/_/ |_|\____/   \____/_____/___/

test_cluster> set test_key test_val
+OK
test_cluster> get test_key
"test_val"
```

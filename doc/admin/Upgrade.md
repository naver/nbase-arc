# Upgrade
* [PGS Upgrade](#pgs-upgrade)
* [Gateway Upgrade](#gateway-upgrade)
* [Configuration Master Upgrade](#configuration-master-upgrade)
* [Zookeeper Upgrade](#zookeeper-upgrade)

## PGS Upgrade
You can upgrade PGS with the administrative script. Please follow the procedure below:
*	Copy the upgrade binary file to $NBASE_ARC_HOME/bin in the machine where the administrative 
script has been installed, and go to the directory where the script is located.
*	Open the `mgmt/config/conf_dnode.py` file, and change the values of REDIS_VERSION and SMR_VERSION to the upgrade binary's version.
```
REDIS_VERSION = <Version of nbase-arc>
SMR_VERSION = <Version of nbase-arc>
```
*	Run the script.
```
$ cd $NBASE_ARC_HOME/mgmt
$ fab main
```
*	Select "Upgrade PGS."
*	Enter the name of cluster you want to upgrade for Cluster Name, when the prompt message that asks 
for upgrade options is displayed.
*	When the upgrade is finished, use "Show cluster information" of the script to check the status of
upgraded PGSes. Each PG should contain one master PGS and several slave PGSes.

## Gateway Upgrade
You can upgrade gateways with the administrative script. Please follow the procedure below:
*	Copy the upgrade binary file to $NBASE_ARC_HOME/bin in the machine where the administrative script has been installed, 
and go to the directory where the script is located.
*	Open the `mgmt/config/conf_dnode.py` file, and change the value of GW_VERSION to the upgrade binary's version.
```
GW_VERSION = <Version of nbase-arc>
```
*	Run the script.
```
$ cd $NBASE_ARC_HOME/mgmt
$ fab main
```
*	Select "Upgrade GW."
*	Enter the name of cluster you want to upgrade for Cluster Name, when the prompt message that asks 
for upgrade options is displayed.
*	When the upgrade is finished, use "Show cluster information" of the script to check if the status of upgraded gateways is N (Normal).


## Configuration Master Upgrade
Configuration Master does not support automatic upgrade with the script. Please follow the procedure below to 
manually upgrade Configuration Master.
*	Copy the new version of Configuration Master to `$HOME/nbase-arc/confmaster`.
*	Kill all Configuration Master processes that are running on multiple machines.
```
$ kill -9 <confmaster pid>
```
*	Check if z-nodes used to manage Configuration Master are deleted from ZooKeeper. Check the folders, /RC/CC /LE and /RC/CC/FD. They should not be in those folders. If the z-nodes still exist in the folders even after you terminate all the Configuration Master processes, check again after a while.
```
$ zkCli.sh
zookeeper prompt$ ls /RC/CC/FD
[]
zookeeper prompt$ ls /RC/CC/LE
[]
```
*	Run the new version of Configuration Master.
```
$ cd $NBASE_ARC_HOME/confmaster
$ ./confmaster.sh
```

## Zookeeper Upgrade
You can upgrade ZooKeeper by using rolling restart. Please refer to 
"What are the options-process for upgrading ZooKeeper?" in the official website of ZooKeeper 
(https://cwiki.apache.org/confluence/display/ZOOKEEPER/FAQ), for more information.

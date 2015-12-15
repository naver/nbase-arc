# Replacement
* [PGS Replacement](#pgs-replacement)
* [Gateway Replacement](#gateway-replacement)
* [Configuration Master Replacement](#configuration-master-replacement)
* [Zookeeper Replacement](#zookeeper-replacement)

## PGS Replacement
The replacement operation is to make a additional replicas and then terminate the existing ones.

### Add replica to a new machine
You can add replicas to the new machine with the administrative script. By adding replicas to the new machine, the data load is distributed over both the existing and the new machines. Follow the procedure below:
* Set up the environment of new machine
* Create a configuration file for the new cluster, in the following form
```
# CLUSTER PGS_ID PG_ID PM_NAME PM_IP PORT
test_cluster 0 0 testmachine01.dev 123.123.123.123 7000
test_cluster 1 0 testmachine02.dev 123.123.123.124 7000
```
* Run the administrative script.
```
$ fab main
```
* Select "Add replication"
* Enter the location of the configuration file that you created previously, when you are prompted to input file name that is required to add replicas, as shown below.

<pre>
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
>> 10
Confirm Mode? [Y/n] <b>Y</b>
Read PGS information from command line(Y) or a file(n). [Y/n] <b>n</b>
Input file name: <b>setting_add_replicas</b>
</pre>

* Use "Show cluster information" of the script to check if the replicas are successfully added.

### Remove the Existing Replica
Remove the replicas on the existing machine, with the administrative script. Then, overall data load is flowed into the new machine. Follow the procedure below:

* Create a configuration file that will be used to remove replicas, in the following form
```
# CLUSTER PGS_ID
test_cluster 0
test_cluster 1
```
* Run the administrative script.
```
$ fab main
```
* Select "Uninstall PGS."
* Enter the location of the configuration file that you created previously, when you are prompted to input file name that is required to remove replicas. 
* Use "Show cluster information" of the script to check if the replicas are successfully removed.

## Gateway Replacement
First, you should install gateway on the new machine, and then remove the one from the existing machine. Once you add the gateway to the new machine, the data load is distributed over both the existing and the new machines. Then, remove the existing gateway so that the overall data load is flowed into the new machine only.

### Adding a new gateway
* Create a configuration file for the new gateway.
```
# GATEWAY CLUSTER GW_ID PM_NAME PM_IP PORT
test_cluster 1 testmachine01.dev 123.123.123.123 6000
test_cluster 2 testmachine02.dev 123.123.123.124 6000
```
* Run the administrative script.
* Select "Install GW."
* Enter the location of the configuration file that you created previously, when you are prompted to input file name that is required to install the gateway.
* Use "Show cluster information" of the script to check if the gateway is successfully added to the new machine.

### Remove the existing gateway
*	Create a configuration file for the gateway to remove.
```
# GATEWAY CLUSTER GW_ID
test_cluster 1
test_cluster 2
```
*	Run the administrative script.
*	Select "Uninstall GW."
*	Enter the location of the configuration file that you created previously, when you are prompted to input file name that is required to uninstall the gateway.
*	Use "Show cluster information" of the script to check if the gateway is successfully removed from the existing machine.

## Configuration Master Replacement
First, you should terminate the Configuration Master processes on the existing machine, and then run the Configuration Master on the new machine.
*	Copy the Configuration Master to $NBASE_ARC_HOME/confmaster folder on the new machine.
*	Kill all the Configuration Master processes that are running on the existing machine.
```
kill -9 <confmaster pid>
```
*	Check if z-nodes used to manage Configuration Master are deleted from ZooKeeper. Check the folders, /RC/CC /LE and /RC/CC/FD. They should not be in those folders. If the z-nodes still exist in the folders even after you terminate all the Configuration Master processes, check again after a while.
```
$ zkCli.sh
zookeeper prompt$ ls /RC/CC/FD
[]
zookeeper prompt$ ls /RC/CC/LE
[]
```
*	Run the Configuration Master on the new machine.
```
$ ./confmaster.sh
```

## Zookeeper Replacement
To replace ZooKeeper, you should install new ZooKeeper first, add it to the existing ensemble and remove the existing ZooKeeper one by one from the ensemble.

### Add Zookeeper to New Machine
*	Check if all ZooKeepers in the ensemble are normally running. They should be composed of one leader and the other followers.
*	Add the addresses of the existing and the new ZooKeepers in the ZooKeeper configuration file of the new machine.
```
#Existing ZooKeepers
server.1=123.123.123.123:2881:3881
server.2=123.123.123.123:2882:3882
server.3=123.123.123.123:2883:3883

#New ZooKeepers
server.4=123.123.123.124:2884:3884
server.5=123.123.123.124:2885:3885
```
*	Run ZooKeepers on the new machine to add them to the existing ZooKeeper ensemble. 
*	Open the ZooKeeper configuration file of the existing machine, and add the ZooKeeper connection addresses of the new machine.
```
#New ZooKeeper
server.4=123.123.123.124:2884:3884
server.5=123.123.123.124:2885:3885
```
*	Restart ZooKeepers of the existing machine, one by one.

### Remove the Existing ZooKeeper
*	Check if ZooKeepers included in the ensemble are normally running. They should be composed of one leader and the other followers.
*	Terminate one of the existing ZooKeeper servers, open the configuration file of the rest of them, and restart the servers except the one that is terminated.
*	Repeat the previous step 2 for the existing ZooKeepers to terminate them all, so that only the ZooKeepers on the new machine can run.

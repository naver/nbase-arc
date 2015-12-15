# Fault Handling
* [Cluster Auto Repair](#cluster-auto-repair)
* [Configuration Master Repair](#configuration-master-repair)
* [Zookeeper Repair](#zookeeper-repair)	


## Cluster Auto Repair
You can automatically repair gateways and PGs, with the administrative script.

##### Run the administrative script.  

```$ fab main```

##### Select `Repair Cluster` and enter the name of cluster to repair.

You can also adjust backup interval of the repaired server during this process. Enter 4 as in the example below, and the servers to be repaired will back up the data 4 times a day (every 6 hour).

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
>> 13
Confirm Mode? [Y/n] <b>n</b>
Cluster information (CLUSTER) <b>your_cluster</b>
Cronsave number <b>4</b>
</pre>

The entire status of the cluster is displayed once you enter the cluster's name. You can repair the gateway first, and then the rest of the cluster. Type all to repair all gateways, or press Enter key if there is no gateway to repair.

<pre>
+-------+----------------+-------+-------+-----------+
| GW_ID |       IP       |  PORT | STATE | CLNT_CONN |
+-------+----------------+-------+-------+-----------+
|     1 | 10.100.100.100 |  6000 |  <i>F(F)</i> |         4 |
|     2 | 10.100.100.101 |  6000 |  N(N) |         4 |
+-------+----------------+-------+-------+-----------+
GW information ([GW_ID or all]) <b>all</b>
</pre>

When the gateway has been completely repaired, it is time to repair PG. Type all to repair all PGs, or type PG_ID to repair a specific PG only.

<pre>
+-------+--------------------+------+--------+------+----------------+-------+------+--------+
| PG_ID |        SLOT        | MGEN | PGS_ID | MGEN |       IP       |  PORT | ROLE | QUORUM |
+-------+--------------------+------+--------+------+----------------+-------+------+--------+
|     0 |             0:4095 |    3 |      0 |    3 | 10.100.100.100 |  7009 | <i>?(N)</i> |        |
|       |                    |      |     50 |    3 | 10.100.100.101 |  7009 | M(M) |      1 |
+-------+--------------------+------+--------+------+----------------+-------+------+--------+
|     1 |          4096:8191 |    3 |      1 |    3 | 10.100.100.100 |  7019 | <i>?(N)</i> |      1 |
|       |                    |      |     51 |    3 | 10.100.100.101 |  7019 | M(M) |        |
+-------+--------------------+------+--------+------+----------------+-------+------+--------+
PG information ([PG_ID or all]) <b>all</b>
</pre>

> **Caution**  
Even after auto repair is performed, a fault may occur again unless you find out the cause and fix it. For example, you should  check if there is a hardware problem in the machine before the auto repair, and fix it if any. Otherwise, the same problem may  occur again and cause service deterioration.

## Configuration Master Repair
Configuration Master is composed of multiple processes for availability. It performs two types of roles: leader and follower. Only one Configuration Master takes the role of leader, the others take the role of followers. When the leader is interrupted, one of the followers becomes a new leader for service availability.

### Clean Up Configuration Master Process
Sometimes a process is running even after the Configuration Master has been stopped (hanging process). You should check if there is a process running in the Configuration Master to repair, and kill it if there is.

##### Check the Configuration Master process  
Connect to the Configuration Master and check if it responds normally. If you failed to connect with Telnet or the ping test was not successful, the Configuration Master is not running.

<pre>
$ telnet <mgmt-ip> <mgmt-port>
Trying xxx.xxx.xxx.xxx...
Connected to localhost.localdomain (xxx.xxx.xxx.xxx).
Escape character is '^]'.
ping
{"state":"success",<b>"msg":"+PONG"</b>}
</pre>

##### Kill the Configuration Master process

Connect to the Configuration Master machine, and execute kill -9 to stop the Configuration Master process if it is still running. 

```$ kill -9 <mgmt pid>```

After killing the process, check if z-node used to manage Configuration Master is deleted from ZooKeeper. Check the folder, /RC/CC/FD. The name of z-node, server_ip:server_port, is made by combining the values of server_ip and server_port from cc.properties. 
If z-node still exists in the /RC/CC/FD folder even after you terminate all the Configuration Master processes, check again after a while.

### Start Configuration Master Process
The configuration information of Configuration Master is stored in ZooKeeper. Therefore, you can just restart the Configuration Master for repair.

```$ confmaster.sh```

##	Zookeeper Repair
Generally, you can just restart ZooKeeper to repair it. However, if the machine has a problem, refer to "Zookeeper Replacement"
Please refer to the ZooKeeper's guide in http://zookeeper.apache.org/ for how to repair ZooKeeper.

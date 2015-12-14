# Migration
* [Migration Plan](#migration-plan)
* [How to Perform Migration](#how-to-perform-migration)

## Migration Plan
Migration of nbase-arc is performed by SLOT, a unit of key distribution. A cluster contains 8192 SLOTs. Keys of all data 
are hashed into numbers from 0 to 8191 that are used as SLOT numbers. For each SLOT, a PG is assigned.  
This chapter describes nbase-arc migration process, with the following configuration example.
![MigrationPlan1](../images/admin/MigrationPlan1.png)
* PG 0 stores the data of SLOT 0 ~ 4095.
* PG 1 stores the data of SLOT 4095 ~ 8191.

Let's say, you want to add PG 2 so that some of the data load that was originally flowed into PG 0 and 
PG 1 can be migrated to PG 2. When this migration is finished, the configuration will be changed to the following figure.
![MigrationPlan2](../images/admin/MigrationPlan2.png)

## How to Perform Migration
* Run the administrative script
```
$ fab main
```
* Select "Migration", and enter the name of cluster to migrate.

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
>> 12
Confirm Mode? [Y/n] <b>n</b>
Cluster name? <b>your_cluster</b>
</pre>

The PG information of the cluster is displayed as follows.

<pre>
Show PG info? [Y/n] <b>Y</b>
+-------+--------------------+------+
| PG_ID |        SLOT        | MGEN |
+-------+--------------------+------+
|     0 |             0:4095 |    2 |
|     1 |          4096:8191 |    2 |
|     2 |                    |    2 |
+-------+--------------------+------+
</pre>

There are PG_IDs and SLOTs in the table above. SLOT is a unit of data storage, 
where all the data is distributed into SLOT 0 ~ 8191. In the example above, SLOT 0 ~ 4095 are stored in PG 0, 
and SLOT 4096 ~ 8191 in PG 1. There is no data in PG 2 yet, because no SLOT is allocated to it. 
We will transfer the data stored in PG 0 and PG 1 to PG 2.

At first, migrate the data of SLOT 3096 ~ 4095 from PG 0 to PG 2. You should enter Source PG ID, 
Destination PG ID, Slot Range From, Slot Range To, and TPS.

<pre>
Migration information(SRC_PG_ID DST_PG_ID RANGE_FROM RANGE_TO TPS) <b>0 2 3096 4095 50000</b>
</pre>

Then, the migration is performed as follows.

```
[nbase@127.0.0.2] copy_binary begin
[nbase@127.0.0.2] Executing task 'copy_binary'
[nbase@127.0.0.2] copy_binary end

PN PG MAP: 0 4096 1 4096
+-------------------+---------------------------+---------------------------+
|                   | SRC 127.0.0.1:7009        | DST 127.0.0.2:7009        |
+-------------------+---------------------------+---------------------------+
| KEY COUNT(before) | 2030651                   | 0                         |
+-------------------+---------------------------+---------------------------+

Command : migstart 3096-4095
[nbase@127.0.0.2] Executing task 'checkpoint_and_play'
cluster-util --getandplay 127.0.0.1 7009 127.0.0.2 7009 3096-4095 50000
[nbase@127.0.0.2] run: cluster-util --getandplay 127.0.0.1 7009 127.0.0.2 7009 3096-4095 50000
[nbase@127.0.0.2] out: [22917] 07 Jul 18:18:57.795 * Source replied to PING, getdump can continue...
[nbase@127.0.0.2] out: [22917] 07 Jul 18:18:59.299 * Checkpoint Sequence Number:1474126333865
[nbase@127.0.0.2] out: [22917] 07 Jul 18:18:59.299 * SOURCE <-> TARGET sync: receiving 17751438 bytes from source
[nbase@127.0.0.2] out: [22917] 07 Jul 18:18:59.456 * load 0% done
[nbase@127.0.0.2] out: [22917] 07 Jul 18:19:00.457 * load 9% done
... ...
[nbase@127.0.0.2] out: [22917] 07 Jul 18:19:09.463 * load 98% done
[nbase@127.0.0.2] out: [22917] 07 Jul 18:19:09.578 * getandplay success

Sequence : 1474126333865
Command : migrate start 127.0.0.2 7000 1474126333865 50000 8192 0 3095 1 1000 0 4096
Migrate info: +OK log:1474126333950 mig:1474126333865 buf:0 sent:0 acked:0

Log diff:85, Wait? [Y/n] 
```

"Log diff:N, Wait?" that is displayed during the migration means the remaining amount of data to be migrated. 
Enter n if N is smaller than the migration rate (50000 TPS is used for the migration rate in this example).

<pre>
Log diff:0, Wait? [Y/n] <b>Y</b>
Migrate info: +OK log:1474126334844 mig:1474126334844 buf:0 sent:0 acked:0

Log diff:0, Wait? [Y/n] <b>n</b>
</pre>

The migration proceeds as follows.

```
try mig2pc.
Command : mig2pc your_cluster 0 2 3096 4095
mig2pc success.
Command : migrate interrupt
Command : migend
[nbase@127.0.0.2] Executing task 'rangedel'
cluster-util --rangedel 127.0.0.1 7009 3096-4095 50000
[nbase@127.0.0.2] run: cluster-util --rangedel 127.0.0.1 7009 3096-4095 50000
[nbase@127.0.0.2] out: [23896] 07 Jul 18:32:42.909 * Source replied to PING, getdump can continue...
[nbase@127.0.0.2] out: [23896] 07 Jul 18:32:44.484 * Checkpoint Sequence Number:1474126334869
[nbase@127.0.0.2] out: [23896] 07 Jul 18:32:44.484 * SOURCE <-> TARGET sync: receiving 17751438 bytes from source
[nbase@127.0.0.2] out: [23896] 07 Jul 18:32:44.637 * delete 0% done
[nbase@127.0.0.2] out: [23896] 07 Jul 18:32:45.638 * delete 10% done
... ...
```

The following table is shown when the migration is successfully finished. 
It shows key information of migration source (SRC) and migration destination (DST). KEY COUNT(diff) shows that 
the number of keys are decreased by 507745 in SRC while increased by the same amount in DST. 
That is to say, the data of 507745 keys is migrated to PG 2.

```
[account@127.0.0.2] Rangedel success. SRC:127.0.0.1:7009, RANGE:3096-4095, TPS:50000

PN PG MAP: 0 3096 2 1000 1 4096
+-------------------+---------------------------+---------------------------+
|                   | SRC 127.0.0.1:7009        | DST 127.0.0.2:7009        |
+-------------------+---------------------------+---------------------------+
| KEY COUNT(before) | 2030651                   | 0                         |
| KEY COUNT(after)  | 1522906                   | 507745                    |
| KEY COUNT(diff)   | -507745                   | 507745                    |
+-------------------+---------------------------+---------------------------+
```

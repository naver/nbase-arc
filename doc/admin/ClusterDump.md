# Cluster Dump
* [Cluster Dump Overvew](#cluster-dump-overview)
* [Batch Job Configuration](#batch-job-configuration)
* [Output Format](#output-format)

## Cluster Dump Overview
Cluster dump backs up the cluster's data at the specified time, converts the backup data into the specified format 
and delivers it to the user. You can use the default data conversion method or specify your own conversion method by 
creating a plugin. The default method returns the data in JSON format.
nbase-arc's batch daemon provides the cluster dump function by performing the following jobs: periodic cluster backup, 
log play to follow the time specified by the user, data conversion into JSON format and delivery of the converted data 
to the user.

## Batch Job Configuration
The batch daemon gets the synchronized information on the batch jobs registered in the Configuration Master, 
and performs them at the specified time. Therefore, you can perform the cluster dump by registering jobs.
Regarding the cluster dump function, you can execute 3 commands in the Configuration Master: appdata_set, appdata_get, and appdata_del.

### appdata_set
This command is used to register a new cluster dump job.
```
appdata_set <cluster_name> backup <backup_id> <daemon_id> <period> <base_time> <holding period(day)> <net_limit(MB/s)> <output_format> [<service url>]

Example>
appdata_set test_cluster backup 1 1 0 2 * * * * 02:00:00 3 70 base32hex
```
* **cluster_name**: The name of cluster for which a job is performed
* **backup_id**: A unique integer ID for each job. You can use appdata_get to check IDs already registered.
* **daemon_id**: Enter 1. This argument is currently not supported, but it will be used later when the function to distribute jobs over multiple batch daemons is implemented. 
* **period (cron format)**: Enter the period for the batch daemon to perform a job, in CRON format. The batch daemon performs the cluster dump job periodically, based on this value.
*	**base_time**: Base time for backup. With the cluster dump, you can backs up the status of cluster during the specified time period through periodic dump file and log play. Enter this value in HH:MM:SS format.
*	**holding_period**: Retention period to store data (unit: day).
*	**net_limit(MB/s)**: Limitation of network transmission rate (MB/s). The batch daemon uses network while dumping the data of cluster, and may influence the actual service load. This limit value is used to minimize such influence.
*	**output_format**: Output format of cluster data. The default format is base32hex, which outputs cluster data in JSON format and binary data encoded in base32hex. For more information, refer to "8.3 Default Output Format, JSON (base32hex)."
*	**[service url]**: URL to deliver the result of cluster dump to the user. If you leave out this argument, the result is stored in the machine where the batch daemon is running. You can enter proper arguments depending on your delivery method, as follows.

```
<service url> example
rsync –az {FILE_PATH} 111.111.222.222::target_directory/{MACHINE_NAME}-{CLUSTER_NAME}-{DATE}.jsonNote

Note
Pre-defined arguments of <service_url>: 
- {FILE_PATH}: Path of the result file
- {MACHINE_NAME}: Host name of the machine where the result is stored.
- {CLUSTER_NAME}: Name of cluster to dump
- {DATE}: Current date and time
```

### appdata_get
This command is used to check cluster dump jobs that have been already registered.
```
appdata_get <cluster_name> backup <backup_id>

Example>
appdata_get test_cluster backup all
```
Enter all, instead of an integer ID for <backup_id>, to output all the jobs registered in the cluster.

### appdata_del
This command is used to delete a job.
```
appdata_del <cluster_name> backup <backup_id>

Example>
appdata_del test_cluster backup 1
```

## Output Format
The default output format of cluster dump is base32hex, which means that a binary file is encoded in base32hex and output in a JSON format. 
One Redis object is expressed as one JSON character string, and no line feed (\n) or carriage return (\r) character is used. Therefore, the output file includes several lines of JSON character strings where each line expresses a single Redis object. The output result is sorted in ascending order of keys.

Binary data is encoded in base32hex and expressed as text. All character strings used in Redis should be handled as binary data because it is binary safe. The binary data is encoded in base32hex so that it can be output in JSON format.
The following data types are base32hex encoded.
* All REDIS Object: key
* String type: value
* Hash type: field, value
* List type: value
* Set type: member
* Sorted Set type: member
* TripleS (SSS) type: ks (keyspace), field, name, data

```
<Example of output result>
{"key":"64======","expire":-1,"type":"string","value":"64======"}
{"key":"68======","expire":-1,"type":"string","value":"68======"}
{"key":"6c======","expire":-1,"type":"string","value":"6c======"}
```

### String
```
{"key":"64======","expire":-1,"type":"string","value":"64======"}
{"key":"68======","expire":1391417698967,"type":"string","value":"68======"}
```
* key: base32hex encoding
* expire: epoch time (millisecond), -1 if not specified.
* type: "string"
* value: base32hex encoding

### Hash
```
{
   "key":"8pm48sq7btq7is35btk62sr8",
   "expire":-1,
   "type":"hash",
   "value":[
      {
         "hkey":"adl4onrb64======",
         "hval":"apog===="
      },
      {
         "hkey":"8dbm4tivdcp0====",
         "hval":"cdhm2ug="
      }
   ]
}
```
* key: base32hex encoding
* expire: epoch time (millisecond), -1 if not specified. 
* type: "hash"
* value: JSON array of object
  * hkey (hash key): base32hex encoding
  * hval (hash value): base32hex encoding

### List
```
{
   "key":"edfn8ubgclfmoqbjeg======",
   "expire":-1,
   "type":"list",
   "value":[
      "a5ag====",
      "a5ag====",
      "b13m8ko="
   ]
}
```
* key: base32hex encoding
* expire: epoch time (millisecond), -1 if not specified.
* type: "list"
* value: JSON array of object
  * value: base32hex encoding
  * Saves the values of list as an array, in the same order as they are listed.

### Set
```
{
   "key":"cdfn8ubgclfn6pbk",
   "expire":-1,
   "type":"set",
   "value":[
      "alnnatqveoog====",
      "c9m5uthi"
   ]
}
```
* key: base32hex encoding
* expire: epoch time (millisecond), -1 if not specified.
* type: "set"
* value: JSON array of string
  * base32hex encoding
  * Saves the members of set as an array (order does not matter in this case)

### Sorted Set
```
{
   "key":"alfn8ubgclfnksr5eg======",
   "expire":-1,
   "type":"zset",
   "value":[
      {
         "data":"9hfncc8=",
         "score":10
      },
      {
         "data":"9l45uthi",
         "score":20
      }
   ]
}
```
*	key: base32hex encoding
*	expire: epoch time (millisecond), -1 if not specified.
*	type: "zset"
*	value: JSON array of object
  *	data: base32hex encoding
  *	score: Score of the member
  *	The objects are output in the same order as the sorted set.

### TripleS
```
{
   "key":“”,
   "expire":-1,
   "type":"sss",
   "value":[
      {
         "ks":"58======",
         “field":“dhi76cpg6c======”,
         “name":"9hfncc8=",
         "mode":”list",
         "value":[
            {
               "data":"9l45uthi",
               "expire":1391137911130
            },
            {
               "data":"9l45uthi",
               "expire":1391137911130
            }
         ]
      }
   ]
}
```
*	key: base32hex encoding
*	expire: epoch time (millisecond), -1 if not specified.
*	type: "sss"
*	value: JSON array of object
  *	ks (keyspace): base32hex encoding
  *	field: base32hex encoding
  *	name: base32hex encoding
  *	mode: Set of List mode
  *	value: JSON array of object
  *	data: base32hex encoding
  *	expire: epoch time (millisecond), -1 if not specified.

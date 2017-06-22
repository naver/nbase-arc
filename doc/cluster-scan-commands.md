## Cluster Scanning
nBase-ARC supports two kind of scan commands, SCAN and CSCAN. SCAN command is the same as Redis' SCAN and supports for sequential scanning of a whole cluster. CSCAN command supports for parallel scanning of a cluster.

There is a limitation of these scanning commands. During data migration between partitions, nBase-ARC's scanning commands may miss some keys. Therefore, nBase-ARC supports for CSCANDIGEST command to check whether data migration happens during scanning.

### SCAN
`SCAN <cursor> [MATCH pattern] [COUNT count]`

nBase-ARC's SCAN command is the same as Redis' SCAN command.

### CSCAN

#### CSCANLEN Command
`CSCANLEN`

CSCANLEN Command returns nbase-arc's internal partition size. You can CSCAN each partition in parallel.
```
<Example>

CSCANLEN
:8
```

#### CSCAN Command
`CSCAN <Partition ID> <cursor> [MATCH pattern] [COUNT count]`

CSCAN Command is similar to Redis's SCAN command. But, it requires `Partition ID` to scan specific partition of nbase-arc.
`Partition ID` is integer ranging from "0" to "total partition count - 1". You can get total partition count with CSCANLEN command.
```
<Example>

CSCAN 0 0
*2
$4
3072
*10
$14
asdfasdfasdfaf
...
...
```

#### CSCANDIGEST Command
`CSCANDIGEST`

CSCANDIGEST Command returns nBase-arc's configuration in encoded string. If data migration happens, this value changes. You can check return value of CSCANDIGEST before and after scanning. If return value changes, you may miss some keys during scanning cluster.

```
<Example>

CSCANDIGEST
$8
MCA4MTky
```

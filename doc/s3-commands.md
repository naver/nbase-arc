### S3 commands
Beside data structures (strings, hashes, sets etc.) supported by Redis, A data structure called triple-S (S3) is added.
S3 data structure  is a hierarchical map can be thought as following definition
  * `Map<string, Map<string, Map<string, collection>>>`

Each collection is list or set of Redis string values. and each string value have its own TTL (time to live)
Following notations are used for describing S3 commands
  * uuid: Redis key
  * ks  : First level key in S3
  * svc : second level key in S3
  * key : third level key in S3
  * val : value

In the case of Java API, ks is fixed with value '*' so S3 data structure can be thought as follows
  * `Map<string, Map<string, collection>>`
Following notations are used for describing Java API
  * key   : uuid
  * field : svc
  * name  : key
  * value : value

#### S3COUNT
  * Query: S3COUNT ks uuid
  * Return: return the number of svc key
  * Java API
    - slcount(key)
    - sscount(key)

#### S3EXPIRE
  * Query: S3EXPIRE ks uuid ttl
  * Return: returns 1 if at least one value is modified, 0 otherwise
  * Java API
    - slexpire(key, ttl)
    - ssexpire(key, ttl)

#### S3KEYS
  * Query: S3KEYS ks uuid
  * Return: returns array of svc
  * Java API
    - slkeys(key)
    - sskeys(key)

#### S3[LS]ADD
  * Query: S3[LS]ADD ks uuid svc key value ttl [value ttl ...]
  * Return: number of values added
  * Java API
    - sladd(key, field, name, value) 
    - sladd(key, field, name,ttl, values...)
    - ssadd(key, field, name, values...)
    - ssadd(key, field, name, ttl, values...)

#### S3[LS]ADDAT
  * Query: S3[LS]ADDAT ks uuid svc key value timestamp [value timestamp ...]
  * Return: number of values added
  * Java API
    - sladdAt(key, field, name, millisecondsTimestamp, values)
    - ssaddAt(key, field, name, millisecondsTimestamp, values)

#### S3[LS]COUNT
  * Query: S3[LS]COUNT ks uuid [svc [key [value]]]
  * Return: number of svc [key[value [same values]]]
  * Java API
    - slcount(key)
    - slcount(key, field)
    - slcount(key, field, name)
    - slcount(key, field, name, value)
    - sscount(key)
    - sscount(key, field)
    - sscount(key, field, name)
    - sscount(key, field, name, value)

#### S3[LS]EXISTS
  * Query: S3[LS]EXISTS ks uuid svc key [value]
  * Return: returns 1 if there exists a key [value], 0 otherwise
  * Java API
    - slexists(key, field, name)
    - slexists(key, field, name, value)
    - ssexists(key, field, name)
    - ssexists(key, field, name, value)

#### S3[LS]EXPIRE
  * Query: S3[LS]EXPIRE ks uuid ttl [svc [key [value]]]
  * Return: returns 1 if at least one value is modified, 0 otherwise
  * Java API
    - slexpire(key)
    - slexpire(key, field)
    - slexpire(key, field, name)
    - slexpire(key, field, name, value)
    - ssexpire(key)
    - ssexpire(key, field)
    - ssexpire(key, field, name)
    - ssexpire(key, field, name, value)

#### S3[LS]GET
  * Query: S3[LS]GET ks uuid svc key
  * Return: returns array of values 
  * Java API
    - slget(key, field, name)
    - ssget(key, field, name)

#### S3[LS]KEYS
  * Query: S3[LS]KEYS ks uuid [svc]
  * Return: returns array of svc [key]
  * Java API
    - slkeys(key, field)
    - slkeys(key, field, name)
    - sskeys(key, field)
    - sskeys(key, field, name)

#### S3[LS]MADD
  * Query: S3[LS]MADD ks uuid svc key value ttl [key value ttl...］
  * Return: returns the number of values added
  * Java API


#### S3[LS]MEXPIRE
  * Query: S3[LS]EXPIRE ks uuid svc ttl key [key ...]
  * Return: returns 1 if at list one value is modified, 0 otherwise
  * Java API
    - slmexpire(key, field,ttl, name, values...)
    - ssmexpire(key, field,ttl, name, values...)

#### S3[LS]MGET
  * Query: S3[LS]MGET ks uuid svc [key1 [key2 ...]]
  * Return: returns array of key, value pair. If there is no value for the given key then nil is returned.
  * Java API
    - slmget(key, field, name...)
    - ssmget(key, field, name...)

#### S3[LS]MREM
  * Query: S3[LS]MREM ks uuid svc key [key...]
  * Return: returns the number of values deleted
  * Java API
    - slmrem(key, field, names...)
    - ssmrem(key, field, names...)

#### S3[LS]REM
  * Query: S3[LS]REM ks uuid svc [key [value ...]]
  * Return: returns the number of keys [values[values matched]] deleted
  * Java API
    - slrem(key, field)
    - slrem(key, field, name)
    - slrem(key, field, name, value)
    - ssrem(key, field) 
    - ssrem(key, field, name)
    - ssrem(key, field, name, value)

#### S3[LS]REPLACE
  * Query: S3[LS]REPLACE ks uuid svc key oldvalue newvalue ttl
  * Return: returns 1 if modified, 0 otherwise
  * Java API

#### S3[LS]SET
  * Query: S3[LS]SET ks uuid svc key value ttl [value ttl ...]
  * Return: returns 1 if new key is added, 0 otherwise
  * Java API
    - slset(key, field, name,ttl, value)
    - slset(key, field, name,ttl, values...)
    - ssset(key, field, name,ttl, value) 
    - ssset(key, field, name,ttl, values...) 

#### S3[LS]TTL
  * Query: S3[LS]TTL ks uuid svc key [value]
  * Return: returns ttl of given value [first value], -1 otherwise
  * Java API
    - slttl(key, field, name)
    - slttl(key, field, name, value)
    - ssttl(key, field, name)
    - ssttl(key, field, name, value)

#### S3[LS]VALS 
  * Query: S3[LS]VALS ks uuid svc
  * Return: array of values
  * Java API
    - slvals(key, field)
    - ssvals(key, field) 

#### S3MREM
  * Query: S3MREM ks uuid svc [svc ...]
  * Return: returns the number of svc deleted
  * Java API

#### S3REM
  * Query: S3REM ks uuid
  * Return: returns 1 if ks is deleted, 0 otherwise
  * Java API
    - ssdel(key)
    - sldel(key)


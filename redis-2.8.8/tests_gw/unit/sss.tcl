start_server {tags {"sss"}} {
    test {SSS - s3lget} {
	r del uuid
    	r del uuid1
    	assert_error "*wrong number*" { r s3lget ks uuid }
    	r s3ladd ks uuid svc key val 10000
    	r s3ladd ks uuid svc key val2 10000
    	r s3ladd ks uuid svc key val1 10000
    	r s3ladd ks uuid svc key val 10000
    	r s3ladd ks1 uuid svc key val 10000
    	r s3ladd ks uuid1 svc key val 10000
    	assert_equal {val val2 val1 val} [r s3lget ks uuid svc key]
    	assert_equal {val} [r s3lget ks1 uuid svc key]
    	assert_equal {val} [r s3lget ks uuid1 svc key]
    	assert_equal {} [r s3lget nosuchks nosuchid key val]
    } 

    test {SSS - s3sget} {
	r del uuid
    	r del uuid1
    	assert_error "*wrong number*" { r s3sget ks uuid }
    	r s3sadd ks uuid svc key val 10000
    	r s3sadd ks uuid svc key val2 10000
    	r s3sadd ks uuid svc key val1 10000
    	r s3sadd ks uuid svc key val 10000
    	r s3sadd ks1 uuid svc key val 10000
    	r s3sadd ks uuid1 svc key val 10000
    	assert_equal {val val1 val2} [r s3sget ks uuid svc key]
    	assert_equal {val} [r s3sget ks1 uuid svc key]
    	assert_equal {val} [r s3sget ks uuid1 svc key]
    	assert_equal {} [r s3sget nosuchks nosuchid key val]
    }

    test {SSS - s3lmget} {
	r del uuid
    	assert_error "*wrong number*" { r s3lmget ks uuid }
    	r s3ladd ks uuid svc key1 val1 10000
    	r s3ladd ks uuid svc key2 val1 10000
    	r s3ladd ks uuid svc key3 val2 10000
    	r s3ladd ks uuid svc key4 val3 10000
    	r s3ladd ks uuid svc key5 val4 10000
    	assert_error "*wrong kind*" { r s3smget ks uuid svc }
    	assert_equal {key1 val1 key2 val1 key3 val2 key4 val3 key5 val4} [r s3lmget ks uuid svc]
    	assert_equal {key3 val2 key2 val1 nosuchkey {} key1 val1} [r s3lmget ks uuid svc key3 key2 nosuchkey key1]
    	assert_equal {} [r s3lmget ks uuid nosuchsvc]
    	assert_equal {keya {} keyb {} keyc {}} [r s3lmget ks uuid nosuchsvc keya keyb keyc]

    	r s3ladd ks uuid svc2 dupkey dupval 10000
    	r s3ladd ks uuid svc2 dupkey dupval 10000
    	r s3ladd ks uuid svc2 dupkey val2 10000
    	r s3ladd ks uuid svc2 dupkey val3 10000
    	r s3ladd ks uuid svc2 key2 val4 10000
    	assert_equal {dupkey dupval dupkey dupval dupkey val2 dupkey val3} [r s3lmget ks uuid svc2 dupkey]
    	assert_equal {dupkey dupval dupkey dupval dupkey val2 dupkey val3 key2 val4} [r s3lmget ks uuid svc2]
    	assert_equal {key2 val4 dupkey dupval dupkey dupval dupkey val2 dupkey val3} [r s3lmget ks uuid svc2 key2 dupkey]
    }


    test {SSS - s3smget } {
	r del uuid
        assert_error "*wrong number*" { r s3smget ks uuid }
    	r s3sadd ks uuid svc key1 val1 10000
    	r s3sadd ks uuid svc key2 val1 10000
    	r s3sadd ks uuid svc key3 val2 10000
    	r s3sadd ks uuid svc key4 val3 10000
    	r s3sadd ks uuid svc key5 val4 10000
    	assert_error "*wrong kind*" { r s3lmget ks uuid svc }
    	assert_equal {key1 val1 key2 val1 key3 val2 key4 val3 key5 val4} [r s3smget ks uuid svc]
    	assert_equal {key3 val2 key2 val1 nosuchkey {} key1 val1} [r s3smget ks uuid svc key3 key2 nosuchkey key1]
    	assert_equal {} [r s3smget ks uuid nosuchsvc]
    	assert_equal {keya {} keyb {} keyc {}} [r s3smget ks uuid nosuchsvc keya keyb keyc]

    	r s3sadd ks uuid svc2 dupkey dupval 10000
    	r s3sadd ks uuid svc2 dupkey dupval 10000
    	r s3sadd ks uuid svc2 dupkey val2 10000
    	r s3sadd ks uuid svc2 dupkey val3 10000
    	r s3sadd ks uuid svc2 key2 val4 10000
    	assert_equal {dupkey dupval dupkey val2 dupkey val3} [r s3smget ks uuid svc2 dupkey]
    	assert_equal {dupkey dupval dupkey val2 dupkey val3 key2 val4} [r s3smget ks uuid svc2]
    	assert_equal {key2 val4 dupkey dupval dupkey val2 dupkey val3} [r s3smget ks uuid svc2 key2 dupkey]
	assert_equal {key2 val4 key2 val4 key2 val4} [r s3smget ks uuid svc2 key2 key2 key2]
    }

    test {SSS - SET and LIST mode check} {
	r del uuid
	r s3ladd ks uuid svc key val 10000
	r s3ladd ks uuid svc key val 10000
	assert_equal {val val} [r s3lget ks uuid svc key]
	assert_error "*wrong kind of value*" { r s3sget ks uuid svc key }

	assert_equal {1} [r s3lrem ks uuid svc]
	assert_equal {1} [r s3sadd ks uuid svc key val 10000]
	assert_equal {0} [r s3sadd ks uuid svc key val 10000]
	assert_equal {val} [r s3sget ks uuid svc key]
    }

    test {SSS - s3lkeys} {
	r del uuid
    	assert_error "*wrong number*" { r s3lkeys ks uuid svc key}
    	r set uuid 100
    	assert_error "*wrong*" { r s3lkeys ks uuid svc }
    	r del uuid
    	r s3ladd ks uuid svc key4 val 100000
    	r s3ladd ks uuid svc key3 val 100000
    	r s3ladd ks uuid svc key1 val 100000
    	r s3ladd ks uuid svc key2 val 100000
    	r s3ladd ks uuid svc key1 val 100000
    	assert_equal {key4 key3 key1 key2 key1} [r s3lkeys ks uuid svc]
	assert_error "*wrong kind*" { r s3skeys ks uuid svc }

	
	r s3ladd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc2 key1 val 100000
	r s3ladd ks uuid svc2 key2 val 100000
	r s3ladd ks uuid svc2 key3 val 100000
	r s3ladd ks uuid svc3 key1 val 100000
	r s3ladd ks uuid svc4 key1 val 100000
	assert_equal {key1 key1 key1} [r s3lkeys ks uuid svc1]
	assert_equal {key1 key2 key3} [r s3lkeys ks uuid svc2]
	assert_equal {svc svc1 svc2 svc3 svc4} [r s3lkeys ks uuid]
	assert_equal {} [r s3skeys ks uuid]
	assert_error "*wrong kind*" { r s3skeys ks uuid svc2 }
    }

    test {SSS - s3skeys} {
	r del uuid
    	assert_error "*wrong number*" { r s3lkeys ks uuid svc key}
    	r set uuid 100
    	assert_error "*wrong*" { r s3lkeys ks uuid svc }
    	r del uuid
    	r s3sadd ks uuid svc key4 val 100000
    	r s3sadd ks uuid svc key3 val 100000
    	r s3sadd ks uuid svc key1 val 100000
    	r s3sadd ks uuid svc key2 val 100000
    	r s3sadd ks uuid svc key1 val 100000
    	assert_equal {key1 key2 key3 key4} [r s3skeys ks uuid svc]
	assert_error "*wrong kind*" { r s3lkeys ks uuid svc }

	r s3sadd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc2 key1 val 100000
	r s3sadd ks uuid svc2 key2 val 100000
	r s3sadd ks uuid svc2 key3 val 100000
	r s3sadd ks uuid svc3 key1 val 100000
	r s3sadd ks uuid svc4 key1 val 100000
	assert_equal {key1} [r s3skeys ks uuid svc1]
	assert_equal {key1 key2 key3} [r s3skeys ks uuid svc2]
	assert_equal {svc svc1 svc2 svc3 svc4} [r s3skeys ks uuid]
	assert_equal {} [r s3lkeys ks uuid]
	assert_error "*wrong kind*" { r s3lkeys ks uuid svc2 }
    }

    test {SSS - s3lvals} {
	r del uuid
	assert_error "*wrong number*" { r s3lvals ks uuid }
	assert_equal {} [r s3lvals ks uuid svc]
    	r s3ladd ks uuid svc key1 val 100000
    	r s3ladd ks uuid svc key2 val 100000
    	r s3ladd ks uuid svc key1 val 100000
	assert_equal {val val val} [r s3lvals ks uuid svc]

	r del uuid
    	r s3ladd ks uuid svc key1 val1 100000
    	r s3ladd ks uuid svc key2 val2 100000
    	r s3ladd ks uuid svc key1 val3 100000
	assert_equal {val1 val2 val3} [r s3lvals ks uuid svc]
	assert_equal {} [r s3lvals ks uuid no_such_svc]
	assert_error "*wrong kind*" { r s3svals ks uuid svc }
    }

    test {SSS - s3svals} {
	r del uuid
	assert_error "*wrong number*" { r s3svals ks uuid }
	assert_equal {} [r s3svals ks uuid svc]
    	r s3sadd ks uuid svc key1 val 100000
    	r s3sadd ks uuid svc key2 val 100000
    	r s3sadd ks uuid svc key1 val 100000
	assert_equal {val val} [r s3svals ks uuid svc]

	r del uuid
    	r s3sadd ks uuid svc key1 val1 100000
    	r s3sadd ks uuid svc key2 val2 100000
    	r s3sadd ks uuid svc key1 val3 100000
	assert_equal {val1 val3 val2} [r s3svals ks uuid svc]
	assert_equal {} [r s3svals ks uuid no_such_svc]
	assert_error "*wrong kind*" { r s3lvals ks uuid svc }
    }

    test {SSS - s3ladd} {
	r del uuid
	assert_error "*wrong number*" { r s3ladd ks uuid svc key1 val1 100000 key2 val2 100000 val1 100000 }
	assert_error "*ttl value is not a integer or out of range*" { r s3ladd ks uuid svc key1 val1 notaint }
	assert_error "*ttl value is not a integer or out of range*" { r s3ladd ks uuid svc key1 val1 10000000000000000000000000000 }
	assert_equal {3} [r s3ladd ks uuid svc key1 val1 100000 val2 100000 val1 100000]
	assert_error "*wrong kind*" {r s3sget ks uuid svc key1}
	assert_equal {val1 val2 val1} [r s3lget ks uuid svc key1]
	assert_equal {3} [r s3ladd ks uuid svc key2 val 100000 val 100000 val2 100000]
	assert_equal {key2 val key2 val key2 val2 key1 val1 key1 val2 key1 val1} [r s3lmget ks uuid svc key2 key1]
	assert_equal {2} [r s3ladd ks uuid svc key1 val3 100000 val4 100000]
	assert_equal {val1 val2 val1 val3 val4} [r s3lget ks uuid svc key1]
	assert_error "*wrong kind*" { r s3sadd ks uuid svc key3 val1 100000 }

	# setting ttl forerver
	r s3ladd ks uuid svc forever value 0
	assert_equal {value} [r s3lget ks uuid svc forever]
    }

    test {SSS - s3sadd} {
	r del uuid
	assert_error "*wrong number*" { r s3sadd ks uuid svc key1 val1 100000 key2 val2 100000 val1 100000 }
	assert_error "*ttl value is not a integer or out of range*" { r s3sadd ks uuid svc key1 val1 notaint }
	assert_error "*ttl value is not a integer or out of range*" { r s3sadd ks uuid svc key1 val1 10000000000000000000000000000 }
	assert_equal {2} [r s3sadd ks uuid svc key1 val2 100000 val1 100000 val1 100000]
	assert_error "*wrong kind*" {r s3lget ks uuid svc key1}
	assert_equal {val1 val2} [r s3sget ks uuid svc key1]
	assert_equal {2} [r s3sadd ks uuid svc key2 val 100000 val 100000 val2 100000]
	assert_equal {key2 val key2 val2 key1 val1 key1 val2} [r s3smget ks uuid svc key2 key1]
	assert_equal {2} [r s3sadd ks uuid svc key1 val1 100000 val3 100000 val4 100000]
	assert_equal {val1 val2 val3 val4} [r s3sget ks uuid svc key1]
	assert_error "*wrong kind*" { r s3ladd ks uuid svc key3 val1 100000 }

	# setting ttl forerver
	r s3sadd ks uuid svc forever value 0
	assert_equal {value} [r s3sget ks uuid svc forever]
    }

    test {SSS - s3lmadd} {
	r del uuid
	assert_error "*wrong number*" { r s3lmadd ks uuid svc key1 val1 100000 key2 val2 100000 val1 100000 }
	assert_error "*ttl value is not a integer or out of range*" { r s3lmadd ks uuid svc key1 val1 notaint }
	assert_error "*ttl value is not a integer or out of range*" { r s3lmadd ks uuid svc key1 val1 10000000000000000000000000000 }
	assert_equal {3} [r s3lmadd ks uuid svc key1 val1 100000 key2 val2 100000 key1 val2 100000]
	assert_error "*wrong kind*" {r s3smget ks uuid svc key1, key2}
	assert_equal {key2 val2 key1 val1 key1 val2} [r s3lmget ks uuid svc key2 key1]
	assert_equal {3} [r s3lmadd ks uuid svc key2 val 100000 key2 val 100000 key2 val2 100000]
	assert_equal {key2 val2 key2 val key2 val key2 val2 key1 val1 key1 val2} [r s3lmget ks uuid svc key2 key1]
	assert_equal {2} [r s3lmadd ks uuid svc key1 val3 100000 key2 val4 100000]
	assert_equal {val1 val2 val3} [r s3lget ks uuid svc key1]
	assert_equal {val2 val val val2 val4} [r s3lget ks uuid svc key2]
	assert_error "*wrong kind*" { r s3sadd ks uuid svc key3 val1 100000 }

	# setting ttl forerver
	r s3lmadd ks uuid svc forever value 0
	assert_equal {value} [r s3lget ks uuid svc forever]
	assert_equal {0} [r s3lttl ks uuid svc forever]

    }

    test {SSS - s3smadd} {
	r del uuid
	assert_error "*wrong number*" { r s3smadd ks uuid svc key1 val1 100000 key2 val2 100000 val1 100000 }
	assert_error "*ttl value is not a integer or out of range*" { r s3smadd ks uuid svc key1 val1 notaint }
	assert_error "*ttl value is not a integer or out of range*" { r s3smadd ks uuid svc key1 val1 10000000000000000000000000000 }
	assert_equal {2} [r s3smadd ks uuid svc key1 val1 100000 key2 val2 100000 key1 val1 100000]
	assert_error "*wrong kind*" {r s3lmget ks uuid svc key1, key2}
	assert_equal {key2 val2 key1 val1} [r s3smget ks uuid svc key2 key1]
	assert_equal {1} [r s3smadd ks uuid svc key2 val 100000 key2 val 100000 key2 val2 100000]
	assert_equal {key2 val key2 val2 key1 val1} [r s3smget ks uuid svc key2 key1]
	assert_equal {2} [r s3smadd ks uuid svc key1 val3 100000 key2 val4 100000]
	assert_equal {val1 val3} [r s3sget ks uuid svc key1]
	assert_equal {val val2 val4} [r s3sget ks uuid svc key2]
	assert_error "*wrong kind*" { r s3ladd ks uuid svc key3 val1 100000 }

	# setting ttl forerver
	r s3smadd ks uuid svc forever value 0
	assert_equal {value} [r s3sget ks uuid svc forever]
	assert_equal {0} [r s3sttl ks uuid svc forever]
    }
    
    test {SSS - s3lrem} {
	r del uuid
	assert_error "*wrong number*" { r s3lrem ks }
	assert_error "*wrong number*" { r s3lrem ks uuid }
	r s3lmadd ks uuid svc key1 val1 100000 key1 val1 100000 key2 val1 100000
	r s3lmadd ks uuid svc key1 val2 100000 key1 val3 100000 key2 val2 100000
	assert_equal {4} [r s3lrem ks uuid svc key1]
	assert_equal {} [r s3lget ks uuid svc key1]
	assert_equal {val1 val2} [r s3lget ks uuid svc key2]

	r s3lmadd ks uuid svc key val 100000 key val 100000 key val2 100000
	assert_equal {2} [r s3lrem ks uuid svc key val]
	assert_equal {val2} [r s3lget ks uuid svc key]

	r s3lmadd ks uuid svc key val 100000 key val 100000
	assert_equal {val2 val val} [r s3lget ks uuid svc key]
	
	assert_equal {3} [r s3lrem ks uuid svc key]
	assert_equal {key2 val1 key2 val2} [r s3lmget ks uuid svc]
	
	r s3lmadd ks uuid svc2 key1 val1 100000 key2 val2 100000
	assert_equal {key1 val1 key2 val2} [r s3lmget ks uuid svc2]

	assert_equal {2} [r s3lrem ks uuid svc2]
	assert_equal {key2 val1 key2 val2} [r s3lmget ks uuid svc]

        r s3lmadd ks uuid svc2 key1 val1 100000 key1 val1 100000 key2 val1 100000
        assert_equal {2} [r s3lrem ks uuid svc2]

        r s3lmadd ks uuid svc2 key1 val1 100000 key1 val1 100000 key1 val2 100000 key2 val1 100000
        assert_equal {3} [r s3lrem ks uuid svc2 key1]

	assert_error "*wrong kind*" { r s3srem ks uuid svc }
	assert_equal {0} [ r s3srem ks uuid nosuchsvc ]
    }
    
    test {SSS - s3srem} {
	r del uuid
	assert_error "*wrong number*" { r s3srem ks }
	assert_error "*wrong number*" { r s3srem ks uuid }
	r s3smadd ks uuid svc key1 val1 100000 key1 val1 100000 key2 val1 100000
	r s3smadd ks uuid svc key1 val2 100000 key1 val3 100000 key2 val2 100000
	assert_equal {3} [r s3srem ks uuid svc key1]
	assert_equal {} [r s3sget ks uuid svc key1]
	assert_equal {val1 val2} [r s3sget ks uuid svc key2]

	r s3smadd ks uuid svc key val 100000 key val 100000 key val2 100000
	assert_equal {1} [r s3srem ks uuid svc key val]
	assert_equal {val2} [r s3sget ks uuid svc key]

	r s3smadd ks uuid svc key val 100000 key val 100000
	assert_equal {val val2} [r s3sget ks uuid svc key]
	
	assert_equal {2} [r s3srem ks uuid svc key]
	assert_equal {key2 val1 key2 val2} [r s3smget ks uuid svc]
	
	r s3smadd ks uuid svc2 key1 val1 100000 key2 val2 100000
	assert_equal {key1 val1 key2 val2} [r s3smget ks uuid svc2]

	assert_equal {2} [r s3srem ks uuid svc2]
	assert_equal {key2 val1 key2 val2} [r s3smget ks uuid svc]

        r s3smadd ks uuid svc2 key1 val1 100000 key1 val2 100000 key2 val1 100000
        assert_equal {2} [r s3srem ks uuid svc2]

        r s3smadd ks uuid svc2 key1 val1 100000 key1 val1 100000 key1 val2 100000 key2 val1 100000
        assert_equal {2} [r s3srem ks uuid svc2 key1]

	assert_error "*wrong kind*" { r s3lrem ks uuid svc }
	assert_equal {0} [ r s3lrem ks uuid nosuchsvc ]
    }
    
    test {SSS - s3lmrem} {
	r del uuid
	assert_error "*wrong number*" { r s3lmrem ks }
	assert_error "*wrong number*" { r s3lmrem ks uuid }
	assert_error "*wrong number*" { r s3lmrem ks uuid svc }

	r s3lmadd ks uuid svc key1 val1 100000 key1 val1 100000 key2 val1 100000
	r s3lmadd ks uuid svc key1 val2 100000 key1 val3 100000 key2 val2 100000
	assert_equal {6} [r s3lmrem ks uuid svc key1 key2]
	assert_equal {key1 {} key2 {}} [r s3lmget ks uuid svc key1 key2]

	r s3lmadd ks uuid svc key1 val1 100000 key1 val2 100000 key2 val1 100000
	r s3lmadd ks uuid svc2 key1 val1 100000 key2 val2 100000
	r s3lmadd ks uuid svc key3 val1 100000 key4 val1 100000
	assert_equal {key2 val1 key1 val1 key1 val2} [r s3lmget ks uuid svc key2 key1]
	assert_equal {key1 val1 key2 val2} [r s3lmget ks uuid svc2 key1 key2]
	assert_equal {key3 val1 key4 val1} [r s3lmget ks uuid svc key3 key4]

	assert_equal {3} [r s3lmrem ks uuid svc key2 key1]
	assert_equal {key1 val1 key2 val2} [r s3lmget ks uuid svc2 key1 key2]
	assert_equal {key3 val1 key4 val1} [r s3lmget ks uuid svc key3 key4]

	assert_error "*wrong kind*" { r s3smrem ks uuid svc key1 key2}
	assert_equal {0} [ r s3smrem ks uuid nosuchsvc key1 key2]
    }

    test {SSS - s3smrem} {
	r del uuid
	assert_error "*wrong number*" { r s3smrem ks }
	assert_error "*wrong number*" { r s3smrem ks uuid }
	assert_error "*wrong number*" { r s3smrem ks uuid svc }

	r s3smadd ks uuid svc key1 val1 100000 key1 val1 100000 key2 val1 100000
	r s3smadd ks uuid svc key1 val2 100000 key1 val3 100000 key2 val2 100000
	assert_equal {5} [r s3smrem ks uuid svc key1 key2]
	assert_equal {key2 {} key1 {}} [r s3smget ks uuid svc key2 key1]

	r s3smadd ks uuid svc key1 val1 100000 key1 val2 100000 key2 val1 100000
	r s3smadd ks uuid svc2 key1 val1 100000 key2 val2 100000
	r s3smadd ks uuid svc key3 val1 100000 key4 val1 100000
	assert_equal {key2 val1 key1 val1 key1 val2} [r s3smget ks uuid svc key2 key1]
	assert_equal {key1 val1 key2 val2} [r s3smget ks uuid svc2 key1 key2]
	assert_equal {key3 val1 key4 val1} [r s3smget ks uuid svc key3 key4]

	assert_equal {3} [r s3smrem ks uuid svc key2 key1]
	assert_equal {key1 val1 key2 val2} [r s3smget ks uuid svc2 key1 key2]
	assert_equal {key3 val1 key4 val1} [r s3smget ks uuid svc key3 key4]

	assert_error "*wrong kind*" { r s3lmrem ks uuid svc key1 key2}
	assert_equal {0} [ r s3lmrem ks uuid nosuchsvc key1 key2]
    }

    test {SSS - s3lset} {
	r del uuid
	assert_error "*wrong number*" { r s3lset ks uuid svc key }
	assert_error "*wrong number*" { r s3lset ks uuid svc key value}
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3lset ks uuid svc key value ttl}
	assert_equal {1} [r s3lset ks uuid svc key value 100000]
	assert_equal {0} [r s3lset ks uuid svc key value 100000]
	assert_equal {value} [r s3lget ks uuid svc key]
	r s3ladd ks uuid svc key val1 100000
	r s3ladd ks uuid svc key val4 100000
	r s3ladd ks uuid svc key val2 100000
	r s3ladd ks uuid svc key val3 100000
	assert_equal {value val1 val4 val2 val3} [r s3lget ks uuid svc key]
	assert_equal {0} [r s3lset ks uuid svc key newvalue 100000]
	assert_equal {newvalue} [r s3lget ks uuid svc key]

	# setting ttl forerver
	r s3lset ks uuid svc forever value 0
	assert_equal {value} [r s3lget ks uuid svc forever]
	assert_equal {0} [r s3lttl ks uuid svc forever]

	assert_equal {0} [r s3lset ks uuid svc key value1 0 value2 0]
	assert_equal {key value1 key value2} [r s3lmget ks uuid svc key]

    }
    
    test {SSS - s3sset} {
	r del uuid
	assert_error "*wrong number*" { r s3sset ks uuid svc key }
	assert_error "*wrong number*" { r s3sset ks uuid svc key value}
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3sset ks uuid svc key value ttl}
	assert_equal {1} [r s3sset ks uuid svc key value 100000]
	assert_equal {0} [r s3sset ks uuid svc key value 100000]
	assert_equal {value} [r s3sget ks uuid svc key]
	r s3sadd ks uuid svc key val1 100000
	r s3sadd ks uuid svc key val4 100000
	r s3sadd ks uuid svc key val2 100000
	r s3sadd ks uuid svc key val3 100000
	assert_equal {val1 val2 val3 val4 value} [r s3sget ks uuid svc key]
	assert_equal {0} [r s3sset ks uuid svc key newvalue 100000]
	assert_equal {newvalue} [r s3sget ks uuid svc key]

	# setting ttl forerver
	r s3sset ks uuid svc forever value 0
	assert_equal {value} [r s3sget ks uuid svc forever]
	assert_equal {0} [r s3sttl ks uuid svc forever]
    }
    
    test {SSS - s3lreplace} {
	r del uuid
	assert_error "*wrong number*" { r s3lreplace ks uuid svc key }
	assert_error "*wrong number*" { r s3lreplace ks uuid svc key value }
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3lreplace ks uuid svc key value value ttl}
	r s3lset ks uuid svc key oldval 100000
	assert_equal {1} [r s3lreplace ks uuid svc key oldval newval 100000]
	assert_equal {0} [r s3lreplace ks uuid svc key oldval newval 100000]
	assert_equal {newval} [r s3lget ks uuid svc key]
	
	r s3ladd ks uuid svc key val 100000
	r s3ladd ks uuid svc key val 100000
	r s3ladd ks uuid svc key val 100000
	r s3ladd ks uuid svc key val 100000
	assert_equal {1} [r s3lreplace ks uuid svc key val newval2 100000]
	assert_equal {newval newval2} [r s3lget ks uuid svc key]
    }

    test {SSS - s3sreplace} {
	r del uuid
	assert_error "*wrong number*" { r s3sreplace ks uuid svc key }
	assert_error "*wrong number*" { r s3sreplace ks uuid svc key value }
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3sreplace ks uuid svc key value value ttl}
	r s3sset ks uuid svc key oldval 100000
	assert_equal {1} [r s3sreplace ks uuid svc key oldval newval 100000]
	assert_equal {0} [r s3sreplace ks uuid svc key oldval newval 100000]
	assert_equal {newval} [r s3sget ks uuid svc key]
	
	r s3sadd ks uuid svc key val1 100000
	r s3sadd ks uuid svc key val1 100000
	r s3sadd ks uuid svc key val2 100000
	r s3sadd ks uuid svc key val2 100000
	assert_equal {1} [r s3sreplace ks uuid svc key val1 newval2 100000]
	assert_equal {newval newval2 val2} [r s3sget ks uuid svc key]
    }

    test {SSS - s3lcount} {
	r del uuid
	assert_error "*wrong number*" { r s3lcount ks uuid svc key value ttl}
	assert_error "*wrong number*" { r s3lcount ks uuid svc key value dummy dummy}
	assert_equal {0} [r s3lcount ks uuid svc]
	r s3ladd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc1 key2 val 100000
	r s3ladd ks uuid svc2 key1 val 100000
	r s3ladd ks uuid svc2 key2 val 100000
	r s3ladd ks uuid svc2 key2 val 100000
	assert_equal {key1 val key2 val} [r s3lmget ks uuid svc1 key1 key2]
	assert_equal {2} [r s3lcount ks uuid svc1]
	assert_equal {2} [r s3lcount ks uuid svc2]
	assert_equal {2} [r s3lcount ks uuid svc2 key2]
	assert_equal {1} [r s3lcount ks uuid svc2 key1]

	assert_equal {2} [r s3lrem ks uuid svc1]
	assert_equal {2} [r s3lrem ks uuid svc2]
	assert_equal {0} [r s3lcount ks uuid svc1]
	assert_equal {0} [r s3lcount ks uuid svc2]

	r s3ladd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc2 key1 val 100000
	r s3ladd ks uuid svc2 key2 val 100000
	r s3ladd ks uuid svc2 key2 val 100000
	r s3ladd ks uuid svc2 key3 val 100000
	r s3ladd ks uuid svc3 key1 val 100000
	r s3ladd ks uuid svc4 key1 val 100000
	assert_equal {3} [r s3lcount ks uuid svc1 key1 val]
	assert_equal {2} [r s3lcount ks uuid svc2 key2 val]
	assert_equal {3} [r s3lcount ks uuid svc1 key1]
	assert_equal {1} [r s3lcount ks uuid svc2 key1]
	assert_equal {3} [r s3lcount ks uuid svc2]
	assert_equal {1} [r s3lcount ks uuid svc3]
	assert_equal {4} [r s3lcount ks uuid]
	assert_equal {0} [r s3scount ks uuid]
    }

    test {SSS - s3scount} {
	r del uuid
	assert_error "*wrong number*" { r s3scount ks uuid svc key value ttl}
	assert_error "*wrong number*" { r s3scount ks uuid svc key value dummy dummy}
	assert_equal {0} [r s3scount ks uuid svc]
	r s3sadd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc1 key2 val 100000
	r s3sadd ks uuid svc2 key1 val 100000
	r s3sadd ks uuid svc2 key2 val 100000
	r s3sadd ks uuid svc2 key2 val 100000
	assert_equal {key1 val key2 val} [r s3smget ks uuid svc2 key1 key2]
	assert_equal {2} [r s3scount ks uuid svc1]
	assert_equal {2} [r s3scount ks uuid svc2]
	assert_equal {1} [r s3scount ks uuid svc2 key2]
	assert_equal {1} [r s3scount ks uuid svc2 key1]

	assert_equal {2} [r s3srem ks uuid svc1]
	assert_equal {2} [r s3srem ks uuid svc2]
	assert_equal {0} [r s3scount ks uuid svc1]
	assert_equal {0} [r s3scount ks uuid svc2]

	r s3sadd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc2 key1 val1 100000
	r s3sadd ks uuid svc2 key1 val2 100000
	r s3sadd ks uuid svc2 key2 val 100000
	r s3sadd ks uuid svc2 key2 val 100000
	r s3sadd ks uuid svc2 key3 val 100000
	r s3sadd ks uuid svc3 key1 val 100000
	r s3sadd ks uuid svc4 key1 val 100000
	assert_equal {1} [r s3scount ks uuid svc1 key1 val]
	assert_equal {1} [r s3scount ks uuid svc2 key2 val]
	assert_equal {1} [r s3scount ks uuid svc1 key1]
	assert_equal {2} [r s3scount ks uuid svc2 key1]
	assert_equal {3} [r s3scount ks uuid svc2]
	assert_equal {1} [r s3scount ks uuid svc3]
	assert_equal {4} [r s3scount ks uuid]
	assert_equal {0} [r s3lcount ks uuid]
    }

    test {SSS - s3lexists} {
	r del uuid
	assert_error "*wrong number*" { r s3lexists ks uuid svc }
	assert_error "*wrong number*" { r s3lexists ks uuid svc key value value2}

	r s3ladd ks uuid svc key val 100000
	assert_equal {0} [r s3lexists ks uuid svc key val2]
	assert_equal {1} [r s3lexists ks uuid svc key val]
	assert_equal {0} [r s3lexists ks uuid svc key2]
	assert_equal {1} [r s3lexists ks uuid svc key]

	r s3lrem ks uuid svc key
	assert_equal {0} [r s3lexists ks uuid svc key val2]
	assert_equal {0} [r s3lexists ks uuid svc key val]
	assert_equal {0} [r s3lexists ks uuid svc key2]
	assert_equal {0} [r s3lexists ks uuid svc key]
    }

    test {SSS - s3sexists} {
	r del uuid
	assert_error "*wrong number*" { r s3sexists ks uuid svc }
	assert_error "*wrong number*" { r s3sexists ks uuid svc key value value2}

	r s3sadd ks uuid svc key val 100000
	assert_equal {0} [r s3sexists ks uuid svc key val2]
	assert_equal {1} [r s3sexists ks uuid svc key val]
	assert_equal {0} [r s3sexists ks uuid svc key2]
	assert_equal {1} [r s3sexists ks uuid svc key]

	r s3srem ks uuid svc key
	assert_equal {0} [r s3sexists ks uuid svc key val2]
	assert_equal {0} [r s3sexists ks uuid svc key val]
	assert_equal {0} [r s3sexists ks uuid svc key2]
	assert_equal {0} [r s3sexists ks uuid svc key]
    }

    test {SSS - s3lexpire} {
	r del uuid
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3lexpire ks uuid svc }
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3lexpire ks uuid svc key value 100000}
	
	r s3ladd ks uuid svc key1 val1 10000
	r s3ladd ks uuid svc key1 val2 10000
	r s3ladd ks uuid svc key2 val1 10000
	r s3ladd ks uuid svc key2 val2 10000
	r s3ladd ks uuid svc key2 val2 10000
	after 50
	assert {[r s3lttl ks uuid svc key1 val1] <= 9950}
	assert_equal {1} [r s3lexpire ks uuid 1000000 svc key1 val1]
	assert {[r s3lttl ks uuid svc key1 val1] > 9950}

	assert {[r s3lttl ks uuid svc key1 val2] <= 9950}
	assert_equal {1} [r s3lexpire ks uuid 1000000 svc key1]
	assert {[r s3lttl ks uuid svc key1 val2] > 9950}
	
	assert {[r s3lttl ks uuid svc key2 val2] <= 9950}
	assert_equal {1} [r s3lexpire ks uuid 1000000 svc key2 val2]
	assert {[r s3lttl ks uuid svc key2 val2] > 9950}

	assert {[r s3lttl ks uuid svc key2 val1] <= 9950}
	assert_equal {1} [r s3lexpire ks uuid 1000000 svc]
	assert {[r s3lttl ks uuid svc key2 val1] > 9950}

	assert_equal {0} [r s3lexpire ks uuid 1000000 svc2]
	assert_equal {0} [r s3lexpire ks uuid 1000000 svc3 key1]
	assert_equal {0} [r s3lexpire ks uuid 1000000 svc3 key1 value]

	r del uuid
	r s3ladd ks uuid svc1 key1 val1 10000
	r s3ladd ks uuid svc1 key1 val2 10000
	r s3ladd ks uuid svc2 key2 val1 10000
	r s3ladd ks uuid svc3 key2 val2 10000
	r s3ladd ks uuid svc4 key2 val2 10000
	after 50
	assert {[r s3lttl ks uuid svc1 key1 val1] <= 9950}
	assert_equal {1} [r s3lexpire ks uuid 1000000]
	assert {[r s3lttl ks uuid svc1 key1 val1] > 9950}
	assert {[r s3lttl ks uuid svc1 key1 val2] > 9950}
	assert {[r s3lttl ks uuid svc2 key2 val1] > 9950}
	assert {[r s3lttl ks uuid svc3 key2 val2] > 9950}
	assert {[r s3lttl ks uuid svc4 key2 val2] > 9950}

	r del uuid
	r s3ladd ks uuid svc1 key1 val1 10000
	r s3ladd ks uuid svc1 key1 val2 10000
	r s3ladd ks uuid svc2 key2 val1 10000
	r s3ladd ks uuid svc3 key2 val2 10000
	r s3ladd ks uuid svc4 key2 val2 10000
	after 50
	assert {[r s3lttl ks uuid svc1 key1 val1] <= 9950}
	assert_equal {0} [r s3sexpire ks uuid 1000000]
	assert {[r s3lttl ks uuid svc1 key1 val1] <= 9950}
	assert {[r s3lttl ks uuid svc1 key1 val2] <= 9950}
	assert {[r s3lttl ks uuid svc2 key2 val1] <= 9950}
	assert {[r s3lttl ks uuid svc3 key2 val2] <= 9950}
	assert {[r s3lttl ks uuid svc4 key2 val2] <= 9950}

        # if svc type is mixed (list, set)
        r del uuid
        r s3ladd ks uuid svc1 key1 val1 10000
        r s3sadd ks uuid svc2 key1 val1 10000
        after 50
        assert {[r s3lttl ks uuid svc1 key1 val1] <= 9950}
        assert {[r s3sttl ks uuid svc2 key1 val1] <= 9950}
        assert_equal {1} [r s3lexpire ks uuid 1000000]
	assert {[r s3lttl ks uuid svc1 key1 val1] > 9950}
	assert {[r s3sttl ks uuid svc2 key1 val1] <= 9950}
        assert_equal {1} [r s3sexpire ks uuid 1000000]
	assert {[r s3lttl ks uuid svc1 key1 val1] > 9950}
	assert {[r s3sttl ks uuid svc2 key1 val1] > 9950}
    }

    test {SSS - s3sexpire} {
	r del uuid
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3sexpire ks uuid svc }
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3sexpire ks uuid svc key value 100000}
	
	r s3sadd ks uuid svc key1 val1 10000
	r s3sadd ks uuid svc key1 val2 10000
	r s3sadd ks uuid svc key2 val1 10000
	r s3sadd ks uuid svc key2 val2 10000
	r s3sadd ks uuid svc key2 val2 10000
	after 50
	assert {[r s3sttl ks uuid svc key1 val1] <= 9950}
	assert_equal {1} [r s3sexpire ks uuid 1000000 svc key1 val1]
	assert {[r s3sttl ks uuid svc key1 val1] > 9950}

	assert {[r s3sttl ks uuid svc key1 val2] <= 9950}
	assert_equal {1} [r s3sexpire ks uuid 1000000 svc key1]
	assert {[r s3sttl ks uuid svc key1 val2] > 9950}
	
	assert {[r s3sttl ks uuid svc key2 val2] <= 9950}
	assert_equal {1} [r s3sexpire ks uuid 1000000 svc key2 val2]
	assert {[r s3sttl ks uuid svc key2 val2] > 9950}

	assert {[r s3sttl ks uuid svc key2 val1] <= 9950}
	assert_equal {1} [r s3sexpire ks uuid 1000000 svc]
	assert {[r s3sttl ks uuid svc key2 val1] > 9950}

	assert_equal {0} [r s3sexpire ks uuid 1000000 svc2]
	assert_equal {0} [r s3sexpire ks uuid 1000000 svc3 key1]
	assert_equal {0} [r s3sexpire ks uuid 1000000 svc3 key1 value]

	r del uuid
	r s3sadd ks uuid svc1 key1 val1 10000
	r s3sadd ks uuid svc1 key1 val2 10000
	r s3sadd ks uuid svc2 key2 val1 10000
	r s3sadd ks uuid svc3 key2 val2 10000
	r s3sadd ks uuid svc4 key2 val2 10000
	after 50
	assert {[r s3sttl ks uuid svc1 key1 val1] <= 9950}
	assert_equal {1} [r s3sexpire ks uuid 1000000]
	assert {[r s3sttl ks uuid svc1 key1 val1] > 9950}
	assert {[r s3sttl ks uuid svc1 key1 val2] > 9950}
	assert {[r s3sttl ks uuid svc2 key2 val1] > 9950}
	assert {[r s3sttl ks uuid svc3 key2 val2] > 9950}
	assert {[r s3sttl ks uuid svc4 key2 val2] > 9950}

	r del uuid
	r s3sadd ks uuid svc1 key1 val1 10000
	r s3sadd ks uuid svc1 key1 val2 10000
	r s3sadd ks uuid svc2 key2 val1 10000
	r s3sadd ks uuid svc3 key2 val2 10000
	r s3sadd ks uuid svc4 key2 val2 10000
	after 50
	assert {[r s3sttl ks uuid svc1 key1 val1] <= 9950}
	assert_equal {0} [r s3lexpire ks uuid 1000000]
	assert {[r s3sttl ks uuid svc1 key1 val1] <= 9950}
	assert {[r s3sttl ks uuid svc1 key1 val2] <= 9950}
	assert {[r s3sttl ks uuid svc2 key2 val1] <= 9950}
	assert {[r s3sttl ks uuid svc3 key2 val2] <= 9950}
	assert {[r s3sttl ks uuid svc4 key2 val2] <= 9950}

        # if svc type is mixed (list, set)
        r del uuid
        r s3ladd ks uuid svc1 key1 val1 10000
        r s3sadd ks uuid svc2 key1 val1 10000
        after 50
        assert {[r s3lttl ks uuid svc1 key1 val1] <= 9950}
        assert {[r s3sttl ks uuid svc2 key1 val1] <= 9950}
        assert_equal {1} [r s3sexpire ks uuid 1000000]
	assert {[r s3lttl ks uuid svc1 key1 val1] <= 9950}
	assert {[r s3sttl ks uuid svc2 key1 val1] > 9950}
        assert_equal {1} [r s3lexpire ks uuid 1000000]
	assert {[r s3lttl ks uuid svc1 key1 val1] > 9950}
	assert {[r s3sttl ks uuid svc2 key1 val1] > 9950}
    }

    test {SSS - s3lttl} {
	r del uuid
	assert_error "*wrong number*" { r s3lttl ks uuid svc }
	assert_error "*wrong number*" { r s3lttl ks uuid svc key value value }

	r s3ladd ks uuid svc key value2 50000
	r s3ladd ks uuid svc key value 100000
	after 50
	assert {[r s3lttl ks uuid svc key value] <= 99950}
	assert {[r s3lttl ks uuid svc key value2] <= 49950}
	assert {[r s3lttl ks uuid svc key] <= 49950}

        r del uuid
        r s3ladd ks uuid svc key val 50
        after 100
        assert_equal {-1} [r s3lttl ks uuid svc key val]

	r s3ladd ks uuid svc forever value 0
	assert {[r s3lttl ks uuid svc forever value] == 0}
	assert {[r s3lttl ks uuid svc forever] == 0}
    }

    test {SSS - s3sttl} {
	r del uuid
	assert_error "*wrong number*" { r s3sttl ks uuid svc }
	assert_error "*wrong number*" { r s3sttl ks uuid svc key value value }

	r s3sadd ks uuid svc key value 50000
	r s3sadd ks uuid svc key value2 100000
	after 50
	assert {[r s3sttl ks uuid svc key value2] <= 99950}
	assert {[r s3sttl ks uuid svc key value] <= 49950}
	assert {[r s3sttl ks uuid svc key] <= 49950}

        r del uuid
        r s3sadd ks uuid svc key val 50
        after 100
        assert_equal {-1} [r s3sttl ks uuid svc key val]

	r s3sadd ks uuid svc forever value 0
	assert {[r s3sttl ks uuid svc forever value] == 0}
	assert {[r s3sttl ks uuid svc forever] == 0}
    }

    test {SSS - s3lmexpire} {
	r del uuid

	r s3lset ks uuid svc key1 val1 10000
	r s3lset ks uuid svc key1 val1 10000
	r s3lset ks uuid svc key2 val2 10000
	assert_equal {1} [r s3lmexpire ks uuid svc 100000 key1]
	assert {[r s3lttl ks uuid svc key2 val2] <= 10000}

	r del uuid

	assert_error "*wrong number*" { r s3lmexpire ks uuid svc }
	assert_error "*wrong number*" { r s3lmexpire ks uuid svc 100000 }
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3lmexpire ks uuid svc key value 100000}
	
	r s3ladd ks uuid svc key1 val1 10000
	r s3ladd ks uuid svc key1 val2 10000
	r s3ladd ks uuid svc key2 val1 10000
	r s3ladd ks uuid svc key2 val2 10000
	r s3ladd ks uuid svc key2 val2 10000
	after 50
	assert {[r s3lttl ks uuid svc key1 val1] <= 9950}
	assert {[r s3lttl ks uuid svc key1 val2] <= 9950}
	assert_equal {1} [r s3lmexpire ks uuid svc 100000 key1]
	assert {[r s3lttl ks uuid svc key1 val1] > 9950}
	assert {[r s3lttl ks uuid svc key1 val2] > 9950}

	assert {[r s3lttl ks uuid svc key2 val1] <= 9950}
	assert {[r s3lttl ks uuid svc key2 val2] <= 9950}
	assert_equal {1} [r s3lmexpire ks uuid svc 1000000 key1 key2]
	assert {[r s3lttl ks uuid svc key1 val1] > 99500}
	assert {[r s3lttl ks uuid svc key1 val2] > 99500}
	assert {[r s3lttl ks uuid svc key2 val1] > 99500}
	assert {[r s3lttl ks uuid svc key2 val2] > 99500}
	
	assert_equal {0} [r s3lexpire ks uuid 1000000 svc2]
	assert_equal {0} [r s3lexpire ks uuid 1000000 svc3 key1]
	assert_equal {0} [r s3lexpire ks uuid 1000000 svc3 key1 value]

	assert_error "*wrong kind*" { r s3smexpire ks uuid svc 100000 key1 }
    }

    test {SSS - s3smexpire} {
	r del uuid
	assert_error "*wrong number*" { r s3smexpire ks uuid svc }
	assert_error "*wrong number*" { r s3smexpire ks uuid svc 100000 }
	assert_error "*ERR ttl value is not a integer or out of range*" { r s3smexpire ks uuid svc key value 100000}
	
	r s3sadd ks uuid svc key1 val1 10000
	r s3sadd ks uuid svc key1 val2 10000
	r s3sadd ks uuid svc key2 val1 10000
	r s3sadd ks uuid svc key2 val2 10000
	r s3sadd ks uuid svc key2 val2 10000
	after 50
	assert {[r s3sttl ks uuid svc key1 val1] <= 9950}
	assert {[r s3sttl ks uuid svc key1 val2] <= 9950}
	assert_equal {1} [r s3smexpire ks uuid svc 100000 key1]
	assert {[r s3sttl ks uuid svc key1 val1] > 9950}
	assert {[r s3sttl ks uuid svc key1 val2] > 9950}

	assert {[r s3sttl ks uuid svc key2 val1] <= 9950}
	assert {[r s3sttl ks uuid svc key2 val2] <= 9950}
	assert_equal {1} [r s3smexpire ks uuid svc 1000000 key1 key2]
	assert {[r s3sttl ks uuid svc key1 val1] > 99500}
	assert {[r s3sttl ks uuid svc key1 val2] > 99500}
	assert {[r s3sttl ks uuid svc key2 val1] > 99500}
	assert {[r s3sttl ks uuid svc key2 val2] > 99500}
	
	assert_equal {0} [r s3sexpire ks uuid 1000000 svc2]
	assert_equal {0} [r s3sexpire ks uuid 1000000 svc3 key1]
	assert_equal {0} [r s3sexpire ks uuid 1000000 svc3 key1 value]

	assert_error "*wrong kind*" { r s3lmexpire ks uuid svc 100000 key1 }
    }

    test {SSS - s3rem} {
	r del uuid
	r del uuid2
	assert_error "*wrong number*" { r s3rem ks }
	r s3lmadd ks uuid svc1 key1 val1 100000 key1 val1 100000 key2 val1 100000
	assert_error "*wrong kind*" { r s3smadd ks uuid svc1 key1 val1 100000 }
	r s3smadd ks uuid svc2 key1 val1 100000 key2 val2 100000 key3 val3 100000

	assert_equal {1} [r s3rem ks uuid]
	assert_equal {} [r s3lkeys ks uuid svc1]
	assert_equal {} [r s3lvals ks uuid svc1]
	assert_equal {} [r s3skeys ks uuid svc2]
	assert_equal {} [r s3svals ks uuid svc2]

	r s3lmadd ks uuid svc2 key1 val1 100000 key1 val1 100000 key2 val1 100000
	r s3smadd ks uuid svc1 key1 val1 100000 key2 val2 100000 key3 val3 100000
	assert_equal {key1 key1 key2} [r s3lkeys ks uuid svc2]
	assert_equal {val1 val1 val1} [r s3lvals ks uuid svc2]
	assert_equal {key1 key2 key3} [r s3skeys ks uuid svc1]
	assert_equal {val1 val2 val3} [r s3svals ks uuid svc1]

	r s3lmadd ks uuid2 svc1 key1 val1 100000 key1 val1 100000 key1 val1 100000
	r s3lmadd ks uuid2 svc2 key1 val1 100000 key2 val2 100000 key2 val2 100000

	r s3lmadd ks2 uuid2 svc1 key1 val1 100000 key1 val1 100000 key1 val1 100000
	r s3lmadd ks2 uuid2 svc2 key1 val1 100000 key2 val2 100000 key2 val2 100000

#	assert_equal {2} [r s3rem ks uuid uuid2 nosuch_uuid]
#	assert_equal {} [r s3lkeys ks uuid svc1]
#	assert_equal {} [r s3lvals ks uuid svc1]
#	assert_equal {} [r s3skeys ks uuid svc2]
#	assert_equal {} [r s3svals ks uuid svc2]
#
#	assert_equal {} [r s3lkeys ks uuid2 svc1]
#	assert_equal {} [r s3lvals ks uuid2 svc1]
#	assert_equal {} [r s3skeys ks uuid2 svc2]
#	assert_equal {} [r s3svals ks uuid2 svc2]
#
#	assert_equal {key1 key1 key1} [r s3lkeys ks2 uuid2 svc1]
#	assert_equal {val1 val1 val1} [r s3lvals ks2 uuid2 svc1]
#	assert_equal {key1 key2 key2} [r s3lkeys ks2 uuid2 svc2]
#	assert_equal {val1 val2 val2} [r s3lvals ks2 uuid2 svc2]
    }

    test {SSS - s3mrem} {
	r del uuid
	assert_error "*wrong number*" { r s3mrem ks }
	assert_error "*wrong number*" { r s3mrem ks uuid }
	r s3lmadd ks uuid svc1 key1 val1 100000 key1 val1 100000 key2 val1 100000
	assert_error "*wrong kind*" { r s3smadd ks uuid svc1 key1 val1 100000 }
	r s3smadd ks uuid svc2 key1 val1 100000 key2 val2 100000 key3 val3 100000

	assert_equal {1} [r s3mrem ks uuid svc1 nosuchsvc]
	assert_equal {} [r s3lkeys ks uuid]
	assert_equal {svc2} [r s3skeys ks uuid]
	assert_equal {1} [r s3mrem ks uuid svc2]
	assert_equal {0} [r s3mrem ks uuid svc1 svc2]

	r s3lmadd ks uuid svc1 key1 val1 100000 key1 val1 100000 key2 val1 100000
	r s3smadd ks uuid svc2 key1 val1 100000 key2 val2 100000 key3 val3 100000
	assert_equal {2} [r s3mrem ks uuid svc1 svc2]
	assert_equal {} [r s3lkeys ks uuid]
	assert_equal {} [r s3skeys ks uuid]
    }

    test {SSS - s3keys} {
        r del uuid
        assert_error "*wrong number*" { r s3keys ks }
        assert_error "*wrong number*" { r s3keys ks uuid svc }
	r s3lmadd ks uuid svc1 key1 val1 100000 key1 val1 100000 key2 val1 100000
	r s3smadd ks uuid svc2 key1 val1 100000 key2 val2 100000 key3 val3 100000
        assert_equal {svc1 svc2} [r s3keys ks uuid]
        r s3lrem ks uuid svc1
        assert_equal {svc2} [r s3keys ks uuid]

        r del uuid
	r s3ladd ks uuid svc1 key1 val 100000
	r s3sadd ks uuid svc2 key1 val 100000
	r s3ladd ks uuid svc3 key3 val 100000
	r s3sadd ks uuid svc4 key1 val 100000
	r s3ladd ks uuid svc5 key1 val 100000
        assert_equal {svc1 svc2 svc3 svc4 svc5} [r s3keys ks uuid]

        r set uuid 10000
        assert_error "*wrong kind*" {r s3keys ks uuid}
    }

    test {SSS - s3count} {
	r del uuid
        assert_error "*wrong number*" { r s3count ks }
        assert_error "*wrong number*" { r s3count ks uuid svc }
	assert_equal {0} [r s3count ks uuid]
	r s3sadd ks uuid svc1 key1 val 100000
	r s3ladd ks uuid svc2 key2 val 100000
	r s3sadd ks uuid svc3 key1 val 100000
	r s3ladd ks uuid svc4 key2 val 100000
	r s3sadd ks uuid svc5 key2 val 100000
	assert_equal {2} [r s3lcount ks uuid]
	assert_equal {3} [r s3scount ks uuid]
	assert_equal {5} [r s3count ks uuid]
        r s3srem ks uuid svc1
	assert_equal {4} [r s3count ks uuid]
        r s3lrem ks uuid svc4
	assert_equal {3} [r s3count ks uuid]

        r set uuid 10000
        assert_error "*wrong kind*" {r s3count ks uuid}
    }

    test {SSS - s3expire} {
	r del uuid
	assert_error "*wrong number*" { r s3expire ks }
	assert_error "*wrong number*" { r s3expire ks uuid }
	assert_error "*wrong number*" { r s3expire ks uuid svc key }
	
	r s3ladd ks uuid svc1 key val 10000
	r s3sadd ks uuid svc2 key val 10000
	r s3ladd ks uuid svc3 key val 10000
	r s3sadd ks uuid svc4 key val 10000
	r s3ladd ks uuid svc5 key val 10000
	after 50
	assert {[r s3lttl ks uuid svc1 key val] <= 9950}
	assert {[r s3sttl ks uuid svc2 key val] <= 9950}
	assert {[r s3lttl ks uuid svc3 key val] <= 9950}
	assert {[r s3sttl ks uuid svc4 key val] <= 9950}
	assert {[r s3lttl ks uuid svc5 key val] <= 9950}
        assert_equal {1} [r s3expire ks uuid 100000]
	assert {[r s3lttl ks uuid svc1 key val] > 9950}
	assert {[r s3sttl ks uuid svc2 key val] > 9950}
	assert {[r s3lttl ks uuid svc3 key val] > 9950}
	assert {[r s3sttl ks uuid svc4 key val] > 9950}
	assert {[r s3lttl ks uuid svc5 key val] > 9950}
        assert_equal {1} [r s3lexpire ks uuid 8000]
	assert {[r s3lttl ks uuid svc1 key val] <= 8000}
	assert {[r s3sttl ks uuid svc2 key val] > 9950}
	assert {[r s3lttl ks uuid svc3 key val] <= 8000}
	assert {[r s3sttl ks uuid svc4 key val] > 9950}
	assert {[r s3lttl ks uuid svc5 key val] <= 8000}
        assert_equal {1} [r s3expire ks uuid 8000]
	assert {[r s3lttl ks uuid svc1 key val] <= 8000}
	assert {[r s3sttl ks uuid svc2 key val] <= 8000}
	assert {[r s3lttl ks uuid svc3 key val] <= 8000}
	assert {[r s3sttl ks uuid svc4 key val] <= 8000}
	assert {[r s3lttl ks uuid svc5 key val] <= 8000}
    }
}

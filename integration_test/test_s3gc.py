#
# Copyright 2015 Naver Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import testbase
import util
import time
import copy
import redis_sock
import config
import default_cluster
import random


class TestS3GC( unittest.TestCase ):
    cluster = config.clusters[0]

    @classmethod
    def setUpClass( cls ):
        cls.conf_checker = default_cluster.initialize_starting_up_smr_before_redis( cls.cluster )
        assert cls.conf_checker != None, 'failed to initialize cluster'

    @classmethod
    def tearDownClass( cls ):
        testbase.defaultTearDown(cls)

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestS3GC_%s' % self._testMethodName )
        server = self.cluster['servers'][0]
        self.redis = redis_sock.RedisClient(server['ip'], server['redis_port'])

    def tearDown( self ):
        if self.redis != None:
            self.redis.close()
        return 0

    def test_s3gc_works ( self ):
        redis = self.redis
        ok, prev_dbsize = redis.do_request('dbsize\r\n')
        assert (ok == True)

        # make 3 s3 object
        util.log('before make 3 s3 object')
        for i in range(0, 10000):
            ok, data = redis.do_request('s3sadd * key1 svc key%d val%d %d\r\n' % (i, i, 100))
            assert (ok == True)
            ok, data = redis.do_request('s3sadd * key2 svc key%d val%d %d\r\n' % (i, i, 100))
            assert (ok == True)
            ok, data = redis.do_request('s3sadd * key3 svc key%d val%d %d\r\n' % (i, i, 100))
            assert (ok == True)
        util.log('after make 3 s3 object')
        time.sleep (0.2)

        # force gc
        util.log('before force gc')
        tot_ret = 0
        for i in range(0, 8192):
            ok, ret = redis.do_request('s3gc 0\r\n')
            assert(ok == True)
            tot_ret = tot_ret + ret
        # we can not assert on the value of tot_ret (fixed rate gc can intervene)
        #util.log('after force gc')
        #util.log('tot_ret=%d' % tot_ret)

        ok, curr_dbsize = redis.do_request('dbsize\r\n')
        assert (ok == True)
        assert (curr_dbsize == prev_dbsize)

    def test_big_s3object ( self ):
        redis = self.redis

        # save current dbsize
        ok, prev_dbsize = redis.do_request('dbsize\r\n')
        assert (ok == True)

        # save and set current sss-gc-interval long enough
        ok, resp = redis.do_request('config get sss-gc-interval\r\n')
        assert (ok == True)
        prev_gc_interval = int(resp[1])

        ok, dummy = redis.do_request('config set sss-gc-interval 1000000\r\n')
        assert (ok == True)

        # make big s3 object (10000000 elements)
        # Note max inline request size is about 64k
        make_begin = int(round(time.time() * 1000))
        num_keys = 10000
        num_vals = 1000
        for i in range (0, num_keys):
            r = random.randint(0, 100000000)
            cmd = 's3sadd * uuid svc key%d' % r
            for i in range (0, num_vals):
                r = random.randint(0, 100000000)
                cmd = cmd + ' val%d 1' % r
            cmd = cmd + '\r\n'
            ok, r = redis.do_request(cmd)
            assert (ok == True)
        make_end = int(round(time.time() * 1000))

        # iterate all gc lines
        for i in range (0, 8192):
            st = int(round(time.time() * 1000))
            ok, r = redis.do_request('s3gc 0\r\n')
            assert (ok == True)
            ok, r = redis.do_request('ping\r\n')
            assert (ok == True)
            et = int(round(time.time() * 1000))
            assert (et - st < 500) # 500 msec is hard limit?

        #  wait for the big object purged and deleted
        limit = time.time() + (make_end - make_begin)/1000.0
        while time.time() < limit + 1:
            ok, curr_dbsize = redis.do_request('dbsize\r\n')
            assert (ok == True)
            if curr_dbsize == prev_dbsize:
                break
            time.sleep(0.5)

        # check number of keys are unchanged
        ok, curr_dbsize = redis.do_request('dbsize\r\n')
        assert (ok == True)
        assert (curr_dbsize == prev_dbsize)

        # restore gc-interval
        ok, dummy = redis.do_request('config set sss-gc-interval %d\r\n' % prev_gc_interval)
        assert (ok == True)

    def test_s3gc_eager_big_key_values(self):
        # opensource#182
        redis = self.redis
        ok, prev_dbsize = redis.do_request('dbsize\r\n')
        assert (ok == True)

        util.log('before make some s3 objects with lots of long living values')
        for i in range(0, 3000):
            ok, data = redis.do_request('s3sadd * key1 svc key val%d 1000000\r\n' % i)
            assert (ok == True)
            ok, data = redis.do_request('s3sadd * key2 svc key val%d 1000000\r\n' % i)
            assert (ok == True)
            ok, data = redis.do_request('s3sadd * key3 svc key val%d 1000000\r\n' % i)
            assert (ok == True)
        util.log('after make some s3 objects with lots of long living values')
        time.sleep (0.2)

        # iterate all gc lines
        for i in range (0, 8192):
            st = int(round(time.time() * 1000))
            ok, r = redis.do_request('s3gc 0\r\n')
            assert (ok == True)
            ok, r = redis.do_request('ping\r\n')
            assert (ok == True)
            et = int(round(time.time() * 1000))
            assert (et - st < 500) # 500 msec is hard limit?

        # we should see s3gc_eager_loops:0 eventually
        def get_s3gc_eager_loops():
            ok, data = redis.do_request("info stats\r\n")
            assert(ok == True)
            items = data.split('\r\n')
            for item in items:
                prefix = 's3gc_eager_loops:'
                if item.startswith(prefix):
                    return int(item[len(prefix):].strip())

        # we should see idle eager gc mode
        see_idle_gc = False
        for i in range(50):
            if get_s3gc_eager_loops() == 0:
                see_idle_gc = True
                break
            time.sleep(0.02)
        assert(see_idle_gc), get_s3gc_eager_loops()

        # check number of keys are unchanged
        redis.do_request('del key1\r\n')
        redis.do_request('del key2\r\n')
        redis.do_request('del key3\r\n')
        ok, curr_dbsize = redis.do_request('dbsize\r\n')
        assert (ok == True)
        assert (curr_dbsize == prev_dbsize)

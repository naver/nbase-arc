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

class TestBackgroundDelete( unittest.TestCase ):
    cluster = config.clusters[0]

    @classmethod
    def setUpClass( cls ):
        ret = default_cluster.initialize_starting_up_smr_before_redis( cls.cluster )
        if ret is not 0:
            default_cluster.finalize( cls.cluster )
        assert (ret == 0)
        return 0

    @classmethod
    def tearDownClass( cls ):
        ret = default_cluster.finalize( cls.cluster )
        assert (ret == 0)
        return 0

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestBackgroundDelete_%s' % self._testMethodName )
        server = self.cluster['servers'][0]
        self.redis = redis_sock.RedisClient(server['ip'], server['redis_port'])

    def tearDown( self ):
        if self.redis != None:
            self.redis.close()
        return 0

    def get_info(self, section, key):
        redis = self.redis
        ok, data = redis.do_request("info %s\r\n" % section)
        assert (ok == True)
        items = data.split('\r\n')
        assert (len(items) > 0)
        for item in items:
            if item.startswith(key):
                return item[len(key)+1:].strip()
        return None

    def test_config_get_set ( self ):
        redis = self.redis

        ok, data = redis.do_request("config get object-bio-delete-min-elems\r\n");
        val = int(data[1].strip())
        assert (ok == True and val > 0)

        ok, data = redis.do_request("config set object-bio-delete-min-elems %d\r\n" %  (val - 1))
        assert (ok == True)

        ok, data = redis.do_request("config get object-bio-delete-min-elems\r\n");
        newval = int(data[1].strip())
        assert (ok == True and newval == (val - 1))

        ok, data = redis.do_request("config set object-bio-delete-min-elems %d\r\n" %  val)
        assert (ok == True)
        redis.close()

    def test_bgdel_config_works (self):
        redis = self.redis

        # save original min-elems, delete_keys
        ok, data = redis.do_request("config get object-bio-delete-min-elems\r\n");
        min_elem_saved = int(data[1].strip())
        del_keys = int(self.get_info('stats', 'background_deleted_keys'))

        # min-elems 0 means no background delete
        min_elems = 0
        ok, data = redis.do_request("config set object-bio-delete-min-elems %d\r\n" % min_elems)
        assert (ok == True)
        for i in range (0, 10000): # big enough
            ok, data = redis.do_request("lpush list_key 1\r\n")
            assert (ok == True)
        ok, data = redis.do_request("del list_key\r\n")
        assert (ok == True)
        time.sleep(1)
        assert (del_keys == int(self.get_info('stats', 'background_deleted_keys')))

        # set min-elems to 10000
        min_elems = 10000
        ok, data = redis.do_request("config set object-bio-delete-min-elems %d\r\n" % min_elems)
        assert (ok == True)

        # key that is not a target
        for i in range (0, min_elems - 1):
            ok, data = redis.do_request("lpush list_key 1\r\n")
            assert (ok == True)
        ok, data = redis.do_request("del list_key\r\n")
        assert (ok == True)
        time.sleep(1)
        assert (del_keys == int(self.get_info('stats', 'background_deleted_keys')))

        # key that is a target
        for i in range (0, min_elems):
            ok, data = redis.do_request("lpush list_key 1\r\n")
            assert (ok == True)
        ok, data = redis.do_request("del list_key\r\n")
        assert (ok == True)
        time.sleep(1) # wait sufficient time (delete in background)
        assert ((del_keys + 1) == int(self.get_info('stats', 'background_deleted_keys')))

        # restore min-elems
        min_elems = min_elem_saved
        ok, data = redis.do_request("config set object-bio-delete-min-elems %d\r\n" % min_elems)
        assert (ok == True)

    def test_response_time (self):
        redis = self.redis

        # save deleted keys
        del_keys = int(self.get_info('stats', 'background_deleted_keys'))

        # make big keys
        list_elems = '1 2 3 4 5 6 1 2 3 4 5 6 1 2 3 4 5 6 1 2 3 4 5 6 1 2 3 4 5 6'
        for i in range (0, 10000):
            ok, data = redis.do_request('hset key_hash key%d value%d\r\n' % (i, i))
            assert (ok == True)
            ok, data = redis.do_request('zadd key_zset %d value%d\r\n' % (i, i))
            assert (ok == True)
            ok, data = redis.do_request('lpush key_list %s\r\n' % list_elems)
            assert (ok == True)
            ok, data = redis.do_request('lpush key_list %s\r\n' % list_elems)
            assert (ok == True)
            ok, data = redis.do_request('lpush key_list %s\r\n' % list_elems)
            assert (ok == True)
            ok, data = redis.do_request('sadd key_set elem%d\r\n' % i)
            assert (ok == True)
            ok, data = redis.do_request('s3sadd * key_sss svc key%d val%d 1000000\r\n' % (i, i))
            assert (ok == True)

        # delete keys
        delete_begin = int(round(time.time() * 1000))

        ok, data = redis.do_request('del key_hash\r\n')
        assert (ok == True)
        ok, data = redis.do_request('del key_zset\r\n')
        assert (ok == True)
        ok, data = redis.do_request('del key_list\r\n')
        assert (ok == True)
        ok, data = redis.do_request('del key_set\r\n')
        assert (ok == True)
        ok, data = redis.do_request('del key_sss\r\n')
        assert (ok == True)

        delete_end = int(round(time.time() * 1000))
        assert (delete_end - delete_begin < 10) # 10 msec should suffice

        # wait background delete completes
        count = 0
        del_keys_now = 0
        while count < 20: # wait at most about 10 sec. (0.5*20)
            del_keys_now = int(self.get_info('stats', 'background_deleted_keys'))
            if del_keys + 5 == del_keys_now:
                break
            time.sleep(0.5)
            count = count + 1
        assert(del_keys + 5 == del_keys_now)

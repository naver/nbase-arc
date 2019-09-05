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
import redis_sock
import config
import default_cluster


# NOTE
# oom state persistence test is done by hand (hard to automate in this test framework)
#
class TestDenyOOM( unittest.TestCase ):
    cluster = config.clusters[0]
    leader_cm = config.clusters[0]['servers'][0]

    @classmethod
    def setUpClass(cls):
        cls.conf_checker = default_cluster.initialize_starting_up_smr_before_redis( cls.cluster )
        assert cls.conf_checker != None, 'failed to initialize cluster'

    @classmethod
    def tearDownClass( cls ):
        testbase.defaultTearDown(cls)

    def setUp( self ):
        util.set_process_logfile_prefix( 'TestDenyOOM_%s' % self._testMethodName )
        server = self.cluster['servers'][0]
        self.redis = redis_sock.RedisClient(server['ip'], server['redis_port'])

    def tearDown( self ):
        if self.redis != None:
            self.redis.close()
        return 0

    def check_oom(self, is_oom = False):
        m,s1,s2 = util.get_mss(self.cluster)
        mr = redis_sock.RedisClient(m['ip'], m['redis_port'])
        sr = redis_sock.RedisClient(s1['ip'], s1['redis_port'])
        try:
            ok, data = mr.do_request("get nosuchkey_check_oom\r\n")
            assert(ok), ok
            ok, data = sr.do_request("get nosuchkey_check_oom\r\n")
            assert(ok), ok
            expected_ok = not is_oom
            ok, data = mr.do_request("set nosuchkey_check_oom 100\r\n")
            assert(ok == expected_ok), (ok, data)
            ok, data = sr.do_request("set nosuchkey_check_oom 100\r\n")
            assert(ok == expected_ok), (ok, data)
        finally:
            if mr != None:
                mr.close()
            if sr != None:
                sr.close()

    def test_basic ( self ):
        util.print_frame()
        redis = self.redis
        # set oom
        ok, resp = redis.do_request('deny-oom 1\r\n')
        assert(resp == 'OK'), resp
        self.check_oom(True)
        # reset oom
        ok, resp = redis.do_request('deny-oom 0\r\n')
        assert(resp == 'OK'), resp
        self.check_oom(False)

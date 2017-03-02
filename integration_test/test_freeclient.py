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
import config
import redis_mgmt
import smr_mgmt
import default_cluster
import util
import subprocess
import copy
import time
import testbase

class TestFreeClient(unittest.TestCase):
    cluster = config.clusters[0]

    def setUp(self):
        util.set_process_logfile_prefix( 'TestFreeClient_%s' % self._testMethodName )
        self.conf_checker = default_cluster.initialize_starting_up_smr_before_redis( self.cluster )
        self.assertIsNotNone(self.conf_checker, 'failed to initialize cluster')

    def tearDown(self):
        testbase.defaultTearDown(self)

    def numOpenFds(self, pid):
        p = util.exec_proc_async(util.cluster_util_dir(0), 'ls /proc/%s/fd | wc -l' % pid,
                                 True, None, subprocess.PIPE, None)
        ret = p.wait()
        return p.stdout.read()[:-1]

    def test_fd_leak(self):
        util.print_frame()

        server = util.get_server_by_role_and_pg(self.cluster['servers'], 'master', 0)
        redis = redis_mgmt.Redis(server['id'])
        ret = redis.connect(server['ip'], server['redis_port'])
        self.assertEquals(ret, 0, 'failed to connect to redis')
        smr = smr_mgmt.SMR(server['id'])
        ret = smr.connect(server['ip'], server['smr_mgmt_port'])
        self.assertEquals(ret, 0, 'failed to connect to smr')

        redis.write('info server\r\n')
        res = redis.read_until('process_id:')
        res = redis.read_until('\r\n')
        redis.write('quit\r\n')

        pid = copy.copy(res[:-2])
        num1 = self.numOpenFds(pid)
        print "Initial : Open Fds: %s" % self.numOpenFds(pid)

        smr.write('fi delay sleep 1 1000000\r\n')
        smr.read_until('\r\n')

        for i in range(5):
            ret = redis.connect(server['ip'], server['redis_port'])
            self.assertEquals(ret, 0, 'failed to connect to redis')
            redis.write('ping\r\n')
            res = redis.read_until('\r\n', 1)
            print "Try Ping : Open Fds: %s" % self.numOpenFds(pid)
            redis.disconnect()
            print "Disconnect : Open Fds: %s" % self.numOpenFds(pid)

            ret = redis.connect(server['ip'], server['redis_port'])
            self.assertEquals(ret, 0, 'failed to connect to redis')
            redis.write('*1\r\nasdf\r\n')
            time.sleep(1)
            res = redis.read_until('\r\n', 1)
            print "Protocol Error : Open Fds: %s" % self.numOpenFds(pid)
            redis.disconnect()
            print "Disconnect : Open Fds: %s" % self.numOpenFds(pid)

        print "End : Open Fds: %s" % self.numOpenFds(pid)

        num2 = self.numOpenFds(pid)
        self.assertEquals(num1, num2)

        # Go back to initial configuration
        self.assertTrue(util.shutdown_pgs(server, self.cluster['servers'][0]),
                'recover pgs fail. (shutdown_pgs)')
        self.assertTrue(util.recover_pgs(server, self.cluster['servers'][0]),
                'recover pgs fail. (shutdown_pgs)')

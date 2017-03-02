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

import subprocess
import unittest
import testbase
import default_cluster
import util
import os
import constant
import config
import time
import telnetlib
import signal

class TestBasicOpCAPI(unittest.TestCase):
    cluster = config.clusters[2]

    @classmethod
    def setUpClass(cls):
        return 0

    @classmethod
    def tearDownClass(cls):
        return 0

    def setUp(self):
        util.set_process_logfile_prefix( 'TestBasicOp_%s' % self._testMethodName )
        self.conf_checker = default_cluster.initialize_starting_up_smr_before_redis(self.cluster, arch=self.arch)
        self.assertIsNotNone(self.conf_checker, 'failed to initialize cluster')

    def tearDown(self):
        testbase.defaultTearDown(self)

    def run_capi_server(self):
        # run capi test server
        _capi_server_conf = """
zookeeper 127.0.0.1:2181
cluster_name %s
port 6200
daemonize no
num_conn_per_gw 2
init_timeout_millis 10000
log_level INFO
log_file_prefix "capi_server"
max_fd 4096
conn_reconnect_millis 1000
zk_reconnect_millis 1000
zk_session_timeout_millis 10000
local_proxy_query_timeout_millis 10000
        """ % self.cluster['cluster_name']
        old_cwd = os.path.abspath( os.getcwd() )
        os.chdir(util.capi_dir(0))
        f = open('capi_server.conf', 'w')
        f.write(_capi_server_conf)
        f.close()
        os.chdir(old_cwd)

        if self.arch is 32:
            cmd = "./%s capi_server.conf" % constant.CAPI32_TEST_SERVER
        else:
            cmd = "./%s capi_server.conf" % constant.CAPI_TEST_SERVER

        capi_server = util.exec_proc_async(util.capi_dir(0),
                            cmd, True, None, subprocess.PIPE, None)

        # ping check
        while True:
            try:
                t = telnetlib.Telnet('127.0.0.1', 6200)
                break
            except:
                time.sleep(1)
                continue

        t.write("ping\r\n")
        t.read_until('+PONG\r\n')
        t.close()

        return capi_server

    def stop_process(self, capi_server):
        capi_server.send_signal(signal.SIGTERM)
        capi_server.wait()

    def test_basic_op_capi(self):

        capi_server = self.run_capi_server()

        f = open("%s/test_basicop_output_capi%d" % (constant.logdir, self.arch), 'w')
        p = util.exec_proc_async("../redis-%s" % constant.REDISVER,
                            "./runtest_gw --accurate --gw-port 6200",
                            True, None, f, None)

        ret = p.wait()
        f.close()
        self.assertEquals(0, ret)

        self.stop_process(capi_server)
